"""Real CLI against disposable Unix peers. No Claude settings or live messages."""
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import threading
import time
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[1]

class CLI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.build = tempfile.TemporaryDirectory(prefix='cc-build.', dir='/tmp')
        cls.binary = str(Path(cls.build.name) / 'claude-socket')
        subprocess.run(['go', 'build', '-race', '-o', cls.binary, './cmd/claude-socket'], cwd=ROOT, check=True)

    @classmethod
    def tearDownClass(cls):
        cls.build.cleanup()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='cc-e2e.', dir='/tmp')
        self.root = Path(self.tmp.name)
        self.cfg = self.root / 'config'
        (self.cfg / 'sessions').mkdir(parents=True)
        self.sockdir = self.root / 'cc-socks'
        self.sockdir.mkdir(mode=0o700)
        self.path = str(self.sockdir / f'{os.getpid()}.sock')
        self.sid = str(uuid.uuid4())
        self.registry = self.cfg / 'sessions' / f'{os.getpid()}.json'
        env = dict(os.environ, LC_ALL='C', TZ='UTC')
        start = subprocess.check_output(['ps', '-o', 'lstart=', '-p', str(os.getpid())], env=env, text=True).strip()
        if os.uname().sysname == 'Linux':
            start = Path(f'/proc/{os.getpid()}/stat').read_text().rsplit(')', 1)[1].split()[19]
        self.record = dict(sessionId=self.sid, procStart=start, peerProtocol=1, name='fixture', messagingSocketPath=self.path)
        self.registry.write_text(json.dumps(self.record))
        self.env = dict(os.environ, CLAUDE_CONFIG_DIR=str(self.cfg), CLAUDE_SOCKET_STATE_DIR=str(self.root / 'state'), GORACE='atexit_sleep_ms=0')
        self.env.pop('CLAUDE_CODE_MESSAGING_SOCKET', None)
        self.env.pop('CLAUDE_CODE_MESSAGING_TOKEN', None)
        self.server = socket.socket(socket.AF_UNIX)
        self.server.bind(self.path)
        self.server.listen(10)
        self.server.settimeout(.1)
        self.stop = threading.Event()
        self.messages = []
        self.errors = []
        self.handler = lambda frame: None
        self.worker = threading.Thread(target=self.serve)
        self.worker.start()

    def tearDown(self):
        self.stop.set()
        self.worker.join(3)
        self.server.close()
        self.tmp.cleanup()
        self.assertEqual(self.errors, [])

    def serve(self):
        while not self.stop.is_set():
            try:
                conn, _ = self.server.accept()
            except socket.timeout:
                continue
            with conn:
                conn.settimeout(3)
                data = b''
                while True:
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    data += chunk
            for line in data.splitlines():
                frame = json.loads(line)
                if frame.get('type') == 'user':
                    self.messages.append(frame)
                    try:
                        self.handler(frame)
                    except Exception as e:
                        self.errors.append(repr(e))

    def reply(self, frame, status=None, wrong_id=False, wrong_sender=False, text=None):
        if status:
            out = dict(type='control', action='peer_message_status', status=status,
                       orig_msg_id='other' if wrong_id else frame['msg_id'])
        else:
            marker = re.search(r'CLAUDE_SOCKET_REPLY:[a-f0-9-]+', frame['message']['content']).group()
            body = text if text is not None else marker + '\nACK'
            out = dict(type='user', message=dict(role='user', content=f'<cross-session-message from="uds:{self.path}">\n{body}\n</cross-session-message>'))
        out['from'] = 'uds:/wrong.sock' if wrong_sender else 'uds:' + self.path
        with socket.socket(socket.AF_UNIX) as s:
            s.connect(frame['from'][4:])
            s.sendall(json.dumps(out).encode() + b'\n')
            # Claude's Darwin sender keeps the connection alive for peer PID verification.
            if os.uname().sysname == 'Darwin':
                time.sleep(.15)

    def command(self, verb='ask', *extra):
        return [self.binary, verb, '--session', self.sid, '--timeout', '1200ms', '--text', 'hello', *extra]

    def run_cli(self, verb='ask', *extra):
        p = subprocess.run(self.command(verb, *extra), env=self.env, text=True, capture_output=True, timeout=4)
        events = [json.loads(line) for line in p.stdout.splitlines()]
        self.assertNotIn('DATA RACE', p.stderr)
        return p, events

    def test_ask_matches_reply_in_target_namespace(self):
        def handler(f):
            self.assertEqual(Path(f['from'][4:]).parent, self.sockdir)
            self.reply(f, 'delivered', wrong_id=True)
            self.reply(f, text='unrelated reply')
            self.reply(f)
        self.handler = handler
        p, events = self.run_cli()
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertEqual(events[-1]['status'], 'reply')
        self.assertEqual(events[-1]['text'], 'ACK')
        self.assertEqual(len(self.messages), 1)
        self.assertFalse(Path(self.messages[0]['from'][4:]).exists())

    def test_receipt_states(self):
        for status, code, terminal in [('delivered', 0, 'delivered'), ('denied', 2, 'denied'), ('expired', 2, 'expired'), ('refused', 2, 'refused'), ('dropped', 2, 'dropped'), ('mystery', 3, 'unknown'), ('held', 3, 'held')]:
            with self.subTest(status=status):
                self.handler = lambda f: self.reply(f, status)
                p, events = self.run_cli('send')
                self.assertEqual(p.returncode, code, p.stderr)
                self.assertEqual(events[-1]['status'], terminal)

    def test_wrong_receipts_cannot_succeed(self):
        self.handler = lambda f: (self.reply(f, 'delivered', wrong_id=True), self.reply(f, 'delivered', wrong_sender=True))
        p, events = self.run_cli('send')
        self.assertEqual(p.returncode, 3)
        self.assertEqual(events[-1]['status'], 'unknown')
        self.assertEqual(len(self.messages), 1)

    def test_delivered_is_not_an_answer(self):
        self.handler = lambda f: self.reply(f, 'delivered')
        p, events = self.run_cli()
        self.assertEqual(p.returncode, 3)
        self.assertEqual(events[-1]['status'], 'delivered')
        self.assertEqual(events[-1]['reason'], 'reply timeout; target work may still be running')

    def test_fail_before_send_for_invalid_target(self):
        for changes in [dict(peerProtocol=99), dict(procStart='recycled')]:
            self.registry.write_text(json.dumps(dict(self.record, **changes)))
            p, _ = self.run_cli()
            self.assertEqual(p.returncode, 1, p.stderr)
        self.assertEqual(self.messages, [])

    def test_oversized_encoded_frame_is_definitely_not_sent(self):
        # Valid input can exceed the wire limit after JSON control-byte escaping.
        p = subprocess.run([self.binary, 'ask', '--session', self.sid],
                           input='\x01' * 200000, env=self.env, text=True,
                           capture_output=True, timeout=4)
        self.assertEqual(p.returncode, 1, p.stdout + p.stderr)
        self.assertEqual(self.messages, [])

    def test_reject_multiple_targets_and_flags_after_text(self):
        p, _ = self.run_cli('send', '--pid', str(os.getpid()))
        self.assertEqual(p.returncode, 1)
        p, _ = self.run_cli('send', 'stray', '--timeout', '2s')
        self.assertEqual(p.returncode, 1)
        self.assertEqual(self.messages, [])

    def test_same_session_waits_until_first_request_finishes(self):
        seen = threading.Event()
        self.handler = lambda f: seen.set()
        a = subprocess.Popen(self.command(), env=self.env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            self.assertTrue(seen.wait(2))
            b = subprocess.Popen(self.command(), env=self.env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                time.sleep(.15)
                self.assertEqual(len(self.messages), 1)
                a.communicate(timeout=3)
                b.communicate(timeout=3)
                self.assertEqual(a.returncode, 3)
                self.assertEqual(b.returncode, 3)
            finally:
                if b.poll() is None: b.kill(); b.communicate()
        finally:
            if a.poll() is None: a.kill(); a.communicate()

    def test_interrupt_wait_keeps_target_alive_and_cleans_socket(self):
        import signal
        seen = threading.Event()
        self.handler = lambda f: seen.set()
        p = subprocess.Popen(self.command(), env=self.env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            self.assertTrue(seen.wait(2))
            p.send_signal(signal.SIGINT)
            out, err = p.communicate(timeout=2)
            self.assertEqual(p.returncode, 3, err)
            self.assertFalse(Path(self.messages[0]['from'][4:]).exists())
            self.assertTrue(Path(self.path).exists())
        finally:
            if p.poll() is None: p.kill(); p.communicate()

if __name__ == '__main__':
    unittest.main()
