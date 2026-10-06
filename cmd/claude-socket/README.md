# Claude session CLI

A CLI for correlated replies from existing Claude Code sessions on macOS and Linux.
The original `cc-send` command remains available with its existing interface.

## Use

Requires Go 1.25+ on macOS or Linux. The launcher builds a local cached binary
when source changes, then executes it. No daemon, service installation, or
Claude configuration change is required.

```sh
./scripts/claude-socket list
./scripts/claude-socket doctor
./scripts/claude-socket ask --session UUID --text 'What is the current status?'
printf '%s' 'What is the current status?' | ./scripts/claude-socket ask --session UUID
./scripts/claude-socket send --session UUID --file /path/to/message.txt --timeout 30s
```

Choose exactly one of `--session`, `--pid`, or exact `--name`. All input is flags;
use `--text`, `--file`, or stdin rather than positional arguments. Prefer stdin
or files for sensitive text so it does not appear in process arguments.

`ask` sends once and asks Claude to return one reply to a temporary local socket.
The reply must contain a random request marker. `send` waits for a native receipt.
Claude does not emit a positive receipt for every immediately accepted message,
so `ask` is the useful round-trip command. A held receipt is reported while the
CLI waits; the CLI never approves it or changes `crossSessionInbound`.

The receiver keeps its own permissions. Desktop can hold an external message
without displaying an approval dialog. A timeout does not mean the message was
rejected, and stopping this CLI does not stop work already admitted by Claude.
No command automatically retries, forwards a reply, or changes target settings.

Output is newline-delimited JSON. `request_id` correlates events, `session_id`
identifies the target, `status` gives the observed state, and `text` carries a
reply. `written` is only transport progress. `reply` proves Claude replied,
not that any action described in the reply actually happened. `delivered` is
not a completed turn.

| Exit | Meaning |
| --- | --- |
| 0 | Requested reply or delivery receipt confirmed |
| 1 | Invalid input, target, configuration, or failure before sending |
| 2 | Denied, expired, refused, or dropped |
| 3 | Timeout, canceled wait, or uncertain transport outcome after send began |

## Account profiles and remote hosts

Run the launcher through SSH as the session owner. If `list` is empty while
Claude is running, use its account profile rather than starting another session:

```sh
CLAUDE_CONFIG_DIR=/path/to/claude-account ./scripts/claude-socket list
CLAUDE_CONFIG_DIR=/path/to/claude-account ./scripts/claude-socket ask --session UUID --text 'Question'
```

The active profile's `settings.json` can link to the owner's default settings.
With explicit owner approval, set `crossSessionInbound` to `accept` there to
allow external peers; this changes inbound acceptance for sessions using those
settings. The CLI never makes this change itself or changes tool permissions.

## Boundaries

- Registry session ID, kernel process start token, socket owner, protocol, and
  kernel peer PID identify the target. Names alone are not durable identities.
- Reply sockets bind beside the resolved target. Both peer PID and message
  correlation are checked. An unverified sender cannot produce success.
- Per-session file locks serialize this CLI's requests, including reply waits.
  They do not lock out human input or other clients. Locks are released by the
  OS on process exit. Empty lock files intentionally remain to avoid inode races.
- Locks live in the user's cache under `claude-socket`.
  `CLAUDE_SOCKET_STATE_DIR` can select another private directory. Claude registry
  discovery honors `CLAUDE_CONFIG_DIR`.
- Discovery uses at most eight workers; an inbox admits at most sixteen open
  connections. Socket reads, sends, lock waits, and command waits are bounded.
- The CLI sends as an external peer. It does not borrow Claude's auth tokens or
  assert Claude's permission class. Credential lookup remains only in the
  library API, which the CLI does not use.
- This command runs on the session's owning host and OS account. Sharing the
  source across hosts does not make Unix sockets remotely addressable.
- Protocol 1 is supported. Unknown protocols and changed processes fail closed.
  `doctor` checks discovery and identity; it does not certify live delivery.
  Repeat a harmless, explicitly authorized ask/reply after Claude updates.
- The wire format is not a stable, complete session-control API. There is no
  remote turn cancellation, restart, archive, or takeover command.

## Validation

Run from the repository root:

```sh
go test -race ./...
go vet ./...
python3 tests/test_cli.py
```

The Python harness builds the real CLI with the Go race detector and runs it
against disposable Unix sockets and session registries. It covers correlated
replies, rejection states, unknown and unrelated receipts, target identity,
argument errors, per-session serialization, timeout uncertainty, and interruption.
Library lifecycle tests cover established-I/O cancellation and callbacks after
shutdown, which process-exit tests cannot detect. Fixtures use short private
paths to respect macOS's socket path limit. Test commands emit their results;
fixtures and test binaries are removed afterwards.

The CLI and library changes fix cancellation and
inbox cleanup, avoid process-global umask mutation and stale-socket deletion,
add peer verification, and remove redundant timeout tests and their test-only
global. The sender-name limit is conservatively restricted for current
Claude versions. Run the tests above after transport changes.
