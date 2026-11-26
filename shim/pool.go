package shim

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"time"

	ccsock "github.com/PeterSR/claude-code-socket-transport"
)

// Inbound is a message a Claude Code session sent to a shim with its native
// SendMessage.
type Inbound struct {
	Shim       string // the shim's name, i.e. the agent it stands for
	FromSocket string // the sending session's socket path
	FromName   string // the name the sending session gave itself
	Body       string
}

// Receipt reports the fate of a message delivered with a shim as its reply
// address: the receiving session sends it back to that address.
type Receipt struct {
	Shim      string // the shim the receipt arrived at
	OrigMsgID string // the id of the message it is about
	Status    ccsock.ReceiptStatus
	// From is the "uds:" address of the session that sent the receipt, when
	// it gave one.
	From string
	// Reason is the receiving session's explanation, mostly for a denial.
	Reason string
}

// Options configures a Pool. Only Args is required.
type Options struct {
	// Exe is the program to run; empty means this executable.
	Exe string
	// Args builds the command line Exe is started with for one shim; it must
	// lead to Run(name, cwd, ...).
	Args func(name, cwd string) []string
	// Max caps the number of shims. Zero or less means no cap.
	Max int
	// Log receives the pool's warnings; nil discards them.
	Log *slog.Logger
	// Stderr receives what the shim processes write to their stderr; nil
	// means this process's own stderr. Use io.Discard to drop it.
	Stderr io.Writer
	// OnMessage and OnReceipt run on a goroutine of the shim the event
	// arrived at, so events for one shim arrive in order and a slow callback
	// holds up only that shim. Close waits for them to return, so a callback
	// must not call Close.
	OnMessage func(Inbound)
	OnReceipt func(Receipt)
}

// Pool keeps one shim process per named agent, so those agents appear in
// Claude Code's native ListAgents under their own name and receive its native
// SendMessage.
//
// Its methods are safe for concurrent use. They are also safe on a nil
// *Pool, which behaves as a pool with no shims, so a program can leave its
// pool nil when the feature is turned off.
type Pool struct {
	opt Options

	mu      sync.Mutex
	running map[string]*shimProc
	procs   map[*shimProc]struct{} // every started shim not yet waited for, wanted or not
	failed  map[string]*failure
	closed  bool
	capped  bool           // the cap warning has been logged since it last cleared
	calls   sync.WaitGroup // the dispatch goroutines, which run the callbacks
}

type shimProc struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	sent   stateLine      // last state handed to the writer
	state  chan stateLine // the newest state not yet written, at most one
	addr   string         // the shim socket's uds: address, once ready
	events chan shimEvent
	done   chan struct{} // closed once the process has been waited for
}

type failure struct {
	at     time.Time
	logged bool
}

// Spec is one shim to keep running.
type Spec struct {
	Name    string // shown in ListAgents; also the key
	CWD     string // shown as the session's working directory
	Status  string // shown in ListAgents: busy, idle, shell or waiting
	Started int64  // unix ms the agent started, shown as "started ... ago"; 0 keeps the shim's own
	// Bridge is the agent's Remote Control id, if it is a Claude Code
	// session on Remote Control. Claude Code then lists the shim in place
	// of that session's Remote Control row. A value ValidBridge rejects is
	// ignored.
	Bridge string
}

// NewPool returns an empty pool. Reconcile starts its shims.
func NewPool(opt Options) (*Pool, error) {
	if opt.Args == nil {
		return nil, errors.New("shim: Options.Args is required")
	}
	if opt.Exe == "" {
		exe, err := os.Executable()
		if err != nil {
			return nil, err
		}
		opt.Exe = exe
	}
	if opt.Log == nil {
		opt.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opt.OnMessage == nil {
		opt.OnMessage = func(Inbound) {}
	}
	if opt.OnReceipt == nil {
		opt.OnReceipt = func(Receipt) {}
	}
	if opt.Stderr == nil {
		opt.Stderr = os.Stderr
	}
	return &Pool{opt: opt, running: map[string]*shimProc{}, procs: map[*shimProc]struct{}{},
		failed: map[string]*failure{}}, nil
}

// ReplyAddr is the "uds:" address of name's shim, which a Claude Code session
// should reply to, or "" when there is none or it is not ready yet.
func (s *Pool) ReplyAddr(name string) string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if p := s.running[name]; p != nil {
		return p.addr
	}
	return ""
}

// Len is the number of running shims.
func (s *Pool) Len() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.running)
}

// Reconcile makes the running shims match want: it starts shims that are
// wanted but missing, updates the state shown for the others, and stops
// shims that are no longer wanted. want is in priority order: past
// Options.Max, the tail is dropped. A shim that failed is retried by a later
// call, at most every ten seconds. Call it again whenever the agents or
// their state change; it does nothing after Close.
func (s *Pool) Reconcile(want []Spec) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	keep := map[string]Spec{}
	for _, a := range want {
		if s.opt.Max > 0 && len(keep) >= s.opt.Max {
			break
		}
		keep[a.Name] = a
	}
	if over := s.opt.Max > 0 && len(want) > s.opt.Max; over != s.capped {
		s.capped = over
		if over {
			s.opt.Log.Warn("shim cap reached; some agents will be missing from native ListAgents", "max", s.opt.Max, "wanted", len(want))
		}
	}
	for name, p := range s.running {
		if _, ok := keep[name]; !ok {
			p.stdin.Close()
			delete(s.running, name)
		}
	}
	for name := range s.failed {
		if _, ok := keep[name]; !ok {
			delete(s.failed, name)
		}
	}
	for name, a := range keep {
		if p := s.running[name]; p != nil {
			s.sendState(p, a)
			continue
		}
		if f := s.failed[name]; f != nil && time.Since(f.at) < 10*time.Second {
			continue
		}
		if err := s.start(a); err != nil {
			s.noteFailure(name, err)
		}
	}
}

// noteFailure records a failed or exited shim, logging only the first in a
// run so a crash loop does not flood the log. Call with s.mu held.
func (s *Pool) noteFailure(name string, err error) {
	f := s.failed[name]
	if f == nil {
		f = &failure{}
		s.failed[name] = f
	}
	f.at = time.Now()
	if !f.logged {
		f.logged = true
		s.opt.Log.Warn("shim failed; retrying quietly", "agent", name, "err", err)
	}
}

func (s *Pool) start(a Spec) error {
	cmd := exec.Command(s.opt.Exe, s.opt.Args(a.Name, a.CWD)...)
	cmd.SysProcAttr = shimSysProcAttr()
	cmd.Stderr = s.opt.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	p := &shimProc{cmd: cmd, stdin: stdin, events: make(chan shimEvent, 256), done: make(chan struct{}),
		state: make(chan stateLine, 1)}
	s.running[a.Name] = p
	s.procs[p] = struct{}{}
	go p.writeStates()
	s.sendState(p, a)
	go s.read(a.Name, p, stdout)
	s.calls.Add(1)
	go s.dispatch(a.Name, p)
	go s.awaitReady(p)
	return nil
}

// sendState hands a shim the agent's status, start time and Remote Control
// id when they changed. It never blocks: a newer state replaces one the shim has not
// taken yet, so a shim that stops reading costs only its own updates, never
// the pool's lock. Call with s.mu held.
func (s *Pool) sendState(p *shimProc, a Spec) {
	st := stateLine{T: "state", Status: shownStatus(a.Status), Started: a.Started, Bridge: a.Bridge}
	if st == p.sent {
		return
	}
	select {
	case <-p.state: // drop the stale one
	default:
	}
	p.state <- st // cannot block: we are the only sender and just made room
	p.sent = st
}

// writeStates writes the newest state to the shim until it exits.
func (p *shimProc) writeStates() {
	for {
		select {
		case <-p.done:
			return
		case st := <-p.state:
			line, err := json.Marshal(st)
			if err != nil {
				continue
			}
			if _, err := p.stdin.Write(append(line, '\n')); err != nil {
				return // the shim is gone; read notices and restarts it
			}
		}
	}
}

// shimReadyTimeout bounds how long a shim may take to bind its socket.
const shimReadyTimeout = 10 * time.Second

// awaitReady kills a shim that never reports ready, so it is retried rather
// than sitting in the pool without an address.
func (s *Pool) awaitReady(p *shimProc) {
	select {
	case <-p.done:
	case <-time.After(shimReadyTimeout):
		s.mu.Lock()
		ready := p.addr != ""
		s.mu.Unlock()
		if !ready {
			_ = p.cmd.Process.Kill()
		}
	}
}

// maxEventLine fits a 1 MiB Claude frame even after JSON escaping.
const maxEventLine = 8 << 20

func (s *Pool) read(name string, p *shimProc, r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), maxEventLine)
	for sc.Scan() {
		var ev shimEvent
		if json.Unmarshal(sc.Bytes(), &ev) != nil {
			continue
		}
		if ev.T == "ready" {
			s.mu.Lock()
			p.addr = ev.Addr
			delete(s.failed, name)
			s.mu.Unlock()
			continue
		}
		p.events <- ev // blocks when the dispatcher is behind: backpressure
	}
	if sc.Err() != nil {
		// A line we cannot read leaves the stream unusable; restart the shim.
		_ = p.cmd.Process.Kill()
	}
	close(p.events)
	err := p.cmd.Wait()
	close(p.done)
	s.mu.Lock()
	delete(s.procs, p)
	if s.running[name] == p {
		delete(s.running, name)
		if !s.closed {
			s.noteFailure(name, err)
		}
	}
	s.mu.Unlock()
}

// dispatch hands events to the callbacks off the reader goroutine, so a slow
// route never stalls the pipe the shim writes to.
func (s *Pool) dispatch(name string, p *shimProc) {
	defer s.calls.Done()
	for ev := range p.events {
		switch ev.T {
		case "msg":
			env := ccsock.ParseEnvelope(ev.Text)
			from := ev.From
			if from == "" {
				from = env.From
			}
			path, err := ccsock.ParseAddress(from)
			if err != nil {
				s.opt.Log.Warn("native message without a usable sender address; dropped", "to", name)
				continue
			}
			s.opt.OnMessage(Inbound{Shim: name, FromSocket: path, FromName: env.FromName, Body: env.Body})
		case "receipt":
			s.opt.OnReceipt(Receipt{Shim: name, OrigMsgID: ev.OrigMsgID, Status: ccsock.ReceiptStatus(ev.Status),
				From: ev.From, Reason: ev.Reason})
		}
	}
}

// Close stops every shim, waits briefly for them to remove their registry
// entries, and kills any that do not. It returns once every shim has exited
// and the last OnMessage or OnReceipt call has returned, so no callback runs
// after Close; a callback that calls Close therefore never returns.
func (s *Pool) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.closed = true
	s.running = map[string]*shimProc{}
	// Include shims a Reconcile already stopped but that have not exited
	// yet: their events still reach the callbacks until they do.
	procs := make([]*shimProc, 0, len(s.procs))
	for p := range s.procs {
		procs = append(procs, p)
	}
	s.mu.Unlock()
	for _, p := range procs {
		p.stdin.Close()
	}
	// One deadline for all of them; a channel from time.After would fire
	// for only the first shim still running when it passes.
	deadline := time.Now().Add(3 * time.Second)
	for _, p := range procs {
		select {
		case <-p.done:
		case <-time.After(time.Until(deadline)):
			_ = p.cmd.Process.Kill()
			<-p.done
		}
	}
	s.calls.Wait()
}
