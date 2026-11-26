// The shim is the child process that makes one agent that is not a Claude
// Code session look like one to every Claude Code session on the machine.
//
// Claude Code's ListAgents lists a registry entry <config>/sessions/<pid>.json
// only while that PID is alive with the recorded start token, and its
// SendMessage refuses to write unless the process listening on the socket is
// that same PID (checked with SO_PEERCRED). One process can therefore stand
// for exactly one agent, which is why each agent gets its own shim rather
// than its owner registering many entries itself.
//
// The shim binds a socket, writes the registry entry, and reports every frame
// it receives as a JSON line on stdout. Its owner (a Pool) sends it state
// lines on stdin (the agent's status, start time and Remote Control id),
// which it copies into the registry entry so ListAgents shows them. Claude
// Code lists a Remote Control session only once when a registry entry
// carries its id, so a shim with one stands in for that session's Remote
// Control row. It exits, removing both files, when its stdin closes (the
// owner went away) or on SIGTERM, SIGINT or SIGHUP.

package shim

import (
	"bufio"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	ccsock "github.com/PeterSR/claude-code-socket-transport"
)

// shimEvent is one line on the shim's stdout, read by Pool.
type shimEvent struct {
	T         string `json:"t"` // ready | msg | receipt
	Addr      string `json:"addr,omitempty"`
	From      string `json:"from,omitempty"`
	Text      string `json:"text,omitempty"`
	Status    string `json:"status,omitempty"`
	OrigMsgID string `json:"origMsgId,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

// stateLine is a line a Pool sends a shim on its stdin.
type stateLine struct {
	T       string `json:"t"`                 // state
	Status  string `json:"status,omitempty"`  // busy | idle | shell | waiting
	Started int64  `json:"started,omitempty"` // unix ms the agent started
	Bridge  string `json:"bridge,omitempty"`  // the agent's Remote Control id
}

// ValidBridge reports whether s is a well-formed Remote Control id, the
// "session_" value Claude Code writes as bridgeSessionId in a registry entry.
// A shim never writes any other value: a Spec.Bridge that fails this check is
// treated as no Remote Control id at all, so callers of Pool need not check it
// themselves. It is exported for code that reads bridgeSessionId out of
// registry entries (ccsock.Session.Raw) and wants the same rule applied there.
func ValidBridge(s string) bool {
	rest, ok := strings.CutPrefix(s, "session_")
	if !ok || rest == "" || len(rest) > 64 {
		return false
	}
	for _, c := range rest {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

// shownStatus keeps the status to values Claude Code itself writes.
func shownStatus(s string) string {
	switch s {
	case "busy", "idle", "shell", "waiting":
		return s
	}
	return "idle"
}

// stopSignals end a shim. Each one runs the same cleanup as stdin closing.
var stopSignals = []os.Signal{syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP}

// Run is the shim process's main: a program that runs shims for a Pool
// calls it when started with the arguments its Options.Args builds. name is
// the agent's name shown in ListAgents; cwd is the working directory shown
// next to it. It returns, having removed its registry entry and socket, when
// stdin closes or on SIGTERM, SIGINT or SIGHUP.
//
// Run is meant to be the whole of the process: it changes process-wide
// signal handling while it runs and restores it when it returns.
func Run(name, cwd string, stdin io.Reader, stdout io.Writer) error {
	if name == "" {
		return errors.New("shim: a name is required")
	}
	// Catch the stop signals before anything is created, so one that arrives
	// while the socket or the registry entry is being set up still leads to
	// the cleanup below instead of killing the process and leaving an entry
	// behind that ListAgents would show until the PID is gone.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, stopSignals...)
	defer signal.Stop(sig)
	// A write to an owner that has gone must fail with an error, not kill the
	// shim before it has removed its entry.
	if !signal.Ignored(syscall.SIGPIPE) {
		signal.Ignore(syscall.SIGPIPE)
		defer signal.Reset(syscall.SIGPIPE)
	}

	var mu sync.Mutex
	enc := json.NewEncoder(stdout)
	enc.SetEscapeHTML(false) // keep a 1 MiB frame close to 1 MiB on the pipe
	emit := func(e shimEvent) {
		mu.Lock()
		defer mu.Unlock()
		_ = enc.Encode(e)
	}

	in, err := ccsock.Listen(ccsock.InboxConfig{
		OnMessage: func(text, from string) { emit(shimEvent{T: "msg", From: from, Text: text}) },
		OnReceipt: func(r ccsock.Receipt) {
			emit(shimEvent{T: "receipt", Status: string(r.Status), OrigMsgID: r.OrigMsgID, From: r.From, Reason: r.Reason})
		},
	})
	if err != nil {
		return err
	}
	defer in.Close()

	reg, err := newRegistry(name, cwd, in.Path())
	if err != nil {
		return err
	}
	// states carries the owner's state lines; it is closed when stdin is.
	states := make(chan stateLine, 1)
	go func() {
		defer close(states)
		sc := bufio.NewScanner(stdin)
		for sc.Scan() {
			var st stateLine
			if json.Unmarshal(sc.Bytes(), &st) != nil || st.T != "state" {
				continue
			}
			states <- st
		}
	}()
	// The owner sends the agent's state first. Take it before the entry is
	// first written, so ListAgents never shows the shim with a made-up state
	// beside the Remote Control row it is to replace.
	select {
	case st, ok := <-states:
		if !ok {
			return nil // the owner went away
		}
		reg.apply(st)
	case <-sig:
		return nil
	case <-time.After(time.Second):
	}
	if err := reg.write(); err != nil {
		return err
	}
	defer reg.remove()

	emit(shimEvent{T: "ready", Addr: in.Address()})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for st := range states {
			_ = reg.update(st)
		}
	}()
	select {
	case <-done:
	case <-sig:
	}
	return nil
}

// registry is the shim's entry in Claude Code's session registry.
type registry struct {
	path   string
	mu     sync.Mutex
	rec    map[string]any
	dirty  bool // the last write failed
	closed bool // removed; never written again
}

func newRegistry(name, cwd, sockPath string) (*registry, error) {
	dir, err := ccsock.SessionsDir()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	pid := os.Getpid()
	tok, err := ccsock.ProcessStartToken(pid)
	if err != nil {
		return nil, fmt.Errorf("shim: reading own start token: %w", err)
	}
	now := time.Now().UnixMilli()
	rec := map[string]any{
		"pid":                 pid,
		"sessionId":           uuid(),
		"cwd":                 cwd,
		"startedAt":           now,
		"procStart":           tok,
		"peerProtocol":        1,
		"kind":                "interactive",
		"entrypoint":          "ccsock-shim",
		"messagingSocketPath": sockPath,
		"name":                name,
		"nameSource":          "user",
		"nameSince":           now,
		"status":              "idle",
		"updatedAt":           now,
		"statusUpdatedAt":     now,
		Marker:                true,
	}
	if d := pidDomain(); d != "" {
		rec["pidDomain"] = d
	}
	return &registry{path: filepath.Join(dir, fmt.Sprintf("%d.json", pid)), rec: rec}, nil
}

// update applies a state line and rewrites the entry if anything changed,
// or if the last write failed.
func (r *registry) update(st stateLine) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.apply(st) && !r.dirty {
		return nil
	}
	return r.writeLocked()
}

// remove deletes the entry for good: a state line arriving after it cannot
// write it back.
func (r *registry) remove() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	os.Remove(r.path + ".tmp")
	os.Remove(r.path)
}

// apply copies a state line into the entry and reports whether anything
// changed. Call with r.mu held, or before the entry is shared.
func (r *registry) apply(st stateLine) bool {
	now := time.Now().UnixMilli()
	changed := false
	if s := shownStatus(st.Status); r.rec["status"] != s {
		r.rec["status"], r.rec["statusUpdatedAt"] = s, now
		changed = true
	}
	if st.Started > 0 && st.Started <= now && r.rec["startedAt"] != st.Started {
		r.rec["startedAt"] = st.Started
		changed = true
	}
	bridge := st.Bridge
	if !ValidBridge(bridge) {
		bridge = ""
	}
	if old, _ := r.rec["bridgeSessionId"].(string); old != bridge {
		if bridge == "" {
			delete(r.rec, "bridgeSessionId")
		} else {
			r.rec["bridgeSessionId"] = bridge
		}
		changed = true
	}
	if changed {
		r.rec["updatedAt"] = now
	}
	return changed
}

func (r *registry) write() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.writeLocked()
}

// writeLocked replaces the entry atomically, so ListAgents never reads half
// of one.
func (r *registry) writeLocked() error {
	if r.closed {
		return nil
	}
	data, err := json.Marshal(r.rec)
	if err != nil {
		return err
	}
	tmp := r.path + ".tmp"
	err = os.WriteFile(tmp, data, 0o600)
	if err == nil {
		err = os.Rename(tmp, r.path)
	}
	r.dirty = err != nil
	return err
}

// Sweep removes registry entries left by shims that are no longer running
// (an owner that was killed hard). Claude Code also drops entries for dead
// processes, but only when it lists sessions. Call it before starting a Pool.
//
// It leaves alone entries written in another PID namespace, such as by a
// container that shares this config directory: their PIDs mean nothing here,
// so a shim that is running there would look dead from here.
func Sweep() {
	all, err := ccsock.ListSessions()
	if err != nil {
		return
	}
	ours := pidDomain()
	for _, s := range all {
		if !IsShim(s) || s.File == "" {
			continue
		}
		if d, _ := s.Raw["pidDomain"].(string); d != ours {
			continue
		}
		if !live(s) {
			_ = os.Remove(s.File)
		}
	}
}

// Marker is the registry field every shim writes, so a shim is never
// mistaken for a real Claude Code session.
const Marker = "ccsockShim"

// IsShim reports whether a registry entry is a shim's rather than a real
// Claude Code session's.
func IsShim(s ccsock.Session) bool {
	return s.Raw[Marker] == true
}

// live is true when the registry entry's process is running and is the one
// that wrote it, not an unrelated process on a recycled PID.
func live(s ccsock.Session) bool {
	if !s.Running() {
		return false
	}
	if s.ProcStart == "" {
		return true
	}
	tok, err := ccsock.ProcessStartToken(s.PID)
	return err != nil || tok == s.ProcStart
}

// pidDomain mirrors what Claude Code writes: the machine id plus the PID
// namespace, so a session in another container is not mistaken for ours.
func pidDomain() string {
	if runtime.GOOS != "linux" {
		return ""
	}
	mid, err := os.ReadFile("/etc/machine-id")
	if err != nil {
		return ""
	}
	ns, err := os.Readlink("/proc/self/ns/pid")
	if err != nil {
		return ""
	}
	return "linux:" + strings.TrimSpace(string(mid)) + ":" + ns
}

func uuid() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
