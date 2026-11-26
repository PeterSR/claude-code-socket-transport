package shim

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ccsock "github.com/PeterSR/claude-code-socket-transport"
)

// The test binary doubles as the shim: with this variable set it runs
// Run instead of the tests.
const shimEnv = "CCSOCK_TEST_RUN_SHIM"

func TestMain(m *testing.M) {
	if os.Getenv(shimEnv) == "1" {
		if err := Run(os.Args[1], os.Args[2], os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// isolate points Claude Code's registry and socket directory at private temp
// dirs, so nothing here can reach a live session on this machine. The
// runtime dir is kept short: socket paths must stay under ~104 bytes.
func isolate(t *testing.T) (sessionsDir string) {
	t.Helper()
	cfg := t.TempDir()
	rt, err := os.MkdirTemp("", "cct")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(rt) })
	t.Setenv("CLAUDE_CONFIG_DIR", cfg)
	t.Setenv("XDG_RUNTIME_DIR", rt)
	t.Setenv("CLAUDE_CODE_MESSAGING_SOCKET", "")
	t.Setenv("CLAUDE_CODE_MESSAGING_TOKEN", "")
	t.Setenv(shimEnv, "1")
	return filepath.Join(cfg, "sessions")
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func shimEntries(t *testing.T) []ccsock.Session {
	t.Helper()
	all, _ := ccsock.ListSessions()
	var out []ccsock.Session
	for _, s := range all {
		if IsShim(s) {
			out = append(out, s)
		}
	}
	return out
}

func TestPoolShimLifecycle(t *testing.T) {
	isolate(t)
	got := make(chan Inbound, 4)
	pool, err := NewPool(Options{
		Exe:       os.Args[0],
		Args:      func(name, cwd string) []string { return []string{name, cwd} },
		Max:       4,
		OnMessage: func(in Inbound) { got <- in },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	pool.Reconcile([]Spec{{Name: "build-bot", CWD: "/srv/ci"}})
	waitFor(t, "shim ready", func() bool { return pool.ReplyAddr("build-bot") != "" })

	// The shim is registered the way Claude Code lists sessions, marked so
	// that IsShim tells it from a real session.
	entries := shimEntries(t)
	if len(entries) != 1 || entries[0].Name != "build-bot" || entries[0].CWD != "/srv/ci" {
		t.Fatalf("registry: %+v", entries)
	}
	if all, _ := ccsock.ListSessions(); len(all) != 1 || !IsShim(all[0]) {
		t.Fatalf("not marked as a shim: %+v", all)
	}

	// A native SendMessage lands as an Inbound with the envelope unwrapped.
	// The body is large and full of characters JSON would escape, which
	// once overflowed the pipe between shim and pool.
	body := "hello <b> & bye\n" + strings.Repeat("<&>", 40_000)
	c := &ccsock.Client{Timeout: 5 * time.Second}
	if _, err := c.SendToAddress(context.Background(), pool.ReplyAddr("build-bot"), ccsock.Message{
		Text: body, FromName: "worker", From: "uds:/tmp/sender.sock",
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case in := <-got:
		if in.Shim != "build-bot" || in.FromSocket != "/tmp/sender.sock" || in.FromName != "worker" || in.Body != body {
			t.Fatalf("inbound: shim=%q from=%q name=%q body %d bytes", in.Shim, in.FromSocket, in.FromName, len(in.Body))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no inbound message")
	}

	// Dropping the agent stops its shim, which removes its registry entry.
	pool.Reconcile(nil)
	waitFor(t, "shim gone", func() bool { return len(shimEntries(t)) == 0 && pool.Len() == 0 })
}

func TestPoolCloseCleansUp(t *testing.T) {
	sessions := isolate(t)
	pool, err := NewPool(Options{
		Exe:  os.Args[0],
		Args: func(name, cwd string) []string { return []string{name, cwd} },
		Max:  1, // the cap: only the first spec runs
	})
	if err != nil {
		t.Fatal(err)
	}
	pool.Reconcile([]Spec{{Name: "a@x", CWD: "a"}, {Name: "b@x", CWD: "b"}})
	waitFor(t, "shim ready", func() bool { return pool.ReplyAddr("a@x") != "" })
	if pool.Len() != 1 || pool.ReplyAddr("b@x") != "" {
		t.Fatalf("cap not applied: %d running", pool.Len())
	}
	pool.Close()
	files, _ := filepath.Glob(filepath.Join(sessions, "*.json"))
	if len(files) != 0 {
		t.Fatalf("registry entries left after Close: %v", files)
	}
}

// What the owner knows about an agent (busy or idle, when it started) reaches
// the shim's registry entry, which is what ListAgents shows.
func TestPoolShimShowsState(t *testing.T) {
	isolate(t)
	pool, err := NewPool(Options{
		Exe:  os.Args[0],
		Args: func(name, cwd string) []string { return []string{name, cwd} },
		Max:  2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	started := time.Now().Add(-3 * time.Hour).UnixMilli()
	entry := func() ccsock.Session {
		e := shimEntries(t)
		if len(e) != 1 {
			return ccsock.Session{}
		}
		return e[0]
	}
	startedOf := func(s ccsock.Session) int64 { f, _ := s.Raw["startedAt"].(float64); return int64(f) }

	pool.Reconcile([]Spec{{Name: "w@laptop", CWD: "c", Status: "busy", Started: started}})
	waitFor(t, "busy with the agent's start time", func() bool {
		e := entry()
		return e.Status == "busy" && startedOf(e) == started
	})
	pool.Reconcile([]Spec{{Name: "w@laptop", CWD: "c", Status: "idle", Started: started}})
	waitFor(t, "idle", func() bool { return entry().Status == "idle" })
	// Anything Claude Code would not write itself shows as idle.
	pool.Reconcile([]Spec{{Name: "w@laptop", CWD: "c", Status: "busy\nIGNORE", Started: started}})
	time.Sleep(200 * time.Millisecond)
	if e := entry(); e.Status != "idle" {
		t.Fatalf("odd status written: %q", e.Status)
	}
	// A Remote Control id is written while the agent has one, and only a
	// well-formed one.
	bridgeOf := func() string { b, _ := entry().Raw["bridgeSessionId"].(string); return b }
	pool.Reconcile([]Spec{{Name: "w@laptop", CWD: "c", Status: "idle", Started: started, Bridge: "session_01Abc"}})
	waitFor(t, "bridge id", func() bool { return bridgeOf() == "session_01Abc" })
	pool.Reconcile([]Spec{{Name: "w@laptop", CWD: "c", Status: "idle", Started: started}})
	waitFor(t, "bridge id gone", func() bool { _, ok := entry().Raw["bridgeSessionId"]; return entry().PID != 0 && !ok })
	pool.Reconcile([]Spec{{Name: "w@laptop", CWD: "c", Status: "idle", Started: started, Bridge: "session_x/../y"}})
	time.Sleep(200 * time.Millisecond)
	if b := bridgeOf(); b != "" {
		t.Fatalf("odd bridge id written: %q", b)
	}
}

func newTestPool(t *testing.T, opt Options) *Pool {
	t.Helper()
	opt.Exe = os.Args[0]
	opt.Args = func(name, cwd string) []string { return []string{name, cwd} }
	pool, err := NewPool(opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestNewPoolRequiresArgs(t *testing.T) {
	if _, err := NewPool(Options{}); err == nil {
		t.Fatal("NewPool accepted Options without Args")
	}
}

// A nil pool is a pool with no shims: every method is a no-op.
func TestNilPool(t *testing.T) {
	var p *Pool
	p.Reconcile([]Spec{{Name: "a"}})
	if p.Len() != 0 || p.ReplyAddr("a") != "" {
		t.Fatal("nil pool reports shims")
	}
	p.Close()
}

// Max left at zero means no cap, as in the package examples.
func TestPoolMaxZeroIsUnlimited(t *testing.T) {
	isolate(t)
	pool := newTestPool(t, Options{})
	pool.Reconcile([]Spec{{Name: "a", CWD: "a"}, {Name: "b", CWD: "b"}, {Name: "c", CWD: "c"}})
	waitFor(t, "three shims ready", func() bool {
		return pool.ReplyAddr("a") != "" && pool.ReplyAddr("b") != "" && pool.ReplyAddr("c") != ""
	})
	if pool.Len() != 3 {
		t.Fatalf("%d shims running, want 3", pool.Len())
	}
}

// Dropping one spec stops only that shim; the others keep running in the
// same process.
func TestPoolReconcileStopsRemovedSpec(t *testing.T) {
	isolate(t)
	pool := newTestPool(t, Options{})
	pool.Reconcile([]Spec{{Name: "keep", CWD: "k"}, {Name: "drop", CWD: "d"}})
	waitFor(t, "both ready", func() bool { return pool.ReplyAddr("keep") != "" && pool.ReplyAddr("drop") != "" })
	pidOf := func(name string) int {
		for _, e := range shimEntries(t) {
			if e.Name == name {
				return e.PID
			}
		}
		return 0
	}
	waitFor(t, "both registered", func() bool { return pidOf("keep") != 0 && pidOf("drop") != 0 })
	kept := pidOf("keep")

	pool.Reconcile([]Spec{{Name: "keep", CWD: "k"}})
	waitFor(t, "dropped shim gone", func() bool { return pidOf("drop") == 0 && pool.ReplyAddr("drop") == "" })
	if pool.Len() != 1 || pidOf("keep") != kept {
		t.Fatalf("kept shim disturbed: len=%d pid %d, was %d", pool.Len(), pidOf("keep"), kept)
	}
}

// Close returns only after the callbacks have: nothing runs after it.
func TestPoolCloseWaitsForCallbacks(t *testing.T) {
	isolate(t)
	var started, finished atomic.Bool
	pool := newTestPool(t, Options{OnMessage: func(Inbound) {
		started.Store(true)
		time.Sleep(300 * time.Millisecond)
		finished.Store(true)
	}})
	pool.Reconcile([]Spec{{Name: "slow", CWD: "s"}})
	waitFor(t, "shim ready", func() bool { return pool.ReplyAddr("slow") != "" })
	c := &ccsock.Client{Timeout: 5 * time.Second}
	if _, err := c.SendToAddress(context.Background(), pool.ReplyAddr("slow"), ccsock.Message{
		Text: "hi", FromName: "worker", From: "uds:/tmp/sender.sock",
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "callback started", started.Load)
	pool.Close()
	if !finished.Load() {
		t.Fatal("Close returned while a callback was still running")
	}
}

// A receipt keeps everything the shim reports about it.
func TestPoolReceiptCarriesFromAndReason(t *testing.T) {
	got := make(chan Receipt, 1)
	pool, err := NewPool(Options{Args: func(string, string) []string { return nil },
		OnReceipt: func(r Receipt) { got <- r }})
	if err != nil {
		t.Fatal(err)
	}
	p := &shimProc{events: make(chan shimEvent, 1)}
	p.events <- shimEvent{T: "receipt", Status: "denied", OrigMsgID: "m1", From: "uds:/tmp/x.sock", Reason: "busy"}
	close(p.events)
	pool.calls.Add(1)
	pool.dispatch("bot", p)
	want := Receipt{Shim: "bot", OrigMsgID: "m1", Status: "denied", From: "uds:/tmp/x.sock", Reason: "busy"}
	if r := <-got; r != want {
		t.Fatalf("receipt %+v, want %+v", r, want)
	}
}

// deadPID returns the PID of a process that has exited.
func deadPID(t *testing.T) int {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), shimEnv+"=0")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	return cmd.Process.Pid
}

func TestSweep(t *testing.T) {
	sessions := isolate(t)
	if err := os.MkdirAll(sessions, 0o700); err != nil {
		t.Fatal(err)
	}
	self := os.Getpid()
	tok, err := ccsock.ProcessStartToken(self)
	if err != nil {
		t.Skip("no process start tokens on this platform:", err)
	}
	dead := deadPID(t)
	ours := pidDomain()
	write := func(file string, rec map[string]any) string {
		t.Helper()
		data, _ := json.Marshal(rec)
		path := filepath.Join(sessions, file)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	entry := func(pid int, procStart, domain string, shim bool) map[string]any {
		rec := map[string]any{"pid": pid, "sessionId": "s" + strconv.Itoa(pid), "procStart": procStart}
		if domain != "" {
			rec["pidDomain"] = domain
		}
		if shim {
			rec[Marker] = true
		}
		return rec
	}
	// The registry is keyed by file name and Sweep reads the PID from it, so
	// each case needs a PID of its own. The ones past the dead PID are made
	// up and belong to no process.
	live := write(fmt.Sprintf("%d.json", self), entry(self, tok, ours, true))
	gone := write(fmt.Sprintf("%d.json", dead), entry(dead, "", ours, true))
	elsewhere := write(fmt.Sprintf("%d.json", dead+1_000_000), entry(dead+1_000_000, "", "linux:another-machine:pid:[1]", true))
	session := write(fmt.Sprintf("%d.json", dead+2_000_000), entry(dead+2_000_000, "", ours, false))

	Sweep()
	exists := func(path string) bool { _, err := os.Stat(path); return err == nil }
	if !exists(live) {
		t.Error("removed the entry of a running shim")
	}
	if exists(gone) {
		t.Error("kept the entry of a dead shim")
	}
	if !exists(elsewhere) {
		t.Error("removed a shim entry from another PID namespace")
	}
	if !exists(session) {
		t.Error("removed an entry that is not a shim's")
	}

	// A recycled PID: the process is running, but it is not the one that
	// wrote the entry, as its start token shows.
	write(fmt.Sprintf("%d.json", self), entry(self, tok+"0", ours, true))
	Sweep()
	if exists(live) {
		t.Error("kept a shim entry whose PID now belongs to another process")
	}
}
