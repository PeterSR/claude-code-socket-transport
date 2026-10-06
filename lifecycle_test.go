package ccsock

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// macOS socket paths have a 103-byte limit. Go's default test directory embeds
// the full test name, so use a private short root for every socket fixture.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("/tmp", "cc-test.")
	if err != nil {
		panic(err)
	}
	os.Setenv("TMPDIR", dir)
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// Cancellation after the write must interrupt an established connection whose
// receiver stalls. CLI process exit cannot prove the library releases this I/O.
func TestCancelEstablishedSend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := l.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := (&Client{NoAuth: true, Timeout: 2 * time.Second}).SendToSocket(ctx, path, Message{Text: "ping"})
		done <- err
	}()
	conn := <-accepted
	defer conn.Close()
	// Read the half-close, which occurs after Darwin's required write delay.
	b := make([]byte, 4096)
	for {
		if _, err := conn.Read(b); err != nil {
			break
		}
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancel returned success")
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("cancellation left established I/O running")
	}
}

// Library embedders keep running after Close; no old connections or callbacks
// may survive it. A CLI exiting the process masks this lifecycle failure.
func TestCloseStopsAcceptedConnection(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	events := make(chan string, 2)
	in, err := Listen(InboxConfig{OnMessage: func(text, from string) { events <- text }})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	conn, err := net.Dial("unix", in.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	frame := []byte("{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"hello\"}}\n")
	conn.Write(frame)
	select {
	case <-events:
	case <-time.After(time.Second):
		t.Fatal("no initial callback")
	}
	if err := in.Close(); err != nil {
		t.Fatal(err)
	}
	conn.Write(frame)
	select {
	case <-events:
		t.Fatal("callback survived Close")
	case <-time.After(50 * time.Millisecond):
	}
	conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection still usable")
	} else if e, ok := err.(net.Error); ok && e.Timeout() {
		t.Fatal("connection not closed")
	}
}
