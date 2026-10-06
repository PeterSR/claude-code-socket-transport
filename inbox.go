package ccsock

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const inboxIdleTimeout = 30 * time.Second
const maxInboxConnections = 16

type ReceiptStatus string

const (
	StatusHeld      ReceiptStatus = "held"
	StatusDenied    ReceiptStatus = "denied"
	StatusExpired   ReceiptStatus = "expired"
	StatusDelivered ReceiptStatus = "delivered"
	StatusRefused   ReceiptStatus = "refused"
	StatusDropped   ReceiptStatus = "dropped"
)

type Receipt struct {
	Status    ReceiptStatus
	OrigMsgID string
	From      string
	Reason    string
}

type incomingFrame struct {
	Type      string        `json:"type"`
	Action    string        `json:"action"`
	Status    ReceiptStatus `json:"status"`
	OrigMsgID string        `json:"orig_msg_id"`
	From      string        `json:"from"`
	Reason    string        `json:"reason"`
	Message   *struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"message"`
}

// Callbacks run concurrently and must be short; they must not call Close.
// Directory should be the resolved target's socket directory, not its caller's
// TMPDIR. ExpectedPID checks kernel peer credentials, not a claimed JSON field.
type InboxConfig struct {
	Directory   string
	ExpectedPID int
	OnReceipt   func(Receipt)
	OnMessage   func(text, from string)
}

type Inbox struct {
	cfg         InboxConfig
	listener    net.Listener
	path        string
	mu          sync.Mutex
	closed      bool
	connections map[net.Conn]struct{}
	workers     sync.WaitGroup
	acceptDone  chan struct{}
	closeOnce   sync.Once
	closeErr    error
}

func Listen(cfg InboxConfig) (*Inbox, error) {
	dir := cfg.Directory
	if dir == "" {
		dir = filepath.Dir(DefaultSocketPath(os.Getpid()))
	}
	if err := ensureSocketDir(dir); err != nil {
		return nil, err
	}
	id, err := newUUID()
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir, fmt.Sprintf("%d-%s.sock", os.Getpid(), id[:8]))
	if len(path) > maxSocketPathBytes {
		return nil, fmt.Errorf("reply socket path exceeds %d bytes", maxSocketPathBytes)
	}
	// The checked 0700 parent prevents exposure while the socket is chmodded.
	// Never remove an existing pathname or change the process-global umask.
	l, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err = os.Chmod(path, 0600); err != nil {
		l.Close()
		return nil, err
	}
	in := &Inbox{cfg: cfg, listener: l, path: path, connections: make(map[net.Conn]struct{}), acceptDone: make(chan struct{})}
	go in.serve()
	return in, nil
}
func (in *Inbox) Path() string    { return in.path }
func (in *Inbox) Address() string { return Address(in.path) }

// Close waits for callbacks and closes only this inbox's accepted connections.
// It never controls or terminates the Claude process.
func (in *Inbox) Close() error {
	in.closeOnce.Do(func() {
		in.mu.Lock()
		in.closed = true
		in.closeErr = in.listener.Close()
		for conn := range in.connections {
			conn.Close()
		}
		in.mu.Unlock()
		<-in.acceptDone
		in.workers.Wait()
	})
	return in.closeErr
}
func (in *Inbox) serve() {
	defer close(in.acceptDone)
	for {
		conn, err := in.listener.Accept()
		if err != nil {
			return
		}
		in.mu.Lock()
		if in.closed || len(in.connections) >= maxInboxConnections {
			in.mu.Unlock()
			conn.Close()
			continue
		}
		in.connections[conn] = struct{}{}
		in.workers.Add(1)
		in.mu.Unlock()
		go in.handle(conn)
	}
}
func (in *Inbox) handle(conn net.Conn) {
	defer func() { conn.Close(); in.mu.Lock(); delete(in.connections, conn); in.mu.Unlock(); in.workers.Done() }()
	if in.cfg.ExpectedPID > 0 {
		if err := VerifyPeerPID(conn, in.cfg.ExpectedPID); err != nil {
			return
		}
	}
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 0, 4096), maxFrameBytes)
	// One absolute deadline also bounds peers that drip bytes indefinitely.
	conn.SetReadDeadline(time.Now().Add(inboxIdleTimeout))
	for scanner.Scan() {
		in.mu.Lock()
		closed := in.closed
		in.mu.Unlock()
		if closed {
			return
		}
		var f incomingFrame
		if json.Unmarshal(scanner.Bytes(), &f) != nil {
			continue
		}
		switch {
		case f.Type == "control" && f.Action == "peer_message_status":
			if in.cfg.OnReceipt != nil {
				in.cfg.OnReceipt(Receipt{f.Status, f.OrigMsgID, f.From, f.Reason})
			}
		case f.Type == "user" && f.Message != nil && f.Message.Role == "user":
			if in.cfg.OnMessage != nil {
				in.cfg.OnMessage(f.Message.Content, f.From)
			}
		}
	}
}
