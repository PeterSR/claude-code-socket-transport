package ccsock

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
)

// ReceiptStatus is the outcome a receiving session reports back to a sender.
type ReceiptStatus string

const (
	// StatusHeld means the message is waiting for the receiving user's
	// approval. A second receipt follows when they decide.
	StatusHeld ReceiptStatus = "held"
	// StatusDenied means the receiving user declined the message.
	StatusDenied ReceiptStatus = "denied"
	// StatusExpired means a held message timed out unapproved.
	StatusExpired ReceiptStatus = "expired"
	// StatusDelivered means a previously held message was released to Claude.
	StatusDelivered ReceiptStatus = "delivered"
)

// Receipt is a delivery outcome for a message this process sent.
type Receipt struct {
	// Status is the outcome.
	Status ReceiptStatus
	// OrigMsgID is the ID returned by the Send call this receipt answers.
	OrigMsgID string
	// From is the reporting session's reply address.
	From string
	// Reason is the human-readable explanation the session included.
	Reason string
}

// incomingFrame is the subset of the wire format an Inbox decodes.
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

// Inbox binds a socket that speaks the receiving half of the protocol, so a Go
// program can collect the delivery receipts its own sends generate.
//
// It is optional: sending works without one. Without an inbox there is no
// address to put in Message.From, so a session has nowhere to report that it
// held, denied, or later delivered your message.
//
// An Inbox is not a Claude Code session. It binds beside the real sockets so
// receipts are routed to it, but it does not register itself in the session
// registry and will not appear in another session's agent list.
type Inbox struct {
	// OnReceipt is called for each delivery receipt. It runs on the
	// connection's goroutine, so keep it short.
	OnReceipt func(Receipt)

	// OnMessage is called when a peer sends this inbox a user message.
	// Optional.
	OnMessage func(text, from string)

	listener net.Listener
	path     string
	mu       sync.Mutex
	closed   bool
}

// Listen binds an inbox socket in the same directory as the session sockets, so
// receipts pass the receiving session's check that a reply address sits inside
// its own socket namespace. A socket bound elsewhere is refused.
//
// The returned Inbox must be closed; Close removes the socket file.
func Listen() (*Inbox, error) {
	dir := SocketsDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("ccsock: creating %s: %w", dir, err)
	}
	path := filepath.Join(dir, fmt.Sprintf("%d.sock", os.Getpid()))
	// A previous process with this PID may have left a socket behind. Only
	// clear it when nothing is listening.
	if Probe(path, defaultProbeTimeout) {
		return nil, fmt.Errorf("ccsock: %s is already live", path)
	}
	_ = os.Remove(path)

	l, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("ccsock: binding %s: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = l.Close()
		return nil, fmt.Errorf("ccsock: restricting %s: %w", path, err)
	}

	in := &Inbox{listener: l, path: path}
	go in.serve()
	return in, nil
}

// Path returns the bound socket path.
func (in *Inbox) Path() string { return in.path }

// Address returns the "uds:" address to put in Message.From.
func (in *Inbox) Address() string { return Address(in.path) }

// Close stops accepting and removes the socket file.
func (in *Inbox) Close() error {
	in.mu.Lock()
	if in.closed {
		in.mu.Unlock()
		return nil
	}
	in.closed = true
	in.mu.Unlock()

	err := in.listener.Close()
	_ = os.Remove(in.path)
	return err
}

func (in *Inbox) serve() {
	for {
		conn, err := in.listener.Accept()
		if err != nil {
			in.mu.Lock()
			closed := in.closed
			in.mu.Unlock()
			if closed {
				return
			}
			continue
		}
		go in.handle(conn)
	}
}

func (in *Inbox) handle(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 0, 64*1024), maxFrameBytes)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var f incomingFrame
		if err := json.Unmarshal(line, &f); err != nil {
			continue
		}
		switch {
		case f.Type == "control" && f.Action == "peer_message_status":
			if in.OnReceipt != nil {
				in.OnReceipt(Receipt{
					Status:    f.Status,
					OrigMsgID: f.OrigMsgID,
					From:      f.From,
					Reason:    f.Reason,
				})
			}
		case f.Type == "user" && f.Message != nil:
			if in.OnMessage != nil {
				in.OnMessage(f.Message.Content, f.From)
			}
		}
	}
}
