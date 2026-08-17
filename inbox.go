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

// inboxIdleTimeout bounds how long a connection may sit without sending a
// full line. Without it, a local process that opens a connection and never
// writes would pin a goroutine and a file descriptor forever.
const inboxIdleTimeout = 30 * time.Second

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

// InboxConfig configures the callbacks an Inbox invokes as frames arrive. It
// is consumed by Listen before the accept loop starts, so there is no window
// in which a frame can arrive before the callbacks are wired up.
type InboxConfig struct {
	// OnReceipt is called for each delivery receipt. It runs on the
	// connection's goroutine, so keep it short.
	OnReceipt func(Receipt)
	// OnMessage is called when a peer sends this inbox a user message.
	// Optional.
	OnMessage func(text, from string)
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
	onReceipt func(Receipt)
	onMessage func(text, from string)

	listener net.Listener
	path     string
	mu       sync.Mutex
	closed   bool
}

// Listen binds an inbox socket in the same directory as the session sockets, so
// receipts pass the receiving session's check that a reply address sits inside
// its own socket namespace. A socket bound elsewhere is refused.
//
// Listen fails rather than binding when that directory is not a directory we
// own with no group or world access, since anything else lets another local
// user replace the socket and forge receipts.
//
// The returned Inbox must be closed; Close removes the socket file.
func Listen(cfg InboxConfig) (*Inbox, error) {
	// DefaultSocketPath applies the same 103-byte sun_path fallback Claude
	// Code itself uses, so our bind directory matches what a receiving
	// session actually checks against.
	path := DefaultSocketPath(os.Getpid())
	dir := filepath.Dir(path)
	if err := ensureSocketDir(dir); err != nil {
		return nil, err
	}

	// A previous process with this PID may have left a socket behind. Only
	// clear it when nothing is listening.
	if Probe(path, defaultProbeTimeout) {
		return nil, fmt.Errorf("ccsock: %s is already live", path)
	}
	_ = os.Remove(path)

	// net.Listen creates the socket file honoring the process umask, which is
	// typically 0755: briefly connectable by any local user before the Chmod
	// below runs. withTightUmask tightens the umask for the duration of the
	// call so the file never exists in a world-writable state, even for an
	// instant.
	var l net.Listener
	if err := withTightUmask(func() error {
		var lErr error
		l, lErr = net.Listen("unix", path)
		return lErr
	}); err != nil {
		return nil, fmt.Errorf("ccsock: binding %s: %w", path, err)
	}
	// Belt and braces: restrict the mode explicitly too, in case the umask
	// above did not cover every path net.Listen takes to create the file.
	if err := os.Chmod(path, 0o600); err != nil {
		_ = l.Close()
		return nil, fmt.Errorf("ccsock: restricting %s: %w", path, err)
	}

	in := &Inbox{
		listener:  l,
		path:      path,
		onReceipt: cfg.OnReceipt,
		onMessage: cfg.OnMessage,
	}
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
	// Mirrors the backoff net/http.Server.Serve uses around Accept: a
	// transient error such as EMFILE under fd exhaustion would otherwise spin
	// this goroutine at 100% CPU retrying immediately.
	var retryDelay time.Duration
	for {
		conn, err := in.listener.Accept()
		if err != nil {
			in.mu.Lock()
			closed := in.closed
			in.mu.Unlock()
			if closed {
				return
			}
			if retryDelay == 0 {
				retryDelay = 5 * time.Millisecond
			} else {
				retryDelay *= 2
			}
			if retryDelay > time.Second {
				retryDelay = time.Second
			}
			time.Sleep(retryDelay)
			continue
		}
		retryDelay = 0
		go in.handle(conn)
	}
}

func (in *Inbox) handle(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 0, 64*1024), maxFrameBytes)
	for {
		// Refresh the deadline before every read, not just once at connect
		// time, so a peer that opens a connection and never writes cannot pin
		// this goroutine and its fd forever.
		_ = conn.SetReadDeadline(time.Now().Add(inboxIdleTimeout))
		if !scanner.Scan() {
			break
		}
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
			if in.onReceipt != nil {
				in.onReceipt(Receipt{
					Status:    f.Status,
					OrigMsgID: f.OrigMsgID,
					From:      f.From,
					Reason:    f.Reason,
				})
			}
		case f.Type == "user" && f.Message != nil:
			if in.onMessage != nil {
				in.onMessage(f.Message.Content, f.From)
			}
		}
	}
}
