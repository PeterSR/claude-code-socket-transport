package ccsock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"time"
)

const (
	// defaultTimeout matches the send timeout Claude Code uses.
	defaultTimeout = 5 * time.Second
	// defaultProbeTimeout matches the liveness probe Claude Code uses.
	defaultProbeTimeout = 250 * time.Millisecond
	// maxFrameBytes is the receiver's per-line limit. A longer frame makes the
	// receiver drop the connection without reading it.
	maxFrameBytes = 1 << 20
)

// Client sends frames to session inboxes. The zero value is usable; New
// returns one with the defaults spelled out.
type Client struct {
	// Timeout bounds a single send, from dial to the receiver closing the
	// connection. Zero means 5s.
	Timeout time.Duration

	// ProbeTimeout bounds a liveness probe. Zero means 250ms.
	ProbeTimeout time.Duration

	// Token overrides auth-token discovery. Leave empty to let the client find
	// the token the target published, which is what you want in almost every
	// case.
	Token string

	// NoAuth sends no auth frame even when a token is available. The receiver
	// then classes the sender as an unverified peer, which a session in
	// bypassPermissions mode holds for the user's approval.
	NoAuth bool
}

// New returns a Client with default timeouts.
func New() *Client { return &Client{} }

var defaultClient = &Client{}

func (c *Client) timeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return defaultTimeout
}

func (c *Client) probeTimeout() time.Duration {
	if c.ProbeTimeout > 0 {
		return c.ProbeTimeout
	}
	return defaultProbeTimeout
}

// Send delivers a message to a session found through the registry.
//
// When the caller left Message.SessionID empty, Send fills it from the target
// session, so a registry entry that has gone stale cannot misdeliver the
// message to whatever now listens on that path. Send returns the message ID,
// which correlates with the receipts delivered to Message.From.
func (c *Client) Send(ctx context.Context, s Session, m Message) (string, error) {
	if s.SocketPath == "" {
		return "", fmt.Errorf("ccsock: session %d has no inbox socket", s.PID)
	}
	if m.SessionID == "" {
		m.SessionID = s.SessionID
	}
	return c.SendToSocket(ctx, s.SocketPath, m)
}

// SendToPID delivers a message to the session registered under a PID.
func (c *Client) SendToPID(ctx context.Context, pid int, m Message) (string, error) {
	s, err := FindByPID(pid)
	if err != nil {
		return "", err
	}
	return c.Send(ctx, s, m)
}

// SendToSessionID delivers a message to the session with the given session
// UUID.
func (c *Client) SendToSessionID(ctx context.Context, sessionID string, m Message) (string, error) {
	s, err := FindBySessionID(sessionID)
	if err != nil {
		return "", err
	}
	return c.Send(ctx, s, m)
}

// SendToName delivers a message to the session answering to a name.
func (c *Client) SendToName(ctx context.Context, name string, m Message) (string, error) {
	s, err := FindByName(name)
	if err != nil {
		return "", err
	}
	return c.Send(ctx, s, m)
}

// SendToAddress delivers a message to a "uds:" address or an absolute socket
// path, bypassing the registry. Use it with the value from /status or from
// CLAUDE_CODE_MESSAGING_SOCKET.
func (c *Client) SendToAddress(ctx context.Context, addr string, m Message) (string, error) {
	path, err := ParseAddress(addr)
	if err != nil {
		return "", err
	}
	return c.SendToSocket(ctx, path, m)
}

// SendToSocket delivers a message to an inbox socket by path.
//
// Nothing in the protocol acknowledges delivery, so a nil error means the
// receiver accepted the connection and read the frame, not that Claude saw the
// message. The receiver still applies its own inbound controls, which can hold
// or drop it. Set Message.From and run an Inbox to learn the outcome.
func (c *Client) SendToSocket(ctx context.Context, socketPath string, m Message) (string, error) {
	frame, err := m.frame()
	if err != nil {
		return "", err
	}
	if err := c.writeFrame(ctx, socketPath, frame); err != nil {
		return "", err
	}
	return frame.MsgID, nil
}

// Rename asks a session to change the name it answers to. The session applies
// it without prompting, so use it only on sessions you own.
func (c *Client) Rename(ctx context.Context, socketPath, name string) error {
	if name == "" {
		return fmt.Errorf("ccsock: rename needs a non-empty name")
	}
	msgID, err := newUUID()
	if err != nil {
		return err
	}
	return c.writeFrame(ctx, socketPath, controlFrame{
		MsgV:   protocolVersion,
		MsgID:  msgID,
		Type:   "control",
		Action: "rename",
		Name:   name,
	})
}

// writeFrame opens the socket, writes the optional auth line followed by one
// JSON line, and half-closes so the receiver sees the end of the stream.
func (c *Client) writeFrame(ctx context.Context, socketPath string, frame any) error {
	payload, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("ccsock: encoding frame: %w", err)
	}
	if len(payload) >= maxFrameBytes {
		return fmt.Errorf("ccsock: frame is %d bytes, over the receiver's %d byte line limit", len(payload), maxFrameBytes)
	}

	token, err := c.resolveToken(socketPath)
	if err != nil {
		return err
	}

	var line []byte
	if token != "" {
		auth, err := json.Marshal(authFrame{Type: "auth", Token: token})
		if err != nil {
			return fmt.Errorf("ccsock: encoding auth frame: %w", err)
		}
		line = append(append(line, auth...), '\n')
	}
	line = append(append(line, payload...), '\n')

	ctx, cancel := context.WithTimeout(ctx, c.timeout())
	defer cancel()

	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", socketPath)
	if err != nil {
		return fmt.Errorf("ccsock: connecting to %s: %w", socketPath, err)
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if _, err := conn.Write(line); err != nil {
		return fmt.Errorf("ccsock: writing to %s: %w", socketPath, err)
	}

	unix, ok := conn.(*net.UnixConn)
	if !ok {
		return fmt.Errorf("ccsock: %s is not a unix socket", socketPath)
	}
	if runtime.GOOS == "darwin" {
		// Claude Code delays its own half-close on macOS, where closing
		// immediately after a write can cost the receiver the last frame.
		time.Sleep(150 * time.Millisecond)
	}
	if err := unix.CloseWrite(); err != nil {
		return fmt.Errorf("ccsock: half-closing %s: %w", socketPath, err)
	}

	// The receiver closes once it has taken the frame. Waiting for that turns
	// a rejected connection, such as one dropped for a bad auth frame, into a
	// timeout rather than a silent success.
	if _, err := io.Copy(io.Discard, conn); err != nil && !errors.Is(err, net.ErrClosed) {
		return fmt.Errorf("ccsock: waiting for %s to close: %w", socketPath, err)
	}
	return nil
}

// resolveToken picks the auth token for a target: an explicit override first,
// then the token this process was handed for its own session, then the token
// the target published for peers.
func (c *Client) resolveToken(socketPath string) (string, error) {
	if c.NoAuth {
		return "", nil
	}
	if c.Token != "" {
		return c.Token, nil
	}
	if self := SelfSocketPath(); self != "" && self == socketPath {
		if tok := SelfToken(); tok != "" {
			return tok, nil
		}
	}
	return LookupToken(socketPath)
}

// Probe reports whether something is listening on a socket path. It is the
// cheapest way to tell a live session from a stale registry entry.
func Probe(socketPath string, timeout time.Duration) bool {
	if timeout <= 0 {
		timeout = defaultProbeTimeout
	}
	conn, err := net.DialTimeout("unix", socketPath, timeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// Send delivers a message using the default client. See Client.Send.
func Send(ctx context.Context, s Session, m Message) (string, error) {
	return defaultClient.Send(ctx, s, m)
}

// SendToPID delivers a message using the default client. See Client.SendToPID.
func SendToPID(ctx context.Context, pid int, m Message) (string, error) {
	return defaultClient.SendToPID(ctx, pid, m)
}

// SendToSessionID delivers a message using the default client.
// See Client.SendToSessionID.
func SendToSessionID(ctx context.Context, sessionID string, m Message) (string, error) {
	return defaultClient.SendToSessionID(ctx, sessionID, m)
}

// SendToName delivers a message using the default client. See Client.SendToName.
func SendToName(ctx context.Context, name string, m Message) (string, error) {
	return defaultClient.SendToName(ctx, name, m)
}

// SendToAddress delivers a message using the default client.
// See Client.SendToAddress.
func SendToAddress(ctx context.Context, addr string, m Message) (string, error) {
	return defaultClient.SendToAddress(ctx, addr, m)
}
