package ccsock

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

// Priority controls where a message lands in the receiving session's input
// queue.
type Priority string

const (
	// PriorityNext is the default: the message is delivered to Claude at the
	// next opportunity, between tool calls or as a new turn when idle.
	PriorityNext Priority = "next"
	// PriorityLater queues the message behind whatever is already pending.
	PriorityLater Priority = "later"
	// PriorityNow asks the receiver to process the frame ahead of its queue.
	// Claude Code handles a "now" frame off the ordered processing chain, so it
	// can overtake earlier frames from the same sender. It does not bypass the
	// receiver's inbound controls.
	PriorityNow Priority = "now"
)

// protocolVersion is the msgV Claude Code stamps on every frame it sends.
const protocolVersion = 1

// Message is a message to deliver into a session.
//
// Only Text is required. The zero value of Priority means PriorityNext.
type Message struct {
	// Text is the message body. It must be non-empty; a session ignores a
	// frame whose content is empty or not a string.
	Text string

	// Priority selects the receiving queue. Empty means PriorityNext.
	Priority Priority

	// SessionID, when set, is checked by the receiver against its own session
	// ID and the message is dropped on a mismatch. Set it to make delivery
	// safe against a recycled PID or a socket path reused by a newer session.
	SessionID string

	// From is the sender's reply address, in "uds:" form. Set it to receive
	// delivery receipts and to let the receiving Claude reply. SelfAddress
	// returns the right value when running inside a Claude Code session; an
	// Inbox reports its own. Empty means the receiver sees the sender as
	// unknown and cannot reply.
	From string

	// FromName, when set, wraps the text in the attribution envelope Claude
	// Code uses between sessions, so the message appears under this name in
	// the receiving conversation. Empty sends the text unwrapped.
	FromName string

	// MsgID correlates a message with the receipts sent back to From. Empty
	// generates a UUID, which Send returns.
	MsgID string

	// UUID identifies the queued input on the receiving side. Empty lets the
	// receiver generate one.
	UUID string
}

// userFrame is the wire form of a user message.
type userFrame struct {
	MsgV      int          `json:"msgV"`
	MsgID     string       `json:"msg_id"`
	Type      string       `json:"type"`
	Message   framePayload `json:"message"`
	Priority  Priority     `json:"priority"`
	From      string       `json:"from,omitempty"`
	SessionID string       `json:"session_id,omitempty"`
	UUID      string       `json:"uuid,omitempty"`
}

type framePayload struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// controlFrame is the wire form of a control message.
type controlFrame struct {
	MsgV      int    `json:"msgV"`
	MsgID     string `json:"msg_id"`
	Type      string `json:"type"`
	Action    string `json:"action"`
	Name      string `json:"name,omitempty"`
	SessionID string `json:"session_id,omitempty"`
}

type authFrame struct {
	Type  string `json:"type"`
	Token string `json:"token"`
}

func (m Message) frame() (userFrame, error) {
	if m.Text == "" {
		return userFrame{}, fmt.Errorf("ccsock: message text is empty")
	}
	priority := m.Priority
	switch priority {
	case "":
		priority = PriorityNext
	case PriorityNow, PriorityNext, PriorityLater:
	default:
		return userFrame{}, fmt.Errorf("ccsock: unknown priority %q", priority)
	}

	msgID := m.MsgID
	if msgID == "" {
		id, err := newUUID()
		if err != nil {
			return userFrame{}, err
		}
		msgID = id
	}

	content := m.Text
	if m.FromName != "" {
		content = wrapEnvelope(m.From, m.FromName, m.Text)
	}

	return userFrame{
		MsgV:      protocolVersion,
		MsgID:     msgID,
		Type:      "user",
		Message:   framePayload{Role: "user", Content: content},
		Priority:  priority,
		From:      m.From,
		SessionID: m.SessionID,
		UUID:      m.UUID,
	}, nil
}

// envelopeTag is the element Claude Code wraps peer message text in so the
// receiving session can attribute it.
const envelopeTag = "cross-session-message"

// fromNameRe rejects the characters that would break the envelope's attribute
// quoting, matching the receiver's own parse.
var fromNameRe = regexp.MustCompile(`["<>\n\r]`)

// maxFromName is the length Claude Code truncates a sender name to.
const maxFromName = 200

// wrapEnvelope builds the attribution envelope. The receiver re-serializes what
// it parses and compares it against the original, so attribute order and
// spacing here have to match exactly: from, then from-name.
func wrapEnvelope(from, name, body string) string {
	var attrs strings.Builder
	if from != "" {
		fmt.Fprintf(&attrs, ` from=%q`, from)
	}
	if clean := sanitizeName(name); clean != "" {
		fmt.Fprintf(&attrs, ` from-name=%q`, clean)
	}
	return fmt.Sprintf("<%s%s>\n%s\n</%s>", envelopeTag, attrs.String(), escapeBody(body), envelopeTag)
}

func sanitizeName(name string) string {
	name = fromNameRe.ReplaceAllString(name, "")
	name = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, name)
	name = strings.TrimSpace(name)
	if len(name) > maxFromName {
		name = strings.TrimSpace(name[:maxFromName])
	}
	return name
}

// escapeBody neutralizes a closing envelope tag inside the body so a message
// cannot forge the end of its own envelope and claim a different sender.
func escapeBody(body string) string {
	return strings.ReplaceAll(body, "</"+envelopeTag, `<\/`+envelopeTag)
}

func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("ccsock: generating message id: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32], nil
}
