package ccsock

import (
	"regexp"
	"strings"
)

// Envelope is a parsed <cross-session-message> wrapper: what a Claude Code
// session's native SendMessage puts around a message (see Inbox for
// receiving one).
type Envelope struct {
	From     string // the sender's "uds:" reply address
	FromName string
	Body     string
}

var (
	envOpen  = regexp.MustCompile(`^<cross-session-message((?:\s+[a-z-]+="[^"]*")*)>\n`)
	envAttr  = regexp.MustCompile(`([a-z-]+)="([^"]*)"`)
	envClose = "</cross-session-message>"
)

// ParseEnvelope unwraps the envelope Claude Code puts around a message sent
// with its native SendMessage. Text without one comes back as the body.
//
// Attribute values are taken verbatim, with no XML entity decoding, because
// neither Claude Code nor this package encodes them: both drop the quote and
// angle bracket characters from a sender name and write everything else,
// including "&", as is. A name that arrives as "R&amp;D" was sent as
// "R&amp;D", and decoding it would change it.
func ParseEnvelope(text string) Envelope {
	m := envOpen.FindStringSubmatchIndex(text)
	trimmed := strings.TrimRight(text, "\n")
	if m == nil || !strings.HasSuffix(trimmed, "\n"+envClose) {
		return Envelope{Body: text}
	}
	var e Envelope
	for _, kv := range envAttr.FindAllStringSubmatch(text[m[2]:m[3]], -1) {
		switch kv[1] {
		case "from":
			e.From = kv[2]
		case "from-name":
			e.FromName = kv[2]
		}
	}
	rest := trimmed[m[1]:]
	if rest == envClose {
		return e // empty body
	}
	body := strings.TrimSuffix(rest, "\n"+envClose)
	// Claude Code escapes a closing tag inside the body so it cannot end the
	// envelope early; undo that.
	e.Body = strings.ReplaceAll(body, `<\/cross-session-message`, "</cross-session-message")
	return e
}
