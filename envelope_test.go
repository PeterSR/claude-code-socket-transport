package ccsock

import "testing"

func TestParseEnvelope(t *testing.T) {
	native := "<cross-session-message from=\"uds:/run/user/1000/cc-socks/42.sock\" from-session=\"abc\" from-name=\"worker\" from-mode=\"prompting\">\nhello\nsee <\\/cross-session-message> inside\n</cross-session-message>"
	e := ParseEnvelope(native)
	if e.From != "uds:/run/user/1000/cc-socks/42.sock" || e.FromName != "worker" {
		t.Fatalf("attrs: %+v", e)
	}
	if e.Body != "hello\nsee </cross-session-message> inside" {
		t.Fatalf("body: %q", e.Body)
	}
	if got := ParseEnvelope("plain text"); got.Body != "plain text" || got.From != "" {
		t.Fatalf("plain: %+v", got)
	}
}

func TestParseEnvelopeEmptyBodyAndTruncated(t *testing.T) {
	if e := ParseEnvelope("<cross-session-message from-name=\"x\">\n</cross-session-message>"); e.Body != "" || e.FromName != "x" {
		t.Fatalf("empty body: %+v", e)
	}
	in := "<cross-session-message from-name=\"x\">\n"
	if e := ParseEnvelope(in); e.Body != in {
		t.Fatalf("truncated: %+v", e)
	}
}

// ParseEnvelope is the exact inverse of the envelope the package writes:
// what looks like an XML entity in a name is the name, not an escape.
func TestParseEnvelopeRoundTripsNamesVerbatim(t *testing.T) {
	for _, name := range []string{"R&D", "a &amp; b", "&quot;quoted&quot;", "tom & jerry's bot"} {
		wrapped := wrapEnvelope("uds:/tmp/a.sock", name, "body & <more>")
		e := ParseEnvelope(wrapped)
		if e.FromName != name || e.From != "uds:/tmp/a.sock" || e.Body != "body & <more>" {
			t.Fatalf("%q: wrapped %q parsed as %+v", name, wrapped, e)
		}
	}
	// Quote and angle bracket characters are dropped on the way out, as
	// Claude Code does, so they never need decoding on the way in.
	if e := ParseEnvelope(wrapEnvelope("", `say "hi" <now>`, "x")); e.FromName != "say hi now" {
		t.Fatalf("sanitized name: %+v", e)
	}
}
