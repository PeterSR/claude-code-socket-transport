package ccsock

import (
	"fmt"
	"os"
	"strings"
)

// addrSafe reports whether a byte may appear unescaped in a "uds:" address.
// Claude Code percent-encodes everything outside this set.
func addrSafe(b byte) bool {
	switch {
	case b >= 'A' && b <= 'Z', b >= 'a' && b <= 'z', b >= '0' && b <= '9':
		return true
	case b == ':', b == '_', b == '/', b == '.', b == '\\', b == '-':
		return true
	}
	return false
}

// Address renders a socket path as the "uds:" reply address Claude Code uses,
// percent-encoding the bytes that are not address-safe.
//
//	ccsock.Address("/run/user/1000/cc-socks/1234.sock")
//	// "uds:/run/user/1000/cc-socks/1234.sock"
func Address(socketPath string) string {
	var b strings.Builder
	b.WriteString("uds:")
	for i := 0; i < len(socketPath); i++ {
		c := socketPath[i]
		if addrSafe(c) {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String()
}

// ParseAddress extracts the socket path from an address. It accepts the "uds:"
// form Claude Code emits as well as a bare absolute path, so a caller can pass
// either a /status "Peer address" value or a plain path.
func ParseAddress(addr string) (string, error) {
	switch {
	case strings.HasPrefix(addr, "uds:"):
		return percentDecode(addr[len("uds:"):]), nil
	case strings.HasPrefix(addr, "/"):
		return addr, nil
	default:
		return "", fmt.Errorf("%q is not a uds address or absolute socket path", addr)
	}
}

func percentDecode(s string) string {
	if !strings.Contains(s, "%") {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			hi, ok1 := unhex(s[i+1])
			lo, ok2 := unhex(s[i+2])
			if ok1 && ok2 {
				b.WriteByte(hi<<4 | lo)
				i += 2
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func unhex(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// SelfSocketPath returns the inbox socket of the Claude Code session this
// process is running under, from CLAUDE_CODE_MESSAGING_SOCKET. It is empty for
// a process started outside a session, or in a session that has not yet bound
// its inbox.
func SelfSocketPath() string { return os.Getenv("CLAUDE_CODE_MESSAGING_SOCKET") }

// SelfToken returns CLAUDE_CODE_MESSAGING_TOKEN, the token a hook or Bash
// command uses to prove to its own session that it is that session's child.
// Client uses it automatically when the target is the session it runs under.
func SelfToken() string { return os.Getenv("CLAUDE_CODE_MESSAGING_TOKEN") }

// SelfAddress returns the "uds:" reply address of the session this process runs
// under, or "" when there is none. Set it as Message.From to receive delivery
// receipts; see Inbox.
func SelfAddress() string {
	sock := SelfSocketPath()
	if sock == "" {
		return ""
	}
	return Address(sock)
}
