package ccsock

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// maxSocketPathBytes is the length past which Claude Code abandons
// $XDG_RUNTIME_DIR and falls back to a /tmp socket directory. The sun_path
// limit on Linux is 108 bytes and 104 on macOS; Claude Code uses 103.
const maxSocketPathBytes = 103

// ConfigDir returns the Claude Code configuration directory: $CLAUDE_CONFIG_DIR
// when set, otherwise ~/.claude.
func ConfigDir() (string, error) {
	if d := os.Getenv("CLAUDE_CONFIG_DIR"); d != "" {
		return d, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("locating home directory: %w", err)
	}
	return filepath.Join(home, ".claude"), nil
}

// SessionsDir returns the directory holding session registry files
// (<pid>.json) and inbox auth keys (<pid>.<hash>.key).
func SessionsDir() (string, error) {
	cfg, err := ConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(cfg, "sessions"), nil
}

// SocketsDir returns the directory Claude Code binds inbox sockets in for the
// current user. Prefer the socket path recorded in a session's registry entry;
// this is the fallback used when computing a path from a PID alone.
func SocketsDir() string {
	base := os.Getenv("XDG_RUNTIME_DIR")
	if base == "" {
		base = os.Getenv("CLAUDE_CODE_TMPDIR")
	}
	if base == "" {
		base = os.TempDir()
	}
	return filepath.Join(base, "cc-socks")
}

// DefaultSocketPath reproduces the path Claude Code binds for a given PID.
//
// It is a fallback for when a session has no registry entry. The authoritative
// path is Session.SocketPath, which the session writes to its registry file.
func DefaultSocketPath(pid int) string {
	p, err := filepath.Abs(filepath.Join(SocketsDir(), fmt.Sprintf("%d.sock", pid)))
	if err == nil && len(p) <= maxSocketPathBytes {
		return p
	}
	fallback := "/tmp"
	if prefix := os.Getenv("PREFIX"); os.Getenv("TERMUX_VERSION") != "" && prefix != "" {
		fallback = filepath.Join(prefix, "tmp")
	}
	return filepath.Join(fallback, fmt.Sprintf("cc-socks-%d", os.Getuid()), fmt.Sprintf("%d.sock", pid))
}

// canonicalSocketPath normalizes a socket path the same way Claude Code does
// before hashing it into an auth-key filename. Paths containing a ".." segment
// are refused rather than cleaned, matching Claude Code's own refusal.
func canonicalSocketPath(socketPath string) (string, error) {
	if socketPath == "" {
		return "", fmt.Errorf("empty socket path")
	}
	for _, seg := range strings.Split(filepath.ToSlash(socketPath), "/") {
		if seg == ".." {
			return "", fmt.Errorf("socket path %q contains a %q segment", socketPath, "..")
		}
	}
	abs, err := filepath.Abs(socketPath)
	if err != nil {
		return "", fmt.Errorf("resolving socket path %q: %w", socketPath, err)
	}
	return abs, nil
}
