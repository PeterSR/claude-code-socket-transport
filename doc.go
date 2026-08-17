// Package ccsock sends messages into a running Claude Code session over its
// per-session Unix domain socket ("cross-session messaging").
//
// It is for programs that are not Claude. A Claude Code session already has the
// SendMessage and ListAgents tools and needs nothing here. Everything else on
// the machine has no way in: a CI runner, a file watcher, a deploy script, a
// systemd unit. This package is that way in.
//
// Every Claude Code session on macOS or Linux binds an inbox socket, by default
// at $XDG_RUNTIME_DIR/cc-socks/<pid>.sock, and registers itself in a JSON file
// at <config>/sessions/<pid>.json, where <config> is $CLAUDE_CONFIG_DIR or
// ~/.claude. A peer speaks a newline-delimited JSON protocol over that socket:
// an optional auth frame, then one frame per line.
//
// The smallest useful thing:
//
//	msgID, err := ccsock.SendToSessionID(ctx, sessionID, ccsock.Message{
//	    Text: "migration finished, rebasing on main is safe",
//	})
//
// Discovery, auth-token lookup, and the wire encoding are handled for you.
// See Client for the configurable form, and Inbox if you want delivery receipts.
//
// The package builds under GOOS=windows for sending only. Listen refuses there,
// because the standard library gives no way to verify that the socket directory
// is one only you can write.
package ccsock
