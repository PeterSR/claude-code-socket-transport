# claude-code-socket-transport

A Go library for delivering a message into a running Claude Code session over
that session's Unix domain socket.

This is for programs that are not Claude. A Claude Code session already has
`SendMessage` and `ListAgents` built into its harness and needs nothing from
this package. What has no way in is everything else on the machine: a CI runner
that wants to tell the session its build went red, a file watcher, a deploy
script, a systemd unit, a monitoring daemon. This package gives those a way to
put a message in front of a session.

Claude Code sessions on macOS and Linux each bind an inbox socket and speak a
small newline-delimited JSON protocol on it. Anthropic documents that a script
can post into a session that way, but not the frame format, the discovery files,
or the auth-key layout. This package implements all of it, so a Go program can
address a session by session ID, PID, name, or socket path and have its message
appear in that session's conversation exactly as one from another Claude.

```go
msgID, err := ccsock.SendToSessionID(ctx, "0dd4b9a6-0000-4000-8000-000000000000",
    ccsock.Message{
        Text:     "The schema migration finished. Rebasing on main is safe now.",
        FromName: "deploy-bot",
    })
```

Two version numbers matter and they are not the same one. Cross-session
messaging exists from Claude Code v2.1.224, which is the floor for a session to
bind an inbox at all. The protocol implemented here was read out of v2.1.233
and verified against it; earlier versions in that range are untested. Runs on
macOS and Linux, including WSL 2. Native Windows has no cross-session
messaging.

## Install

```
go get github.com/PeterSR/claude-code-socket-transport
```

## Usage

### Find sessions

`ListSessions` reads the on-disk registry every session writes itself into.
Entries can outlive the process that wrote them, so check `Reachable` before
trusting one.

```go
sessions, err := ccsock.ListSessions()
for _, s := range sessions {
    if s.Reachable(250 * time.Millisecond) {
        fmt.Println(s.PID, s.Name, s.SessionID, s.CWD, s.Address())
    }
}
```

`FindByPID`, `FindBySessionID`, and `FindByName` do the lookup for you. A
session ID survives a resume onto a new PID and names are not unique, so both
prefer the reachable entry and return `ErrAmbiguous` rather than guess between
two live candidates.

### Send

```go
client := &ccsock.Client{Timeout: 5 * time.Second}

msgID, err := client.SendToPID(ctx, 4242, ccsock.Message{
    Text:     "CI went red on main, hold off on merging.",
    Priority: ccsock.PriorityNext,
    FromName: "ci-watcher",
})
```

When you target a session found through the registry, the client stamps the
message with that session's ID, and the receiver drops it on a mismatch. A
registry entry that has gone stale therefore cannot misdeliver your message to
whatever now listens on that path.

To skip the registry entirely, pass the address from `/status`'s `Peer address`
row or from `$CLAUDE_CODE_MESSAGING_SOCKET`:

```go
msgID, err := client.SendToAddress(ctx, "uds:/run/user/1000/cc-socks/4242.sock", msg)
```

A nil error means the receiver took the frame off the socket, not that Claude
saw it. What happens next is the receiving session's decision, and it can hold
the message for its user's approval or drop it. The API cannot tell you which
synchronously, because the answer arrives later on a separate connection. If
you need it, give the message a return address and listen:

```go
inbox, _ := ccsock.Listen()
defer inbox.Close()
inbox.OnReceipt = func(r ccsock.Receipt) { log.Println(r.OrigMsgID, r.Status) }

msgID, err := client.SendToPID(ctx, 4242, ccsock.Message{Text: "build is green", From: inbox.Address()})
```

See [Receipts](#receipts) for what the statuses mean.

### Attribution

Set `Message.FromName` and the text is wrapped in the same envelope Claude Code
uses between sessions, so the message shows up under that name in the receiving
conversation. Leave it empty and the text goes unwrapped, arriving from an
unnamed peer.

Set `Message.From` to a reply address so the receiving Claude can answer. Inside
a Claude Code session, `SelfAddress()` returns the right value. From a standalone
program, an `Inbox` provides one.

### Receipts

Sending is one-way. If you want to know what became of a message, bind an
`Inbox` and use its address as `Message.From`. The receiving session reports back
over the same protocol when it holds, denies, expires, or later delivers your
message.

```go
inbox, err := ccsock.Listen()
defer inbox.Close()
inbox.OnReceipt = func(r ccsock.Receipt) {
    log.Printf("%s: %s (%s)", r.OrigMsgID, r.Status, r.Reason)
}

msgID, err := client.SendToName(ctx, "api-worker", ccsock.Message{
    Text: "deploy finished",
    From: inbox.Address(),
})
```

An `Inbox` is passive. It is not a Claude Code session: it does not register
itself, and it will not appear in another session's agent list. A receiving
session only sends receipts to an address inside its own socket namespace, which
is why `Listen` binds beside the real sockets rather than anywhere you like.

### CLI

`cmd/cc-send` exercises the library from a shell.

```
go build ./cmd/cc-send

./cc-send list
./cc-send send --session 0dd4b9a6-… "the migration finished"
./cc-send send --pid 4242 --name-as deploy-bot "build is green"
./cc-send rename --pid 4242 api-worker
./cc-send self

# --wait-receipt binds a temporary inbox, uses it as the return address, and
# reports for 30s whether the session held, denied, expired, or delivered it
./cc-send send --to uds:/run/user/1000/cc-socks/4242.sock "hello" --wait-receipt 30s
```

`send` prints the bare message ID on stdout and nothing else, so it pipes.

## The protocol

Everything below is reverse-engineered from the Claude Code binary (v2.1.233)
and confirmed against a live session. It is not a public interface and can
change in any release.

### The socket

Each session binds `$XDG_RUNTIME_DIR/cc-socks/<pid>.sock`, mode `0600`, in a
directory it insists is `0700` and owned by you. If that path would exceed 103
bytes it falls back to `/tmp/cc-socks-<uid>/<pid>.sock`, or to `$PREFIX/tmp`
under Termux. A session exports its
own path as `CLAUDE_CODE_MESSAGING_SOCKET` to hooks and Bash commands, and shows
it in `/status` as `Peer address`, prefixed `uds:`.

In a `uds:` address every byte outside `A-Za-z0-9:_/.\-` is percent-encoded.

### Frames

The client opens the socket, writes one JSON object per line, and half-closes.
The receiver reads line by line and drops the connection if 1 MiB arrives
without a newline.

An optional auth frame comes first:

```json
{"type":"auth","token":"0123456789abcdef0123456789abcdef"}
```

Then the message:

```json
{"msgV":1,"msg_id":"<uuid>","type":"user","message":{"role":"user","content":"hello"},"priority":"next","from":"uds:/run/user/1000/cc-socks/4242.sock","session_id":"<uuid>"}
```

Only `type` and a non-empty string `message.content` are load-bearing. The rest:

| Field | Meaning |
| --- | --- |
| `priority` | `now`, `next` (default), or `later`. A `now` frame is handled off the ordered processing chain and can overtake earlier frames from the same sender. It does not bypass inbound controls. |
| `session_id` | Checked against the receiver's own session ID; a mismatch drops the message. Guards against a recycled PID or a reused socket path. |
| `from` | Reply address, in `uds:` form. Needed for receipts and for the receiving Claude to answer. |
| `msg_id` | UUID correlating the message with its receipts. |
| `uuid` | Identifies the queued input on the receiving side. |
| `msgV` | Protocol version, currently `1`. |

Control frames use the same connection:

```json
{"msgV":1,"msg_id":"<uuid>","type":"control","action":"rename","name":"api-worker"}
```

and receipts come back as:

```json
{"type":"control","action":"peer_message_status","status":"held","orig_msg_id":"<uuid>","from":"uds:…","reason":"…"}
```

with `status` one of `held`, `denied`, `expired`, `delivered`.

### Auth

Auth is optional on macOS and Linux and required only on native Windows, where
cross-session messaging does not run anyway. It is worth sending regardless,
because the token tells the receiver which class the sender belongs to, and an
unclassed sender gets held for approval by a session running in
`bypassPermissions` mode.

When a session binds its inbox it writes
`<config>/sessions/<pid>.<sha256(socket path)>.key`, mode `0600`, containing:

```json
{"peerToken":"<32 hex>","procStart":"<kernel start token>"}
```

`<config>` is `$CLAUDE_CONFIG_DIR` or `~/.claude`. The hash is over the resolved
absolute socket path; a path containing a `..` segment is refused rather than
cleaned. `procStart` is field 22 of `/proc/<pid>/stat` on Linux and the `ps
-o lstart=` string on macOS, and it separates a live owner from a recycled PID.
`LookupToken` reads that file; when several name the same socket it ranks them
the way Claude Code does and takes the best-corroborated one.

A process posting to *its own* session's socket instead uses the token it was
handed in `CLAUDE_CODE_MESSAGING_TOKEN`, which identifies it as that session's
own child. The client picks this automatically when the target socket is the one
in `CLAUDE_CODE_MESSAGING_SOCKET`.

The receiver also reads the sender's PID off the socket credentials and, on
Linux, walks its ancestry to recognize its own children. Nothing on the client
side affects this.

### The session registry

Each session maintains `<config>/sessions/<pid>.json` and rewrites it as things
change. The fields this package models are on `Session`; the rest stay in
`Session.Raw`. The file is written by the session, so it can describe a process
that has since exited. `Running` checks the PID and `Reachable` connects to the
socket, which is the check Claude Code itself trusts.

### Attribution envelope

`FromName` wraps the text as:

```
<cross-session-message from="uds:…" from-name="deploy-bot">
body text
</cross-session-message>
```

The receiver re-serializes what it parses and compares it against what arrived,
so attribute order and spacing have to be exact. A closing tag inside the body
is escaped, so a message body cannot forge the end of its own envelope and claim
a different sender.

## What this does not do

- **Reach sessions on other machines or on the web.** Those go through
  Anthropic's servers over Remote Control. This package is local sockets only.
- **Read a conversation.** The socket is write-only from a peer's side. Nothing
  comes back except delivery receipts.
- **Bypass the receiver's controls.** `crossSessionInbound` and the
  permission-mode defaults apply to everything you send. A session set to
  `refuse` silently drops your message and reports nothing.
- **Guarantee delivery.** Claude Code rate-limits repeated messages per sender,
  drops identical repeats arriving close together, and caps undelivered accepted
  messages at 50 per session.

## Prior art

Several projects connect Claude Code sessions to each other. As far as I can
tell none of them speak Claude Code's own inbox socket: each builds a parallel
transport alongside it and needs cooperating code running inside every session.
That is the difference in one line. This package talks to a stock session that
knows nothing about it, which is what lets a program that is not Claude, and
was never launched by Claude, get a message in front of one.

- [yilunzhang/claude-code-inter-session](https://github.com/yilunzhang/claude-code-inter-session)
  (Python) runs a local WebSocket bus and delivers over Claude Code's `Monitor`
  tool, so latency is milliseconds with no polling. Every participating session
  has to connect to the bus.
- [PatilShreyas/claude-code-session-bridge](https://github.com/PatilShreyas/claude-code-session-bridge)
  (Shell) is a Claude Code plugin that passes messages through files under
  `~/.claude/session-bridge/`, with a listen script polling every three seconds.
  It is built for a request-and-answer exchange between two repos.
- [Jesse-njx/dsh-crosstalk](https://github.com/Jesse-njx/dsh-crosstalk)
  (TypeScript) is not for Claude Code at all. It ports the `ListAgents` and
  `SendMessage` model onto another harness, over a heartbeat registry of files.
  Useful as a second reading of the same design.

On the Go side, [lancekrogers/claude-code-go](https://github.com/lancekrogers/claude-code-go)
and [ProjAnvil/claude-agent-sdk-golang](https://github.com/ProjAnvil/claude-agent-sdk-golang)
are SDKs for launching and driving Claude from Go. They start a session and own
it. This package does the opposite: it addresses a session someone else already
started and is sitting in front of.

The feature itself is
[documented by Anthropic](https://code.claude.com/docs/en/cross-session-messaging),
including the fact that a script can post to a session's socket. The frame
format, the registry files, and the auth-key layout are not documented; that
part is read out of the binary.

## Stability

The wire format, the registry files, and the key-file layout are Claude Code
internals with no compatibility promise, and a Claude Code release can change
them without notice. `Session.PeerProtocol` carries the peer protocol version a
session advertises; this package targets `1`, as read out of v2.1.233.

## License

MIT
