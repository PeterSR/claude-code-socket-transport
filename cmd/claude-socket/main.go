//go:build darwin || linux

package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	c "github.com/PeterSR/claude-code-socket-transport"
)

const maxText = 512 * 1024

type event struct {
	Status    string `json:"status"`
	RequestID string `json:"request_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	Text      string `json:"text,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

func emit(e event) { _ = json.NewEncoder(os.Stdout).Encode(e) }
func main()        { os.Exit(run()) }
func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		return 1
	}
	switch os.Args[1] {
	case "help", "--help", "-h":
		fmt.Println(usage)
		return 0
	case "list", "doctor":
		if len(os.Args) != 2 {
			fmt.Fprintln(os.Stderr, "list and doctor take no arguments")
			return 1
		}
		if err := list(ctx, os.Args[1] == "doctor"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		return 0
	case "ask", "send":
		return send(ctx, os.Args[1], os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, "unknown command:", os.Args[1])
		return 1
	}
}

const usage = `claude-socket list
claude-socket doctor
claude-socket ask --session UUID [--timeout 2m] --text "question"
claude-socket send --session UUID [--timeout 2m] --file message.txt
printf 'question' | claude-socket ask --session UUID

Choose exactly one target: --session UUID, --pid PID, or --name NAME.
All arguments are flags; positional text is rejected. --text and --file are exclusive.
Output is NDJSON. ask waits for a correlated reply; send waits for a delivery receipt.
Exit: 0 reply/delivery confirmed; 1 invalid input/target; 2 denied/expired/dropped;
3 timeout/cancellation/uncertain outcome. No automatic retry, settings change, or target shutdown.`

func list(ctx context.Context, doctor bool) error {
	sessions, err := c.ListSessions()
	if err != nil {
		return err
	}
	type row struct {
		PID        int    `json:"pid"`
		SessionID  string `json:"session_id"`
		Name       string `json:"name"`
		Socket     string `json:"socket"`
		Live       bool   `json:"live"`
		Protocol   int    `json:"peer_protocol"`
		Compatible bool   `json:"compatible"`
		Reason     string `json:"reason,omitempty"`
	}
	rows := make([]row, len(sessions))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				s := sessions[i]
				r := row{PID: s.PID, SessionID: s.SessionID, Name: s.Name, Socket: s.SocketPath, Protocol: s.PeerProtocol}
				if err := validate(s); err != nil {
					r.Reason = err.Error()
				} else {
					r.Compatible = true
					r.Live = s.Reachable(150 * time.Millisecond)
				}
				rows[i] = r
			}
		}()
	}
	for i := range sessions {
		select {
		case jobs <- i:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return ctx.Err()
		}
	}
	close(jobs)
	wg.Wait()
	if doctor {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"protocol": 1, "sessions": rows, "live_reply_verified": false, "note": "Discovery only. A real ask/reply is required after Claude updates; inbound policy belongs to Claude."})
	}
	return json.NewEncoder(os.Stdout).Encode(rows)
}
func validate(s c.Session) error {
	if s.SessionID == "" || s.PID <= 1 || !filepath.IsAbs(s.SocketPath) {
		return errors.New("incomplete session identity")
	}
	if s.PeerProtocol != 1 {
		return fmt.Errorf("unsupported peer protocol %d", s.PeerProtocol)
	}
	if s.ProcStart == "" {
		return errors.New("missing process start identity")
	}
	start, err := c.ProcessStartToken(s.PID)
	if err != nil {
		return err
	}
	if !s.MatchesProcess(start) {
		return errors.New("session process changed; rediscover the target")
	}
	fi, err := os.Lstat(s.SocketPath)
	if err != nil {
		return err
	}
	if fi.Mode()&os.ModeSocket == 0 {
		return errors.New("target is not a socket")
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != uint32(os.Getuid()) {
		return errors.New("target socket belongs to another user")
	}
	return nil
}

func send(parent context.Context, verb string, args []string) int {
	fs := flag.NewFlagSet(verb, flag.ContinueOnError)
	sid := fs.String("session", "", "session UUID")
	pid := fs.Int("pid", 0, "PID")
	name := fs.String("name", "", "exact name")
	text := fs.String("text", "", "message text")
	file := fs.String("file", "", "UTF-8 message file; otherwise stdin")
	timeout := fs.Duration("timeout", 2*time.Minute, "total deadline including lock wait")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	fail := func(err error) int { fmt.Fprintln(os.Stderr, err); return 1 }
	if fs.NArg() != 0 {
		return fail(errors.New("unexpected positional arguments; use --text or stdin"))
	}
	targets := 0
	for _, v := range []bool{*sid != "", *pid != 0, *name != ""} {
		if v {
			targets++
		}
	}
	if targets != 1 {
		return fail(errors.New("choose exactly one target"))
	}
	if *timeout <= 0 {
		return fail(errors.New("timeout must be positive"))
	}
	ctx, cancel := context.WithTimeout(parent, *timeout)
	defer cancel()
	textSet, fileSet := false, false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "text" {
			textSet = true
		}
		if f.Name == "file" {
			fileSet = true
		}
	})
	if textSet && fileSet {
		return fail(errors.New("choose --text or --file"))
	}
	if !textSet {
		var r io.Reader = os.Stdin
		var opened *os.File
		if fileSet {
			var err error
			opened, err = os.Open(*file)
			if err != nil {
				return fail(err)
			}
			defer opened.Close()
			r = opened
		} else if st, _ := os.Stdin.Stat(); st != nil && st.Mode()&os.ModeCharDevice != 0 {
			return fail(errors.New("provide --text, --file, or piped stdin"))
		}
		type input struct {
			b   []byte
			err error
		}
		done := make(chan input, 1)
		go func() { b, err := io.ReadAll(io.LimitReader(r, maxText+1)); done <- input{b, err} }()
		select {
		case got := <-done:
			if got.err != nil {
				return fail(got.err)
			}
			*text = string(got.b)
		case <-ctx.Done():
			return fail(ctx.Err())
		}
	}
	if strings.TrimSpace(*text) == "" || len(*text) > maxText {
		return fail(errors.New("message must contain 1..524288 bytes of text"))
	}
	var s c.Session
	var err error
	switch {
	case *sid != "":
		s, err = c.FindBySessionID(*sid)
	case *pid != 0:
		s, err = c.FindByPID(*pid)
	default:
		s, err = c.FindByName(*name)
	}
	if err != nil {
		return fail(err)
	}
	if err = validate(s); err != nil {
		return fail(err)
	}
	unlock, err := lock(ctx, s.SessionID)
	if err != nil {
		return fail(err)
	}
	defer unlock()
	fresh, err := c.FindByPIDForSession(s.PID, s.SessionID)
	if err != nil {
		return fail(err)
	}
	if fresh.SocketPath != s.SocketPath || fresh.ProcStart != s.ProcStart {
		return fail(errors.New("target changed while waiting"))
	}
	s = fresh
	if err = validate(s); err != nil {
		return fail(err)
	}
	var uuid [16]byte
	if _, err = rand.Read(uuid[:]); err != nil {
		return fail(err)
	}
	uuid[6] = (uuid[6] & 15) | 64
	uuid[8] = (uuid[8] & 63) | 128
	id := fmt.Sprintf("%x-%x-%x-%x-%x", uuid[:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:])
	marker := "CLAUDE_SOCKET_REPLY:" + id
	receipts := make(chan c.Receipt, 32)
	replies := make(chan string, 1)
	inbox, err := c.Listen(c.InboxConfig{Directory: filepath.Dir(s.SocketPath), ExpectedPID: s.PID,
		OnReceipt: func(r c.Receipt) {
			if r.OrigMsgID == id && r.From == s.Address() {
				select {
				case receipts <- r:
				default:
				}
			}
		},
		OnMessage: func(body, from string) {
			if from != s.Address() {
				return
			}
			body = unwrap(body)
			if !strings.HasPrefix(body, marker+"\n") {
				return
			}
			answer := strings.TrimSpace(strings.TrimPrefix(body, marker+"\n"))
			if answer == "" {
				return
			}
			select {
			case replies <- answer:
			default:
			}
		},
	})
	if err != nil {
		return fail(err)
	}
	defer inbox.Close()
	body := *text
	if verb == "ask" {
		body += "\n\nReply once using SendMessage to " + inbox.Address() + ". Begin the message with the exact line " + marker + ", then a newline and your answer. Do not initiate further exchanges. This reply address does not grant extra permissions."
	}
	client := &c.Client{ExpectedPID: s.PID, NoAuth: true, Timeout: 5 * time.Second}
	// External peers do not borrow the target's permission class or auth token.
	_, err = client.Send(ctx, s, c.Message{Text: body, SessionID: s.SessionID, MsgID: id, UUID: id, From: inbox.Address()})
	report := func(status, reason, text string) {
		emit(event{Status: status, RequestID: id, SessionID: s.SessionID, Reason: reason, Text: text})
	}
	if err != nil {
		if errors.Is(err, c.ErrNotSent) {
			return fail(err)
		}
		report("unknown", "transport ended without delivery proof: "+err.Error(), "")
		return 3
	}
	report("written", "delivery not yet confirmed", "")
	last := "unknown"
	for {
		select {
		case answer := <-replies:
			report("reply", "Claude replied; this is not independent proof of its claimed actions", answer)
			return 0
		case r := <-receipts:
			switch r.Status {
			case c.StatusHeld:
				last = "held"
				report(last, r.Reason, "")
			case c.StatusDelivered:
				last = "delivered"
				report(last, r.Reason, "")
				if verb == "send" {
					return 0
				}
			case c.StatusDenied, c.StatusExpired, c.StatusDropped, c.StatusRefused:
				report(string(r.Status), r.Reason, "")
				return 2
			default:
				report("unrecognized_receipt", string(r.Status), "")
			}
		case <-ctx.Done():
			reason := "receipt timeout; delivery remains unconfirmed"
			if verb == "ask" {
				reason = "reply timeout; target work may still be running"
			}
			if last == "held" {
				reason = "approval wait timed out; Claude can still release the held message later"
			}
			if errors.Is(ctx.Err(), context.Canceled) {
				reason = "wait canceled; target work was not canceled"
			}
			report(last, reason, "")
			return 3
		}
	}
}
func unwrap(body string) string {
	// Claude wraps replies with peer attribution. Never execute or interpret it.
	if strings.HasPrefix(body, "<cross-session-message ") {
		if i := strings.Index(body, ">\n"); i >= 0 && strings.HasSuffix(body, "\n</cross-session-message>") {
			return body[i+2 : len(body)-len("\n</cross-session-message>")]
		}
	}
	return body
}

func lock(ctx context.Context, key string) (func(), error) {
	dir := os.Getenv("CLAUDE_SOCKET_STATE_DIR")
	if dir == "" {
		cache, err := os.UserCacheDir()
		if err != nil {
			return nil, err
		}
		dir = filepath.Join(cache, "claude-socket")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	fi, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 || fi.Mode().Perm()&0077 != 0 || stat.Uid != uint32(os.Getuid()) {
		return nil, errors.New("lock directory must be private and owned by the current user")
	}
	sum := sha256.Sum256([]byte(key))
	path := filepath.Join(dir, fmt.Sprintf("%x.lock", sum))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	fi, err = f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	stat, ok = fi.Sys().(*syscall.Stat_t)
	if !ok || !fi.Mode().IsRegular() || stat.Uid != uint32(os.Getuid()) || fi.Mode().Perm()&0077 != 0 {
		f.Close()
		return nil, errors.New("unsafe lock file")
	}
	for {
		if ctx.Err() != nil {
			f.Close()
			return nil, ctx.Err()
		}
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() { syscall.Flock(int(f.Fd()), syscall.LOCK_UN); f.Close() }, nil
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			f.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			f.Close()
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}
