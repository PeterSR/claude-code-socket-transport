// Command cc-send exercises the ccsock library from the shell: list the Claude
// Code sessions registered on this machine, and send one of them a message.
//
//	cc-send list
//	cc-send send --session <uuid> "the migration finished"
//	cc-send send --pid 1234 --name-as deploy-bot "build is green"
//	cc-send send --to uds:/run/user/1000/cc-socks/1234.sock "hello" --wait-receipt
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	ccsock "github.com/PeterSR/claude-code-socket-transport"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "cc-send:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return errors.New("no command given")
	}
	switch args[0] {
	case "list":
		return listCmd(args[1:])
	case "send":
		return sendCmd(args[1:])
	case "rename":
		return renameCmd(args[1:])
	case "self":
		return selfCmd()
	case "-h", "--help", "help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage:
  cc-send list [--json] [--all]
  cc-send send (--session <uuid> | --pid <n> | --name <name> | --to <addr>) [flags] <text>
  cc-send rename (--pid <n> | --to <addr>) <new-name>
  cc-send self

send flags:
  --priority now|next|later   queue placement (default next)
  --name-as <name>            attribute the message to this sender name
  --from <uds-addr>           reply address to put on the message
  --wait-receipt <duration>   bind a temporary inbox, set --from to it, and
                              report delivery receipts for this long
  --no-auth                   send without an auth frame
  --timeout <duration>        send timeout (default 5s)
`)
}

func listCmd(args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "emit JSON")
	all := fs.Bool("all", false, "include sessions whose socket does not answer")
	if err := fs.Parse(args); err != nil {
		return err
	}

	sessions, err := ccsock.ListSessions()
	if err != nil {
		return err
	}

	type row struct {
		PID       int    `json:"pid"`
		SessionID string `json:"sessionId"`
		Name      string `json:"name"`
		Kind      string `json:"kind"`
		Status    string `json:"status"`
		CWD       string `json:"cwd"`
		Socket    string `json:"socket"`
		Address   string `json:"address"`
		Live      bool   `json:"live"`
	}
	rows := make([]row, 0, len(sessions))
	for _, s := range sessions {
		live := s.Reachable(300 * time.Millisecond)
		if !live && !*all {
			continue
		}
		rows = append(rows, row{
			PID: s.PID, SessionID: s.SessionID, Name: s.Name, Kind: s.Kind,
			Status: s.Status, CWD: s.CWD, Socket: s.SocketPath,
			Address: s.Address(), Live: live,
		})
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(rows)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "PID\tLIVE\tNAME\tKIND\tSTATUS\tSESSION ID\tCWD")
	for _, r := range rows {
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
			r.PID, yesNo(r.Live), dash(r.Name), dash(r.Kind), dash(r.Status), dash(r.SessionID), dash(r.CWD))
	}
	return w.Flush()
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func sendCmd(args []string) error {
	fs := flag.NewFlagSet("send", flag.ContinueOnError)
	sessionID := fs.String("session", "", "target session UUID")
	pid := fs.Int("pid", 0, "target PID")
	name := fs.String("name", "", "target session name")
	to := fs.String("to", "", "target uds: address or socket path")
	priority := fs.String("priority", "next", "now, next, or later")
	nameAs := fs.String("name-as", "", "sender name to attribute the message to")
	from := fs.String("from", "", "reply address to put on the message")
	waitReceipt := fs.Duration("wait-receipt", 0, "bind a temporary inbox and report receipts for this long")
	noAuth := fs.Bool("no-auth", false, "send without an auth frame")
	timeout := fs.Duration("timeout", 5*time.Second, "send timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}

	text := strings.TrimSpace(strings.Join(fs.Args(), " "))
	if text == "" {
		return errors.New("no message text given")
	}

	client := &ccsock.Client{Timeout: *timeout, NoAuth: *noAuth}
	msg := ccsock.Message{
		Text:     text,
		Priority: ccsock.Priority(*priority),
		FromName: *nameAs,
		From:     *from,
	}

	var inbox *ccsock.Inbox
	receipts := make(chan ccsock.Receipt, 8)
	if *waitReceipt > 0 {
		var err error
		inbox, err = ccsock.Listen()
		if err != nil {
			return err
		}
		defer inbox.Close()
		inbox.OnReceipt = func(r ccsock.Receipt) { receipts <- r }
		msg.From = inbox.Address()
		fmt.Fprintf(os.Stderr, "listening for receipts on %s\n", inbox.Address())
	}
	if msg.From == "" {
		msg.From = ccsock.SelfAddress()
	}

	ctx := context.Background()
	var (
		msgID string
		err   error
	)
	switch {
	case *sessionID != "":
		msgID, err = client.SendToSessionID(ctx, *sessionID, msg)
	case *pid != 0:
		msgID, err = client.SendToPID(ctx, *pid, msg)
	case *name != "":
		msgID, err = client.SendToName(ctx, *name, msg)
	case *to != "":
		msgID, err = client.SendToAddress(ctx, *to, msg)
	default:
		return errors.New("pick a target with --session, --pid, --name, or --to")
	}
	if err != nil {
		return err
	}
	fmt.Println(msgID)

	if *waitReceipt > 0 {
		deadline := time.After(*waitReceipt)
		for {
			select {
			case r := <-receipts:
				fmt.Fprintf(os.Stderr, "receipt: %s (msg %s) %s\n", r.Status, r.OrigMsgID, r.Reason)
				if r.Status != ccsock.StatusHeld {
					return nil
				}
			case <-deadline:
				fmt.Fprintln(os.Stderr, "no further receipts")
				return nil
			}
		}
	}
	return nil
}

func renameCmd(args []string) error {
	fs := flag.NewFlagSet("rename", flag.ContinueOnError)
	pid := fs.Int("pid", 0, "target PID")
	to := fs.String("to", "", "target uds: address or socket path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	newName := strings.TrimSpace(strings.Join(fs.Args(), " "))
	if newName == "" {
		return errors.New("no new name given")
	}

	var socket string
	switch {
	case *pid != 0:
		s, err := ccsock.FindByPID(*pid)
		if err != nil {
			return err
		}
		socket = s.SocketPath
	case *to != "":
		p, err := ccsock.ParseAddress(*to)
		if err != nil {
			return err
		}
		socket = p
	default:
		return errors.New("pick a target with --pid or --to")
	}
	return ccsock.New().Rename(context.Background(), socket, newName)
}

func selfCmd() error {
	sock := ccsock.SelfSocketPath()
	if sock == "" {
		fmt.Println("not running inside a Claude Code session with messaging")
		return nil
	}
	fmt.Println("socket: ", sock)
	fmt.Println("address:", ccsock.SelfAddress())
	fmt.Println("token:  ", tokenState())
	return nil
}

func tokenState() string {
	if ccsock.SelfToken() == "" {
		return "unset"
	}
	return "set"
}
