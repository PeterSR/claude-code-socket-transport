// Package shim makes agents that are not Claude Code sessions show up in
// Claude Code's own ListAgents, under their own names, and lets Claude Code
// sessions reach them with their native SendMessage.
//
// Each such agent gets a shim: a small child process that binds a messaging
// socket and writes a session registry entry for itself, as a Claude Code
// session would. A Pool keeps one shim running per agent you give it, keeps
// each entry's status, start time and Remote Control id current, and hands
// you every message a Claude Code session sends to one of them (Inbound),
// plus the delivery receipts for messages you sent with a shim's address as
// the reply address (Receipt).
//
// The program that owns the Pool also runs the shims: Options.Args builds the
// command line it is re-run with, and that command line must lead to Run.
//
//	pool, _ := shim.NewPool(shim.Options{
//	    Args:      func(name, cwd string) []string { return []string{"shim", name, cwd} },
//	    OnMessage: func(m shim.Inbound) { fmt.Println(m.Shim, "got", m.Body) },
//	})
//	pool.Reconcile([]shim.Spec{{Name: "build-bot", CWD: "/srv/ci", Status: "idle"}})
//
// and in main, before anything else:
//
//	if len(os.Args) == 4 && os.Args[1] == "shim" {
//	    if err := shim.Run(os.Args[2], os.Args[3], os.Stdin, os.Stdout); err != nil {
//	        log.Fatal(err)
//	    }
//	    return
//	}
//
// The registry format and the socket protocol are not a public Claude Code
// interface; see the parent package. IsShim tells a shim's registry entry from
// a real session's, and Sweep removes entries left by shims that were killed.
package shim
