package shim

import "syscall"

// shimSysProcAttr makes the kernel stop a shim if its owner dies without
// closing its stdin, so a crashed owner leaves no ghost agents listed.
//
// Linux sends the signal when the thread that forked the shim exits, not
// when the owner process does. The Go runtime keeps its threads for the life
// of the process, with one exception: a goroutine that calls
// runtime.LockOSThread and returns without unlocking takes its thread down
// with it. A shim started by Reconcile on such a goroutine is therefore sent
// SIGTERM when that goroutine ends, and the Pool restarts it on the next
// Reconcile. Do not call Reconcile from a goroutine that locks its thread and
// then exits.
func shimSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}
}
