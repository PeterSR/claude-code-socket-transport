//go:build linux

package ccsock

import (
	"fmt"
	"net"
	"syscall"
)

func VerifyPeerPID(conn net.Conn, expected int) error {
	unix, ok := conn.(*net.UnixConn)
	if !ok {
		return fmt.Errorf("not a Unix connection")
	}
	raw, err := unix.SyscallConn()
	if err != nil {
		return err
	}
	var cred *syscall.Ucred
	var callErr error
	err = raw.Control(func(fd uintptr) {
		cred, callErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	})
	if err != nil {
		return err
	}
	if callErr != nil {
		return callErr
	}
	if int(cred.Pid) != expected {
		return fmt.Errorf("socket peer PID %d differs from expected %d", cred.Pid, expected)
	}
	return nil
}
