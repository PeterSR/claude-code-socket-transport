//go:build darwin

package ccsock

import (
	"fmt"
	"net"
	"syscall"
	"unsafe"
)

// LOCAL_PEERPID on Darwin returns the kernel identity of a Unix socket peer.
func VerifyPeerPID(conn net.Conn, expected int) error {
	unix, ok := conn.(*net.UnixConn)
	if !ok {
		return fmt.Errorf("not a Unix connection")
	}
	raw, err := unix.SyscallConn()
	if err != nil {
		return err
	}
	var pid int32
	size := uint32(unsafe.Sizeof(pid))
	var callErr syscall.Errno
	err = raw.Control(func(fd uintptr) {
		_, _, callErr = syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, 0, 2, uintptr(unsafe.Pointer(&pid)), uintptr(unsafe.Pointer(&size)), 0)
	})
	if err != nil {
		return err
	}
	if callErr != 0 {
		return callErr
	}
	if int(pid) != expected {
		return fmt.Errorf("socket peer PID %d differs from expected %d", pid, expected)
	}
	return nil
}
