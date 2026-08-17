//go:build windows

package ccsock

import (
	"fmt"
	"os"
	"syscall"
)

// processQueryLimitedInformation is PROCESS_QUERY_LIMITED_INFORMATION, the
// access right needed to open a handle to a process without asking for more
// than existence. Go's syscall package does not export it on windows.
const processQueryLimitedInformation = 0x1000

// processAlive reports whether a PID exists, by attempting to open a limited
// handle to it. An error means the process could not be opened, which is
// treated as not alive.
func processAlive(pid int) bool {
	h, err := syscall.OpenProcess(processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		return false
	}
	_ = syscall.CloseHandle(h)
	return true
}

// openNoFollow opens path with a plain os.Open. Windows has no O_NOFOLLOW
// equivalent in the standard library, so the symlink guarantee this gives
// readSmallFile on unix is unix-only; on Windows a path swapped for a
// symlink between check and read can still be followed.
func openNoFollow(path string) (*os.File, error) {
	return os.Open(path)
}

// checkSocketDirOwnership always refuses. The standard library gives no way
// on Windows to verify a directory's ownership, and binding an inbox without
// that guarantee would let another local user replace the socket and forge
// receipts, so Listen refuses outright rather than skipping the check.
// Sending is unaffected: only Listen calls this.
func checkSocketDirOwnership(dir string, fi os.FileInfo) error {
	return fmt.Errorf("ccsock: binding an inbox is not supported on Windows: %s's ownership cannot be verified; sending is unaffected", dir)
}

// withTightUmask just calls fn: Windows has no umask.
func withTightUmask(fn func() error) error {
	return fn()
}
