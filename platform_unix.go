//go:build unix

package ccsock

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
)

// processAlive reports whether a PID exists. EPERM means the process is
// there but owned by someone else, which still makes it alive.
func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	if err == nil {
		return true
	}
	return errors.Is(err, syscall.EPERM)
}

// openNoFollow opens path without following a symlink, so a path swapped for
// a symlink between the caller's check and its read cannot be followed.
func openNoFollow(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
}

// checkSocketDirOwnership verifies dir is owned by us and not group or world
// accessible, so ensureSocketDir can refuse to bind into a directory someone
// else can write.
func checkSocketDirOwnership(dir string, fi os.FileInfo) error {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("ccsock: %s: cannot verify ownership on this platform", dir)
	}
	if st.Uid != uint32(os.Getuid()) {
		return fmt.Errorf("ccsock: %s is owned by uid %d, not us: wrong owner", dir, st.Uid)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("ccsock: %s has mode %s: group or world accessible", dir, fi.Mode().Perm())
	}
	return nil
}

// umaskMu guards the umask changes withTightUmask makes. Umask is
// process-global, so concurrent binds elsewhere in the process must not stomp
// on each other's setting.
var umaskMu sync.Mutex

// withTightUmask tightens the umask for the duration of fn, then restores it,
// so a file fn creates cannot exist in a world-writable state even for an
// instant.
func withTightUmask(fn func() error) error {
	umaskMu.Lock()
	defer umaskMu.Unlock()
	old := syscall.Umask(0o177)
	defer syscall.Umask(old)
	return fn()
}
