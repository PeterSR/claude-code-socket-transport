//go:build linux

package ccsock

import (
	"fmt"
	"os"
	"strings"
)

// processStartToken returns the kernel start token Claude Code records as
// procStart. On Linux it is field 22 of /proc/<pid>/stat, the process start
// time in clock ticks since boot, which distinguishes a live process from a
// recycled PID.
func processStartToken(pid int) (string, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", err
	}
	// The comm field is parenthesized and may itself contain spaces, so fields
	// are counted from after its closing paren. State is the first field there,
	// making starttime the 20th.
	s := string(data)
	commEnd := strings.LastIndex(s, ")")
	if commEnd < 0 || commEnd+2 > len(s) {
		return "", fmt.Errorf("unparseable /proc/%d/stat", pid)
	}
	fields := strings.Split(s[commEnd+2:], " ")
	if len(fields) < 20 {
		return "", fmt.Errorf("/proc/%d/stat has %d fields after comm, want at least 20", pid, len(fields))
	}
	return fields[19], nil
}
