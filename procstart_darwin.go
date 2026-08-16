//go:build darwin

package ccsock

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// processStartToken returns the kernel start token Claude Code records as
// procStart. macOS has no /proc, so Claude Code shells out to ps and stores the
// formatted start date; this reproduces that call, including the locale and
// timezone pinning that makes the string stable.
func processStartToken(pid int) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ps", "-o", "lstart=", "-p", strconv.Itoa(pid))
	cmd.Env = append(cmd.Environ(), "LC_ALL=C", "TZ=UTC")
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(out))
	if token == "" {
		return "", fmt.Errorf("ps reported no start time for pid %d", pid)
	}
	return token, nil
}
