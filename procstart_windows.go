//go:build windows

package ccsock

import "fmt"

// processStartToken has no Windows implementation. GetProcessTimes could
// produce something, but we do not know what string Claude Code itself
// records as procStart on Windows, so a token built from it would not match
// even a genuinely live process. rankKeyOwner scores a mismatched token 0
// (dead), which is strictly worse than the error path here, which it scores
// 1 (alive but unverifiable): an error is the honest answer, a wrong guess is
// not.
func processStartToken(pid int) (string, error) {
	return "", fmt.Errorf("ccsock: process start token is not available on Windows")
}
