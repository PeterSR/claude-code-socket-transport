//go:build !linux && !darwin && !windows

package ccsock

import (
	"fmt"
	"runtime"
)

// processStartToken has no implementation on the remaining Unixes -- the BSDs,
// Solaris, and anything else that builds with the unix tag but is neither Linux
// nor macOS.
//
// The reason is the same one procstart_windows.go gives, and it is worth
// repeating because the temptation is stronger here: these platforms do have a
// readable start time, so a token could be produced. It would be the wrong
// token. procStart is only ever compared against the string Claude Code itself
// wrote into its registry, and Claude Code does not run on these platforms, so
// there is no string to match and no way to learn its format. A locally
// invented token would differ from anything real and would be scored as a dead
// process by rankKeyOwner, which is worse than admitting ignorance: an error
// scores 1, alive but unverifiable, while a mismatch scores 0.
//
// This file exists so the package COMPILES everywhere, which matters to
// dependents that cross-compile a release. Nothing here can be reached at
// runtime on a machine that is also running Claude Code.
func processStartToken(pid int) (string, error) {
	return "", fmt.Errorf("ccsock: process start token is not available on %s", runtime.GOOS)
}
