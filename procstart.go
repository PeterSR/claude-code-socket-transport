package ccsock

// ProcessStartToken returns the kernel start token Claude Code records as
// procStart for a process, the value that distinguishes a live process from
// a recycled PID. Its form is platform specific and only meaningful when
// compared against another token from the same platform.
//
// It returns an error on platforms where the token cannot be read.
func ProcessStartToken(pid int) (string, error) {
	return processStartToken(pid)
}
