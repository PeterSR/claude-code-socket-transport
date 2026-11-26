//go:build !linux

package shim

import "syscall"

// shimSysProcAttr: outside Linux a shim notices its owner is gone when its
// stdin closes.
func shimSysProcAttr() *syscall.SysProcAttr { return nil }
