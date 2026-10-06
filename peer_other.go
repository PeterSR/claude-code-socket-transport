//go:build !darwin && !linux

package ccsock

import (
	"fmt"
	"net"
)

func VerifyPeerPID(conn net.Conn, expected int) error {
	return fmt.Errorf("peer verification requires macOS or Linux")
}
