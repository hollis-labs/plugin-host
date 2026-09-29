//go:build unix

package pluginhosttest

import (
	"errors"
	"syscall"
)

const havePIDs = true

// pidExists reports whether pid names a process, zombies included: a child
// that was killed but never waited for still counts as existing, which is
// how an unreaped child is told from a reaped one.
func pidExists(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func killPID(pid int) { _ = syscall.Kill(pid, syscall.SIGKILL) }
