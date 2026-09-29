//go:build unix

package pluginhost

import (
	"errors"
	"os/exec"
	"syscall"
)

// configureProcessGroup puts a spawned plugin in a fresh process group so
// killProcessGroup can reach anything it forks.
//
// A plugin runtime that spawns helpers (node, python, a shell wrapper) leaves
// grandchildren that a SIGKILL to the child's own pid does not touch, and a
// grandchild holding an inherited pipe open is a process this host would wait
// on and never see exit. Nanite found that first; the answer is copied here.
func configureProcessGroup(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

// killProcessGroup SIGKILLs the group led by pid. The negative pid is what
// makes it the group rather than the one process. A group that is already
// gone is not an error, and neither is EPERM: for a process we spawned it can
// only mean the leader is a zombie that has exited but is not yet reaped,
// which macOS reports as EPERM rather than ESRCH.
func killProcessGroup(pid int) error {
	err := syscall.Kill(-pid, syscall.SIGKILL)
	if err != nil && !errors.Is(err, syscall.ESRCH) && !errors.Is(err, syscall.EPERM) {
		return err
	}
	return nil
}
