//go:build !unix

package pluginhost

import (
	"errors"
	"os"
	"os/exec"
)

// configureProcessGroup is a no-op where process groups are not available.
// The child is still spawned and still ends on its own stdin EOF; what is
// lost is the ability to reach a grandchild with a signal.
func configureProcessGroup(*exec.Cmd) {}

// killProcessGroup falls back to killing the one process. A plugin runtime
// that forks leaves grandchildren behind on this platform: a limitation of
// the platform, not of the design.
func killProcessGroup(pid int) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	if err := process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return nil
}
