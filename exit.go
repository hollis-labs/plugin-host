package pluginhost

import (
	"os"
	"syscall"
)

// ExitInfo describes how a plugin process ended.
type ExitInfo struct {
	// Code is the exit status, or -1 when the process was killed by a signal.
	Code int
	// Signal names the terminating signal (for example "killed"); empty when
	// the process exited on its own.
	Signal string
	// Err is what (*exec.Cmd).Wait returned: nil for a clean exit, an
	// *exec.ExitError for a non-zero one, or exec.ErrWaitDelay when a
	// grandchild held a pipe past the reap timeout.
	Err error
}

// classifyExit reads the exit code and signal from a process's terminal
// state. It is adapted from ClassifyExit in github.com/hollis-labs/go-mcp
// (supervise/exit.go), copied rather than imported for the same reason as
// [Tail]. On a platform where the state does not report a syscall.WaitStatus
// the signal name is unavailable and only the exit code is reported.
func classifyExit(state *os.ProcessState, waitErr error) ExitInfo {
	info := ExitInfo{Code: -1, Err: waitErr}
	if state == nil {
		return info
	}
	info.Code = state.ExitCode()
	if status, ok := state.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		info.Signal = status.Signal().String()
	}
	return info
}
