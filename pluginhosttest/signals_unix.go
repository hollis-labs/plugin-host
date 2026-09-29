//go:build unix

package pluginhosttest

import (
	"os/signal"
	"syscall"
)

// ignoreSignals makes the process survive the polite signals, as a wedged
// plugin does.
func ignoreSignals() { signal.Ignore(syscall.SIGTERM, syscall.SIGINT) }
