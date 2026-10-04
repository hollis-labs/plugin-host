package pluginhost

import (
	"errors"
	"runtime"
	"strings"
	"testing"
)

func retentionProcess(redact func(string) string) *Process {
	return &Process{done: true, tail: &Tail{Bytes: 64}, spec: Spec{Redact: redact}, exit: ExitInfo{Code: 23}}
}

func TestStatusSnapshotOwnsBoundedRedactorAllocation(t *testing.T) {
	for _, shortView := range []bool{false, true} {
		t.Run(map[bool]string{false: "expanded", true: "short_view"}[shortView], func(t *testing.T) {
			p := retentionProcess(func(string) string {
				expanded := strings.Repeat("X", 16<<20)
				if shortView {
					return expanded[len(expanded)-64:]
				}
				return expanded
			})
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			exit := snapshotExit(p)
			runtime.GC()
			runtime.ReadMemStats(&after)
			if len(exit.StderrTail) != 64 || after.HeapAlloc > before.HeapAlloc+(2<<20) {
				t.Errorf("visible=%d retained heap delta=%d", len(exit.StderrTail), int64(after.HeapAlloc)-int64(before.HeapAlloc))
			}
			runtime.KeepAlive(exit)
		})
	}
}

func TestSupervisorStatusTerminalOwnsBoundedDiagnostics(t *testing.T) {
	for _, shortView := range []bool{false, true} {
		t.Run(map[bool]string{false: "expanded", true: "short_view"}[shortView], func(t *testing.T) {
			p := retentionProcess(func(string) string {
				expanded := strings.Repeat("X", 16<<20)
				if shortView {
					return expanded[len(expanded)-64:]
				}
				return expanded
			})
			sup := Supervise(p.spec, SuperviseOptions{}) // Retention matters without a callback.
			defer sup.cancel()
			sentinel := errors.New("classified exit")
			cause := &TransientError{Code: "child_exit", Cause: sentinel}
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			sup.giveUp(p, 2, cause, true)
			status := sup.Status()
			runtime.GC()
			runtime.ReadMemStats(&after)
			var typed *TransientError
			if !errors.Is(status.LastFailure, sentinel) || !errors.As(status.LastFailure, &typed) || typed != cause {
				t.Fatal("classified typed cause lost")
			}
			if len(status.LastFailure.Cause.Error()) > 4096 || after.HeapAlloc > before.HeapAlloc+(2<<20) {
				t.Errorf("terminal cause bytes=%d retained heap delta=%d", len(status.LastFailure.Cause.Error()), int64(after.HeapAlloc)-int64(before.HeapAlloc))
			}
			runtime.KeepAlive(status)
			runtime.KeepAlive(sup)
		})
	}
}
