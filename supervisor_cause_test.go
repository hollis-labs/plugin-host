package pluginhost

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSupervisorLateProbeCannotReplaceFirstHealthKillCause(t *testing.T) {
	peer := newPeer(t)
	process := &Process{spec: Spec{ReapTimeout: time.Second}, tail: &Tail{}, conn: peer.conn, client: NewClient(peer.conn), exited: make(chan struct{})}
	supervisor := Supervise(Spec{}, SuperviseOptions{HealthInterval: time.Millisecond, HealthTimeout: time.Second, KillAfterUnhealthy: 1})
	defer supervisor.cancel()
	// Recreate the post-kill state before the exit notification is observed:
	// a local deadline caused the kill, while a ready tick can still start a
	// probe. No OS child is needed; Exited closes before Kill is called again.
	supervisor.healthKilled = process
	first := errors.Join(ErrUnhealthy, context.DeadlineExceeded)
	supervisor.healthFailure = first
	watched := make(chan bool, 1)
	go func() { watched <- supervisor.watch(process) }()
	peer.request() // The late probe is in flight before exit becomes ready.
	close(process.exited)
	peer.conn.fail(ErrGone)
	select {
	case unexpected := <-watched:
		if !unexpected {
			t.Fatal("unexpected exit reported as host stop")
		}
	case <-time.After(testWait):
		t.Fatal("watch did not observe exit")
	}
	if supervisor.healthKilled != process || !errors.Is(supervisor.healthFailure, ErrUnhealthy) || !errors.Is(supervisor.healthFailure, context.DeadlineExceeded) || errors.Is(supervisor.healthFailure, ErrGone) {
		t.Fatal("late probe replaced original health kill cause", supervisor.healthFailure)
	}
}
