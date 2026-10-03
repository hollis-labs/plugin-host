package pluginhost

import (
	"context"
	"errors"
	"testing"
)

func TestRetiredExitWatcherCannotDisposeReplacement(t *testing.T) {
	exited := make(chan struct{})
	close(exited)
	l, err := NewLifecycle("fixture", LifecycleOptions{HostInstance: "watcher-test-epoch", Generations: &MemoryGenerationStore{}, Callbacks: LifecycleCallbacks{Plan: func(context.Context) (Plan, error) { return Plan{}, errors.New("unexpected plan") }}})
	if err != nil {
		t.Fatal(err)
	}
	newer := &incarnation{owner: Owner{HostInstance: "watcher-test-epoch", OwnerID: "fixture", OwnerGeneration: 2}}
	old := &incarnation{owner: Owner{HostInstance: "watcher-test-epoch", OwnerID: "fixture", OwnerGeneration: 1}, process: &Process{exited: exited}, cancel: func() { t.Error("stale watcher canceled old scope again") }}
	l.current = newer
	l.status.State = StateRunning
	l.status.DesiredEnabled = true
	l.watch(context.Background(), old)
	if l.current != newer || l.status.State != StateRunning || len(l.Status().Disposals) != 0 {
		t.Fatal("stale watcher changed replacement")
	}
}
