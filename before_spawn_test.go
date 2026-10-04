//go:build unix

package pluginhost_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	pluginhost "github.com/hollis-labs/plugin-host"
	"github.com/hollis-labs/plugin-host/pluginhosttest"
)

func TestBeforeSpawnRefusalLeavesNoChild(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	refusal := errors.New("bundle changed")
	var checks atomic.Int32
	spec.BeforeSpawn = func(context.Context) error { checks.Add(1); return refusal }
	process, err := pluginhost.Start(context.Background(), spec)
	if process != nil || !errors.Is(err, refusal) || checks.Load() != 1 {
		t.Fatalf("process=%v err=%v checks=%d", process, err, checks.Load())
	}
}

func TestBeforeSpawnRunsOnSupervisedRestarts(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	refusal := errors.New("approved bundle changed")
	var checks atomic.Int32
	var reject atomic.Bool
	spec.BeforeSpawn = func(context.Context) error {
		checks.Add(1)
		if reject.Load() {
			return refusal
		}
		return nil
	}
	events := newEvents()
	supervisor := pluginhost.Supervise(spec, freshInitOptions(spec, events.options(fastPolicy(2))))
	if err := supervisor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = supervisor.Stop(context.Background()) })
	reject.Store(true)
	if err := supervisor.Current().Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-events.giveUp:
		if !errors.Is(err, refusal) {
			t.Fatalf("restart refusal: %v", err)
		}
	case <-time.After(supervisorWait):
		t.Fatal("restart checks never refused")
	}
	if checks.Load() < 2 || events.starts.Load() != 1 {
		t.Fatalf("checks=%d starts=%d", checks.Load(), events.starts.Load())
	}
}

func TestStopCancelsBeforeSpawnValidation(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	entered := make(chan struct{})
	spec.BeforeSpawn = func(ctx context.Context) error { close(entered); <-ctx.Done(); return ctx.Err() }
	supervisor := pluginhost.Supervise(spec, pluginhost.SuperviseOptions{})
	started := make(chan error, 1)
	go func() { started <- supervisor.Start(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(supervisorWait):
		t.Fatal("validation never began")
	}
	stopped := make(chan error, 1)
	go func() { stopped <- supervisor.Stop(context.Background()) }()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(supervisorWait):
		t.Fatal("stop did not cancel validation")
	}
	select {
	case err := <-started:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("start: %v", err)
		}
	case <-time.After(supervisorWait):
		t.Fatal("start stuck")
	}
	if supervisor.Current() != nil {
		t.Fatal("canceled validation installed a child")
	}
}
