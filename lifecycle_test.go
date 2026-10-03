//go:build unix

package pluginhost_test

import (
	"context"
	"errors"
	pluginhost "github.com/hollis-labs/plugin-host"
	"github.com/hollis-labs/plugin-host/pluginhosttest"
	"sync/atomic"
	"testing"
	"time"
)

func TestLifecycleCallbacksCanReadStatusAndDependenciesRefuseBeforeMutation(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	spec.ExpectedVersion = "1.0.0"
	var l *pluginhost.Lifecycle
	var deny atomic.Bool
	epoch, err := pluginhost.NewHostInstance()
	if err != nil {
		t.Fatal(err)
	}
	l, err = pluginhost.NewLifecycle("fixture", pluginhost.LifecycleOptions{HostInstance: epoch, Generations: &pluginhost.MemoryGenerationStore{}, Callbacks: pluginhost.LifecycleCallbacks{
		Plan: func(context.Context) (pluginhost.Plan, error) {
			_ = l.Status()
			return pluginhost.Plan{Spec: spec}, nil
		},
		Activate: func(context.Context, pluginhost.Owner, *pluginhost.Process) error { _ = l.Status(); return nil },
		Revoke:   func(context.Context, pluginhost.Owner) error { _ = l.Status(); return nil },
		Dispose:  func(context.Context, pluginhost.Owner) error { _ = l.Status(); return nil },
		BeforeDisable: func(context.Context) error {
			if deny.Load() {
				return pluginhost.ErrDependency
			}
			return nil
		},
		BeforeReload: func(context.Context) error {
			if deny.Load() {
				return pluginhost.ErrDependency
			}
			return nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { deny.Store(false); _ = l.Disable(context.Background()) })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = l.Enable(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	old := l.Current()
	owner := l.Status().Owner
	if err = l.Enable(context.Background()); err != nil || l.Current() != old {
		t.Fatal("enable was not idempotent", err)
	}
	deny.Store(true)
	if !errors.Is(l.Disable(context.Background()), pluginhost.ErrDependency) || !errors.Is(l.Reload(context.Background()), pluginhost.ErrDependency) || !l.IsCurrent(owner) {
		t.Fatal("dependency rejection changed live intent")
	}
	deny.Store(false)
	if err = l.Disable(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSupervisorDefaultsToTerminalCrash(t *testing.T) {
	ev := newEvents()
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	sup := pluginhost.Supervise(spec, pluginhost.SuperviseOptions{OnGiveUp: func(err error) { ev.giveUp <- err }})
	if err := sup.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background()) })
	_ = sup.Current().Kill()
	select {
	case <-ev.giveUp:
	case <-time.After(time.Second):
		t.Fatal("default crash did not terminate")
	}
	if sup.Restarts() != 0 || sup.Current() != nil {
		t.Fatal("unclassified crash retried")
	}
}
