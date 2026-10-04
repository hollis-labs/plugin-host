//go:build unix

package pluginhost_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	pluginhost "github.com/hollis-labs/plugin-host"
)

func rawHealthSpec(t *testing.T, reply string) pluginhost.Spec {
	t.Helper()
	spec, _ := scriptedInitSpec(t, `{"id":"probe","name":"P","version":"1.0.0","description":"","protocol":2,"capability_contract":1}`, false)
	child := strings.Replace(scriptedInitChild, `  if [ -n "$id" ]; then`, `  case "$l" in
    *plugin/health*) `+reply+`; continue ;;
  esac
  if [ -n "$id" ]; then`, 1)
	spec.Args = []string{"-c", child}
	return spec
}

func TestSupervisorKillsSilentHealthWithinOneSecond(t *testing.T) {
	spec := rawHealthSpec(t, ":")
	exited := make(chan pluginhost.ExitInfo, 1)
	gaveUp := make(chan error, 1)
	options := pluginhost.SuperviseOptions{HealthInterval: 200 * time.Millisecond, HealthTimeout: 300 * time.Millisecond, KillAfterUnhealthy: 2, OnExit: func(info pluginhost.ExitInfo, _ bool) { exited <- info }, OnGiveUp: func(err error) { gaveUp <- err }}
	sup := pluginhost.Supervise(spec, options)
	if err := sup.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer sup.Stop(context.Background())
	start := time.Now()
	select {
	case info := <-exited:
		if !info.SupervisorInitiatedKill {
			t.Fatal("silent plugin exited without health kill")
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("silent plugin survived health threshold")
	}
	t.Logf("silent plugin killed after %s", time.Since(start))
	if err := awaitResult(t, gaveUp); !errors.Is(err, pluginhost.ErrUnhealthy) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("health timeout cause lost", err)
	}
}

func TestSupervisorProtocolHealthFailureIsTerminalAndPreservesCause(t *testing.T) {
	spec := rawHealthSpec(t, `printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32602,"message":"health invalid"}}\n' "$id"`)
	ev := newEvents()
	options := freshInitOptions(spec, ev.options(fastPolicy(2)))
	options.HealthInterval = 20 * time.Millisecond
	options.HealthTimeout = 100 * time.Millisecond
	options.KillAfterUnhealthy = 1
	sup := pluginhost.Supervise(spec, options)
	if err := sup.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer sup.Stop(context.Background())
	err := awaitResult(t, ev.giveUp)
	if !errors.Is(err, pluginhost.ErrProtocolMismatch) || !strings.Contains(err.Error(), "health invalid") {
		t.Fatal("health failure cause lost", err)
	}
	if sup.Restarts() != 0 || ev.starts.Load() != 1 {
		t.Fatal("protocol health failure restarted plugin")
	}
}

func TestSupervisorHostCancelledHealthDoesNotKill(t *testing.T) {
	spec := rawHealthSpec(t, ":")
	ev := newEvents()
	options := ev.options(fastPolicy(1))
	options.HealthInterval = 20 * time.Millisecond
	options.HealthTimeout = time.Second
	options.KillAfterUnhealthy = 1
	sup := pluginhost.Supervise(spec, options)
	if err := sup.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, "health request publication", func() bool {
		for _, method := range recordedInitMethods(spec.Init.DataDir) {
			if method == "plugin/health" {
				return true
			}
		}
		return false
	}) // The raw plugin records the probe but never replies.
	if err := sup.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ev.exits.Load() != 0 || ev.exitInfo.Load() != nil {
		t.Fatal("host stop classified as health failure")
	}
	select {
	case err := <-ev.giveUp:
		t.Fatal("host stop gave up", err)
	default:
	}
}
