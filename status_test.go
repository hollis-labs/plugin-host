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

// A real child completes init/load, then emits stderr and exits on its next
// request. Exit codes and diagnostics therefore come from OS reaping, not mocks.
func statusExitSpec(t *testing.T) pluginhost.Spec {
	t.Helper()
	spec, _ := scriptedInitSpec(t, `{"id":"probe","name":"Probe","version":"1.0.0","description":"","protocol":2,"capability_contract":1}`, false)
	epoch, err := pluginhost.NewHostInstance()
	if err != nil {
		t.Fatal(err)
	}
	spec.Init.Incarnation.HostInstance = epoch
	script := strings.Replace(scriptedInitChild, "while IFS= read -r l; do", "n=0\nwhile IFS= read -r l; do\n  n=$((n+1))\n  if [ \"$n\" -gt 1 ]; then\n    printf '%s\\n' \"secret=$STATUS_SECRET private-marker exit-tail\" >&2\n    exit 23\n  fi", 1)
	spec.Args = []string{"-c", script}
	spec.Env = append(spec.Env, "STATUS_SECRET=credential-value")
	spec.Secrets = []string{"credential-value"}
	spec.StderrBytes = 128
	spec.Redact = func(text string) string {
		return strings.ReplaceAll(text, "private-marker", "[host-redacted]")
	}
	return spec
}

func exitStatusChild(p *pluginhost.Process) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _ = p.Client().Conn().Call(ctx, "test/exit", nil)
}

func assertExitStatus(t *testing.T, exit *pluginhost.ExitStatus, owner pluginhost.Owner) {
	t.Helper()
	if exit == nil || exit.Owner != owner || exit.Info.Code != 23 || exit.Info.Signal != "" || exit.Info.Err == nil {
		t.Fatalf("exit = %+v, want owner %+v, code 23", exit, owner)
	}
	if len(exit.StderrTail) > 128 || !strings.Contains(exit.StderrTail, "exit-tail") || !strings.Contains(exit.StderrTail, "[host-redacted]") || strings.Contains(exit.StderrTail, "credential-value") || strings.Contains(exit.StderrTail, "private-marker") {
		t.Fatalf("unsafe or missing exit diagnostics: %q", exit.StderrTail)
	}
}

func TestSupervisorStatusRestartExhaustion(t *testing.T) {
	spec := statusExitSpec(t)
	sup := pluginhost.Supervise(spec, freshInitOptions(spec, pluginhost.SuperviseOptions{
		Policy:       fastPolicy(1),
		ClassifyExit: func(pluginhost.ExitInfo) error { return &pluginhost.TransientError{Code: "child_exit"} },
	})) // No OnGiveUp: status must still retain the terminal reason.
	if err := sup.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background()) })
	first := sup.Current()
	owner := spec.Init.Incarnation
	owner.HostInstance = spec.Init.DataDir
	owner.OwnerGeneration = 1
	exitStatusChild(first)
	second := awaitNewProcess(t, sup, first)
	s := sup.Status()
	assertExitStatus(t, s.LastExit, pluginhost.Owner{HostInstance: owner.HostInstance, OwnerID: owner.OwnerID, OwnerGeneration: owner.OwnerGeneration})
	if !s.Running || s.Exhausted || s.LastFailure != nil {
		t.Fatalf("replacement status: %+v", s)
	}
	exitStatusChild(second)
	eventually(t, supervisorWait, "exhausted supervisor", func() bool { return sup.Status().Exhausted })
	s = sup.Status()
	owner.OwnerGeneration++
	assertExitStatus(t, s.LastExit, pluginhost.Owner{HostInstance: owner.HostInstance, OwnerID: owner.OwnerID, OwnerGeneration: owner.OwnerGeneration})
	if s.Running || s.Restarts != 1 || s.LastFailure == nil || !strings.Contains(s.String(), "restart attempts exhausted") || !strings.Contains(s.String(), "exit=23") {
		t.Fatalf("exhausted status: %+v", s)
	}
	var transient *pluginhost.TransientError
	if !errors.As(s.LastFailure, &transient) || transient.Code != "child_exit" {
		t.Fatalf("typed terminal cause lost: %v", s.LastFailure)
	}
	s.LastExit.Info.Code = 999
	s.LastFailure.Code = "changed"
	if sup.Status().LastExit.Info.Code != 23 || sup.Status().LastFailure.Code == "changed" {
		t.Fatal("status returned mutable internal state")
	}
}

func TestSupervisorStatusPermanentExitIsNotExhaustion(t *testing.T) {
	spec := statusExitSpec(t)
	sup := pluginhost.Supervise(spec, freshInitOptions(spec, pluginhost.SuperviseOptions{Policy: fastPolicy(1)}))
	if err := sup.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background()) })
	exitStatusChild(sup.Current())
	eventually(t, supervisorWait, "terminal failure", func() bool { return sup.Status().LastFailure != nil })
	if s := sup.Status(); s.Exhausted || s.Running || s.LastExit == nil || s.LastExit.Info.Code != 23 {
		t.Fatalf("permanent exit status: %+v", s)
	}
}

func TestLifecycleStatusRestartExhaustion(t *testing.T) {
	spec := statusExitSpec(t)
	l, err := pluginhost.NewLifecycle("probe", pluginhost.LifecycleOptions{
		HostInstance: spec.Init.Incarnation.HostInstance, Generations: &pluginhost.MemoryGenerationStore{},
		Retry:        pluginhost.RetryPolicy{MaxAttempts: 2, Backoff: time.Millisecond},
		ClassifyExit: func(pluginhost.ExitInfo) error { return &pluginhost.TransientError{Code: "child_exit"} },
		Callbacks:    pluginhost.LifecycleCallbacks{Plan: func(context.Context) (pluginhost.Plan, error) { return pluginhost.Plan{Spec: spec}, nil }},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Disable(context.Background()) })
	if err := l.Enable(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := l.Current()
	owner := l.Status().Owner
	exitStatusChild(first)
	eventually(t, supervisorWait, "lifecycle replacement", func() bool {
		candidate := l.Current()
		return candidate != nil && candidate != first
	})
	s := l.Status()
	assertExitStatus(t, s.LastExit, owner)
	if s.Exhausted || s.State != pluginhost.StateRunning {
		t.Fatalf("replacement status: %+v", s)
	}
	owner = s.Owner
	exitStatusChild(l.Current())
	eventually(t, supervisorWait, "exhausted lifecycle", func() bool { return l.Status().Exhausted })
	s = l.Status()
	assertExitStatus(t, s.LastExit, owner)
	if s.State != pluginhost.StateFailed || s.RetryAttempts != 1 || s.LastFailure == nil || !strings.Contains(s.String(), "restart attempts exhausted") || !strings.Contains(s.String(), "exit=23") {
		t.Fatalf("exhausted status: %+v", s)
	}
	s.LastExit.Info.Code = 999
	if l.Status().LastExit.Info.Code != 23 {
		t.Fatal("status returned mutable internal exit")
	}
	if err := l.Enable(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s := l.Status(); s.Exhausted || s.LastExit != nil {
		t.Fatalf("explicit enable did not clear prior cycle: %+v", s)
	}
}

func TestStatusExitTailBoundsHostRedactorExpansion(t *testing.T) {
	spec := statusExitSpec(t)
	var sup *pluginhost.Supervisor
	spec.Redact = func(text string) string {
		_ = sup.Status() // Host callbacks must run outside the status mutex.
		return strings.Repeat("safe ", 100) + strings.ReplaceAll(text, "private-marker", "[host-redacted]")
	}
	sup = pluginhost.Supervise(spec, freshInitOptions(spec, pluginhost.SuperviseOptions{}))
	if err := sup.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background()) })
	exitStatusChild(sup.Current())
	eventually(t, supervisorWait, "terminal status", func() bool { return sup.Status().LastFailure != nil })
	exit := sup.Status().LastExit
	if exit == nil || len(exit.StderrTail) != 128 || !strings.Contains(exit.StderrTail, "exit-tail") || strings.Contains(exit.StderrTail, "credential-value") {
		t.Fatalf("expanded tail escaped bound/redaction: %+v", exit)
	}
}

func TestSupervisorStatusFailedReplacementKeepsLatestExit(t *testing.T) {
	spec := statusExitSpec(t)
	spec.Args[1] = `if [ -e "$DIR/status-started" ]; then
  printf '%s\n' 'replacement-tail private-marker' >&2
  exit 24
fi
: > "$DIR/status-started"
` + spec.Args[1]
	sup := pluginhost.Supervise(spec, freshInitOptions(spec, pluginhost.SuperviseOptions{
		Policy:       fastPolicy(1),
		ClassifyExit: func(pluginhost.ExitInfo) error { return &pluginhost.TransientError{Code: "child_exit"} },
	}))
	if err := sup.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background()) })
	exitStatusChild(sup.Current())
	eventually(t, supervisorWait, "replacement failure", func() bool { return sup.Status().LastFailure != nil })
	s := sup.Status()
	if s.Exhausted || s.Restarts != 1 || s.LastExit == nil || s.LastExit.Info.Code != 24 || s.LastExit.Owner.OwnerGeneration != 2 || !strings.Contains(s.LastExit.StderrTail, "replacement-tail [host-redacted]") {
		t.Fatalf("latest failed candidate lost: %+v exit=%+v", s, s.LastExit)
	}
}

func TestStatusFailedInitialHandshakeRetainsChildExit(t *testing.T) {
	spec := statusExitSpec(t)
	// Exit on load, after successful init.
	spec.Args[1] = strings.Replace(spec.Args[1], `"$n" -gt 1`, `"$n" -gt 0`, 1)
	t.Run("supervisor", func(t *testing.T) {
		sup := pluginhost.Supervise(spec, pluginhost.SuperviseOptions{})
		if err := sup.Start(context.Background()); err == nil {
			t.Fatal("load unexpectedly succeeded")
		}
		t.Cleanup(func() { _ = sup.Stop(context.Background()) })
		s := sup.Status()
		owner := spec.Init.Incarnation
		assertExitStatus(t, s.LastExit, pluginhost.Owner{HostInstance: owner.HostInstance, OwnerID: owner.OwnerID, OwnerGeneration: owner.OwnerGeneration})
		if s.LastFailure == nil || s.Exhausted || s.Running {
			t.Fatalf("failed start status: %+v", s)
		}
	})
	t.Run("lifecycle", func(t *testing.T) {
		l, err := pluginhost.NewLifecycle("probe", pluginhost.LifecycleOptions{
			HostInstance: spec.Init.Incarnation.HostInstance, Generations: &pluginhost.MemoryGenerationStore{},
			Callbacks: pluginhost.LifecycleCallbacks{Plan: func(context.Context) (pluginhost.Plan, error) { return pluginhost.Plan{Spec: spec}, nil }},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = l.Disable(context.Background()) })
		if err := l.Enable(context.Background()); err == nil {
			t.Fatal("load unexpectedly succeeded")
		}
		s := l.Status()
		assertExitStatus(t, s.LastExit, s.Owner)
		if s.LastFailure == nil || s.Exhausted || s.State != pluginhost.StateFailed {
			t.Fatalf("failed load status: %+v", s)
		}
	})
}
