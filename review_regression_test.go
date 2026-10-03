//go:build unix

package pluginhost_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pluginhost "github.com/hollis-labs/plugin-host"
	"github.com/hollis-labs/plugin-host/pluginhosttest"
)

func lifecycleOptions(t *testing.T) pluginhost.LifecycleOptions {
	t.Helper()
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	spec.ExpectedVersion = "1.0.0"
	epoch, err := pluginhost.NewHostInstance()
	if err != nil {
		t.Fatal(err)
	}
	return pluginhost.LifecycleOptions{HostInstance: epoch, Generations: &pluginhost.MemoryGenerationStore{}, CleanupTimeout: 30 * time.Millisecond, Callbacks: pluginhost.LifecycleCallbacks{Plan: func(context.Context) (pluginhost.Plan, error) { return pluginhost.Plan{Spec: spec}, nil }}}
}
func newController(t *testing.T, o pluginhost.LifecycleOptions) *pluginhost.Lifecycle {
	t.Helper()
	l, err := pluginhost.NewLifecycle("fixture", o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Disable(context.Background()) })
	return l
}
func enableController(t *testing.T, l *pluginhost.Lifecycle) {
	t.Helper()
	if err := l.Enable(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func awaitResult(t *testing.T, c <-chan error) error {
	t.Helper()
	select {
	case err := <-c:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("operation did not finish")
		return nil
	}
}

func TestUncooperativeCleanupContinuesAndRequiresExactAcknowledgement(t *testing.T) {
	o := lifecycleOptions(t)
	release := make(chan struct{})
	var disposed atomic.Int32
	o.StateStore = &pluginhost.MemoryLifecycleStateStore{}
	o.Callbacks.Revoke = func(context.Context, pluginhost.Owner) error { <-release; return nil }
	o.Callbacks.Dispose = func(context.Context, pluginhost.Owner) error { disposed.Add(1); return nil }
	l := newController(t, o)
	enableController(t, l)
	old := l.Current()
	owner := l.Status().Owner
	done := make(chan error, 1)
	go func() { done <- l.Disable(context.Background()) }()
	err := awaitResult(t, done)
	var report pluginhost.DisposalReport
	if !errors.As(err, &report) || !report.Incomplete || disposed.Load() != 1 {
		t.Fatal("incomplete cleanup did not continue", err)
	}
	select {
	case <-old.Exited():
	default:
		t.Fatal("child survived cleanup timeout")
	}
	if !errors.Is(l.AcknowledgeDisposal(context.Background(), owner), pluginhost.ErrCleanupPending) {
		t.Fatal("acknowledged live isolated code")
	}
	if !errors.Is(l.AcknowledgeDisposal(context.Background(), pluginhost.Owner{}), pluginhost.ErrUnknownDisposal) {
		t.Fatal("wrong report acknowledged")
	}
	o.Callbacks.Revoke = nil
	replacement := newController(t, o)
	if !errors.Is(replacement.Enable(context.Background()), pluginhost.ErrQuarantined) {
		t.Fatal("recreation bypassed quarantine")
	}
	close(release)
	eventually(t, time.Second, "isolated callback completion", func() bool { return replacement.AcknowledgeDisposal(context.Background(), owner) == nil })
	enableController(t, replacement)
	if replacement.Status().Owner.OwnerGeneration <= owner.OwnerGeneration {
		t.Fatal("acknowledgement reused old incarnation")
	}
}

func TestDisableEpochCannotBeReopenedByQueuedEnable(t *testing.T) {
	o := lifecycleOptions(t)
	base := o.Callbacks.Plan
	entered := make(chan struct{})
	release := make(chan struct{})
	var slow atomic.Bool
	o.Callbacks.Plan = func(ctx context.Context) (pluginhost.Plan, error) {
		if slow.Load() {
			close(entered)
			<-release
		}
		return base(ctx)
	}
	l := newController(t, o)
	enableController(t, l)
	owner := l.Status().Owner
	slow.Store(true)
	reload := make(chan error, 1)
	go func() { reload <- l.Reload(context.Background()) }()
	<-entered
	slow.Store(false)
	disabled := make(chan error, 1)
	go func() { disabled <- l.Disable(context.Background()) }()
	eventually(t, time.Second, "disable accepted", func() bool { return !l.Status().DesiredEnabled })
	enabled := make(chan error, 1)
	go func() { enabled <- l.Enable(context.Background()) }()
	// A timed-out queued enable must also leave admission/intent closed.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if !errors.Is(l.Enable(ctx), context.DeadlineExceeded) {
		t.Fatal("queued enable ignored its context")
	}
	if l.IsCurrent(owner) || l.Current() != nil || l.Status().DesiredEnabled {
		t.Fatal("queued enable reopened a fenced generation")
	}
	close(release)
	if err := awaitResult(t, reload); err == nil {
		t.Fatal("disable did not cancel reload")
	}
	if err := awaitResult(t, disabled); err != nil {
		t.Fatal(err)
	}
	if err := awaitResult(t, enabled); err != nil {
		t.Fatal(err)
	}
	if !l.Status().DesiredEnabled || l.Current() == nil || l.IsCurrent(owner) || l.Status().Owner.OwnerGeneration <= owner.OwnerGeneration {
		t.Fatal("enable did not start a fresh generation after disable")
	}
}

type sequenceStore struct {
	mu     sync.Mutex
	values []uint64
}

func (s *sequenceStore) Next(context.Context, string, string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.values[0]
	if len(s.values) > 1 {
		s.values = s.values[1:]
	}
	return n, nil
}
func TestGenerationStoreCannotReuseDecreaseOrOverflow(t *testing.T) {
	for _, values := range [][]uint64{{7, 7}, {7, 6}, {pluginhost.MaxOwnerGeneration + 1}} {
		o := lifecycleOptions(t)
		o.Generations = &sequenceStore{values: values}
		l := newController(t, o)
		if len(values) == 1 {
			if !errors.Is(l.Enable(context.Background()), pluginhost.ErrInvalidGeneration) {
				t.Fatal("accepted overflow")
			}
			continue
		}
		enableController(t, l)
		old := l.Status().Owner
		if !errors.Is(l.Reload(context.Background()), pluginhost.ErrInvalidGeneration) || l.IsCurrent(old) || l.Current() != nil {
			t.Fatal("accepted reused or decreasing generation")
		}
		recreated := newController(t, o)
		if !errors.Is(recreated.Enable(context.Background()), pluginhost.ErrInvalidGeneration) {
			t.Fatal("recreation reset validation watermark")
		}
	}
}
func TestDisposalHistoryAndOriginAreIndependent(t *testing.T) {
	o := lifecycleOptions(t)
	base := o.Callbacks.Plan
	var initial atomic.Bool
	initial.Store(true)
	o.Retry = pluginhost.RetryPolicy{MaxAttempts: 2, Backoff: time.Millisecond}
	o.Callbacks.Plan = func(ctx context.Context) (pluginhost.Plan, error) {
		if initial.Swap(false) {
			return pluginhost.Plan{}, &pluginhost.TransientError{Code: "fetch"}
		}
		return base(ctx)
	}
	l := newController(t, o)
	enableController(t, l)
	if l.Status().OriginFailure != nil {
		t.Fatal("successful load retained originating failure")
	}
	var fail atomic.Bool
	_ = l.Disable(context.Background())
	o = lifecycleOptions(t)
	base = o.Callbacks.Plan
	o.Callbacks.Plan = func(ctx context.Context) (pluginhost.Plan, error) {
		p, e := base(ctx)
		if fail.Load() {
			p.Spec.Command, p.Spec.Env = pluginhosttest.FixtureCommand(pluginhosttest.BehaviourLoadError, p.Spec.Init.DataDir)
		}
		return p, e
	}
	l = newController(t, o)
	enableController(t, l)
	old := l.Status().Owner
	fail.Store(true)
	if err := l.Reload(context.Background()); err == nil {
		t.Fatal("candidate unexpectedly loaded")
	}
	reports := l.Status().Disposals
	if len(reports) != 2 || reports[0].Owner != old || reports[1].Owner.OwnerGeneration <= old.OwnerGeneration {
		t.Fatal("old disposal report was overwritten", reports)
	}
	reports[0].Incomplete = true
	if l.Status().Disposals[0].Incomplete {
		t.Fatal("history snapshot aliases controller state")
	}
}
func TestProcessFailureDiagnosticsPreserveCausesAndRedact(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	spec.Command = "/missing/fixture-binary"
	_, err := pluginhost.Start(context.Background(), spec)
	if err == nil || !strings.Contains(err.Error(), "no such file") || strings.Contains(err.Error(), "generation 0") {
		t.Fatal("spawn diagnostics lost", err)
	}
	cause := errors.New("sensitive-placeholder failed validation")
	spec.BeforeSpawn = func(context.Context) error { return cause }
	spec.Secrets = []string{"sensitive-placeholder"}
	_, err = pluginhost.Start(context.Background(), spec)
	if !errors.Is(err, cause) || strings.Contains(err.Error(), "sensitive-placeholder") || !strings.Contains(err.Error(), "failed validation") {
		t.Fatal("cause/redaction failed", err)
	}
	bad, _ := fixtureSpec(t, pluginhosttest.BehaviourBadProtocol)
	_, err = pluginhost.Start(context.Background(), bad)
	if !errors.Is(err, pluginhost.ErrProtocolMismatch) || !strings.Contains(err.Error(), "speaks 2") {
		t.Fatal("protocol diagnostic lost", err)
	}
}
func TestSupervisorClassifierPanicAndHealthKillOrigin(t *testing.T) {
	ev := newEvents()
	o := ev.options(fastPolicy(2))
	o.ClassifyExit = func(pluginhost.ExitInfo) error { panic("synthetic classifier panic") }
	sup, _ := startSupervised(t, pluginhosttest.BehaviourEcho, o)
	_ = sup.Current().Kill()
	if err := awaitResult(t, ev.giveUp); !errors.Is(err, pluginhost.ErrCallbackPanic) {
		t.Fatal("classifier panic escaped or lost cause", err)
	}
	ev = newEvents()
	o = ev.options(fastPolicy(1))
	o.HealthInterval = 5 * time.Millisecond
	o.HealthTimeout = 100 * time.Millisecond
	o.KillAfterUnhealthy = 1
	seen := make(chan pluginhost.ExitInfo, 1)
	o.ClassifyExit = func(info pluginhost.ExitInfo) error { seen <- info; return errors.New("health is terminal") }
	sup, _ = startSupervised(t, pluginhosttest.BehaviourEcho, o)
	_, _ = sup.Current().Client().Conn().Call(context.Background(), "mcp/call_tool", map[string]any{"tool_name": "set_health", "arguments": map[string]any{"ok": false}})
	select {
	case info := <-seen:
		if !info.SupervisorInitiatedKill {
			t.Fatal("health kill origin missing")
		}
	case <-time.After(time.Second):
		t.Fatal("health classifier not called")
	}
}

func TestSupervisorPermanentReplacementFailureIsNotRetried(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	refusal := errors.New("permanent replacement refusal")
	var checks atomic.Int32
	spec.BeforeSpawn = func(context.Context) error {
		if checks.Add(1) > 1 {
			return refusal
		}
		return nil
	}
	ev := newEvents()
	sup := pluginhost.Supervise(spec, ev.options(fastPolicy(3)))
	if err := sup.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background()) })
	_ = sup.Current().Kill()
	if err := awaitResult(t, ev.giveUp); !errors.Is(err, refusal) {
		t.Fatal("replacement refusal lost", err)
	}
	if checks.Load() != 2 || sup.Restarts() != 1 {
		t.Fatal("permanent failure retried", checks.Load(), sup.Restarts())
	}
}

type refusingStateStore struct {
	pluginhost.MemoryLifecycleStateStore
	refuse atomic.Bool
}

func (s *refusingStateStore) Save(ctx context.Context, host, id string, r pluginhost.LifecycleRecord) error {
	if s.refuse.Load() {
		return errors.New("state persistence refused")
	}
	return s.MemoryLifecycleStateStore.Save(ctx, host, id, r)
}
func TestPersistedQuarantineRequiresSuccessfulAcknowledgement(t *testing.T) {
	o := lifecycleOptions(t)
	owner := pluginhost.Owner{HostInstance: o.HostInstance, OwnerID: "fixture", OwnerGeneration: 100}
	store := &refusingStateStore{}
	record := pluginhost.LifecycleRecord{Revision: 1, LastGeneration: 100, Disposals: []pluginhost.DisposalReport{{Owner: owner, Incomplete: true}}}
	if err := store.Save(context.Background(), o.HostInstance, "fixture", record); err != nil {
		t.Fatal(err)
	}
	o.StateStore = store
	o.Generations = &sequenceStore{values: []uint64{101}}
	l := newController(t, o)
	if !errors.Is(l.Enable(context.Background()), pluginhost.ErrQuarantined) {
		t.Fatal("persisted quarantine ignored")
	}
	store.refuse.Store(true)
	if l.AcknowledgeDisposal(context.Background(), owner) == nil {
		t.Fatal("failed persistence acknowledged")
	}
	if l.Status().Disposals[0].Acknowledged || !errors.Is(l.Enable(context.Background()), pluginhost.ErrQuarantined) {
		t.Fatal("persistence failure cleared quarantine")
	}
	store.refuse.Store(false)
	if err := l.AcknowledgeDisposal(context.Background(), owner); err != nil {
		t.Fatal(err)
	}
	saved, err := store.Load(context.Background(), o.HostInstance, "fixture")
	if err != nil || !saved.Disposals[0].Acknowledged || !saved.Disposals[0].Incomplete {
		t.Fatal("acknowledgement lost historical report", saved, err)
	}
	enableController(t, l)
	if l.Status().Owner.OwnerGeneration != 101 {
		t.Fatal("lost persistent high watermark")
	}
}
