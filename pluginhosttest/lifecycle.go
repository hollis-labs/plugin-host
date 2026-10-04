package pluginhosttest

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pluginhost "github.com/hollis-labs/plugin-host"
)

// LifecycleDriver is the separate lifecycle contract; process-only hosts can
// keep using Run. Scope callbacks in these cases use a synthetic registry;
// they cannot certify an application's real registration/trust implementation.
type LifecycleDriver interface {
	Enable(context.Context) error
	Disable(context.Context) error
	Reload(context.Context) error
	Status() pluginhost.LifecycleStatus
	Current() *pluginhost.Process
	IsCurrent(pluginhost.Owner) bool
	AcknowledgeDisposal(context.Context, pluginhost.Owner) error
	AcknowledgeReport(context.Context, string) error
}

// LifecycleHarness adapts a host's controller to the normalized library seam.
// Hosts must additionally exercise their actual review, digest and registries.
type LifecycleHarness interface {
	NewLifecycle(string, pluginhost.LifecycleOptions) (LifecycleDriver, error)
	HostAdapterCases(*testing.T, pluginhost.Plan, pluginhost.LifecycleOptions) []LifecycleAdapterCase
}

// LifecycleAdapterCase supplies a host's real review/digest/persistence refusal
// path. The library's own run explicitly supplies SyntheticHostAdapterCases.
type LifecycleAdapterCase struct {
	Name    string
	Plan    pluginhost.Plan
	Options pluginhost.LifecycleOptions
	Refusal error
	Verify  func(*testing.T, LifecycleDriver, error)
}

type lifecycleConfig struct{ waivers map[string]string }

// LifecycleOption configures a host's lifecycle conformance run.
type LifecycleOption func(*lifecycleConfig)

// LifecycleWaive prints a named host exception. A blank reason is invalid.
// The library's own run supplies zero waivers.
func LifecycleWaive(id, reason string) LifecycleOption {
	return func(c *lifecycleConfig) { c.waivers[id] = reason }
}

// LifecycleRunReport records executed and explicitly waived requirements.
type LifecycleRunReport struct{ Executed, Waived []string }

// RunLifecycle exercises R19-R27 with a declared owner for every requirement.
// Host waivers are explicit, reasoned and printed as skipped subtests.
func RunLifecycle(t *testing.T, h LifecycleHarness, opts ...LifecycleOption) LifecycleRunReport {
	var report LifecycleRunReport
	tests := []struct {
		id, name, owner string
		run             func(*lifecycleEnv)
	}{
		{"R19", "stages_and_typed_failures", "lifecycle controller", (*lifecycleEnv).stages},
		{"R20", "version_gates", "normalized compatibility helpers", (*lifecycleEnv).versions},
		{"R21", "classified_finite_retry", "lifecycle retry owner", (*lifecycleEnv).retries},
		{"R22", "partial_scope_cleanup", "controller and host scope adapter", (*lifecycleEnv).partial},
		{"R23", "disable_fences_dispatch", "controller and host dispatch adapter", (*lifecycleEnv).disable},
		{"R24", "reload_generations", "lifecycle controller", (*lifecycleEnv).reload},
		{"R25", "cleanup_continues_and_quarantines", "controller and host cleanup adapter", (*lifecycleEnv).cleanup},
		{"R26", "crash_and_late_start", "lifecycle exit watcher", (*lifecycleEnv).crash},
		{"R27", "host_adapters", "host review/digest/persistence adapters", (*lifecycleEnv).adapters},
	}
	cfg := lifecycleConfig{waivers: make(map[string]string)}
	for _, opt := range opts {
		opt(&cfg)
	}
	for id, reason := range cfg.waivers {
		known := false
		for _, tc := range tests {
			if tc.id == id {
				known = true
			}
		}
		if !known || strings.TrimSpace(reason) == "" {
			t.Fatalf("invalid lifecycle waiver %q: require known ID and nonblank reason", id)
		}
	}
	for _, tc := range tests {
		if _, ok := cfg.waivers[tc.id]; ok {
			report.Waived = append(report.Waived, tc.id)
		} else {
			report.Executed = append(report.Executed, tc.id)
		}
		t.Run(tc.id+"_"+tc.name, func(t *testing.T) {
			t.Logf("OWNER %s: %s", tc.id, tc.owner)
			if reason, ok := cfg.waivers[tc.id]; ok {
				t.Skipf("WAIVED %s (owner %s): %s", tc.id, tc.owner, reason)
			}
			tc.run(&lifecycleEnv{t: t, h: h})
		})
	}
	return report
}

type lifecycleEnv struct {
	t *testing.T
	h LifecycleHarness
}

func (e *lifecycleEnv) plan(behavior string) pluginhost.Plan {
	e.t.Helper()
	dir := e.t.TempDir()
	command, env := FixtureCommand(behavior, dir)
	return pluginhost.Plan{Spec: pluginhost.Spec{ID: "fixture", ExpectedVersion: "1.0.0", Command: command, Env: env, Init: FixtureInit(dir, dir), HandshakeTimeout: 300 * time.Millisecond, UnloadTimeout: 100 * time.Millisecond, ReapTimeout: time.Second}}
}
func (e *lifecycleEnv) options(p pluginhost.Plan) pluginhost.LifecycleOptions {
	epoch, err := pluginhost.NewHostInstance()
	if err != nil {
		e.t.Fatal(err)
	}
	return pluginhost.LifecycleOptions{HostInstance: epoch, Generations: &pluginhost.MemoryGenerationStore{}, CleanupTimeout: 50 * time.Millisecond, Callbacks: pluginhost.LifecycleCallbacks{Plan: func(context.Context) (pluginhost.Plan, error) { return p, nil }}}
}
func (e *lifecycleEnv) new(o pluginhost.LifecycleOptions) LifecycleDriver {
	e.t.Helper()
	l, err := e.h.NewLifecycle("fixture", o)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = l.Disable(context.Background()) })
	return l
}
func (e *lifecycleEnv) enable(l LifecycleDriver) {
	e.t.Helper()
	if err := l.Enable(context.Background()); err != nil {
		e.t.Fatal(err)
	}
	if l.Current() == nil || l.Status().State != pluginhost.StateRunning {
		e.t.Fatal("enabled without a running process")
	}
}
func (e *lifecycleEnv) failed(err error, stage pluginhost.Stage, step string) {
	e.t.Helper()
	var f *pluginhost.Failure
	if !errors.As(err, &f) || f.PluginID != "fixture" || f.Stage != stage || f.Step != step {
		e.t.Fatalf("failure = %v; want fixture %s/%s", err, stage, step)
	}
}
func (e *lifecycleEnv) noChild(p pluginhost.Plan) {
	e.t.Helper()
	if _, err := os.Stat(filepath.Join(p.Spec.Init.DataDir, "pid")); !errors.Is(err, os.ErrNotExist) {
		e.t.Fatalf("refusal spawned a child: %v", err)
	}
}
func (e *lifecycleEnv) gone(p *pluginhost.Process) {
	e.t.Helper()
	select {
	case <-p.Exited():
	case <-time.After(2 * time.Second):
		e.t.Fatal("child was not reaped")
	}
}
func (e *lifecycleEnv) await(what string, fn func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !fn() {
		if time.Now().After(deadline) {
			e.t.Fatal("timed out: " + what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
func (e *lifecycleEnv) stages() {
	e.activationOrder()
	for _, stage := range []pluginhost.Stage{pluginhost.StagePlan, pluginhost.StageResolve, pluginhost.StageCompat} {
		e.t.Run(string(stage), func(t *testing.T) {
			child := &lifecycleEnv{t: t, h: e.h}
			p := child.plan(BehaviourEcho)
			o := child.options(p)
			cause := errors.New("synthetic refusal")
			order := []string{}
			o.Callbacks.Plan = func(context.Context) (pluginhost.Plan, error) {
				order = append(order, "plan")
				if stage == pluginhost.StagePlan {
					return p, cause
				}
				return p, nil
			}
			o.Callbacks.Resolve = func(_ context.Context, p pluginhost.Plan) (pluginhost.Plan, error) {
				order = append(order, "resolve")
				if stage == pluginhost.StageResolve {
					return p, cause
				}
				return p, nil
			}
			o.Callbacks.CheckCompatibility = func(context.Context, pluginhost.Plan) error { order = append(order, "compatibility"); return cause }
			l := child.new(o)
			step := string(stage)
			if stage == pluginhost.StageCompat {
				step = "compatibility"
			}
			err := l.Enable(context.Background())
			child.failed(err, stage, step)
			if !errors.Is(err, cause) {
				t.Fatal("lost cause")
			}
			child.noChild(p)
			want := []string{"plan"}
			if stage != pluginhost.StagePlan {
				want = append(want, "resolve")
			}
			if stage == pluginhost.StageCompat {
				want = append(want, "compatibility")
			}
			if !reflect.DeepEqual(order, want) {
				t.Fatal(order)
			}
		})
	}
	// Identity/version are verified before load, with real child rollback.
	for _, step := range []string{"identity", "version"} {
		p := e.plan(BehaviourEcho)
		if step == "identity" {
			p.Spec.Command, p.Spec.Env = FixtureCommand(BehaviourWrongID, p.Spec.Init.DataDir)
		} else {
			p.Spec.ExpectedVersion = "2.0.0"
		}
		l := e.new(e.options(p))
		e.failed(l.Enable(context.Background()), pluginhost.StageLoad, step)
		if l.Status().Owner.OwnerGeneration == 0 {
			e.t.Fatal("missing generation")
		}
	}
}
func (e *lifecycleEnv) versions() {
	err := pluginhost.CheckVersion(pluginhost.VersionRequirement{Name: "host", Version: "0.5.0", Bounds: pluginhost.VersionBounds{Min: "2.0.0", Max: "1.0.0"}})
	if err == nil || !strings.Contains(err.Error(), "minimum exceeds maximum") {
		e.t.Fatalf("malformed declaration not distinguished from ordinary bound refusal: %v", err)
	}

	// Hosts supply platform baselines independently of declared bounds.
	for _, actual := range []string{"21.9.0", "22.0.0"} {
		p := e.plan(BehaviourEcho)
		p.Versions = []pluginhost.VersionRequirement{
			{Name: "node", Version: actual, Bounds: pluginhost.VersionBounds{Max: "26.0.0"}},
			{Name: "node", Version: actual, Bounds: pluginhost.VersionBounds{Min: "22.0.0"}},
		}
		l := e.new(e.options(p))
		err := l.Enable(context.Background())
		if actual == "21.9.0" {
			e.failed(err, pluginhost.StageCompat, "version")
			e.noChild(p)
		} else if err != nil {
			e.t.Fatal(err)
		}
	}

	tests := []struct {
		version, min, max string
		allow, ok         bool
	}{
		{"0.2.0", "0.2.0", "0.2.0", false, true}, {"0.10.0", "0.2.0", "0.11.0", false, true},
		{"1.0.0-rc.2", "1.0.0-rc.1", "1.0.0", true, true}, {"1.0.0-rc.2", "1.0.0-rc.10", "1.0.0", true, false},
		{"1.0.0-rc.1", "0.1.0", "2.0.0", false, false}, {"dev", "0.1.0", "", false, false},
		{"01.0.0", "0.0.0", "", false, false}, {"1.01.0", "0.0.0", "", false, false}, {"1.0.01", "0.0.0", "", false, false},
		{"1.0.0", "", "", false, false}, {"0.5.0", "2.0.0", "1.0.0", false, false}, {"1.0.0", "bad", "", false, false},
	}
	for _, tc := range tests {
		p := e.plan(BehaviourEcho)
		p.Versions = []pluginhost.VersionRequirement{{Name: "runtime", Version: tc.version, Bounds: pluginhost.VersionBounds{Min: tc.min, Max: tc.max}, AllowPrerelease: tc.allow}}
		l := e.new(e.options(p))
		err := l.Enable(context.Background())
		if tc.ok {
			if err != nil {
				e.t.Fatal(err)
			}
			_ = l.Disable(context.Background())
		} else {
			e.failed(err, pluginhost.StageCompat, "version")
			e.noChild(p)
		}
	}
}
func (e *lifecycleEnv) retries() {
	e.retryGeneration()
	for _, transient := range []bool{false, true} {
		p := e.plan(BehaviourEcho)
		o := e.options(p)
		calls := 0
		cause := errors.New("refusal")
		o.Retry = pluginhost.RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond}
		o.Callbacks.Resolve = func(_ context.Context, p pluginhost.Plan) (pluginhost.Plan, error) {
			calls++
			if transient {
				return p, &pluginhost.TransientError{Code: "fetch", Cause: cause}
			}
			return p, cause
		}
		l := e.new(o)
		err := l.Enable(context.Background())
		if !errors.Is(err, cause) {
			e.t.Fatal("lost retry cause")
		}
		want := 1
		if transient {
			want = 3
		}
		if calls != want {
			e.t.Fatalf("attempts=%d want %d", calls, want)
		}
		if transient && !l.Status().Exhausted {
			e.t.Fatal("exhaustion missing")
		}
		e.noChild(p)
	}
	p := e.plan(BehaviourEcho)
	o := e.options(p)
	entered := make(chan struct{})
	var once sync.Once
	o.Retry = pluginhost.RetryPolicy{MaxAttempts: 3, Backoff: time.Second}
	o.Callbacks.Resolve = func(_ context.Context, p pluginhost.Plan) (pluginhost.Plan, error) {
		once.Do(func() { close(entered) })
		return p, &pluginhost.TransientError{Code: "fetch"}
	}
	l := e.new(o)
	done := make(chan error, 1)
	go func() { done <- l.Enable(context.Background()) }()
	<-entered
	_ = l.Disable(context.Background())
	select {
	case <-done:
	case <-time.After(time.Second):
		e.t.Fatal("disable did not cancel backoff")
	}
	e.noChild(p)
}
func (e *lifecycleEnv) partial() {
	for _, behavior := range []string{BehaviourInitError, BehaviourLoadError, BehaviourEcho} {
		p := e.plan(behavior)
		o := e.options(p)
		registry := &scopeRegistry{}
		var child *pluginhost.Process
		o.Callbacks.PrepareScope = func(_ context.Context, owner pluginhost.Owner, plan pluginhost.Plan) (pluginhost.Spec, error) {
			registry.prepare(owner)
			return plan.Spec, nil
		}
		o.Callbacks.Activate = func(_ context.Context, _ pluginhost.Owner, p *pluginhost.Process) error {
			child = p
			return errors.New("activate refusal")
		}
		o.Callbacks.Revoke = func(_ context.Context, owner pluginhost.Owner) error { registry.revoke(owner); return nil }
		o.Callbacks.Dispose = func(_ context.Context, owner pluginhost.Owner) error { registry.dispose(owner); return nil }
		l := e.new(o)
		if err := l.Enable(context.Background()); err == nil {
			e.t.Fatal("expected failure")
		}
		if !registry.empty() {
			e.t.Fatal("partial-load registrations survived disposal")
		}
		if child != nil {
			e.gone(child)
		}
		if l.Current() != nil {
			e.t.Fatal("partial scope published")
		}
		// Real failed-handshake child must be reaped, not merely hidden.
		if havePIDs {
			pid, err := readPIDFile(filepath.Join(p.Spec.Init.DataDir, "pid"))
			if err != nil {
				e.t.Fatal(err)
			}
			if pidExists(pid) {
				e.t.Fatal("failed child still exists")
			}
		}
	}
}
func (e *lifecycleEnv) disable() {
	p := e.plan(BehaviourEcho)
	o := e.options(p)
	registry := &scopeRegistry{}
	registry.callbacks(&o)
	var revoked, disposed atomic.Int32
	o.Callbacks.Revoke = func(_ context.Context, owner pluginhost.Owner) error {
		registry.revoke(owner)
		revoked.Add(1)
		return nil
	}
	o.Callbacks.Dispose = func(_ context.Context, owner pluginhost.Owner) error {
		registry.dispose(owner)
		disposed.Add(1)
		return nil
	}
	l := e.new(o)
	e.enable(l)
	old := l.Current()
	owner := l.Status().Owner
	captured := e.capturedCallback(l, registry, owner)
	if err := captured(); err != nil {
		e.t.Fatal("active callback refused", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := old.Client().Conn().Call(ctx, "mcp/call_tool", map[string]any{"tool_name": "sleep", "arguments": map[string]any{"ms": 1000}})
		done <- err
	}()
	if err := l.Disable(context.Background()); err != nil {
		e.t.Fatal(err)
	}
	e.gone(old)
	if err := captured(); !errors.Is(err, pluginhost.ErrDisabled) {
		e.t.Fatal("captured callback admitted after revoke")
	}
	if l.IsCurrent(owner) || l.Current() != nil || l.Status().DesiredEnabled {
		e.t.Fatal("stale dispatch admitted")
	}
	_ = l.Disable(context.Background())
	if revoked.Load() != 1 || disposed.Load() != 1 {
		e.t.Fatal("cleanup repeated")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		e.t.Fatal("in-flight call survived")
	}
}
func (e *lifecycleEnv) reload() {
	e.reloadDeadline()
	p := e.plan(BehaviourEcho)
	o := e.options(p)
	registry := &scopeRegistry{}
	registry.callbacks(&o)
	var refuse atomic.Bool
	var loadFail atomic.Bool
	o.Callbacks.Plan = func(context.Context) (pluginhost.Plan, error) {
		if refuse.Load() {
			return p, errors.New("review refusal")
		}
		q := p
		if loadFail.Load() {
			q.Spec.Command, q.Spec.Env = FixtureCommand(BehaviourLoadError, p.Spec.Init.DataDir)
		}
		return q, nil
	}
	l := e.new(o)
	e.enable(l)
	old := l.Current()
	owner := l.Status().Owner
	refuse.Store(true)
	if err := l.Reload(context.Background()); err == nil {
		e.t.Fatal("preflight refusal accepted")
	}
	if l.Current() != old || !l.IsCurrent(owner) {
		e.t.Fatal("preflight killed old generation")
	}
	refuse.Store(false)
	if err := l.Reload(context.Background()); err != nil {
		e.t.Fatal(err)
	}
	e.gone(old)
	if l.Status().Owner.OwnerGeneration <= owner.OwnerGeneration || l.IsCurrent(owner) {
		e.t.Fatal("generation reused")
	}
	second := l.Current()
	loadFail.Store(true)
	if err := l.Reload(context.Background()); err == nil {
		e.t.Fatal("post-stop load succeeded")
	}
	e.gone(second)
	if l.Current() != nil || l.Status().State != pluginhost.StateFailed {
		e.t.Fatal("silently restored old generation")
	}
}

// Both cooperative and still-running preflight timeouts retain serving authority.
func (e *lifecycleEnv) reloadDeadline() {
	for _, cooperative := range []bool{true, false} {
		p := e.plan(BehaviourEcho)
		o := e.options(p)
		o.CallbackTimeout = 30 * time.Millisecond
		var slow atomic.Bool
		release := make(chan struct{})
		o.Callbacks.BeforeReload = func(ctx context.Context) error {
			if cooperative {
				<-ctx.Done()
				return ctx.Err()
			}
			<-release
			return nil
		}
		o.Callbacks.Plan = func(ctx context.Context) (pluginhost.Plan, error) {
			if slow.Load() {
				<-ctx.Done()
				return pluginhost.Plan{}, ctx.Err()
			}
			return p, nil
		}
		l := e.new(o)
		e.enable(l)
		old, owner := l.Current(), l.Status().Owner
		if !errors.Is(l.Reload(context.Background()), context.DeadlineExceeded) {
			e.t.Fatal("reload callback timeout lost")
		}
		if l.Current() != old || !l.IsCurrent(owner) || l.Status().State != pluginhost.StateRunning || l.Status().LastFailure == nil {
			e.t.Fatal("timed-out reload preflight invalidated serving authority")
		}
		if err := l.Enable(context.Background()); err != nil {
			e.t.Fatal("serving Enable lost idempotence", err)
		}
		if !cooperative && !errors.Is(l.Reload(context.Background()), pluginhost.ErrQuarantined) {
			e.t.Fatal("next reload ignored pending callback")
		}
		close(release)
		for _, r := range l.Status().Disposals {
			if r.Incomplete {
				e.await("late preflight reconciliation", func() bool { return l.AcknowledgeReport(context.Background(), r.ID) == nil })
			}
		}
		// Also exercise the Plan timeout, independently of BeforeReload.
		o.Callbacks.BeforeReload = nil
		_ = l.Disable(context.Background())
		// Fresh controller and epoch keep this case independent of its prior report.
		o.HostInstance, _ = pluginhost.NewHostInstance()
		l = e.new(o)
		e.enable(l)
		old, owner = l.Current(), l.Status().Owner
		slow.Store(true)
		if !errors.Is(l.Reload(context.Background()), context.DeadlineExceeded) || l.Current() != old || !l.IsCurrent(owner) || l.Status().State != pluginhost.StateRunning {
			e.t.Fatal("cooperative Plan timeout killed serving generation")
		}
		_ = l.Disable(context.Background())
	}
}

func (e *lifecycleEnv) cleanup() {
	e.uncooperativeCallbacks()
	// Cleanup callbacks may inspect the controller without its mutex held.
	p := e.plan(BehaviourEcho)
	o := e.options(p)
	var inspected LifecycleDriver
	o.Callbacks.Revoke = func(context.Context, pluginhost.Owner) error {
		_ = inspected.Status()
		return nil
	}
	o.Callbacks.Dispose = func(context.Context, pluginhost.Owner) error {
		_ = inspected.Current()
		return nil
	}
	inspected = e.new(o)
	e.enable(inspected)
	if err := inspected.Disable(context.Background()); err != nil {
		e.t.Fatalf("cleanup status inspection blocked: %v", err)
	}

	unsafePlan := e.plan(BehaviourEcho)
	unsafeOptions := e.options(unsafePlan)
	unsafeOptions.Callbacks.Dispose = func(context.Context, pluginhost.Owner) error { return errors.New("unsafe resources") }
	unsafe := e.new(unsafeOptions)
	e.enable(unsafe)
	oldUnsafe := unsafe.Current()
	if !errors.Is(unsafe.Reload(context.Background()), pluginhost.ErrQuarantined) || unsafe.Status().State != pluginhost.StateQuarantined {
		e.t.Fatal("reload quarantine missing")
	}
	e.gone(oldUnsafe)

	for _, behavior := range []string{BehaviourUnloadError, BehaviourWedge} {
		p := e.plan(behavior)
		o := e.options(p)
		cleaned := false
		o.Callbacks.Dispose = func(context.Context, pluginhost.Owner) error { cleaned = true; return nil }
		l := e.new(o)
		e.enable(l)
		old := l.Current()
		err := l.Disable(context.Background())
		var r pluginhost.DisposalReport
		if !errors.As(err, &r) || len(r.Failures) == 0 || r.Failures[0].Step != "unload" || !cleaned {
			e.t.Fatalf("unload report/cleanup = %v %t", err, cleaned)
		}
		e.gone(old)
		if r.Incomplete {
			e.t.Fatal("reaped child with successful host cleanup was marked unsafe")
		}
	}

	for _, mode := range []string{"error", "panic", "timeout"} {
		p := e.plan(BehaviourEcho)
		o := e.options(p)
		log := []string{}
		var logMu sync.Mutex
		addLog := func(step string) { logMu.Lock(); log = append(log, step); logMu.Unlock() }
		o.Callbacks.Revoke = func(ctx context.Context, _ pluginhost.Owner) error {
			addLog("revoke")
			switch mode {
			case "panic":
				panic("synthetic")
			case "timeout":
				<-ctx.Done()
				return ctx.Err()
			default:
				return errors.New("revoke error")
			}
		}
		o.Callbacks.Dispose = func(context.Context, pluginhost.Owner) error {
			addLog("dispose")
			return errors.New("dispose error")
		}
		l := e.new(o)
		e.enable(l)
		old := l.Current()
		err := l.Disable(context.Background())
		var r pluginhost.DisposalReport
		if !errors.As(err, &r) || !r.Incomplete {
			e.t.Fatalf("report=%v", err)
		}
		e.gone(old)
		logMu.Lock()
		snapshot := append([]string(nil), log...)
		logMu.Unlock()
		if !reflect.DeepEqual(snapshot, []string{"revoke", "dispose"}) {
			e.t.Fatal(snapshot)
		}
		if !errors.Is(l.Enable(context.Background()), pluginhost.ErrQuarantined) {
			e.t.Fatal("unsafe replacement allowed")
		}
	}
}
func (e *lifecycleEnv) crash() {
	e.unclassifiedExit()
	// An explicit enable racing the exit watcher must dispose the old scope
	// before installing another process, even when recovery is disabled.
	racePlan := e.plan(BehaviourEcho)
	raceOptions := e.options(racePlan)
	var raceCleaned atomic.Int32
	raceOptions.Callbacks.Dispose = func(context.Context, pluginhost.Owner) error { raceCleaned.Add(1); return nil }
	racing := e.new(raceOptions)
	e.enable(racing)
	crashed := racing.Current()
	_ = crashed.Kill()
	e.enable(racing)
	if raceCleaned.Load() != 1 || racing.Current() == crashed {
		e.t.Fatal("enable retained an exited generation")
	}

	p := e.plan(BehaviourEcho)
	o := e.options(p)
	var cleaned atomic.Int32
	o.Retry = pluginhost.RetryPolicy{MaxAttempts: 2, Backoff: time.Millisecond}
	o.ClassifyExit = func(pluginhost.ExitInfo) error { return &pluginhost.TransientError{Code: "test_exit"} }
	o.Callbacks.Dispose = func(context.Context, pluginhost.Owner) error { cleaned.Add(1); return nil }
	l := e.new(o)
	e.enable(l)
	old := l.Current()
	owner := l.Status().Owner
	_ = old.Kill()
	e.await("crash replacement", func() bool { return l.Current() != nil && l.Current() != old })
	if cleaned.Load() != 1 || l.IsCurrent(owner) || l.Status().Owner.OwnerGeneration <= owner.OwnerGeneration {
		e.t.Fatal("restart reused old scope")
	}
	_ = l.Current().Kill()
	e.await("terminal budget", func() bool { return l.Status().State == pluginhost.StateFailed })
	if !l.Status().Exhausted {
		e.t.Fatal("crash budget reset")
	}
	// Activation deliberately ignores cancellation, then returns late. Disable
	// still wins publication and disposes the real process before returning.
	p = e.plan(BehaviourEcho)
	o = e.options(p)
	entered := make(chan struct{})
	release := make(chan struct{})
	var late *pluginhost.Process
	o.Callbacks.Activate = func(_ context.Context, _ pluginhost.Owner, p *pluginhost.Process) error {
		late = p
		close(entered)
		<-release
		return nil
	}
	l = e.new(o)
	started := make(chan error, 1)
	go func() { started <- l.Enable(context.Background()) }()
	<-entered
	stopped := make(chan error, 1)
	go func() { stopped <- l.Disable(context.Background()) }()
	e.await("disable fence", func() bool { return !l.Status().DesiredEnabled })
	close(release)
	if err := <-started; err == nil {
		e.t.Fatal("enable succeeded after disable won late activation")
	}
	if err := <-stopped; err != nil {
		var report pluginhost.DisposalReport
		if !errors.As(err, &report) || !report.Incomplete {
			e.t.Fatal(err)
		}
	}
	e.gone(late)
	if l.Status().State == pluginhost.StateQuarantined {
		e.await("late activation reconciliation", func() bool { return l.AcknowledgeDisposal(context.Background(), l.Status().Owner) == nil })
	}
	if l.Current() != nil || l.Status().State != pluginhost.StateDisabled {
		e.t.Fatal("late activation resurrected")
	}
}

type refusingStore struct{ err error }

func (s refusingStore) Next(context.Context, string, string) (uint64, error) { return 0, s.err }
func (e *lifecycleEnv) adapters() {
	p := e.plan(BehaviourEcho)
	cases := e.h.HostAdapterCases(e.t, p, e.options(p))
	if len(cases) == 0 {
		e.t.Fatal("host supplied no review/digest/persistence adapter cases")
	}
	for _, tc := range cases {
		e.t.Run(tc.Name, func(t *testing.T) {
			child := &lifecycleEnv{t: t, h: e.h}
			l := child.new(tc.Options)
			err := l.Enable(context.Background())
			if tc.Refusal == nil || !errors.Is(err, tc.Refusal) {
				t.Fatalf("host adapter refusal lost: %v", err)
			}
			child.noChild(tc.Plan)
			if tc.Verify != nil {
				tc.Verify(t, l, err)
			}
		})
	}
	o := e.options(p)
	first := e.new(o)
	e.enable(first)
	generation := first.Status().Owner.OwnerGeneration
	_ = first.Disable(context.Background())
	second := e.new(o)
	e.enable(second)
	if second.Status().Owner.OwnerGeneration <= generation {
		e.t.Fatal("controller recreation reset generation")
	}
}

// SyntheticHostAdapterCases is the library's explicit example adapter. Hosts
// supply their own cases through LifecycleHarness.HostAdapterCases; using this
// helper does not certify their installation/review/persistence implementation.
func SyntheticHostAdapterCases(t *testing.T, p pluginhost.Plan, o pluginhost.LifecycleOptions) []LifecycleAdapterCase {
	t.Helper()
	var cases []LifecycleAdapterCase
	for _, stage := range []string{"review", "digest", "persistence"} {
		options := o
		options.Callbacks = o.Callbacks
		plan := p
		refusal := errors.New("synthetic host refusal")
		switch stage {
		case "review":
			options.Callbacks.Plan = func(context.Context) (pluginhost.Plan, error) { return plan, refusal }
		case "digest":
			plan.Spec.BeforeSpawn = func(context.Context) error { return refusal }
			options.Callbacks.Plan = func(context.Context) (pluginhost.Plan, error) { return plan, nil }
		case "persistence":
			options.Generations = refusingStore{refusal}
		}
		cases = append(cases, LifecycleAdapterCase{Name: stage, Plan: plan, Options: options, Refusal: refusal})
	}
	artifact := filepath.Join(t.TempDir(), "artifact")
	if err := os.WriteFile(artifact, []byte("reviewed"), 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("reviewed"))
	refusal := errors.New("digest changed")
	plan := p
	plan.Spec.BeforeSpawn = func(context.Context) error {
		b, err := os.ReadFile(artifact) //nolint:gosec // G304: artifact is created in the test-owned directory
		if err != nil {
			return err
		}
		if sha256.Sum256(b) != digest {
			return refusal
		}
		return nil
	} //nolint:gosec // synthetic reviewed artifact is under the test's private directory
	options := o
	options.Callbacks = o.Callbacks
	options.Callbacks.Plan = func(context.Context) (pluginhost.Plan, error) { return plan, nil }
	options.Callbacks.PrepareScope = func(_ context.Context, _ pluginhost.Owner, p pluginhost.Plan) (pluginhost.Spec, error) {
		return p.Spec, os.WriteFile(artifact, []byte("changed"), 0o600)
	}
	return append(cases, LifecycleAdapterCase{Name: "changed_reviewed_bytes", Plan: plan, Options: options, Refusal: refusal})
}
