package pluginhosttest

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pluginhost "github.com/hollis-labs/plugin-host"
	"github.com/hollis-labs/plugin-sdk/subprocess"
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
}

// LifecycleHarness adapts a host's controller to the normalized library seam.
// Hosts must additionally exercise their actual review, digest and registries.
type LifecycleHarness interface {
	NewLifecycle(string, pluginhost.LifecycleOptions) (LifecycleDriver, error)
}

// RunLifecycle exercises R19-R27 separately, using real re-executed children.
// There are no lifecycle waivers: every listed behavior is library-owned.
func RunLifecycle(t *testing.T, h LifecycleHarness) {
	tests := []struct {
		id  string
		run func(*lifecycleEnv)
	}{
		{"R19_stages_and_typed_failures", (*lifecycleEnv).stages},
		{"R20_version_gates", (*lifecycleEnv).versions},
		{"R21_classified_finite_retry", (*lifecycleEnv).retries},
		{"R22_partial_scope_cleanup", (*lifecycleEnv).partial},
		{"R23_disable_fences_dispatch", (*lifecycleEnv).disable},
		{"R24_reload_generations", (*lifecycleEnv).reload},
		{"R25_cleanup_continues_and_quarantines", (*lifecycleEnv).cleanup},
		{"R26_crash_and_late_start", (*lifecycleEnv).crash},
		{"R27_host_adapters", (*lifecycleEnv).adapters},
	}
	for _, tc := range tests {
		t.Run(tc.id, func(t *testing.T) { tc.run(&lifecycleEnv{t: t, h: h}) })
	}
}

type lifecycleEnv struct {
	t *testing.T
	h LifecycleHarness
}

func (e *lifecycleEnv) plan(behavior string) pluginhost.Plan {
	e.t.Helper()
	dir := e.t.TempDir()
	command, env := FixtureCommand(behavior, dir)
	return pluginhost.Plan{Spec: pluginhost.Spec{ID: "fixture", ExpectedVersion: "1.0.0", Command: command, Env: env, Init: subprocess.InitParams{DataDir: dir}, HandshakeTimeout: 300 * time.Millisecond, UnloadTimeout: 100 * time.Millisecond, ReapTimeout: time.Second}}
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
		{"1.0.0", "", "", false, false}, {"1.0.0", "2.0.0", "0.1.0", false, false}, {"1.0.0", "bad", "", false, false},
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
		order := []string{}
		var child *pluginhost.Process
		o.Callbacks.PrepareScope = func(_ context.Context, _ pluginhost.Owner, plan pluginhost.Plan) (pluginhost.Spec, error) {
			order = append(order, "prepare")
			return plan.Spec, nil
		}
		o.Callbacks.Activate = func(_ context.Context, _ pluginhost.Owner, p *pluginhost.Process) error {
			child = p
			return errors.New("activate refusal")
		}
		o.Callbacks.Revoke = func(context.Context, pluginhost.Owner) error { order = append(order, "revoke"); return nil }
		o.Callbacks.Dispose = func(context.Context, pluginhost.Owner) error { order = append(order, "dispose"); return nil }
		l := e.new(o)
		if err := l.Enable(context.Background()); err == nil {
			e.t.Fatal("expected failure")
		}
		if !reflect.DeepEqual(order, []string{"prepare", "revoke", "dispose"}) {
			e.t.Fatal(order)
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
	var revoked, disposed atomic.Int32
	o.Callbacks.Revoke = func(context.Context, pluginhost.Owner) error { revoked.Add(1); return nil }
	o.Callbacks.Dispose = func(context.Context, pluginhost.Owner) error { disposed.Add(1); return nil }
	l := e.new(o)
	e.enable(l)
	old := l.Current()
	owner := l.Status().Owner
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
	p := e.plan(BehaviourEcho)
	o := e.options(p)
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
func (e *lifecycleEnv) cleanup() {
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
		o.Callbacks.Revoke = func(ctx context.Context, _ pluginhost.Owner) error {
			log = append(log, "revoke")
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
			log = append(log, "dispose")
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
		if !reflect.DeepEqual(log, []string{"revoke", "dispose"}) {
			e.t.Fatal(log)
		}
		if !errors.Is(l.Enable(context.Background()), pluginhost.ErrQuarantined) {
			e.t.Fatal("unsafe replacement allowed")
		}
	}
}
func (e *lifecycleEnv) crash() {
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
	<-started
	<-stopped
	e.gone(late)
	if l.Current() != nil || l.Status().State != pluginhost.StateDisabled {
		e.t.Fatal("late activation resurrected")
	}
}

type refusingStore struct{ err error }

func (s refusingStore) Next(context.Context, string, string) (uint64, error) { return 0, s.err }
func (e *lifecycleEnv) adapters() {
	// A synthetic host pins reviewed bytes and rechecks at the execution
	// boundary. Mutating them after resolution must never launch a child.
	p0 := e.plan(BehaviourEcho)
	o0 := e.options(p0)
	artifact := filepath.Join(e.t.TempDir(), "artifact")
	if err := os.WriteFile(artifact, []byte("reviewed"), 0o600); err != nil {
		e.t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("reviewed"))
	refusal := errors.New("digest changed")
	p0.Spec.BeforeSpawn = func(context.Context) error {
		b, err := os.ReadFile(artifact) //nolint:gosec // synthetic reviewed artifact under the test temp directory
		if err != nil {
			return err
		}
		if sha256.Sum256(b) != digest {
			return refusal
		}
		return nil
	}
	o0.Callbacks.Plan = func(context.Context) (pluginhost.Plan, error) { return p0, nil }
	o0.Callbacks.PrepareScope = func(_ context.Context, _ pluginhost.Owner, plan pluginhost.Plan) (pluginhost.Spec, error) {
		return plan.Spec, os.WriteFile(artifact, []byte("changed"), 0o600)
	}
	l0 := e.new(o0)
	if err := l0.Enable(context.Background()); !errors.Is(err, refusal) {
		e.t.Fatal(err)
	}
	e.noChild(p0)

	for _, stage := range []string{"review", "digest", "persist"} {
		p := e.plan(BehaviourEcho)
		o := e.options(p)
		cause := errors.New("host refusal")
		switch stage {
		case "review":
			o.Callbacks.Plan = func(context.Context) (pluginhost.Plan, error) { return p, cause }
		case "digest":
			p.Spec.BeforeSpawn = func(context.Context) error { return cause }
			o.Callbacks.Plan = func(context.Context) (pluginhost.Plan, error) { return p, nil }
		case "persist":
			o.Generations = refusingStore{cause}
		}
		l := e.new(o)
		if err := l.Enable(context.Background()); !errors.Is(err, cause) {
			e.t.Fatalf("adapter failure lost: %v", err)
		}
		e.noChild(p)
	}
	p := e.plan(BehaviourEcho)
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
