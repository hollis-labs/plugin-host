package pluginhost

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"
)

// Plan is a host-normalized candidate. The callbacks must treat its slices,
// maps and payloads as immutable, including after returning them. Artifact
// pinning/review and intent persistence are host policy, not manifest parsing.
type Plan struct {
	Spec     Spec
	Versions []VersionRequirement
}

// LifecycleCallbacks run without the controller's state mutex held. They
// must cooperate with context cancellation. Scope callbacks always receive
// the canonical tuple, including partial preparation/failed activation.
// Revoke closes admission/credentials and cancels or boundedly drains host
// work before returning. Dispose removes remaining resources after process
// shutdown, even after earlier cleanup errors. Both are idempotent.
type LifecycleCallbacks struct {
	Plan               func(context.Context) (Plan, error)
	Resolve            func(context.Context, Plan) (Plan, error)
	CheckCompatibility func(context.Context, Plan) error
	// BeforeDisable/BeforeReload enforce host dependency/persistence policy.
	// They must fail without mutating durable intent when returning an error.
	BeforeDisable func(context.Context) error
	BeforeReload  func(context.Context) error
	// PrepareScope returns the generation-bound spawn specification. Build
	// fresh Init/config/environment from reviewed policy here after issuance,
	// without changing the reviewed artifact/identity/version.
	PrepareScope func(context.Context, Owner, Plan) (Spec, error)
	Activate     func(context.Context, Owner, *Process) error
	Revoke       func(context.Context, Owner) error
	Dispose      func(context.Context, Owner) error
}

// RetryPolicy is finite and disabled unless MaxAttempts > 1. Attempts include
// the first load; Backoff defaults to 100ms. Only IsTransient failures retry.
type RetryPolicy struct {
	MaxAttempts int
	Backoff     time.Duration
}

// LifecycleOptions must share HostInstance and Generations across controllers
// in one host. CleanupTimeout bounds each cooperating callback (default 2s).
// ClassifyExit is the only way to opt into retrying unexpected runtime exits.
type LifecycleOptions struct {
	HostInstance   string
	Generations    GenerationStore
	Callbacks      LifecycleCallbacks
	Retry          RetryPolicy
	CleanupTimeout time.Duration
	ClassifyExit   func(ExitInfo) error
}

// State separates desired intent from actual availability.
type State string

const (
	StateDisabled    State = "disabled"
	StateStarting    State = "starting"
	StateRunning     State = "running"
	StateFailed      State = "failed"
	StateQuarantined State = "quarantined"
)

// LifecycleStatus is a snapshot. Disposal failures are copied.
type LifecycleStatus struct {
	DesiredEnabled bool
	State          State
	Owner          Owner
	LastFailure    *Failure
	RetryAttempts  int
	OriginFailure  *Failure
	Exhausted      bool
	Disposal       DisposalReport
}

type incarnation struct {
	owner    Owner
	process  *Process
	cancel   context.CancelFunc
	disposed bool
}

// Lifecycle manages one ID. Operations serialize, but Disable cancels and
// fences an in-flight load before waiting its turn. No Supervisor is nested:
// this controller owns the single retry loop and crash recovery.
type Lifecycle struct {
	id       string
	opts     LifecycleOptions
	gate     chan struct{}
	mu       sync.Mutex
	status   LifecycleStatus
	revision uint64
	tries    int
	pending  context.CancelFunc
	current  *incarnation
}

func NewLifecycle(id string, o LifecycleOptions) (*Lifecycle, error) {
	if id == "" || o.HostInstance == "" || o.Generations == nil || o.Callbacks.Plan == nil {
		return nil, errors.New("pluginhost: lifecycle needs ID, host epoch, generation store and plan callback")
	}
	if o.CleanupTimeout <= 0 {
		o.CleanupTimeout = 2 * time.Second
	}
	l := &Lifecycle{id: id, opts: o, gate: make(chan struct{}, 1), status: LifecycleStatus{State: StateDisabled}}
	l.gate <- struct{}{}
	return l, nil
}
func (l *Lifecycle) acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.gate:
		return nil
	}
}
func (l *Lifecycle) release() { l.gate <- struct{}{} }
func (l *Lifecycle) Status() LifecycleStatus {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.status
	s.Disposal.Failures = slices.Clone(s.Disposal.Failures)
	if s.LastFailure != nil {
		f := *s.LastFailure
		s.LastFailure = &f
	}
	if s.OriginFailure != nil {
		f := *s.OriginFailure
		s.OriginFailure = &f
	}
	return s
}

// Current only returns a callable generation. A captured pointer/callback
// must check IsCurrent at actual dispatch, not only when capturing it.
func (l *Lifecycle) Current() *Process {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.status.DesiredEnabled || l.status.State != StateRunning || l.current == nil {
		return nil
	}
	select {
	case <-l.current.process.Exited():
		return nil
	default:
		return l.current.process
	}
}
func (l *Lifecycle) IsCurrent(o Owner) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.status.DesiredEnabled || l.status.State != StateRunning || l.current == nil || l.current.owner != o {
		return false
	}
	select {
	case <-l.current.process.Exited():
		return false
	default:
		return true
	}
}
func (l *Lifecycle) failure(stage Stage, step string, g uint64, err error) *Failure {
	code := step + "_failed"
	var f *Failure
	if stage == StageLoad && step == "handshake" && errors.As(err, &f) {
		code = f.Code
		step = f.Step
		stage = f.Stage
	}
	return &Failure{PluginID: l.id, Generation: g, Stage: stage, Step: step, Code: code, Retryable: IsTransient(err), Cause: err}
}
func callback(ctx context.Context, fn func() error) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = ErrCallbackPanic
			if cause, ok := value.(error); ok {
				err = errors.Join(err, cause)
			}
		}
	}()
	if err = ctx.Err(); err != nil {
		return err
	}
	err = fn()
	if err == nil {
		err = ctx.Err()
	}
	return err
}
func (l *Lifecycle) preflight(ctx context.Context) (Plan, *Failure) {
	var p Plan
	err := callback(ctx, func() (e error) { p, e = l.opts.Callbacks.Plan(ctx); return e })
	if err != nil {
		return p, l.failure(StagePlan, "plan", 0, err)
	}
	if l.opts.Callbacks.Resolve != nil {
		err = callback(ctx, func() (e error) { p, e = l.opts.Callbacks.Resolve(ctx, p); return e })
		if err != nil {
			return p, l.failure(StageResolve, "resolve", 0, err)
		}
	}
	if p.Spec.ExpectedID == "" {
		p.Spec.ExpectedID = l.id
	}
	p.Spec.ID = l.id
	if p.Spec.ExpectedID != l.id {
		return p, l.failure(StageCompat, "identity", 0, ErrIdentityMismatch)
	}
	if _, e := parseVersion(p.Spec.ExpectedVersion); e != nil {
		return p, l.failure(StageCompat, "version", 0, e)
	}
	for _, v := range p.Versions {
		if err = CheckVersion(v); err != nil {
			return p, l.failure(StageCompat, "version", 0, err)
		}
	}
	if l.opts.Callbacks.CheckCompatibility != nil {
		err = callback(ctx, func() error { return l.opts.Callbacks.CheckCompatibility(ctx, p) })
		if err != nil {
			return p, l.failure(StageCompat, "compatibility", 0, err)
		}
	}
	return snapshotPlan(p), nil
}
func (l *Lifecycle) begin(ctx context.Context, rev uint64) (context.Context, context.CancelFunc, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.revision != rev || !l.status.DesiredEnabled {
		return nil, nil, ErrDisabled
	}
	if l.status.Disposal.Incomplete {
		return nil, nil, ErrQuarantined
	}
	op, cancel := context.WithCancel(ctx)
	l.pending = cancel
	return op, cancel, nil
}
func (l *Lifecycle) end() { l.mu.Lock(); l.pending = nil; l.mu.Unlock() }
func (l *Lifecycle) setFailure(f *Failure) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.status.LastFailure = f
	if l.status.OriginFailure == nil {
		l.status.OriginFailure = f
	}
	l.status.Exhausted = f.Retryable && l.tries >= max(l.opts.Retry.MaxAttempts, 1)
	if l.status.Disposal.Incomplete {
		l.status.State = StateQuarantined
	} else if !l.status.DesiredEnabled {
		l.status.State = StateDisabled
	} else {
		l.status.State = StateFailed
	}
}
func (l *Lifecycle) intent() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.status.DesiredEnabled = true
	return l.revision
}

// Enable is idempotent while running. Reload explicitly requests a new plan.
func (l *Lifecycle) Enable(ctx context.Context) error {
	rev := l.intent()
	if err := l.acquire(ctx); err != nil {
		return err
	}
	defer l.release()
	op, cancel, err := l.begin(ctx, rev)
	if err != nil {
		return err
	}
	defer cancel()
	defer l.end()
	if l.Current() != nil {
		return nil
	}
	if old := l.detach(); old != nil {
		l.dispose(old)
	}
	if l.Status().Disposal.Incomplete {
		return ErrQuarantined
	}
	l.resetAttempts()
	return l.startAttempts(op, rev, nil)
}

// Reload preflights while the old generation serves. Preflight failure retains
// it; failure after teardown leaves unavailable. No automatic rollback.
func (l *Lifecycle) Reload(ctx context.Context) error {
	l.mu.Lock()
	rev := l.revision
	enabled := l.status.DesiredEnabled
	l.mu.Unlock()
	if !enabled {
		return ErrDisabled
	}
	if err := l.acquire(ctx); err != nil {
		return err
	}
	defer l.release()
	op, cancel, err := l.begin(ctx, rev)
	if err != nil {
		return err
	}
	defer cancel()
	defer l.end()
	if l.opts.Callbacks.BeforeReload != nil {
		if err = callback(op, func() error { return l.opts.Callbacks.BeforeReload(op) }); err != nil {
			return l.failure(StagePlan, "reload", 0, err)
		}
	}
	p, f := l.preflight(op)
	if f != nil {
		l.mu.Lock()
		l.status.LastFailure = f
		l.mu.Unlock()
		return f
	}
	if old := l.detach(); old != nil {
		l.dispose(old)
	}
	if l.Status().Disposal.Incomplete {
		return ErrQuarantined
	}
	l.resetAttempts()
	return l.startAttempts(op, rev, &p)
}

// Disable refuses host dependency/persistence errors before changing intent.
// Once accepted it immediately fences dispatch and cancels load/backoff, then
// serializes teardown. Even if ctx expires the fence remains in place.
func (l *Lifecycle) Disable(ctx context.Context) error {
	if l.opts.Callbacks.BeforeDisable != nil {
		if err := callback(ctx, func() error { return l.opts.Callbacks.BeforeDisable(ctx) }); err != nil {
			return l.failure(StagePlan, "disable", 0, err)
		}
	}
	l.mu.Lock()
	l.status.DesiredEnabled = false
	l.revision++
	if l.pending != nil {
		l.pending()
	}
	l.mu.Unlock()
	// Teardown must run even after caller cancellation; callbacks have their
	// own bounded cleanup contexts. Waiting cannot forcibly stop bad host code.
	if err := l.acquire(context.WithoutCancel(ctx)); err != nil {
		return err
	}
	defer l.release()
	if old := l.detach(); old != nil {
		l.dispose(old)
	}
	l.mu.Lock()
	if l.status.Disposal.Incomplete {
		l.status.State = StateQuarantined
	} else {
		l.status.State = StateDisabled
	}
	report := l.status.Disposal
	report.Failures = slices.Clone(report.Failures)
	l.mu.Unlock()
	if len(report.Failures) > 0 {
		return report
	}
	return nil
}
func (l *Lifecycle) startAttempts(ctx context.Context, rev uint64, prepared *Plan) error {
	limit := max(l.opts.Retry.MaxAttempts, 1)
	for l.tries < limit {
		l.tries++
		attempt := l.tries
		if err := ctx.Err(); err != nil {
			f := l.failure(StageLoad, "cancel", 0, err)
			l.setFailure(f)
			return f
		}
		l.mu.Lock()
		l.status.RetryAttempts = attempt - 1
		l.mu.Unlock()
		var p Plan
		var f *Failure
		if prepared != nil {
			p = *prepared
			prepared = nil
		} else {
			p, f = l.preflight(ctx)
		}
		if f == nil {
			f = l.load(ctx, rev, p)
		}
		if f == nil {
			return nil
		}
		l.setFailure(f)
		if !f.Retryable || l.Status().Disposal.Incomplete || attempt == limit {
			return f
		}
		delay := l.opts.Retry.Backoff
		if delay <= 0 {
			delay = 100 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			f = l.failure(StageLoad, "backoff", f.Generation, ctx.Err())
			l.setFailure(f)
			return f
		case <-timer.C:
		}
	}
	panic("unreachable")
}
func (l *Lifecycle) load(ctx context.Context, rev uint64, p Plan) *Failure {
	var g uint64
	err := callback(ctx, func() (e error) { g, e = l.opts.Generations.Next(ctx, l.opts.HostInstance, l.id); return e })
	if err != nil || g == 0 {
		if err == nil {
			err = errors.New("pluginhost: generation store returned zero")
		}
		return l.failure(StageLoad, "generation", 0, err)
	}
	o := Owner{HostInstance: l.opts.HostInstance, OwnerID: l.id, OwnerGeneration: g}
	// The generation lifetime is independent of the caller's start context.
	life, cancel := context.WithCancel(context.Background()) //nolint:gosec // cancel retained in incarnation, called by dispose
	i := &incarnation{owner: o, cancel: cancel}
	l.mu.Lock()
	l.status.Owner = o
	l.status.State = StateStarting
	l.mu.Unlock()
	fail := func(step string, e error) *Failure { f := l.failure(StageLoad, step, g, e); l.dispose(i); return f }
	if l.opts.Callbacks.PrepareScope != nil {
		err = callback(ctx, func() error {
			expectedID, expectedVersion := p.Spec.ExpectedID, p.Spec.ExpectedVersion
			var spec Spec
			var e error
			spec, e = l.opts.Callbacks.PrepareScope(ctx, o, p)
			if e == nil {
				p.Spec = snapshotSpec(spec)
				p.Spec.ExpectedID = expectedID
				p.Spec.ExpectedVersion = expectedVersion
			}
			return e
		})
		if err != nil {
			return fail("prepare", err)
		}
	}
	p.Spec.ID = l.id
	if p.Spec.ExpectedID == "" {
		p.Spec.ExpectedID = l.id
	}
	i.process, err = Spawn(ctx, p.Spec)
	if err != nil {
		return fail("spawn", err)
	}
	_, _, err = i.process.Handshake(ctx)
	if err != nil {
		return fail("handshake", err)
	}
	if l.opts.Callbacks.Activate != nil {
		err = callback(ctx, func() error { return l.opts.Callbacks.Activate(ctx, o, i.process) })
		if err != nil {
			return fail("activate", err)
		}
	}
	l.mu.Lock()
	if l.revision != rev || !l.status.DesiredEnabled || ctx.Err() != nil {
		l.mu.Unlock()
		return fail("fence", ErrDisabled)
	}
	l.current = i
	l.status.State = StateRunning
	l.status.LastFailure = nil
	l.mu.Unlock()
	go l.watch(life, i) //nolint:gosec // G118: generation survives the startup request; dispose cancels its owned context
	return nil
}
func (l *Lifecycle) dispose(i *incarnation) {
	if i.disposed {
		return
	}
	i.disposed = true
	l.mu.Lock()
	l.status.State = StateStarting
	l.mu.Unlock() // fence before host callbacks
	r := DisposalReport{Owner: i.owner}
	run := func(step string, fn func(context.Context) error) {
		ctx, cancel := context.WithTimeout(context.Background(), l.opts.CleanupTimeout)
		defer cancel()
		if err := callback(ctx, func() error { return fn(ctx) }); err != nil {
			r.Failures = append(r.Failures, CleanupFailure{Step: step, Cause: err})
			r.Incomplete = true
		}
	}
	if l.opts.Callbacks.Revoke != nil {
		run("revoke", func(ctx context.Context) error { return l.opts.Callbacks.Revoke(ctx, i.owner) })
	}
	i.cancel()
	if i.process != nil {
		unload, stop := i.process.stopWithReport(context.Background())
		if unload != nil {
			r.Failures = append(r.Failures, CleanupFailure{Step: "unload", Cause: unload})
		}
		if stop != nil {
			r.Failures = append(r.Failures, CleanupFailure{Step: "stop", Cause: stop})
			r.Incomplete = true
		}
	}
	if l.opts.Callbacks.Dispose != nil {
		run("dispose", func(ctx context.Context) error { return l.opts.Callbacks.Dispose(ctx, i.owner) })
	}
	l.mu.Lock()
	l.status.Disposal = r
	if r.Incomplete {
		l.status.State = StateQuarantined
	}
	l.mu.Unlock()
}
func (l *Lifecycle) watch(ctx context.Context, i *incarnation) {
	select {
	case <-ctx.Done():
		return
	case <-i.process.Exited():
	}
	if err := l.acquire(ctx); err != nil {
		return
	}
	defer l.release()
	l.mu.Lock()
	active := l.current == i && l.status.DesiredEnabled
	rev := l.revision
	l.mu.Unlock()
	if !active {
		return
	}
	info, _ := i.process.ExitInfo()
	var err = ErrGone
	if l.opts.ClassifyExit != nil {
		err = callback(context.Background(), func() error { return l.opts.ClassifyExit(info) })
		if err == nil {
			err = ErrGone
		}
	}
	f := l.failure(StageLoad, "exit", i.owner.OwnerGeneration, err)
	l.dispose(i)
	l.mu.Lock()
	l.current = nil
	l.mu.Unlock()
	l.setFailure(f)
	if !f.Retryable || l.Status().Disposal.Incomplete || l.tries >= max(l.opts.Retry.MaxAttempts, 1) {
		return
	}
	op, cancel, e := l.begin(context.Background(), rev)
	if e != nil {
		return
	}
	defer cancel()
	defer l.end()
	// Crash recovery gets a fresh plan and tuple, bounded by the same policy.
	delay := l.opts.Retry.Backoff
	if delay <= 0 {
		delay = 100 * time.Millisecond
	}
	timer := time.NewTimer(delay)
	select {
	case <-op.Done():
		timer.Stop()
		return
	case <-timer.C:
	}
	_ = l.startAttempts(op, rev, nil)
}

// String provides a bounded status summary without callback/config contents.
func (s LifecycleStatus) String() string {
	return fmt.Sprintf("%s generation %d desired=%t", s.State, s.Owner.OwnerGeneration, s.DesiredEnabled)
}

func (l *Lifecycle) detach() *incarnation {
	l.mu.Lock()
	defer l.mu.Unlock()
	old := l.current
	l.current = nil
	return old
}

func (l *Lifecycle) resetAttempts() {
	l.tries = 0
	l.mu.Lock()
	l.status.OriginFailure = nil
	l.status.Exhausted = false
	l.status.RetryAttempts = 0
	l.mu.Unlock()
}

func snapshotSpec(s Spec) Spec {
	s.Args = slices.Clone(s.Args)
	s.Env = slices.Clone(s.Env)
	s.Secrets = slices.Clone(s.Secrets)
	s.ConnOptions = slices.Clone(s.ConnOptions)
	s.Init.Config = maps.Clone(s.Init.Config)
	s.Init.Granted = slices.Clone(s.Init.Granted)
	return s
}
func snapshotPlan(p Plan) Plan {
	p.Spec = snapshotSpec(p.Spec)
	p.Versions = slices.Clone(p.Versions)
	return p
}
