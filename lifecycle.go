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
// should cooperate with cancellation; all load/cleanup waits are isolated and
// bounded. Timed-out callbacks may overlap teardown for the same tuple.
// Scope callbacks always receive
// the canonical tuple, including partial preparation/failed activation.
// Revoke closes admission/credentials and cancels or boundedly drains host
// work before returning. Dispose removes remaining resources after process
// shutdown, even after earlier cleanup errors. Both are idempotent and safe
// to run concurrently with timed-out host callbacks.
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
// in one host. CleanupTimeout bounds each cleanup wait (default 2s). Timed-out callbacks
// remain isolated and block acknowledgement until they return.
// ClassifyExit is the only way to opt into retrying unexpected runtime exits.
type LifecycleOptions struct {
	HostInstance   string
	Generations    GenerationStore
	Callbacks      LifecycleCallbacks
	Retry          RetryPolicy
	CleanupTimeout time.Duration
	// CallbackTimeout bounds load-side host callbacks; default 10s. Disable
	// cancellation interrupts the wait immediately even if host code ignores it.
	CallbackTimeout time.Duration
	ClassifyExit    func(ExitInfo) error
	StateStore      LifecycleStateStore
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
	Disposals      []DisposalReport
}

type incarnation struct {
	owner    Owner
	epoch    uint64
	process  *Process
	cancel   context.CancelFunc
	disposed bool
	pending  []<-chan struct{}
	failures []CleanupFailure
}

// Lifecycle manages one ID. Operations serialize, but Disable cancels and
// fences an in-flight load before waiting its turn. No Supervisor is nested:
// this controller owns the single retry loop and crash recovery.
type Lifecycle struct {
	id             string
	opts           LifecycleOptions
	gate           chan struct{}
	mu             sync.Mutex
	status         LifecycleStatus
	revision       uint64
	tries          int
	pending        context.CancelFunc
	current        *incarnation
	disableBarrier <-chan struct{}
}

func NewLifecycle(id string, o LifecycleOptions) (*Lifecycle, error) {
	if id == "" || o.HostInstance == "" || o.Generations == nil || o.Callbacks.Plan == nil {
		return nil, errors.New("pluginhost: lifecycle needs ID, host epoch, generation store and plan callback")
	}
	if o.CleanupTimeout <= 0 {
		o.CleanupTimeout = 2 * time.Second
	}
	if o.CallbackTimeout <= 0 {
		o.CallbackTimeout = defaultHandshakeTimeout
	}
	l := &Lifecycle{id: id, opts: o, gate: make(chan struct{}, 1), status: LifecycleStatus{State: StateDisabled}}
	l.gate <- struct{}{}
	if o.StateStore != nil {
		ctx, cancel := context.WithTimeout(context.Background(), o.CleanupTimeout)
		defer cancel()
		for _, report := range l.history().Disposals {
			if len(report.Failures) > 0 && report.Failures[0].Step == "state_load" && l.pendingCallbacks(report.ID) {
				return nil, ErrStateStorePending
			}
		}
		record, done, err := isolated(ctx, func() (LifecycleRecord, error) { return o.StateStore.Load(ctx, id) })
		if err != nil {
			if done != nil {
				l.notePending(Owner{HostInstance: o.HostInstance, OwnerID: id}, "state_load", done, err)
				return nil, errors.Join(ErrStateStorePending, err)
			}
			return nil, err
		}
		if err = l.restore(record); err != nil {
			return nil, err
		}
	}
	history := l.history()
	if len(history.Disposals) > 0 {
		l.status.Disposal = history.Disposals[len(history.Disposals)-1]
	}
	if l.quarantined() {
		l.status.State = StateQuarantined
	}
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
	s.Disposals = l.history().Disposals
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
	if !l.status.DesiredEnabled || l.status.State != StateRunning || l.current == nil || l.current.epoch != l.revision {
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
	if !l.status.DesiredEnabled || l.status.State != StateRunning || l.current == nil || l.current.owner != o || l.current.epoch != l.revision {
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
func loadValue[T any](ctx context.Context, l *Lifecycle, fn func(context.Context) (T, error)) (T, error) {
	callCtx, cancel := context.WithTimeout(ctx, l.opts.CallbackTimeout)
	defer cancel()
	value, done, err := isolated(callCtx, func() (T, error) { return fn(callCtx) })
	if done != nil {
		err = &pendingCallbackError{Cause: err, Done: done}
	}
	return value, err
}
func (l *Lifecycle) preflightFailure(stage Stage, step string, err error) *Failure {
	var pending *pendingCallbackError
	if errors.As(err, &pending) {
		l.notePending(Owner{HostInstance: l.opts.HostInstance, OwnerID: l.id}, step, pending.Done, err)
		persistCtx, cancel := context.WithTimeout(context.Background(), l.opts.CleanupTimeout)
		if e := l.persist(persistCtx); e != nil {
			var p *pendingCallbackError
			if errors.As(e, &p) {
				l.notePending(Owner{HostInstance: l.opts.HostInstance, OwnerID: l.id}, "persist", p.Done, e)
			}
		}
		cancel()
	}
	return l.failure(stage, step, 0, err)
}
func (l *Lifecycle) preflight(ctx context.Context) (Plan, *Failure) {
	p, err := loadValue(ctx, l, l.opts.Callbacks.Plan)
	if err != nil {
		return p, l.preflightFailure(StagePlan, "plan", err)
	}
	if l.opts.Callbacks.Resolve != nil {
		input := snapshotPlan(p)
		p, err = loadValue(ctx, l, func(c context.Context) (Plan, error) { return l.opts.Callbacks.Resolve(c, input) })
		if err != nil {
			return p, l.preflightFailure(StageResolve, "resolve", err)
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
		_, err = loadValue(ctx, l, func(c context.Context) (struct{}, error) {
			return struct{}{}, l.opts.Callbacks.CheckCompatibility(c, p)
		})
		if err != nil {
			return p, l.preflightFailure(StageCompat, "compatibility", err)
		}
	}
	return snapshotPlan(p), nil
}
func (l *Lifecycle) begin(ctx context.Context, rev uint64, enabling bool) (context.Context, context.CancelFunc, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.revision != rev || (!enabling && !l.status.DesiredEnabled) {
		return nil, nil, ErrDisabled
	}
	if l.quarantined() && (!enabling || l.current == nil || l.current.epoch != l.revision || l.status.State != StateRunning) {
		return nil, nil, ErrQuarantined
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if enabling {
		l.status.DesiredEnabled = true
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
	if l.quarantined() {
		l.status.State = StateQuarantined
	} else if !l.status.DesiredEnabled {
		l.status.State = StateDisabled
	} else {
		l.status.State = StateFailed
	}
}
func (l *Lifecycle) intent() (uint64, <-chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.revision, l.disableBarrier
}
func waitBarrier(ctx context.Context, barrier <-chan struct{}) error {
	if barrier == nil {
		return ctx.Err()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-barrier:
		return ctx.Err()
	}
}

// Enable is idempotent while running. Reload explicitly requests a new plan.
func (l *Lifecycle) Enable(ctx context.Context) error {
	rev, barrier := l.intent()
	if err := waitBarrier(ctx, barrier); err != nil {
		return err
	}
	if err := l.acquire(ctx); err != nil {
		return err
	}
	defer l.release()
	op, cancel, err := l.begin(ctx, rev, true)
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
	if l.quarantined() {
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
	op, cancel, err := l.begin(ctx, rev, false)
	if err != nil {
		return err
	}
	defer cancel()
	defer l.end()
	if l.opts.Callbacks.BeforeReload != nil {
		if _, err = loadValue(op, l, func(c context.Context) (struct{}, error) { return struct{}{}, l.opts.Callbacks.BeforeReload(c) }); err != nil {
			f := l.preflightFailure(StagePlan, "reload", err)
			l.mu.Lock()
			l.status.LastFailure = f
			l.mu.Unlock()
			return f
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
	if l.quarantined() {
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
		if _, err := loadValue(ctx, l, func(c context.Context) (struct{}, error) { return struct{}{}, l.opts.Callbacks.BeforeDisable(c) }); err != nil {
			return l.preflightFailure(StagePlan, "disable", err)
		}
	}
	l.mu.Lock()
	l.status.DesiredEnabled = false
	l.revision++
	previous := l.disableBarrier
	completed := make(chan struct{})
	l.disableBarrier = completed
	if l.pending != nil {
		l.pending()
	}
	l.mu.Unlock()
	defer close(completed)
	_ = waitBarrier(context.Background(), previous)
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
	if l.quarantined() {
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
		if !f.Retryable || l.quarantined() || attempt == limit {
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
	g, err := loadValue(ctx, l, func(c context.Context) (uint64, error) { return l.opts.Generations.Next(c, l.opts.HostInstance, l.id) })
	if err != nil {
		return l.preflightFailure(StageLoad, "generation", err)
	}
	if err = l.reserve(g); err != nil {
		return l.failure(StageLoad, "generation", 0, err)
	}
	persistCtx, persistCancel := context.WithTimeout(ctx, l.opts.CleanupTimeout)
	err = l.persist(persistCtx)
	persistCancel()
	if err != nil {
		var pending *pendingCallbackError
		if errors.As(err, &pending) {
			l.notePending(Owner{HostInstance: l.opts.HostInstance, OwnerID: l.id, OwnerGeneration: g}, "persist", pending.Done, err)
		}
		return l.failure(StageLoad, "generation", g, err)
	}
	o := Owner{HostInstance: l.opts.HostInstance, OwnerID: l.id, OwnerGeneration: g}
	// The generation lifetime is independent of the caller's start context.
	life, cancel := context.WithCancel(context.Background()) //nolint:gosec // cancel retained in incarnation, called by dispose
	i := &incarnation{owner: o, epoch: rev, cancel: cancel}
	l.mu.Lock()
	l.status.Owner = o
	l.status.State = StateStarting
	l.mu.Unlock()
	fail := func(step string, e error) *Failure {
		f := l.failure(StageLoad, step, g, e)
		if step == "spawn" || step == "handshake" {
			f.Diagnostic = safeDiagnostic(p.Spec, e.Error())
		}
		var pending *pendingCallbackError
		if errors.As(e, &pending) {
			i.pending = append(i.pending, pending.Done)
			i.failures = append(i.failures, CleanupFailure{Step: step, Cause: e})
		}
		l.dispose(i)
		return f
	}
	if l.opts.Callbacks.PrepareScope != nil {
		expectedID, expectedVersion := p.Spec.ExpectedID, p.Spec.ExpectedVersion
		input := snapshotPlan(p)
		var spec Spec
		spec, err = loadValue(ctx, l, func(c context.Context) (Spec, error) { return l.opts.Callbacks.PrepareScope(c, o, input) })
		if err == nil {
			p.Spec = snapshotSpec(spec)
			p.Spec.ExpectedID = expectedID
			p.Spec.ExpectedVersion = expectedVersion
		}
		if err != nil {
			return fail("prepare", err)
		}
	}
	p.Spec.ID = l.id
	if p.Spec.ExpectedID == "" {
		p.Spec.ExpectedID = l.id
	}
	if hook := p.Spec.BeforeSpawn; hook != nil {
		p.Spec.BeforeSpawn = func(c context.Context) error {
			_, e := loadValue(c, l, func(c context.Context) (struct{}, error) { return struct{}{}, hook(c) })
			return e
		}
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
		_, err = loadValue(ctx, l, func(c context.Context) (struct{}, error) {
			return struct{}{}, l.opts.Callbacks.Activate(c, o, i.process)
		})
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
	l.status.OriginFailure = nil
	l.status.Exhausted = false
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
	r := DisposalReport{Owner: i.owner, Failures: slices.Clone(i.failures), Incomplete: len(i.pending) > 0}
	pending := slices.Clone(i.pending)
	run := func(step string, fn func(context.Context) error) {
		ctx, cancel := context.WithTimeout(context.Background(), l.opts.CleanupTimeout)
		defer cancel()
		_, done, err := isolated(ctx, func() (struct{}, error) { return struct{}{}, fn(ctx) })
		if done != nil {
			pending = append(pending, done)
		}
		if err != nil {
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
	r = l.recordDisposal(r, pending)
	persistCtx, persistCancel := context.WithTimeout(context.Background(), l.opts.CleanupTimeout)
	if err := l.persist(persistCtx); err != nil {
		r.Failures = append(r.Failures, CleanupFailure{Step: "persist", Cause: err})
		r.Incomplete = true
		var p *pendingCallbackError
		if errors.As(err, &p) {
			pending = append(pending, p.Done)
		}
		r = l.recordDisposal(r, pending)
	}
	persistCancel()
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
	active := l.current == i && l.status.DesiredEnabled && i.epoch == l.revision
	rev := l.revision
	l.mu.Unlock()
	if !active {
		return
	}
	info, _ := i.process.ExitInfo()
	var err = ErrGone
	if l.opts.ClassifyExit != nil {
		_, err = loadValue(ctx, l, func(c context.Context) (struct{}, error) { return struct{}{}, l.opts.ClassifyExit(info) })
		if err == nil {
			err = ErrGone
		}
	}
	var pending *pendingCallbackError
	if errors.As(err, &pending) {
		i.pending = append(i.pending, pending.Done)
		i.failures = append(i.failures, CleanupFailure{Step: "exit", Cause: err})
	}
	f := l.failure(StageLoad, "exit", i.owner.OwnerGeneration, err)
	l.dispose(i)
	l.mu.Lock()
	l.current = nil
	l.mu.Unlock()
	l.setFailure(f)
	if !f.Retryable || l.quarantined() || l.tries >= max(l.opts.Retry.MaxAttempts, 1) {
		return
	}
	op, cancel, e := l.begin(context.Background(), rev, false)
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
