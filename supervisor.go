package pluginhost

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hollis-labs/plugin-sdk/subprocess"
)

// SuperviseOptions configures a [Supervisor].
type SuperviseOptions struct {
	// Policy is the restart budget and backoff.
	Policy RestartPolicy
	// InitFactory supplies a fresh Init for each attempt, numbered from one.
	// The host owns generation issuance and fresh grants. It must honor ctx;
	// HandshakeTimeout bounds it and Stop cancels it. Without it, exits are terminal.
	InitFactory func(context.Context, uint64) (subprocess.InitParams, error)
	// ClassifyExit must return a typed TransientError to permit restarting.
	// Nil means unexpected exits are terminal. Never infer transience from a timeout.
	ClassifyExit func(ExitInfo) error
	// ClassifyTimeout bounds exit classification (default 10s). Stop cancels it.
	ClassifyTimeout time.Duration

	// HealthInterval, when positive, probes plugin/health that often
	// (Nanite and mcp-host style). Each probe is bounded by HealthTimeout
	// (default 5s). Zero disables periodic probing; hosts that prefer to ask
	// on demand use a [HealthGate] instead.
	HealthInterval time.Duration
	HealthTimeout  time.Duration
	// KillAfterUnhealthy kills the process after this many consecutive bad
	// probes (an error or ok=false); ClassifyExit decides whether Policy restarts it. Zero
	// never kills.
	KillAfterUnhealthy int

	// OnStart runs after every successful start, first one included, with
	// the new process installed. OnExit runs when a process ended without a
	// Stop, before the backoff wait; restarting is false when the budget is
	// spent or classification is terminal. OnGiveUp runs once, with the reason, when supervision ends
	// because no further restart will be made. Callbacks run on the
	// supervisor's goroutine: keep them short.
	OnStart  func(*Process)
	OnExit   func(info ExitInfo, restarting bool)
	OnGiveUp func(error)
}

// Supervisor restarts explicitly transient exits through a full handshake
// with backoff and a budget from [RestartPolicy]. Unknown exits are terminal.
// It optionally kills a persistently unhealthy process and marks that exit
// SupervisorInitiatedKill before asking ClassifyExit.
//
// Stop it through [Supervisor.Stop]. A process stopped behind its back (by
// calling Stop or Kill on [Supervisor.Current]) is an unexpected exit as far
// as the supervisor can tell, and is classified before any restart.
type Supervisor struct {
	spec Spec
	opts SuperviseOptions

	ctx    context.Context
	cancel context.CancelFunc

	mu             sync.Mutex
	cur            *Process
	attempts       uint64
	lastInit       subprocess.InitParams
	reserved       *Owner
	pendingFactory <-chan struct{}
	restarts       int
	started        bool
	stopped        bool
	healthKilled   *Process
	healthFailure  error

	stopOnce   sync.Once
	stopping   chan struct{}
	finished   chan struct{}
	finishOnce sync.Once
}

// finish releases everyone waiting in Stop. It is called by the run loop when
// it ends and by every path that will never run one, so Stop cannot wait on a
// loop that does not exist. It is safe to call more than once.
func (s *Supervisor) finish() { s.finishOnce.Do(func() { close(s.finished) }) }

// Supervise returns a Supervisor for s. Nothing runs until Start.
func Supervise(s Spec, o SuperviseOptions) *Supervisor {
	// cancel is kept on the Supervisor and called by Stop.
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // G118: stored, not dropped
	return &Supervisor{
		spec: s, opts: o, ctx: ctx, cancel: cancel,
		stopping: make(chan struct{}), finished: make(chan struct{}),
	}
}

// Start performs the first spawn and handshake synchronously and, on
// success, begins supervising. ctx bounds that first handshake only. If it
// fails the error is returned and nothing is supervised.
func (s *Supervisor) Start(ctx context.Context) error {
	s.mu.Lock()
	switch {
	case s.stopped:
		s.mu.Unlock()
		return errors.New("pluginhost: supervisor is stopped")
	case s.started:
		s.mu.Unlock()
		return errors.New("pluginhost: supervisor already started")
	}
	s.started = true
	s.mu.Unlock()

	// A Stop that arrives mid-handshake cancels it instead of waiting it out.
	hctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(s.ctx, cancel)()
	spec, err := s.attemptSpec(hctx)
	var p *Process
	if err == nil {
		p, err = Start(hctx, spec)
	}
	if err != nil {
		s.releaseAttempt(nil)
		s.mu.Lock()
		s.started = false
		stopped := s.stopped
		s.mu.Unlock()
		if stopped {
			s.finish() // Stop saw started and is waiting on a loop that will never run
		}
		return err
	}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		stopErr := p.Stop(context.Background())
		s.releaseAttempt(stopErr)
		s.finish()
		return errors.New("pluginhost: supervisor is stopped")
	}
	s.cur = p
	s.mu.Unlock()
	if s.opts.OnStart != nil {
		s.opts.OnStart(p)
	}
	go s.run(p)
	return nil
}

// Current returns the running process, or nil while a restart is pending or
// after supervision ended.
func (s *Supervisor) Current() *Process {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur
}

// Restarts reports how many restart attempts have been made (an attempt
// counts once its spawn has been tried; one aborted by Stop does not). With
// a Policy.StableFor it counts since the budget was last refilled.
func (s *Supervisor) Restarts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.restarts
}

// Stop ends supervision and the plugin. A restart in flight is canceled and
// the child it spawned is stopped, never installed: a Stop that races a
// restart cannot resurrect the plugin. Stop is idempotent.
func (s *Supervisor) Stop(ctx context.Context) error {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.stopped = true
		started := s.started
		s.mu.Unlock()
		close(s.stopping)
		s.cancel()
		if !started {
			s.finish()
		}
	})
	select {
	case <-s.finished:
	case <-ctx.Done():
	}
	s.mu.Lock()
	p := s.cur
	s.cur = nil
	s.mu.Unlock()
	var stopErr error
	if p != nil {
		stopErr = p.Stop(ctx)
		s.releaseAttempt(stopErr)
	}
	if s.PendingFactory() {
		stopErr = errors.Join(stopErr, ErrInitFactoryPending)
	}
	return stopErr
}

func (s *Supervisor) isStopping() bool {
	select {
	case <-s.stopping:
		return true
	default:
		return false
	}
}

// run is the supervision loop: watch a process, and on an unexpected exit
// restart it until the budget runs out or Stop is called.
func (s *Supervisor) run(p *Process) {
	defer s.finish()
	startedAt := time.Now()
	for {
		if !s.watch(p) {
			return
		}
		s.releaseAttempt(nil)
		info, _ := p.ExitInfo()
		s.mu.Lock()
		info.SupervisorInitiatedKill = s.healthKilled == p
		healthFailure := s.healthFailure
		s.healthKilled = nil
		s.healthFailure = nil
		s.mu.Unlock()

		s.mu.Lock()
		s.cur = nil
		if s.opts.Policy.StableFor > 0 && time.Since(startedAt) >= s.opts.Policy.StableFor {
			s.restarts = 0
		}
		s.mu.Unlock()

		next, ok := s.restart(p, info, healthFailure)
		if !ok {
			return
		}
		p, startedAt = next, time.Now()
	}
}

// restart tries to bring a replacement up, retrying failed spawns while the
// budget lasts. ok is false when supervision is over.
func (s *Supervisor) restart(crashed *Process, info ExitInfo, healthFailure error) (*Process, bool) {
	notified := false
	var lastErr error
	if s.opts.ClassifyExit != nil {
		timeout := s.opts.ClassifyTimeout
		if timeout <= 0 {
			timeout = 10 * time.Second
		}
		ctx, cancel := context.WithTimeout(s.ctx, timeout)
		_, _, lastErr = isolated(ctx, func() (struct{}, error) { return struct{}{}, s.opts.ClassifyExit(info) })
		cancel()
	}
	if info.SupervisorInitiatedKill {
		lastErr = errors.Join(lastErr, healthFailure)
	}
	retryable := IsTransient(lastErr) && !errors.Is(healthFailure, ErrProtocolMismatch)
	if s.opts.InitFactory == nil {
		lastErr = errors.Join(lastErr, processFailure(s.spec, "init", ErrInitFactoryRequired))
		retryable = false
	}
	for {
		s.mu.Lock()
		attempt := s.restarts
		s.mu.Unlock()

		delay, allowed := s.opts.Policy.Backoff(attempt)
		allowed = allowed && retryable
		if !notified {
			notified = true
			if s.opts.OnExit != nil {
				s.opts.OnExit(info, allowed)
			}
		}
		if !allowed {
			s.giveUp(crashed, attempt, lastErr)
			return nil, false
		}

		timer := time.NewTimer(delay)
		select {
		case <-s.stopping:
			timer.Stop()
			return nil, false
		case <-timer.C:
		}

		// s.ctx is canceled by Stop, which fails a handshake in flight fast.
		spec, err := s.attemptSpec(s.ctx)
		var next *Process
		if err == nil {
			next, err = Start(s.ctx, spec)
		}

		s.mu.Lock()
		if s.isStopping() {
			s.mu.Unlock()
			if err == nil {
				// Installing it now would spawn a child after the host
				// decided to stop this plugin.
				stopErr := next.Stop(context.Background())
				s.releaseAttempt(stopErr)
			} else {
				// Start reaps a child whose canceled handshake failed.
				s.releaseAttempt(nil)
			}
			return nil, false
		}
		s.restarts++
		if err != nil {
			s.mu.Unlock()
			s.releaseAttempt(nil)
			lastErr = err
			retryable = IsTransient(err)
			continue
		}
		s.cur = next
		s.mu.Unlock()
		if s.opts.OnStart != nil {
			s.opts.OnStart(next)
		}
		return next, true
	}
}

func (s *Supervisor) giveUp(crashed *Process, attempt int, lastErr error) {
	if s.opts.OnGiveUp == nil {
		return
	}
	err := fmt.Errorf("pluginhost: %s stopped after %d restarts (terminal failure or exhausted budget)%s",
		s.spec.label(), attempt, crashed.diagnosticsText())
	if lastErr != nil {
		err = &Failure{PluginID: s.spec.ID, Stage: StageLoad, Step: "exit", Code: "supervision_ended", Cause: errors.Join(err, lastErr), Diagnostic: safeDiagnostic(s.spec, err.Error()+"; reason: "+lastErr.Error())}
	}
	s.opts.OnGiveUp(err)
}

// watch blocks until p exits (true) or the supervisor is stopped (false),
// probing health on the way when configured.
func (s *Supervisor) watch(p *Process) bool {
	var tick <-chan time.Time
	if s.opts.HealthInterval > 0 {
		ticker := time.NewTicker(s.opts.HealthInterval)
		defer ticker.Stop()
		tick = ticker.C
	}
	timeout := s.opts.HealthTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	bad := 0
	for {
		select {
		case <-p.Exited():
			return !s.isStopping()
		case <-s.stopping:
			return false
		case <-tick:
			ctx, cancel := context.WithTimeout(s.ctx, timeout)
			health, err := p.Client().Health(ctx)
			cancel()
			if s.isStopping() {
				return false
			}
			if errors.Is(err, ErrHealthInconclusive) || (err == nil && health.OK) {
				bad = 0
				continue
			}
			bad++
			if s.opts.KillAfterUnhealthy > 0 && bad >= s.opts.KillAfterUnhealthy {
				s.mu.Lock()
				s.healthKilled = p
				s.healthFailure = err
				if err == nil {
					s.healthFailure = ErrUnhealthy
				}
				s.mu.Unlock()
				_ = p.Kill()
			}
		}
	}
}

// ErrInitFactoryRequired reports a restart lacking host-issued Init authority.
var ErrInitFactoryRequired = errors.New("pluginhost: supervised restart requires an Init factory")

func (s *Supervisor) attemptSpec(ctx context.Context) (Spec, error) {
	if s.PendingFactory() {
		return s.spec, processFailure(s.spec, "init", ErrInitFactoryPending)
	}
	if s.attempts == ^uint64(0) {
		return s.spec, initFailure(s.spec, &subprocess.InitError{Code: subprocess.InitInvalid, Field: "attempt"})
	}
	s.attempts++
	attempt := s.attempts
	spec := snapshotSpec(s.spec.normalized())
	if s.opts.InitFactory != nil {
		bounded, cancel := context.WithTimeout(ctx, spec.HandshakeTimeout)
		params, done, err := isolated(bounded, func() (subprocess.InitParams, error) { return s.opts.InitFactory(bounded, attempt) })
		cancel()
		if done != nil {
			s.mu.Lock()
			s.pendingFactory = done
			s.mu.Unlock()
			err = &pendingCallbackError{Cause: errors.Join(err, ErrInitFactoryPending), Done: done}
		}
		if err != nil {
			return spec, initFailure(spec, err)
		}
		spec.Init = params
	} else if s.attempts > 1 {
		return spec, processFailure(spec, "init", ErrInitFactoryRequired)
	}
	spec = snapshotSpec(spec.normalized())
	if err := validateInit(spec); err != nil {
		return spec, err
	}
	current, previous := spec.Init.Incarnation, s.lastInit.Incarnation
	if previous.OwnerGeneration > 0 && (current.HostInstance != previous.HostInstance || current.OwnerID != previous.OwnerID || current.OwnerGeneration <= previous.OwnerGeneration) {
		return spec, initFailure(spec, &subprocess.InitError{Code: subprocess.InitInvalid, Field: "incarnation"})
	}
	if s.opts.InitFactory != nil {
		owner := Owner{HostInstance: current.HostInstance, OwnerID: current.OwnerID, OwnerGeneration: current.OwnerGeneration}
		if err := reserveOwner(owner); err != nil {
			return spec, processFailure(spec, "generation", err)
		}
		s.mu.Lock()
		s.reserved = &owner
		s.mu.Unlock()
	}
	s.lastInit = spec.Init
	return spec, nil
}

// ErrInitFactoryPending reports host factory code still running after cancellation.
var ErrInitFactoryPending = errors.New("pluginhost: Init factory is still running")

// PendingFactory reports isolated host work that has not returned yet. Stop is
// bounded and returns ErrInitFactoryPending while that work remains; hosts must
// reconcile its external effects before discarding the supervisor.
func (s *Supervisor) PendingFactory() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingFactory == nil {
		return false
	}
	select {
	case <-s.pendingFactory:
		s.pendingFactory = nil
		return false
	default:
		return true
	}
}

func (s *Supervisor) releaseAttempt(cause error) {
	s.mu.Lock()
	owner := s.reserved
	s.reserved = nil
	s.mu.Unlock()
	if owner == nil {
		return
	}
	report := DisposalReport{Owner: *owner, Incomplete: cause != nil}
	if cause != nil {
		report.Failures = []CleanupFailure{{Step: "stop", Cause: cause}}
	}
	_ = recordOwnerDisposal([2]string{owner.HostInstance, owner.OwnerID}, report, nil)
}
