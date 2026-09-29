package pluginhost

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// SuperviseOptions configures a [Supervisor].
type SuperviseOptions struct {
	// Policy is the restart budget and backoff.
	Policy RestartPolicy

	// HealthInterval, when positive, probes plugin/health that often
	// (Nanite and mcp-host style). Each probe is bounded by HealthTimeout
	// (default 5s). Zero disables periodic probing; hosts that prefer to ask
	// on demand use a [HealthGate] instead.
	HealthInterval time.Duration
	HealthTimeout  time.Duration
	// KillAfterUnhealthy kills the process after this many consecutive bad
	// probes (an error or ok=false), which restarts it under Policy. Zero
	// never kills.
	KillAfterUnhealthy int

	// OnStart runs after every successful start, first one included, with
	// the new process installed. OnExit runs when a process ended without a
	// Stop, before the backoff wait; restarting is false when the budget is
	// spent. OnGiveUp runs once, with the reason, when supervision ends
	// because no further restart will be made. Callbacks run on the
	// supervisor's goroutine: keep them short.
	OnStart  func(*Process)
	OnExit   func(info ExitInfo, restarting bool)
	OnGiveUp func(error)
}

// Supervisor keeps one plugin running: it restarts the process when it exits
// unexpectedly, re-running the full handshake each time, with backoff and a
// budget from [RestartPolicy], and optionally kills it when it stays
// unhealthy.
//
// Stop it through [Supervisor.Stop]. A process stopped behind its back (by
// calling Stop or Kill on [Supervisor.Current]) is an unexpected exit as far
// as the supervisor can tell, and is restarted.
type Supervisor struct {
	spec Spec
	opts SuperviseOptions

	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	cur      *Process
	restarts int
	started  bool
	stopped  bool

	stopOnce sync.Once
	stopping chan struct{}
	finished chan struct{}
}

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

	p, err := Start(ctx, s.spec)
	if err != nil {
		s.mu.Lock()
		s.started = false
		s.mu.Unlock()
		return err
	}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		_ = p.Stop(context.Background())
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
			close(s.finished)
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
	if p == nil {
		return nil
	}
	return p.Stop(ctx)
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
	defer close(s.finished)
	startedAt := time.Now()
	for {
		if !s.watch(p) {
			return
		}
		info, _ := p.ExitInfo()

		s.mu.Lock()
		s.cur = nil
		if s.opts.Policy.StableFor > 0 && time.Since(startedAt) >= s.opts.Policy.StableFor {
			s.restarts = 0
		}
		s.mu.Unlock()

		next, ok := s.restart(p, info)
		if !ok {
			return
		}
		p, startedAt = next, time.Now()
	}
}

// restart tries to bring a replacement up, retrying failed spawns while the
// budget lasts. ok is false when supervision is over.
func (s *Supervisor) restart(crashed *Process, info ExitInfo) (*Process, bool) {
	notified := false
	var lastErr error
	for {
		s.mu.Lock()
		attempt := s.restarts
		s.mu.Unlock()

		delay, allowed := s.opts.Policy.Backoff(attempt)
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
		next, err := Start(s.ctx, s.spec)

		s.mu.Lock()
		if s.isStopping() {
			s.mu.Unlock()
			if err == nil {
				// Installing it now would spawn a child after the host
				// decided to stop this plugin.
				_ = next.Stop(context.Background())
			}
			return nil, false
		}
		s.restarts++
		if err != nil {
			s.mu.Unlock()
			lastErr = err
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
	err := fmt.Errorf("pluginhost: %s crashed and its restart budget is spent after %d restarts%s",
		s.spec.label(), attempt, crashed.diagnosticsText())
	if lastErr != nil {
		err = fmt.Errorf("%w; last restart failed: %w", err, lastErr)
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
			if err == nil && health.OK {
				bad = 0
				continue
			}
			bad++
			if s.opts.KillAfterUnhealthy > 0 && bad >= s.opts.KillAfterUnhealthy {
				_ = p.Kill()
			}
		}
	}
}
