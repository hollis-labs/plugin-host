package pluginhost

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ExitStatus is the last failed, reaped child in an attempt cycle. Owner identifies
// that child's authority tuple, including when a replacement is running.
// StderrTail passes through Tail and Spec.Redact and is bounded to the
// configured StderrBytes (default 4096), even if the host redactor expands it.
// Treat Info.Err and other retained error causes as immutable.
type ExitStatus struct {
	Owner      Owner
	Info       ExitInfo
	StderrTail string
}

// SupervisorStatus is a copied snapshot. Exhausted means a retryable failure
// could not restart because the policy budget ran out (including a disabled
// restart budget), not because classification or Init authority refused it.
// LastFailure is the terminal supervision/start failure; LastExit may remain
// available after a successful automatic restart or Stop.
type SupervisorStatus struct {
	Running     bool
	Stopped     bool
	Restarts    int
	Exhausted   bool
	LastFailure *Failure
	LastExit    *ExitStatus
}

func (s *Supervisor) Status() SupervisorStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	running := s.cur != nil && !s.stopped
	if running {
		select {
		case <-s.cur.Exited():
			running = false
		default:
		}
	}
	return SupervisorStatus{Running: running, Stopped: s.stopped, Restarts: s.restarts, Exhausted: s.exhausted, LastFailure: cloneFailure(s.lastFailure), LastExit: cloneExitStatus(s.lastExit)}
}

func (s SupervisorStatus) String() string {
	text := fmt.Sprintf("running=%t stopped=%t restarts=%d", s.Running, s.Stopped, s.Restarts)
	if s.Exhausted {
		text += " restart attempts exhausted"
	}
	return exitSummary(text, s.LastExit)
}

func exitSummary(text string, exit *ExitStatus) string {
	if exit != nil {
		text += fmt.Sprintf(" exit=%d signal=%s", exit.Info.Code, boundedLabel(exit.Info.Signal))
	}
	return text
}

func cloneFailure(f *Failure) *Failure {
	if f == nil {
		return nil
	}
	snapshot := *f
	return &snapshot
}
func cloneExitStatus(exit *ExitStatus) *ExitStatus {
	if exit == nil {
		return nil
	}
	snapshot := *exit
	return &snapshot
}

func snapshotExit(p *Process) *ExitStatus {
	if p == nil {
		return nil
	}
	info, done := p.ExitInfo()
	if !done {
		return nil
	}
	text := retainedStderr(p)
	tuple := p.spec.Init.Incarnation
	return &ExitStatus{Owner: Owner{HostInstance: tuple.HostInstance, OwnerID: tuple.OwnerID, OwnerGeneration: tuple.OwnerGeneration}, Info: info, StderrTail: text}
}

// Own the bounded bytes so a redactor's large string (or a small view into one)
// cannot remain reachable through a status snapshot or formatted process error.
func retainedStderr(p *Process) string {
	text := p.Diagnostics() // Calls the host redactor without a status lock held.
	limit := p.tail.limit()
	if len(text) > limit {
		text = text[len(text)-limit:]
	}
	return strings.Clone(text)
}

func (s *Supervisor) recordExit(p *Process) {
	exit := snapshotExit(p)
	if exit == nil {
		return
	}
	s.mu.Lock()
	s.lastExit = exit
	s.mu.Unlock()
}

// Retain the failed child's pointer after Handshake's bounded cleanup so status
// can report its exit and stderr. This is Start's spawn/handshake contract.
func startStatusProcess(ctx context.Context, spec Spec) (*Process, error) {
	p, err := Spawn(ctx, spec)
	if err != nil {
		var failure *Failure
		if errors.As(err, &failure) {
			return nil, err
		}
		return nil, processFailure(spec, "spawn", err)
	}
	_, _, err = p.Handshake(ctx)
	return p, err
}

func (l *Lifecycle) recordExit(p *Process) {
	exit := snapshotExit(p)
	if exit == nil {
		return
	}
	l.mu.Lock()
	l.status.LastExit = exit
	l.mu.Unlock()
}
