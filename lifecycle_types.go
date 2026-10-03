package pluginhost

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
)

// Stage identifies the ordered load phase. Host policy remains in callbacks.
type Stage string

const (
	StagePlan    Stage = "plan"
	StageResolve Stage = "resolve"
	StageCompat  Stage = "compat"
	StageLoad    Stage = "load"
)

// Failure preserves the original cause without exposing its potentially
// sensitive text in operator diagnostics. Inspect Cause with errors.Is/As.
type Failure struct {
	PluginID   string
	Generation uint64
	Stage      Stage
	Step       string
	Code       string
	Retryable  bool
	Cause      error
}

func (f *Failure) Error() string {
	return fmt.Sprintf("pluginhost: %s generation %d: %s/%s (%s)", boundedLabel(f.PluginID), f.Generation, boundedLabel(string(f.Stage)), boundedLabel(f.Step), boundedLabel(f.Code))
}
func (f *Failure) Unwrap() error { return f.Cause }

// TransientError is an explicit host/library classification, not an inference
// from a timeout or an unexpected process exit. Code must be a safe label.
type TransientError struct {
	Code  string
	Cause error
}

func (e *TransientError) Error() string { return "pluginhost: transient " + boundedLabel(e.Code) }
func (e *TransientError) Unwrap() error { return e.Cause }

// IsTransient never retries cancellation, timeouts or permanent wire failures.
func IsTransient(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrProtocolMismatch) || errors.Is(err, ErrNoPluginID) || errors.Is(err, ErrIdentityMismatch) || errors.Is(err, ErrVersionMismatch) {
		return false
	}
	var f *Failure
	if errors.As(err, &f) {
		return f.Retryable && IsTransient(f.Cause)
	}
	var t *TransientError
	return errors.As(err, &t) && t.Code != ""
}

// Owner is the canonical runtime authority tuple. Never reuse a generation.
type Owner struct {
	HostInstance    string
	OwnerID         string
	OwnerGeneration uint64
}

// NewHostInstance creates a random epoch once per host process. Share this
// value and the GenerationStore across every controller in that process.
func NewHostInstance() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// GenerationStore atomically persists and returns a number strictly greater
// than every previously issued number for this (host instance, owner ID).
// The host must share the store across controller recreation. Errors fail
// before scope preparation/spawn; a consumed number is never rolled back.
type GenerationStore interface {
	Next(context.Context, string, string) (uint64, error)
}

// MemoryGenerationStore is safe across controllers, but not host restarts.
// Host restarts MUST create a new random epoch.
type MemoryGenerationStore struct {
	mu     sync.Mutex
	values map[[2]string]uint64
}

func (s *MemoryGenerationStore) Next(ctx context.Context, host, id string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.values == nil {
		s.values = make(map[[2]string]uint64)
	}
	key := [2]string{host, id}
	if s.values[key] == ^uint64(0) {
		return 0, errors.New("pluginhost: generation exhausted")
	}
	s.values[key]++
	return s.values[key], nil
}

// CleanupFailure identifies one failed cleanup action in execution order.
type CleanupFailure struct {
	Step  string
	Cause error
}

func (f CleanupFailure) Error() string {
	return "pluginhost: cleanup " + boundedLabel(f.Step) + " failed"
}
func (f CleanupFailure) Unwrap() error { return f.Cause }

// DisposalReport preserves cleanup errors separately from the load failure.
// Incomplete means replacement is quarantined until host reconciliation.
type DisposalReport struct {
	Owner      Owner
	Failures   []CleanupFailure
	Incomplete bool
}

func (r DisposalReport) Error() string {
	return fmt.Sprintf("pluginhost: disposal generation %d: %d failures, incomplete=%t", r.Owner.OwnerGeneration, len(r.Failures), r.Incomplete)
}
func (r DisposalReport) Unwrap() []error {
	out := make([]error, len(r.Failures))
	for i := range r.Failures {
		out[i] = r.Failures[i]
	}
	return out
}

var (
	ErrIdentityMismatch = errors.New("pluginhost: plugin identity differs from plan")
	ErrVersionMismatch  = errors.New("pluginhost: plugin version differs from plan")
	ErrQuarantined      = errors.New("pluginhost: incomplete disposal requires host reconciliation")
	ErrDisabled         = errors.New("pluginhost: operation superseded by disable")
	ErrCallbackPanic    = errors.New("pluginhost: lifecycle callback panicked")
	// ErrDependency is returned by a host preflight when loaded dependents
	// prevent disable/reload. Dependency ordering/cascade is host policy.
	ErrDependency = errors.New("pluginhost: loaded dependents prevent operation")
)

// Labels originate in host policy. Bound them, and never include raw causes.
func boundedLabel(s string) string {
	if len(s) > 96 {
		return s[:96] + "..."
	}
	return s
}
