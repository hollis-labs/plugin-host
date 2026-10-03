package pluginhost

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
)

// MaxOwnerGeneration is the largest integer represented exactly by Go/JS
// authority DTOs. Exhaustion fails closed; never wrap or reuse a generation.
const MaxOwnerGeneration uint64 = 9007199254740991

// LifecycleRecord persists an epoch-specific generation watermark and owner
// disposal history across host restarts.
// Revision orders snapshots, including acknowledgements of incomplete reports.
type LifecycleRecord struct {
	HostInstance   string
	Revision       uint64
	LastGeneration uint64
	Disposals      []DisposalReport
	// Active is written before scope preparation. A restart without completed
	// disposal restores quarantine even if the final cleanup save failed.
	Active *Owner
}

// LifecycleStateStore is optional host storage for quarantine/history. Load
// is keyed by stable owner ID, not host epoch. Load returns the latest snapshot.
// Save must atomically reject an older Revision;
// an isolated, timed-out save may finish after a newer save. Save must not
// discard any unreconciled report. Enabled preference is still host-owned.
type LifecycleStateStore interface {
	Load(context.Context, string) (LifecycleRecord, error)
	Save(context.Context, string, LifecycleRecord) error
}

// MemoryLifecycleStateStore shares state across controller recreation. Durable
// hosts implement the same atomic revision contract in their own storage.
type MemoryLifecycleStateStore struct {
	mu      sync.Mutex
	records map[string]LifecycleRecord
}

func (s *MemoryLifecycleStateStore) Load(ctx context.Context, id string) (LifecycleRecord, error) {
	if err := ctx.Err(); err != nil {
		return LifecycleRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneRecord(s.records[id]), nil
}
func (s *MemoryLifecycleStateStore) Save(ctx context.Context, id string, r LifecycleRecord) error {
	if len(r.Disposals) > MaxDisposalRecords {
		return ErrInvalidLifecycleRecord
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.records == nil {
		s.records = make(map[string]LifecycleRecord)
	}
	key := id
	if r.Revision < s.records[key].Revision {
		return errors.New("pluginhost: stale lifecycle snapshot")
	}
	s.records[key] = cloneRecord(r)
	return nil
}

// The process ledger independently validates host-issued generations and
// retains quarantine across controller recreation, including without a store.
// Generation accounting has a separate key for each host epoch. Durable
// owner quarantine crosses epochs through LifecycleStateStore.
var processLedger = struct {
	sync.Mutex
	records map[[2]string]*authorityRecord
}{records: make(map[[2]string]*authorityRecord)}

type authorityRecord struct {
	record        LifecycleRecord
	pending       map[string][]<-chan struct{}
	acknowledging map[string]bool
}

func cloneReport(r DisposalReport) DisposalReport { r.Failures = slices.Clone(r.Failures); return r }
func cloneRecord(r LifecycleRecord) LifecycleRecord {
	if r.Active != nil {
		owner := *r.Active
		r.Active = &owner
	}
	r.Disposals = slices.Clone(r.Disposals)
	for i := range r.Disposals {
		r.Disposals[i] = cloneReport(r.Disposals[i])
	}
	return r
}
func ledgerRecord(key [2]string) *authorityRecord {
	r := processLedger.records[key]
	if r == nil {
		r = &authorityRecord{pending: make(map[string][]<-chan struct{}), acknowledging: make(map[string]bool)}
		processLedger.records[key] = r
	}
	return r
}
func (l *Lifecycle) key() [2]string { return [2]string{l.opts.HostInstance, l.id} }
func (l *Lifecycle) history() LifecycleRecord {
	processLedger.Lock()
	defer processLedger.Unlock()
	a := ledgerRecord(l.key())
	pruneRecord(a)
	return cloneRecord(a.record)
}

// CompletedDisposalHistory is the retained completed/acknowledged tail.
const CompletedDisposalHistory = 32

// MaxDisposalRecords caps persisted input/output; unresolved reports are never
// silently dropped. Oversized durable input fails closed.
const MaxDisposalRecords = 128

func pruneRecord(a *authorityRecord) {
	completed := 0
	for _, d := range a.record.Disposals {
		if !d.Incomplete || d.Acknowledged {
			completed++
		}
	}
	kept := a.record.Disposals[:0]
	for _, d := range a.record.Disposals {
		if !d.Incomplete || d.Acknowledged {
			if completed > CompletedDisposalHistory {
				completed--
				delete(a.pending, d.ID)
				continue
			}
		}
		kept = append(kept, d)
	}
	clear(a.record.Disposals[len(kept):])
	a.record.Disposals = kept
	for id, pending := range a.pending {
		live := pending[:0]
		for _, done := range pending {
			select {
			case <-done:
			default:
				live = append(live, done)
			}
		}
		if len(live) == 0 {
			delete(a.pending, id)
		} else {
			a.pending[id] = live
		}
	}
}
func (l *Lifecycle) restore(r LifecycleRecord) error {
	processLedger.Lock()
	defer processLedger.Unlock()
	a := ledgerRecord(l.key())
	if r.LastGeneration > MaxOwnerGeneration || len(r.Disposals) > MaxDisposalRecords || (r.LastGeneration > 0 && r.HostInstance == "") {
		return ErrInvalidLifecycleRecord
	}
	if r.Active != nil && (r.Active.OwnerID != l.id || r.Active.HostInstance == "" || r.Active.OwnerGeneration == 0 || r.Active.OwnerGeneration > MaxOwnerGeneration) {
		return ErrInvalidLifecycleRecord
	}
	seen := make(map[string]bool, len(r.Disposals))
	for _, d := range r.Disposals {
		if seen[d.ID] {
			return ErrInvalidLifecycleRecord
		}
		seen[d.ID] = true
		if r.Active != nil && d.Owner == *r.Active && (!d.Incomplete || d.Acknowledged) {
			return ErrInvalidLifecycleRecord
		}
		if d.Owner.OwnerID != l.id || d.Owner.HostInstance == "" || d.Owner.OwnerGeneration > MaxOwnerGeneration || d.ID == "" {
			return ErrInvalidLifecycleRecord
		}
	}
	if r.HostInstance == l.opts.HostInstance {
		a.record.LastGeneration = max(a.record.LastGeneration, r.LastGeneration)
	}
	a.record.HostInstance = l.opts.HostInstance
	a.record.Revision = max(a.record.Revision, r.Revision)
	for _, d := range r.Disposals {
		found := false
		for _, existing := range a.record.Disposals {
			if existing.ID == d.ID {
				found = true
				break
			}
		}
		if !found {
			a.record.Disposals = append(a.record.Disposals, cloneReport(d))
		}
	}
	if r.Active != nil {
		active := *r.Active
		a.record.Active = &active
		found := false
		for _, d := range a.record.Disposals {
			if d.Owner == *r.Active {
				found = true
				break
			}
		}
		if !found {
			a.record.Revision++
			a.record.Disposals = append(a.record.Disposals, DisposalReport{ID: fmt.Sprintf("%s#%d", l.opts.HostInstance, a.record.Revision), Owner: *r.Active, Incomplete: true, Failures: []CleanupFailure{{Step: "restart", Cause: ErrUnreconciledIncarnation}}})
		}
	}
	pruneRecord(a)
	if len(a.record.Disposals) > MaxDisposalRecords {
		return ErrInvalidLifecycleRecord
	}
	return nil
}
func (l *Lifecycle) reserve(g uint64) error {
	processLedger.Lock()
	defer processLedger.Unlock()
	a := ledgerRecord(l.key())
	if g == 0 || g > MaxOwnerGeneration || g <= a.record.LastGeneration {
		return ErrInvalidGeneration
	}
	a.record.HostInstance = l.opts.HostInstance
	a.record.LastGeneration = g
	a.record.Active = &Owner{HostInstance: l.opts.HostInstance, OwnerID: l.id, OwnerGeneration: g}
	a.record.Revision++
	return nil
}
func (l *Lifecycle) recordDisposal(r DisposalReport, pending []<-chan struct{}) DisposalReport {
	processLedger.Lock()
	defer processLedger.Unlock()
	a := ledgerRecord(l.key())
	a.record.HostInstance = l.opts.HostInstance
	a.record.Revision++
	if r.ID == "" {
		r.ID = fmt.Sprintf("%s#%d", l.opts.HostInstance, a.record.Revision)
	}
	found := false
	for i := range a.record.Disposals {
		if a.record.Disposals[i].ID == r.ID {
			a.record.Disposals[i] = cloneReport(r)
			found = true
			break
		}
	}
	if !found {
		a.record.Disposals = append(a.record.Disposals, cloneReport(r))
	}
	if !r.Incomplete && a.record.Active != nil && *a.record.Active == r.Owner {
		a.record.Active = nil
	}
	a.pending[r.ID] = append(a.pending[r.ID], pending...)
	pruneRecord(a)
	return r
}
func (l *Lifecycle) pendingCallbacks(reportID string) bool {
	processLedger.Lock()
	defer processLedger.Unlock()
	var owner Owner
	for _, r := range ledgerRecord(l.key()).record.Disposals {
		if r.ID == reportID {
			owner = r.Owner
			break
		}
	}
	for key, a := range processLedger.records {
		if key[1] != l.id {
			continue
		}
		pruneRecord(a)
		if len(a.pending[reportID]) > 0 {
			return true
		}
		if owner.OwnerGeneration > 0 {
			for _, r := range a.record.Disposals {
				if r.Owner == owner && len(a.pending[r.ID]) > 0 {
					return true
				}
			}
		}
	}
	return false
}
func (l *Lifecycle) notePending(owner Owner, step string, done <-chan struct{}, cause error) {
	r := l.recordDisposal(DisposalReport{Owner: owner, Incomplete: true, Failures: []CleanupFailure{{Step: step, Cause: cause}}}, []<-chan struct{}{done})
	l.mu.Lock()
	l.status.Disposal = r
	l.status.State = StateQuarantined
	l.mu.Unlock()
}
func (l *Lifecycle) quarantined() bool {
	for _, r := range l.history().Disposals {
		if r.Incomplete && !r.Acknowledged {
			return true
		}
	}
	return false
}
func (l *Lifecycle) persist(ctx context.Context) error {
	if l.opts.StateStore == nil {
		return nil
	}
	r := l.history()
	if len(r.Disposals) > MaxDisposalRecords {
		return ErrInvalidLifecycleRecord
	}
	_, done, err := isolated(ctx, func() (struct{}, error) { return struct{}{}, l.opts.StateStore.Save(ctx, l.id, r) })
	if done != nil {
		return &pendingCallbackError{Cause: err, Done: done}
	}
	return err
}

// AcknowledgeDisposal is an explicit host assertion that this exact report's
// dispatch/resources are safe. It never runs cleanup again, or enables a child.
// A stale/wrong tuple or still-running isolated callback is refused. Optional
// persistence must succeed before the acknowledgement becomes effective.
func (l *Lifecycle) AcknowledgeDisposal(ctx context.Context, owner Owner) error {
	if owner.OwnerGeneration == 0 {
		return ErrUnknownDisposal
	}
	for _, r := range l.history().Disposals {
		if r.Owner == owner {
			return l.AcknowledgeReport(ctx, r.ID)
		}
	}
	return ErrUnknownDisposal
}

// AcknowledgeReport reconciles an exact report ID, including callbacks that
// timed out before a generation existed. No operation gate is held across Save.
func (l *Lifecycle) AcknowledgeReport(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if l.pendingCallbacks(id) {
		return ErrCleanupPending
	}
	processLedger.Lock()
	a := ledgerRecord(l.key())
	if a.acknowledging[id] {
		processLedger.Unlock()
		return ErrCleanupPending
	}
	r := cloneRecord(a.record)
	index := -1
	for i, d := range r.Disposals {
		if d.ID == id {
			index = i
			break
		}
	}
	if index < 0 {
		processLedger.Unlock()
		return ErrUnknownDisposal
	}
	if r.Disposals[index].Acknowledged {
		processLedger.Unlock()
		return nil
	}
	a.record.Revision++
	r.Revision = a.record.Revision
	base := r.Revision
	a.acknowledging[id] = true
	defer func() { processLedger.Lock(); delete(ledgerRecord(l.key()).acknowledging, id); processLedger.Unlock() }()
	r.Disposals[index].Acknowledged = true
	if r.Active != nil && *r.Active == r.Disposals[index].Owner {
		r.Active = nil
	}
	processLedger.Unlock()
	if l.opts.StateStore != nil {
		saveCtx, cancel := context.WithTimeout(ctx, l.opts.CleanupTimeout)
		_, done, err := isolated(saveCtx, func() (struct{}, error) { return struct{}{}, l.opts.StateStore.Save(saveCtx, l.id, r) })
		cancel()
		if err != nil {
			if done != nil {
				processLedger.Lock()
				a = ledgerRecord(l.key())
				a.pending[id] = append(a.pending[id], done)
				processLedger.Unlock()
			}
			return err
		}
	}
	acknowledged := cloneReport(r.Disposals[index])
	processLedger.Lock()
	a = ledgerRecord(l.key())
	if a.record.Revision != base {
		processLedger.Unlock()
		return ErrLifecycleStateChanged
	}
	a.record = r
	delete(a.pending, id)
	pruneRecord(a)
	processLedger.Unlock()
	l.mu.Lock()
	if l.status.Disposal.ID == id {
		l.status.Disposal = acknowledged
	}
	if l.status.State == StateQuarantined && !l.quarantined() {
		if l.status.DesiredEnabled {
			l.status.State = StateFailed
		} else {
			l.status.State = StateDisabled
		}
	}
	l.mu.Unlock()
	return nil
}

type pendingCallbackError struct {
	Cause error
	Done  <-chan struct{}
}

func (e *pendingCallbackError) Error() string { return e.Cause.Error() }
func (e *pendingCallbackError) Unwrap() error { return e.Cause }

// isolated never lets uncooperative host code prevent cancellation/cleanup. It does not kill Go code: a timeout keeps quarantine until the host
// reconciles, and late results are never written into controller state.
func isolated[T any](ctx context.Context, fn func() (T, error)) (T, <-chan struct{}, error) {
	if err := ctx.Err(); err != nil {
		var zero T
		return zero, nil, err
	}
	type result struct {
		value T
		err   error
	}
	reply := make(chan result, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		var value T
		err := callback(ctx, func() (err error) { value, err = fn(); return err })
		reply <- result{value, err}
	}() //nolint:gosec // G118: callback owns this bounded cleanup context; late completion is quarantined
	select {
	case r := <-reply:
		return r.value, nil, r.err
	case <-ctx.Done():
		var zero T
		return zero, done, ctx.Err()
	}
}
