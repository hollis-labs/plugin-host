package pluginhost

import (
	"context"
	"errors"
	"slices"
	"sync"
)

// MaxOwnerGeneration is the largest integer represented exactly by Go/JS
// authority DTOs. Exhaustion fails closed; never wrap or reuse a generation.
const MaxOwnerGeneration uint64 = 9007199254740991

// LifecycleRecord persists generation high watermarks and disposal history.
// Revision orders snapshots, including acknowledgements of incomplete reports.
type LifecycleRecord struct {
	Revision       uint64
	LastGeneration uint64
	Disposals      []DisposalReport
}

// LifecycleStateStore is optional host storage for quarantine/history. Load
// returns the latest snapshot. Save must atomically reject an older Revision;
// an isolated, timed-out save may finish after a newer save. Save must not
// discard any unreconciled report. Enabled preference is still host-owned.
type LifecycleStateStore interface {
	Load(context.Context, string, string) (LifecycleRecord, error)
	Save(context.Context, string, string, LifecycleRecord) error
}

// MemoryLifecycleStateStore shares state across controller recreation. Durable
// hosts implement the same atomic revision contract in their own storage.
type MemoryLifecycleStateStore struct {
	mu      sync.Mutex
	records map[[2]string]LifecycleRecord
}

func (s *MemoryLifecycleStateStore) Load(ctx context.Context, host, id string) (LifecycleRecord, error) {
	if err := ctx.Err(); err != nil {
		return LifecycleRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneRecord(s.records[[2]string{host, id}]), nil
}
func (s *MemoryLifecycleStateStore) Save(ctx context.Context, host, id string, r LifecycleRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.records == nil {
		s.records = make(map[[2]string]LifecycleRecord)
	}
	key := [2]string{host, id}
	if r.Revision < s.records[key].Revision {
		return errors.New("pluginhost: stale lifecycle snapshot")
	}
	s.records[key] = cloneRecord(r)
	return nil
}

// The process ledger independently validates host-issued generations and
// retains quarantine across controller recreation, including without a store.
// A fresh random host epoch gives a separate key after a host restart.
var processLedger = struct {
	sync.Mutex
	records map[[2]string]*authorityRecord
}{records: make(map[[2]string]*authorityRecord)}

type authorityRecord struct {
	record  LifecycleRecord
	pending map[Owner][]<-chan struct{}
}

func cloneReport(r DisposalReport) DisposalReport { r.Failures = slices.Clone(r.Failures); return r }
func cloneRecord(r LifecycleRecord) LifecycleRecord {
	r.Disposals = slices.Clone(r.Disposals)
	for i := range r.Disposals {
		r.Disposals[i] = cloneReport(r.Disposals[i])
	}
	return r
}
func ledgerRecord(key [2]string) *authorityRecord {
	r := processLedger.records[key]
	if r == nil {
		r = &authorityRecord{pending: make(map[Owner][]<-chan struct{})}
		processLedger.records[key] = r
	}
	return r
}
func (l *Lifecycle) key() [2]string { return [2]string{l.opts.HostInstance, l.id} }
func (l *Lifecycle) history() LifecycleRecord {
	processLedger.Lock()
	defer processLedger.Unlock()
	return cloneRecord(ledgerRecord(l.key()).record)
}
func (l *Lifecycle) restore(r LifecycleRecord) error {
	processLedger.Lock()
	defer processLedger.Unlock()
	a := ledgerRecord(l.key())
	if r.LastGeneration > MaxOwnerGeneration {
		return ErrInvalidGeneration
	}
	for _, d := range r.Disposals {
		if d.Owner.HostInstance != l.opts.HostInstance || d.Owner.OwnerID != l.id || d.Owner.OwnerGeneration == 0 || d.Owner.OwnerGeneration > r.LastGeneration {
			return ErrInvalidGeneration
		}
	}
	a.record.LastGeneration = max(a.record.LastGeneration, r.LastGeneration)
	a.record.Revision = max(a.record.Revision, r.Revision)
	for _, d := range r.Disposals {
		found := false
		for _, existing := range a.record.Disposals {
			if existing.Owner == d.Owner {
				found = true
				break
			}
		}
		if !found {
			a.record.Disposals = append(a.record.Disposals, cloneReport(d))
		}
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
	a.record.LastGeneration = g
	a.record.Revision++
	return nil
}
func (l *Lifecycle) recordDisposal(r DisposalReport, pending []<-chan struct{}) {
	processLedger.Lock()
	defer processLedger.Unlock()
	a := ledgerRecord(l.key())
	found := false
	for i := range a.record.Disposals {
		if a.record.Disposals[i].Owner == r.Owner {
			a.record.Disposals[i] = cloneReport(r)
			found = true
			break
		}
	}
	if !found {
		a.record.Disposals = append(a.record.Disposals, cloneReport(r))
	}
	a.pending[r.Owner] = pending
	a.record.Revision++
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
	_, _, err := isolated(ctx, func() (struct{}, error) { return struct{}{}, l.opts.StateStore.Save(ctx, l.opts.HostInstance, l.id, r) })
	return err
}

// AcknowledgeDisposal is an explicit host assertion that this exact report's
// dispatch/resources are safe. It never runs cleanup again, or enables a child.
// A stale/wrong tuple or still-running isolated callback is refused. Optional
// persistence must succeed before the acknowledgement becomes effective.
func (l *Lifecycle) AcknowledgeDisposal(ctx context.Context, owner Owner) error {
	if err := l.acquire(ctx); err != nil {
		return err
	}
	defer l.release()
	processLedger.Lock()
	a := ledgerRecord(l.key())
	r := cloneRecord(a.record)
	index := -1
	for i, d := range r.Disposals {
		if d.Owner == owner {
			index = i
			break
		}
	}
	if index < 0 || owner.HostInstance != l.opts.HostInstance || owner.OwnerID != l.id {
		processLedger.Unlock()
		return ErrUnknownDisposal
	}
	for _, done := range a.pending[owner] {
		select {
		case <-done:
		default:
			processLedger.Unlock()
			return ErrCleanupPending
		}
	}
	if r.Disposals[index].Acknowledged {
		processLedger.Unlock()
		return nil
	}
	r.Revision++
	r.Disposals[index].Acknowledged = true
	processLedger.Unlock()
	if l.opts.StateStore != nil {
		_, _, err := isolated(ctx, func() (struct{}, error) { return struct{}{}, l.opts.StateStore.Save(ctx, l.opts.HostInstance, l.id, r) })
		if err != nil {
			return err
		}
	}
	processLedger.Lock()
	a = ledgerRecord(l.key())
	a.record = r
	delete(a.pending, owner)
	processLedger.Unlock()
	l.mu.Lock()
	if l.status.Disposal.Owner == owner {
		l.status.Disposal = r.Disposals[index]
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

// isolated never lets an uncooperative cleanup callback prevent subsequent
// cleanup. It does not kill Go code: a timeout keeps quarantine until the host
// reconciles, and late results are never written into controller state.
func isolated[T any](ctx context.Context, fn func() (T, error)) (T, <-chan struct{}, error) {
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
