//go:build unix

package pluginhost_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pluginhost "github.com/hollis-labs/plugin-host"
)

type callbackGenerationStore struct {
	next func(context.Context) (uint64, error)
}

func (s callbackGenerationStore) Next(ctx context.Context, _ string, _ string) (uint64, error) {
	return s.next(ctx)
}
func TestEveryLoadCallbackCanBeFencedWithoutWaitingForHostCode(t *testing.T) {
	for _, step := range []string{"plan", "resolve", "compatibility", "prepare", "activate", "reload", "generation", "before_spawn"} {
		t.Run(step, func(t *testing.T) {
			o := lifecycleOptions(t)
			o.CallbackTimeout = time.Second
			base := o.Callbacks.Plan
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			entered := make(chan struct{})
			wait := func() { close(entered); <-release }
			var child *pluginhost.Process
			switch step {
			case "plan":
				o.Callbacks.Plan = func(ctx context.Context) (pluginhost.Plan, error) { wait(); return base(ctx) }
			case "resolve":
				o.Callbacks.Resolve = func(_ context.Context, p pluginhost.Plan) (pluginhost.Plan, error) { wait(); return p, nil }
			case "compatibility":
				o.Callbacks.CheckCompatibility = func(context.Context, pluginhost.Plan) error { wait(); return nil }
			case "prepare":
				o.Callbacks.PrepareScope = func(_ context.Context, _ pluginhost.Owner, p pluginhost.Plan) (pluginhost.Spec, error) {
					wait()
					return p.Spec, nil
				}
			case "activate":
				o.Callbacks.Activate = func(_ context.Context, _ pluginhost.Owner, p *pluginhost.Process) error {
					child = p
					wait()
					return nil
				}
			case "reload":
				o.Callbacks.BeforeReload = func(context.Context) error { wait(); return nil }
			case "generation":
				o.Generations = callbackGenerationStore{next: func(context.Context) (uint64, error) { wait(); return 1, nil }}
			case "before_spawn":
				o.Callbacks.Plan = func(ctx context.Context) (pluginhost.Plan, error) {
					p, e := base(ctx)
					p.Spec.BeforeSpawn = func(context.Context) error { wait(); return nil }
					return p, e
				}
			}
			l := newController(t, o)
			started := make(chan error, 1)
			if step == "reload" {
				enableController(t, l)
				child = l.Current()
				go func() { started <- l.Reload(context.Background()) }()
			} else {
				go func() { started <- l.Enable(context.Background()) }()
			}
			<-entered
			stopped := make(chan error, 1)
			go func() { stopped <- l.Disable(context.Background()) }()
			select {
			case <-stopped:
			case <-time.After(time.Second):
				unblock()
				<-stopped
				t.Fatal("Disable waited for uncooperative host code")
			}
			if err := awaitResult(t, started); err == nil {
				t.Fatal("late callback succeeded")
			}
			if child != nil {
				select {
				case <-child.Exited():
				default:
					t.Fatal("child survived accepted Disable")
				}
			}
			if !errors.Is(l.Enable(context.Background()), pluginhost.ErrQuarantined) {
				t.Fatal("pending callback did not quarantine")
			}
			unblock()
			for _, report := range l.Status().Disposals {
				if report.Incomplete {
					eventually(t, time.Second, "report reconciliation", func() bool { return l.AcknowledgeReport(context.Background(), report.ID) == nil })
				}
			}
			if l.Status().State == pluginhost.StateQuarantined {
				t.Fatal("reconciled reports still quarantine")
			}
		})
	}
}

type countedStateStore struct {
	pluginhost.MemoryLifecycleStateStore
	calls   atomic.Int32
	failAt  int32
	refusal error
	block   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (s *countedStateStore) Save(ctx context.Context, id string, r pluginhost.LifecycleRecord) error {
	n := s.calls.Add(1)
	if s.block.Load() {
		select {
		case s.entered <- struct{}{}:
		default:
		}
		<-s.release
	}
	if n == s.failAt {
		return s.refusal
	}
	return s.MemoryLifecycleStateStore.Save(ctx, id, r)
}
func TestStateSaveFailuresRefuseLoadAndQuarantineDisposal(t *testing.T) {
	for _, at := range []int32{1, 2} {
		t.Run(map[int32]string{1: "load", 2: "dispose"}[at], func(t *testing.T) {
			o := lifecycleOptions(t)
			refusal := errors.New("durable state write refused")
			store := &countedStateStore{failAt: at, refusal: refusal}
			o.StateStore = store
			l := newController(t, o)
			if at == 1 {
				if err := l.Enable(context.Background()); !errors.Is(err, refusal) || l.Current() != nil {
					t.Fatal("failed checkpoint spawned", err)
				}
				return
			}
			enableController(t, l)
			owner := l.Status().Owner
			child := l.Current()
			if err := l.Disable(context.Background()); !errors.Is(err, refusal) {
				t.Fatal("disposal persistence error lost", err)
			}
			select {
			case <-child.Exited():
			default:
				t.Fatal("persistence refusal left child")
			}
			if !errors.Is(l.Enable(context.Background()), pluginhost.ErrQuarantined) {
				t.Fatal("disposal save refusal ignored")
			}
			// The pre-spawn active checkpoint survives a missing final save. A new
			// host epoch cannot interpret absence of a cleanup record as success.
			o.HostInstance, _ = pluginhost.NewHostInstance()
			restarted := newController(t, o)
			if !errors.Is(restarted.Enable(context.Background()), pluginhost.ErrQuarantined) {
				t.Fatal("host restart bypassed failed disposal save")
			}
			if err := restarted.AcknowledgeDisposal(context.Background(), owner); err != nil {
				t.Fatal(err)
			}
			enableController(t, restarted)
			if restarted.Status().Owner.HostInstance == owner.HostInstance || restarted.Status().Owner.OwnerGeneration != 1 {
				t.Fatal("generation counter crossed epochs")
			}
		})
	}
}
func TestAcknowledgementStoreWaitDoesNotOwnTheOperationGate(t *testing.T) {
	o := lifecycleOptions(t)
	o.CleanupTimeout = 300 * time.Millisecond
	store := &countedStateStore{entered: make(chan struct{}, 2), release: make(chan struct{})}
	o.StateStore = store
	var once sync.Once
	unblock := func() { store.block.Store(false); once.Do(func() { close(store.release) }) }
	defer unblock()
	l := newController(t, o)
	enableController(t, l)
	old := l.Status().Owner
	if err := l.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	child := l.Current()
	store.block.Store(true)
	ack := make(chan error, 1)
	go func() { ack <- l.AcknowledgeDisposal(context.Background(), old) }()
	<-store.entered
	stopped := make(chan error, 1)
	go func() { stopped <- l.Disable(context.Background()) }()
	// The child must stop while the acknowledgement is STILL blocked, before
	// its 300ms save bound can release a mistakenly held operation gate.
	select {
	case <-child.Exited():
	case <-time.After(100 * time.Millisecond):
		unblock()
		<-ack
		<-stopped
		t.Fatal("acknowledgement held operation gate")
	}
	if !errors.Is(awaitResult(t, ack), context.DeadlineExceeded) {
		t.Fatal("acknowledgement ignored cleanup bound")
	}
	_ = awaitResult(t, stopped)
	unblock()
}
func TestHungLoadCheckpointDoesNotLeakRepeatedStoreCalls(t *testing.T) {
	o := lifecycleOptions(t)
	store := &countedStateStore{entered: make(chan struct{}, 2), release: make(chan struct{})}
	store.block.Store(true)
	o.StateStore = store
	l := newController(t, o)
	if err := l.Enable(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	for range 5 {
		if !errors.Is(l.Enable(context.Background()), pluginhost.ErrQuarantined) {
			t.Fatal("repeated operation entered hung store")
		}
	}
	if store.calls.Load() != 1 {
		t.Fatal("hung save leaked repeated calls", store.calls.Load())
	}
	store.block.Store(false)
	close(store.release)
	for _, r := range l.Status().Disposals {
		eventually(t, time.Second, "store completion", func() bool { return l.AcknowledgeReport(context.Background(), r.ID) == nil })
	}
}
func TestQuarantinePersistsAcrossEpochsAndInvalidRecordsHaveOwnError(t *testing.T) {
	o := lifecycleOptions(t)
	store := &pluginhost.MemoryLifecycleStateStore{}
	o.StateStore = store
	owner := pluginhost.Owner{HostInstance: o.HostInstance, OwnerID: "fixture", OwnerGeneration: 100}
	record := pluginhost.LifecycleRecord{HostInstance: o.HostInstance, Revision: 1, LastGeneration: 100, Disposals: []pluginhost.DisposalReport{{ID: "old-host-report", Owner: owner, Incomplete: true}}}
	if err := store.Save(context.Background(), "fixture", record); err != nil {
		t.Fatal(err)
	}
	o.HostInstance, _ = pluginhost.NewHostInstance()
	l := newController(t, o)
	if !errors.Is(l.Enable(context.Background()), pluginhost.ErrQuarantined) {
		t.Fatal("new epoch forgot quarantine")
	}
	if err := l.AcknowledgeDisposal(context.Background(), owner); err != nil {
		t.Fatal(err)
	}
	enableController(t, l)
	if l.Status().Owner.OwnerGeneration != 1 {
		t.Fatal("old epoch generation leaked")
	}
	bad := &pluginhost.MemoryLifecycleStateStore{}
	record.Disposals[0].Owner.OwnerID = "wrong-owner"
	if err := bad.Save(context.Background(), "fixture", record); err != nil {
		t.Fatal(err)
	}
	o.StateStore = bad
	if _, err := pluginhost.NewLifecycle("fixture", o); !errors.Is(err, pluginhost.ErrInvalidLifecycleRecord) || errors.Is(err, pluginhost.ErrInvalidGeneration) {
		t.Fatal("misleading persisted record error", err)
	}
}

func TestCallbackDeadlineDiscardsLatePlanAndRequiresReportAcknowledgement(t *testing.T) {
	o := lifecycleOptions(t)
	o.CallbackTimeout = 20 * time.Millisecond
	base := o.Callbacks.Plan
	release := make(chan struct{})
	o.Callbacks.Plan = func(context.Context) (pluginhost.Plan, error) { <-release; return base(context.Background()) }
	l := newController(t, o)
	if err := l.Enable(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		close(release)
		t.Fatal("callback deadline lost", err)
	}
	report := l.Status().Disposal
	if report.ID == "" || report.Owner.OwnerGeneration != 0 || !report.Incomplete {
		close(release)
		t.Fatal("pre-generation report identity lost", report)
	}
	if !errors.Is(l.AcknowledgeReport(context.Background(), report.ID), pluginhost.ErrCleanupPending) {
		close(release)
		t.Fatal("live plan acknowledged")
	}
	close(release)
	eventually(t, time.Second, "late plan completion", func() bool { return l.AcknowledgeReport(context.Background(), report.ID) == nil })
	if l.Current() != nil {
		t.Fatal("discarded late plan spawned")
	}
}

type hangingLoadStore struct {
	pluginhost.MemoryLifecycleStateStore
	calls   atomic.Int32
	block   atomic.Bool
	release chan struct{}
}

func (s *hangingLoadStore) Load(ctx context.Context, id string) (pluginhost.LifecycleRecord, error) {
	s.calls.Add(1)
	if s.block.Load() {
		<-s.release
	}
	return s.MemoryLifecycleStateStore.Load(ctx, id)
}
func TestConstructorDoesNotRepeatHungStateRead(t *testing.T) {
	o := lifecycleOptions(t)
	store := &hangingLoadStore{release: make(chan struct{})}
	store.block.Store(true)
	o.StateStore = store
	if _, err := pluginhost.NewLifecycle("fixture", o); !errors.Is(err, pluginhost.ErrStateStorePending) || !errors.Is(err, context.DeadlineExceeded) {
		close(store.release)
		t.Fatal("state read timeout classification", err)
	}
	for range 5 {
		if _, err := pluginhost.NewLifecycle("fixture", o); !errors.Is(err, pluginhost.ErrStateStorePending) {
			close(store.release)
			t.Fatal("repeated hung read admitted", err)
		}
	}
	if store.calls.Load() != 1 {
		close(store.release)
		t.Fatal("constructor leaked readers", store.calls.Load())
	}
	store.block.Store(false)
	close(store.release)
	var l *pluginhost.Lifecycle
	eventually(t, time.Second, "state read completion", func() bool { var err error; l, err = pluginhost.NewLifecycle("fixture", o); return err == nil })
	for _, r := range l.Status().Disposals {
		if r.Incomplete {
			if err := l.AcknowledgeReport(context.Background(), r.ID); err != nil {
				t.Fatal(err)
			}
		}
	}
}
func TestRealReloadHistoryAndPersistedSnapshotsStayBounded(t *testing.T) {
	o := lifecycleOptions(t)
	store := &pluginhost.MemoryLifecycleStateStore{}
	o.StateStore = store
	l := newController(t, o)
	enableController(t, l)
	for range pluginhost.CompletedDisposalHistory + 5 {
		if err := l.Reload(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	reports := l.Status().Disposals
	if len(reports) != pluginhost.CompletedDisposalHistory || reports[0].Owner.OwnerGeneration != 6 {
		t.Fatal("completed tail not pruned", len(reports))
	}
	persisted, err := store.Load(context.Background(), "fixture")
	if err != nil || len(persisted.Disposals) > pluginhost.CompletedDisposalHistory {
		t.Fatal("persisted tail not bounded", err, len(persisted.Disposals))
	}
}

type suppliedStateRecord struct{ record pluginhost.LifecycleRecord }

func (s suppliedStateRecord) Load(context.Context, string) (pluginhost.LifecycleRecord, error) {
	return s.record, nil
}
func (s suppliedStateRecord) Save(context.Context, string, pluginhost.LifecycleRecord) error {
	return nil
}
func TestPersistedRecordCapFailsClosed(t *testing.T) {
	o := lifecycleOptions(t)
	record := pluginhost.LifecycleRecord{Disposals: make([]pluginhost.DisposalReport, pluginhost.MaxDisposalRecords+1)}
	for i := range record.Disposals {
		record.Disposals[i] = pluginhost.DisposalReport{ID: fmt.Sprintf("report-%d", i), Owner: pluginhost.Owner{HostInstance: o.HostInstance, OwnerID: "fixture", OwnerGeneration: uint64(i + 1)}, Incomplete: true}
	}
	memory := &pluginhost.MemoryLifecycleStateStore{}
	if err := memory.Save(context.Background(), "fixture", record); !errors.Is(err, pluginhost.ErrInvalidLifecycleRecord) {
		t.Fatal("oversized record persisted", err)
	}
	o.StateStore = suppliedStateRecord{record}
	if _, err := pluginhost.NewLifecycle("fixture", o); !errors.Is(err, pluginhost.ErrInvalidLifecycleRecord) {
		t.Fatal("oversized record restored", err)
	}
}
