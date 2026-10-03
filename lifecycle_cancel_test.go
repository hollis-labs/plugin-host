//go:build unix

package pluginhost_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pluginhost "github.com/hollis-labs/plugin-host"
)

func TestCooperativeCancellationNeedsNoAcknowledgement(t *testing.T) {
	for _, step := range []string{"plan", "activate"} {
		t.Run(step, func(t *testing.T) {
			o := lifecycleOptions(t)
			entered := make(chan struct{}, 1)
			cooperative := func(ctx context.Context) error { entered <- struct{}{}; <-ctx.Done(); return ctx.Err() }
			base := o.Callbacks.Plan
			switch step {
			case "plan":
				o.Callbacks.Plan = func(ctx context.Context) (pluginhost.Plan, error) {
					_, err := base(ctx)
					if err != nil {
						return pluginhost.Plan{}, err
					}
					return pluginhost.Plan{}, cooperative(ctx)
				}
			case "activate":
				o.Callbacks.Activate = func(ctx context.Context, _ pluginhost.Owner, _ *pluginhost.Process) error { return cooperative(ctx) }
			}
			l := newController(t, o)
			done := make(chan error, 1)
			go func() { done <- l.Enable(context.Background()) }()
			<-entered
			_ = l.Disable(context.Background())
			if !errors.Is(awaitResult(t, done), context.Canceled) {
				t.Fatal("cancelled load succeeded")
			}

			if l.Status().State != pluginhost.StateDisabled {
				t.Fatalf("cooperative load left quarantine: %+v", l.Status())
			}
			for _, r := range l.Status().Disposals {
				if r.Incomplete {
					t.Fatal("returned callback marked pending")
				}
			}
		})
	}
}

func TestReloadPreflightDeadlinePreservesServingGeneration(t *testing.T) {
	for _, step := range []string{"plan", "resolve", "compatibility", "reload"} {
		for _, cooperative := range []bool{true, false} {
			t.Run(step+map[bool]string{true: "/cooperative", false: "/late"}[cooperative], func(t *testing.T) {
				o := lifecycleOptions(t)
				o.CallbackTimeout = 20 * time.Millisecond
				base := o.Callbacks.Plan
				var slow atomic.Bool
				release := make(chan struct{})
				defer close(release)
				wait := func(ctx context.Context) error {
					if !slow.Load() {
						return nil
					}
					if cooperative {
						<-ctx.Done()
						return ctx.Err()
					}
					<-release
					return nil
				}
				switch step {
				case "plan":
					o.Callbacks.Plan = func(ctx context.Context) (pluginhost.Plan, error) {
						if err := wait(ctx); err != nil {
							return pluginhost.Plan{}, err
						}
						return base(ctx)
					}
				case "resolve":
					o.Callbacks.Resolve = func(ctx context.Context, p pluginhost.Plan) (pluginhost.Plan, error) { return p, wait(ctx) }
				case "compatibility":
					o.Callbacks.CheckCompatibility = func(ctx context.Context, _ pluginhost.Plan) error { return wait(ctx) }
				case "reload":
					o.Callbacks.BeforeReload = wait
				}
				l := newController(t, o)
				enableController(t, l)
				old, owner := l.Current(), l.Status().Owner
				slow.Store(true)
				err := l.Reload(context.Background())
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("preflight deadline lost", err)
				}
				st := l.Status()
				if l.Current() != old || !l.IsCurrent(owner) || st.State != pluginhost.StateRunning || st.LastFailure == nil {
					t.Fatalf("preflight invalidated serving generation: %+v", st)
				}
				select {
				case <-old.Exited():
					t.Fatal("serving child exited")
				default:
				}
				if err = l.Enable(context.Background()); err != nil {
					t.Fatal("idempotent enable refused serving child", err)
				}
				if !cooperative && !errors.Is(l.Reload(context.Background()), pluginhost.ErrQuarantined) {
					t.Fatal("next reload ignored pending preflight")
				}
				slow.Store(false)
				_ = l.Disable(context.Background())
			})
		}
	}
}

func TestBeforeDisableIsBoundedAndRetainsIntentOnRefusal(t *testing.T) {
	o := lifecycleOptions(t)
	o.CallbackTimeout = 20 * time.Millisecond
	var slow atomic.Bool
	release := make(chan struct{})
	defer close(release)
	o.Callbacks.BeforeDisable = func(context.Context) error {
		if slow.Load() {
			<-release
		}
		return nil
	}
	l := newController(t, o)
	enableController(t, l)
	old := l.Current()
	slow.Store(true)
	done := make(chan error, 1)
	go func() { done <- l.Disable(context.Background()) }()
	if !errors.Is(awaitResult(t, done), context.DeadlineExceeded) {
		t.Fatal("disable guard ignored deadline")
	}
	if !l.Status().DesiredEnabled || l.Current() != old || l.Status().State != pluginhost.StateRunning {
		t.Fatal("refused disable changed serving intent")
	}
	slow.Store(false)
	_ = l.Disable(context.Background())
}

func TestAcknowledgementMatchesExactReportID(t *testing.T) {
	o := lifecycleOptions(t)
	owner := pluginhost.Owner{HostInstance: "previous", OwnerID: "fixture", OwnerGeneration: 1}
	o.StateStore = suppliedStateRecord{pluginhost.LifecycleRecord{Revision: 1, Disposals: []pluginhost.DisposalReport{
		{ID: "first", Owner: owner, Incomplete: true}, {ID: "second", Owner: owner, Incomplete: true},
	}}}
	l := newController(t, o)
	if err := l.AcknowledgeReport(context.Background(), "second"); err != nil {
		t.Fatal(err)
	}
	for _, r := range l.Status().Disposals {
		if r.Acknowledged != (r.ID == "second") {
			t.Fatal("wrong report acknowledged", r)
		}
	}
	if !errors.Is(l.Enable(context.Background()), pluginhost.ErrQuarantined) {
		t.Fatal("acknowledging one report cleared the other")
	}
}

// Save deliberately accepts revisions in this probe: the controller's CAS
// must independently stop a delayed acknowledgement replacing newer state.
type acknowledgementBarrierStore struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (*acknowledgementBarrierStore) Load(context.Context, string) (pluginhost.LifecycleRecord, error) {
	return pluginhost.LifecycleRecord{}, nil
}
func (s *acknowledgementBarrierStore) Save(_ context.Context, _ string, r pluginhost.LifecycleRecord) error {
	for _, d := range r.Disposals {
		if d.Acknowledged {
			s.once.Do(func() { close(s.entered) })
			<-s.release
			break
		}
	}
	return nil
}
func TestAcknowledgementRevisionProtectsNewDisposal(t *testing.T) {
	o := lifecycleOptions(t)
	o.CleanupTimeout = time.Second
	store := &acknowledgementBarrierStore{entered: make(chan struct{}), release: make(chan struct{})}
	o.StateStore = store
	l := newController(t, o)
	enableController(t, l)
	first := l.Status().Owner
	if err := l.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	second := l.Status().Owner
	ack := make(chan error, 1)
	go func() { ack <- l.AcknowledgeDisposal(context.Background(), first) }()
	<-store.entered
	_ = l.Disable(context.Background())
	close(store.release)
	if !errors.Is(awaitResult(t, ack), pluginhost.ErrLifecycleStateChanged) {
		t.Fatal("late acknowledgement replaced a newer revision")
	}
	found := false
	for _, d := range l.Status().Disposals {
		if d.Owner == second {
			found = true
		}
	}
	if !found {
		t.Fatal("late acknowledgement erased newer disposal")
	}
}

func TestPersistedRevisionBoundsFailClosed(t *testing.T) {
	for _, revision := range []uint64{pluginhost.MaxLifecycleRevision, ^uint64(0) - 1, ^uint64(0)} {
		o := lifecycleOptions(t)
		o.StateStore = suppliedStateRecord{pluginhost.LifecycleRecord{Revision: revision}}
		if _, err := pluginhost.NewLifecycle("fixture", o); !errors.Is(err, pluginhost.ErrInvalidLifecycleRecord) {
			t.Fatal("invalid revision restored", revision, err)
		}
	}
	memory := &pluginhost.MemoryLifecycleStateStore{}
	if err := memory.Save(context.Background(), "fixture", pluginhost.LifecycleRecord{Revision: ^uint64(0)}); !errors.Is(err, pluginhost.ErrInvalidLifecycleRecord) {
		t.Fatal("overflowing revision persisted", err)
	}
}

func TestSupervisorStopBoundsUncooperativeClassifier(t *testing.T) {
	o := lifecycleOptions(t)
	p, err := o.Callbacks.Plan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	sup := pluginhost.Supervise(p.Spec, pluginhost.SuperviseOptions{ClassifyExit: func(pluginhost.ExitInfo) error { close(entered); <-release; return nil }})
	if err = sup.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = sup.Current().Kill()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err = sup.Stop(ctx); err != nil {
		t.Fatal("classifier blocked Stop", err)
	}
}
