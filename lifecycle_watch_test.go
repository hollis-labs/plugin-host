package pluginhost

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-sdk/subprocess"
)

func TestRetiredExitWatcherCannotDisposeReplacement(t *testing.T) {
	exited := make(chan struct{})
	close(exited)
	l, err := NewLifecycle("fixture", LifecycleOptions{HostInstance: "watcher-test-epoch", Generations: &MemoryGenerationStore{}, Callbacks: LifecycleCallbacks{Plan: func(context.Context) (Plan, error) { return Plan{}, errors.New("unexpected plan") }}})
	if err != nil {
		t.Fatal(err)
	}
	newer := &incarnation{owner: Owner{HostInstance: "watcher-test-epoch", OwnerID: "fixture", OwnerGeneration: 2}}
	old := &incarnation{owner: Owner{HostInstance: "watcher-test-epoch", OwnerID: "fixture", OwnerGeneration: 1}, process: &Process{exited: exited}, cancel: func() { t.Error("stale watcher canceled old scope again") }}
	l.current = newer
	l.status.State = StateRunning
	l.status.DesiredEnabled = true
	l.watch(context.Background(), old)
	if l.current != newer || l.status.State != StateRunning || len(l.Status().Disposals) != 0 {
		t.Fatal("stale watcher changed replacement")
	}
}

func TestEnableBarrierPrecedesDisableGate(t *testing.T) {
	epoch, err := NewHostInstance()
	if err != nil {
		t.Fatal(err)
	}
	command, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	l, err := NewLifecycle("fixture", LifecycleOptions{HostInstance: epoch, Generations: &MemoryGenerationStore{}, Callbacks: LifecycleCallbacks{Plan: func(context.Context) (Plan, error) {
		return Plan{Spec: Spec{ID: "fixture", ExpectedVersion: "1.0.0", Command: command, Env: []string{"PLUGINHOSTTEST_BEHAVIOUR=echo", "PLUGINHOSTTEST_DIR=" + dir, "GORACE=atexit_sleep_ms=0"}, Init: subprocess.InitParams{DataDir: dir}}}, nil //nolint:misspell // fixed fixture environment name
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = l.Enable(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Disable(context.Background()) })
	old := l.Status().Owner
	previous := make(chan struct{})
	l.mu.Lock()
	l.disableBarrier = previous
	l.mu.Unlock()
	disabled := make(chan error, 1)
	go func() { disabled <- l.Disable(context.Background()) }()
	deadline := time.Now().Add(time.Second)
	for l.Status().DesiredEnabled && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if l.Status().DesiredEnabled {
		t.Fatal("disable not accepted")
	}
	// Disable is intentionally held before acquire, leaving the operation gate
	// available. An Enable without its barrier necessarily reaches it first.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	err = l.Enable(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || l.Status().DesiredEnabled || l.Current() != nil {
		close(previous)
		t.Fatal("Enable passed an accepted Disable", err, l.Status())
	}
	close(previous)
	if err = <-disabled; err != nil {
		t.Fatal(err)
	}
	if err = l.Enable(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !l.Status().DesiredEnabled || l.Status().State != StateRunning || l.Current() == nil || l.IsCurrent(old) {
		t.Fatal("desired/actual disagreement after serialized Enable")
	}
}

func TestPrunePreservesUnresolvedReportsAndBoundsPendingMap(t *testing.T) {
	epoch, _ := NewHostInstance()
	l, _ := NewLifecycle("history", LifecycleOptions{HostInstance: epoch, Generations: &MemoryGenerationStore{}, Callbacks: LifecycleCallbacks{Plan: func(context.Context) (Plan, error) { return Plan{}, nil }}})
	done := make(chan struct{})
	unresolved := l.recordDisposal(DisposalReport{Owner: Owner{HostInstance: epoch, OwnerID: "history", OwnerGeneration: 1}, Incomplete: true}, []<-chan struct{}{done})
	for i := uint64(2); i < 80; i++ {
		l.recordDisposal(DisposalReport{Owner: Owner{HostInstance: epoch, OwnerID: "history", OwnerGeneration: i}}, nil)
	}
	if reports := l.history().Disposals; len(reports) != CompletedDisposalHistory+1 || reports[0].ID != unresolved.ID {
		t.Fatal("unresolved report pruned", reports)
	}
	processLedger.Lock()
	pending := len(ledgerRecord(l.key()).pending)
	processLedger.Unlock()
	if pending != 1 {
		t.Fatal("completed callbacks retained", pending)
	}
	close(done)
	if err := l.AcknowledgeReport(context.Background(), unresolved.ID); err != nil {
		t.Fatal(err)
	}
	if reports := l.history().Disposals; len(reports) != CompletedDisposalHistory {
		t.Fatal("acknowledged tail not pruned", len(reports))
	}
}

func TestDisableFenceAtPublicationAfterSuccessfulActivation(t *testing.T) {
	epoch, _ := NewHostInstance()
	command, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p := Plan{Spec: Spec{ID: "fixture", ExpectedVersion: "1.0.0", Command: command, Env: []string{"PLUGINHOSTTEST_BEHAVIOUR=echo", "GORACE=atexit_sleep_ms=0"}, Init: subprocess.InitParams{DataDir: t.TempDir()}}} //nolint:misspell // fixed fixture environment name
	type activation struct {
		ctx   context.Context
		child *Process
	}
	entered := make(chan activation, 1)
	release := make(chan struct{})
	l, _ := NewLifecycle("fixture", LifecycleOptions{HostInstance: epoch, Generations: &MemoryGenerationStore{}, Callbacks: LifecycleCallbacks{Plan: func(context.Context) (Plan, error) { return p, nil }, Activate: func(ctx context.Context, _ Owner, child *Process) error {
		entered <- activation{ctx, child}
		<-release
		return nil
	}}})
	l.status.DesiredEnabled = true
	result := make(chan *Failure, 1)
	go func() { result <- l.load(context.Background(), 0, p) }()
	active := <-entered
	l.mu.Lock()
	close(release)
	// loadValue cancels this callback context after receiving the successful
	// result. Publication is now blocked only by the state mutex.
	<-active.ctx.Done()
	l.status.DesiredEnabled = false
	l.revision++
	l.mu.Unlock()
	if failure := <-result; failure == nil {
		t.Fatal("successful activation published after the Disable fence")
	}
	select {
	case <-active.child.Exited():
	default:
		t.Fatal("fenced candidate survived")
	}
}

func TestMonotonicEpochRemainsAnIndependentAdmissionGuard(t *testing.T) {
	epoch, _ := NewHostInstance()
	l, _ := NewLifecycle("fixture", LifecycleOptions{HostInstance: epoch, Generations: &MemoryGenerationStore{}, Callbacks: LifecycleCallbacks{Plan: func(context.Context) (Plan, error) { return Plan{}, nil }}})
	old := Owner{HostInstance: epoch, OwnerID: "fixture", OwnerGeneration: 1}
	l.current = &incarnation{owner: old, epoch: 0, process: &Process{exited: make(chan struct{})}}
	l.revision = 1
	l.status.State = StateRunning
	l.status.DesiredEnabled = true
	if l.Current() != nil || l.IsCurrent(old) {
		t.Fatal("mutable desired intent admitted an old fence epoch")
	}
}
