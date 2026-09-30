//go:build unix

package pluginhost_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pluginhost "github.com/hollis-labs/plugin-host"
	"github.com/hollis-labs/plugin-host/pluginhosttest"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

const supervisorWait = 20 * time.Second

// events records supervisor callbacks for assertions.
type events struct {
	starts   atomic.Int32
	exits    atomic.Int32
	exitInfo atomic.Pointer[pluginhost.ExitInfo]
	giveUp   chan error
}

func newEvents() *events { return &events{giveUp: make(chan error, 4)} }

func (e *events) options(policy pluginhost.RestartPolicy) pluginhost.SuperviseOptions {
	return pluginhost.SuperviseOptions{
		Policy:  policy,
		OnStart: func(*pluginhost.Process) { e.starts.Add(1) },
		OnExit: func(info pluginhost.ExitInfo, restarting bool) {
			e.exits.Add(1)
			e.exitInfo.Store(&info)
		},
		OnGiveUp: func(err error) { e.giveUp <- err },
	}
}

func fastPolicy(restarts int) pluginhost.RestartPolicy {
	return pluginhost.RestartPolicy{MaxRestarts: restarts, Initial: 30 * time.Millisecond, Max: 100 * time.Millisecond}
}

func startSupervised(t *testing.T, behavior string, o pluginhost.SuperviseOptions, extraEnv ...string) (*pluginhost.Supervisor, string) {
	t.Helper()
	spec, dir := fixtureSpec(t, behavior, extraEnv...)
	sup := pluginhost.Supervise(spec, o)
	if err := sup.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background()) })
	return sup, dir
}

// awaitNewProcess waits for a supervised process other than old.
func awaitNewProcess(t *testing.T, sup *pluginhost.Supervisor, old *pluginhost.Process) *pluginhost.Process {
	t.Helper()
	var got *pluginhost.Process
	eventually(t, supervisorWait, "a replacement process", func() bool {
		got = sup.Current()
		return got != nil && got != old
	})
	return got
}

func TestACrashedChildIsRestartedThroughAFullHandshake(t *testing.T) {
	ev := newEvents()
	sup, _ := startSupervised(t, pluginhosttest.BehaviourEcho, ev.options(fastPolicy(0)))
	first := sup.Current()
	if err := first.Kill(); err != nil {
		t.Fatal(err)
	}
	second := awaitNewProcess(t, sup, first)
	if second.Pid() == first.Pid() {
		t.Fatal("same pid after restart")
	}
	if got := toolAs[[]string](t, second, "trace", nil); len(got) != 2 || got[0] != "plugin/init" || got[1] != "plugin/load" {
		t.Fatalf("a restart must re-run init and load; trace = %v", got)
	}
	if got := toolAs[map[string]string](t, second, "echo", map[string]any{"message": "back"}); got["echo"] != "back" {
		t.Fatal(got)
	}
	if sup.Restarts() != 1 || ev.starts.Load() != 2 || ev.exits.Load() != 1 {
		t.Fatalf("restarts=%d starts=%d exits=%d", sup.Restarts(), ev.starts.Load(), ev.exits.Load())
	}
	if info := ev.exitInfo.Load(); info == nil || info.Signal == "" {
		t.Fatalf("OnExit info = %+v, want a signal exit", info)
	}
}

func TestARestartLoopGivesUpAfterTheBudget(t *testing.T) {
	ev := newEvents()
	sup, _ := startSupervised(t, pluginhosttest.BehaviourEcho, ev.options(fastPolicy(3)))
	current := sup.Current()
	for range 3 {
		_ = current.Kill()
		current = awaitNewProcess(t, sup, current)
	}
	if sup.Restarts() != 3 {
		t.Fatalf("Restarts = %d, want 3", sup.Restarts())
	}
	_ = current.Kill()
	select {
	case err := <-ev.giveUp:
		if err == nil {
			t.Fatal("nil give-up error")
		}
	case <-time.After(supervisorWait):
		t.Fatal("OnGiveUp never ran")
	}
	if sup.Current() != nil {
		t.Fatal("a supervisor that gave up still reports a process")
	}
	time.Sleep(300 * time.Millisecond)
	if ev.starts.Load() != 4 {
		t.Fatalf("starts = %d, want 4 (initial + 3 restarts)", ev.starts.Load())
	}
}

func TestRestartingCanBeDisabled(t *testing.T) {
	ev := newEvents()
	sup, _ := startSupervised(t, pluginhosttest.BehaviourEcho, ev.options(pluginhost.RestartPolicy{MaxRestarts: -1}))
	_ = sup.Current().Kill()
	select {
	case <-ev.giveUp:
	case <-time.After(supervisorWait):
		t.Fatal("OnGiveUp never ran")
	}
	if ev.starts.Load() != 1 || sup.Restarts() != 0 {
		t.Fatalf("starts=%d restarts=%d", ev.starts.Load(), sup.Restarts())
	}
}

func TestStopDuringBackoffNeverResurrectsTheChild(t *testing.T) {
	ev := newEvents()
	policy := pluginhost.RestartPolicy{Initial: 800 * time.Millisecond}
	sup, dir := startSupervised(t, pluginhosttest.BehaviourEcho, ev.options(policy))
	first := sup.Current()
	before := readPID(t, dir, "pid")
	_ = first.Kill()
	eventually(t, supervisorWait, "the supervisor to notice the exit", func() bool { return sup.Current() == nil })
	if err := sup.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond) // past the whole backoff
	if got := readPID(t, dir, "pid"); got != before {
		t.Fatalf("a child (pid %d) was spawned after Stop", got)
	}
	if sup.Restarts() != 0 || ev.starts.Load() != 1 || sup.Current() != nil {
		t.Fatalf("restarts=%d starts=%d", sup.Restarts(), ev.starts.Load())
	}
}

func TestStopDuringARestartHandshakeStopsTheNewChild(t *testing.T) {
	ev := newEvents()
	sup, dir := startSupervised(t, pluginhosttest.BehaviourHangOnRestart, ev.options(fastPolicy(0)))
	first := sup.Current()
	_ = first.Kill()
	// The replacement is spawned and hangs in its handshake (10s budget).
	eventually(t, supervisorWait, "the replacement to be spawned", func() bool {
		return readPID(t, dir, "pid") != first.Pid()
	})
	replacement := readPID(t, dir, "pid")
	start := time.Now()
	if err := sup.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 6*time.Second {
		t.Fatalf("Stop took %s; it waited out the replacement's handshake", took)
	}
	if exists(replacement) {
		t.Fatalf("replacement %d survived Stop", replacement)
	}
	if sup.Current() != nil || ev.starts.Load() != 1 {
		t.Fatalf("the replacement was installed: starts=%d", ev.starts.Load())
	}
}

func TestStableForRefillsTheBudget(t *testing.T) {
	policy := fastPolicy(1)
	policy.StableFor = 300 * time.Millisecond
	ev := newEvents()
	sup, _ := startSupervised(t, pluginhosttest.BehaviourEcho, ev.options(policy))
	current := sup.Current()
	for range 3 { // a cumulative budget of 1 would give up on the second
		time.Sleep(400 * time.Millisecond)
		_ = current.Kill()
		current = awaitNewProcess(t, sup, current)
	}
	select {
	case err := <-ev.giveUp:
		t.Fatalf("gave up despite stable runs: %v", err)
	default:
	}
}

func TestUnhealthyPluginIsKilledAndRestartedWhenAsked(t *testing.T) {
	ev := newEvents()
	o := ev.options(fastPolicy(0))
	o.HealthInterval, o.HealthTimeout, o.KillAfterUnhealthy = 40*time.Millisecond, 2*time.Second, 2
	sup, _ := startSupervised(t, pluginhosttest.BehaviourEcho, o)
	first := sup.Current()
	tool(t, first, "set_health", map[string]any{"ok": false})
	second := awaitNewProcess(t, sup, first)
	select {
	case <-first.Exited():
	default:
		t.Fatal("the unhealthy process is still running")
	}
	health, err := second.Client().Health(context.Background())
	if err != nil || !health.OK {
		t.Fatalf("restarted plugin health = %+v, %v", health, err)
	}
}

func TestHealthProbingWithoutKillOnlyObserves(t *testing.T) {
	ev := newEvents()
	o := ev.options(fastPolicy(0))
	o.HealthInterval = 30 * time.Millisecond
	sup, _ := startSupervised(t, pluginhosttest.BehaviourEcho, o)
	first := sup.Current()
	tool(t, first, "set_health", map[string]any{"ok": false})
	time.Sleep(500 * time.Millisecond)
	if sup.Current() != first {
		t.Fatal("KillAfterUnhealthy=0 must never kill")
	}
}

func TestSupervisorLifecycleEdges(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	sup := pluginhost.Supervise(spec, pluginhost.SuperviseOptions{})
	if err := sup.Stop(context.Background()); err != nil {
		t.Fatalf("Stop before Start: %v", err)
	}
	if err := sup.Start(context.Background()); err == nil {
		t.Fatal("Start after Stop succeeded")
	}

	sup = pluginhost.Supervise(spec, pluginhost.SuperviseOptions{})
	if err := sup.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := sup.Start(context.Background()); err == nil {
		t.Fatal("second Start succeeded")
	}
	p := sup.Current()
	if err := sup.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := sup.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	select {
	case <-p.Exited():
	default:
		t.Fatal("Stop left the plugin running")
	}

	bad, _ := fixtureSpec(t, pluginhosttest.BehaviourBadProtocol)
	sup = pluginhost.Supervise(bad, pluginhost.SuperviseOptions{})
	if err := sup.Start(context.Background()); !errors.Is(err, pluginhost.ErrProtocolMismatch) {
		t.Fatalf("Start = %v, want ErrProtocolMismatch", err)
	}
	if sup.Current() != nil {
		t.Fatal("a failed first start must leave nothing supervised")
	}
}

func TestBackoffTable(t *testing.T) {
	type step struct {
		attempt int
		want    time.Duration
		ok      bool
	}
	cases := []struct {
		name   string
		policy pluginhost.RestartPolicy
		steps  []step
	}{
		{"zero value is 3 restarts, 1s doubling", pluginhost.RestartPolicy{}, []step{
			{0, time.Second, true}, {1, 2 * time.Second, true}, {2, 4 * time.Second, true}, {3, 0, false}, {9, 0, false},
		}},
		{"ceiling", pluginhost.RestartPolicy{MaxRestarts: 10, Initial: time.Second, Max: 5 * time.Second}, []step{
			{2, 4 * time.Second, true}, {3, 5 * time.Second, true}, {9, 5 * time.Second, true}, {10, 0, false},
		}},
		{"factor", pluginhost.RestartPolicy{MaxRestarts: 3, Initial: 100 * time.Millisecond, Factor: 3}, []step{
			{0, 100 * time.Millisecond, true}, {1, 300 * time.Millisecond, true}, {2, 900 * time.Millisecond, true},
		}},
		{"negative disables", pluginhost.RestartPolicy{MaxRestarts: -1}, []step{{0, 0, false}}},
		{"huge attempt does not overflow", pluginhost.RestartPolicy{MaxRestarts: 1 << 30}, []step{
			{1 << 20, 30 * time.Second, true},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, s := range c.steps {
				got, ok := c.policy.Backoff(s.attempt)
				if got != s.want || ok != s.ok {
					t.Errorf("Backoff(%d) = %v, %v; want %v, %v", s.attempt, got, ok, s.want, s.ok)
				}
			}
		})
	}
}

func TestHealthGateCachesForTheTTLAndResets(t *testing.T) {
	var probes atomic.Int32
	answer := subprocess.HealthResult{OK: true}
	var mu sync.Mutex
	gate := pluginhost.NewHealthGate(func(context.Context) (subprocess.HealthResult, error) {
		probes.Add(1)
		mu.Lock()
		defer mu.Unlock()
		return answer, nil
	}, 200*time.Millisecond)

	if err := gate.Check(); err != nil {
		t.Fatalf("unknown must be healthy: %v", err)
	}
	if v := gate.Probe(context.Background()); !v.OK || !v.Reachable {
		t.Fatalf("verdict = %+v", v)
	}
	gate.Probe(context.Background())
	if probes.Load() != 1 {
		t.Fatalf("probes = %d, want 1 (cached)", probes.Load())
	}

	mu.Lock()
	answer = subprocess.HealthResult{OK: false, Message: "db down"}
	mu.Unlock()
	time.Sleep(250 * time.Millisecond) // TTL passes
	if v := gate.Probe(context.Background()); v.OK || !v.Reachable || v.Message != "db down" {
		t.Fatalf("verdict = %+v", v)
	}
	err := gate.Check()
	if !errors.Is(err, pluginhost.ErrUnhealthy) {
		t.Fatalf("Check = %v, want ErrUnhealthy", err)
	}
	before := probes.Load()
	_ = gate.Check()
	if probes.Load() != before {
		t.Fatal("Check must never probe")
	}

	gate.Reset()
	if err := gate.Check(); err != nil {
		t.Fatalf("Check after Reset = %v; a restarted plugin starts with a clean slate", err)
	}
}

func TestHealthGateSeparatesUnreachableFromSaidUnhealthy(t *testing.T) {
	gate := pluginhost.NewHealthGate(func(context.Context) (subprocess.HealthResult, error) {
		return subprocess.HealthResult{}, pluginhost.ErrGone
	}, -1)
	v := gate.Probe(context.Background())
	if v.OK || v.Reachable {
		t.Fatalf("verdict = %+v; a failed probe is unreachable, not 'said unhealthy'", v)
	}
	if err := gate.Check(); !errors.Is(err, pluginhost.ErrUnhealthy) {
		t.Fatalf("Check = %v", err)
	}
}

func TestHealthGateOverARealPlugin(t *testing.T) {
	p, _ := startFixture(t, pluginhosttest.BehaviourEcho)
	gate := pluginhost.NewHealthGate(p.Client().Health, -1)
	if v := gate.Probe(context.Background()); !v.OK || !v.Reachable {
		t.Fatalf("healthy plugin: %+v", v)
	}
	tool(t, p, "set_health", map[string]any{"ok": false})
	if v := gate.Probe(context.Background()); v.OK || !v.Reachable || v.Message != "forced unhealthy" {
		t.Fatalf("what the plugin said was not reported: %+v", v)
	}
	if err := gate.Check(); !errors.Is(err, pluginhost.ErrUnhealthy) {
		t.Fatal(err)
	}
	tool(t, p, "set_health", map[string]any{"ok": true})
	if v := gate.Probe(context.Background()); !v.OK {
		t.Fatalf("probing must be able to bring it back: %+v", v)
	}
	if err := gate.Check(); err != nil {
		t.Fatal(err)
	}
	_ = p.Kill()
	if v := gate.Probe(context.Background()); v.Reachable {
		t.Fatalf("a dead plugin is unreachable: %+v", v)
	}
}

func TestStopDuringTheFirstHandshakeReturnsAndLeavesNothingBehind(t *testing.T) {
	settle := func(want int) int {
		var n int
		for range 100 {
			runtime.GC()
			if n = runtime.NumGoroutine(); n <= want {
				return n
			}
			time.Sleep(50 * time.Millisecond)
		}
		return n
	}
	// Warm up lazily started runtime goroutines so they are in the baseline.
	warm, _ := startFixture(t, pluginhosttest.BehaviourEcho)
	_ = warm.Stop(context.Background())
	before := settle(0)

	spec, dir := fixtureSpec(t, pluginhosttest.BehaviourHangOnInit) // handshake budget is 10s
	sup := pluginhost.Supervise(spec, pluginhost.SuperviseOptions{})
	startErr := make(chan error, 1)
	go func() { startErr <- sup.Start(context.Background()) }()
	eventually(t, supervisorWait, "the child to be spawned", func() bool {
		_, err := os.Stat(filepath.Join(dir, "pid"))
		return err == nil
	})

	stopped := make(chan error, 1)
	go func() { stopped <- sup.Stop(context.Background()) }()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("Stop = %v", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("Stop is still blocked while Start's handshake is in flight")
	}
	select {
	case err := <-startErr:
		if err == nil {
			t.Fatal("Start succeeded after Stop")
		}
	case <-time.After(6 * time.Second):
		t.Fatal("Start never returned")
	}
	if exists(readPID(t, dir, "pid")) {
		t.Fatal("the child survived")
	}
	if sup.Current() != nil {
		t.Fatal("a process is still installed")
	}
	if err := sup.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop = %v", err)
	}
	if after := settle(before); after > before {
		t.Fatalf("goroutines: %d before, %d after", before, after)
	}
}

func TestNotifyToAPluginThatStoppedReadingIsBoundedAndDoesNotWedgeCalls(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourDeaf)
	spec.ConnOptions = []pluginhost.ConnOption{pluginhost.WithDefaultTimeout(500 * time.Millisecond)}
	p, err := pluginhost.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	// Kill, not Stop: Stop sends an unload, which needs the write lock a
	// regression here would leave held.
	t.Cleanup(func() { _ = p.Kill() })

	notified := make(chan error, 1)
	go func() {
		// Far more than a pipe buffer holds, so the write must block.
		notified <- p.Client().Conn().Notify(subprocess.MethodEventHandle,
			subprocess.EventHandleParams{Type: "x", Data: map[string]any{"pad": strings.Repeat("a", 4<<20)}})
	}()
	select {
	case err := <-notified:
		if err == nil {
			t.Fatal("Notify to a plugin that reads nothing succeeded")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("Notify is blocked on a plugin that stopped reading stdin")
	}
	callDone := make(chan error, 1)
	go func() {
		_, err := p.Client().Health(context.Background())
		callDone <- err
	}()
	select {
	case err := <-callDone:
		if err == nil {
			t.Fatal("a call to a deaf plugin succeeded")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("a later call deadlocked behind the blocked Notify")
	}
}
