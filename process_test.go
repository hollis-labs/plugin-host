//go:build unix

package pluginhost_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	pluginhost "github.com/hollis-labs/plugin-host"
	"github.com/hollis-labs/plugin-host/pluginhosttest"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

// fixtureSpec builds a Spec that re-executes this test binary as behaviour.
// Budgets are short but far above what a healthy fixture needs, so a loaded
// CI machine does not turn a slow start into a failure.
func fixtureSpec(t *testing.T, behaviour string, extraEnv ...string) (pluginhost.Spec, string) {
	t.Helper()
	dir := t.TempDir()
	command, env := pluginhosttest.FixtureCommand(behaviour, dir, extraEnv...)
	return pluginhost.Spec{
		ID:               "fixture",
		Command:          command,
		Env:              env,
		Init:             subprocess.InitParams{DataDir: dir, CacheDir: filepath.Join(dir, "cache")},
		HandshakeTimeout: 10 * time.Second,
		UnloadTimeout:    time.Second,
		ReapTimeout:      2 * time.Second,
	}, dir
}

func startFixture(t *testing.T, behaviour string, extraEnv ...string) (*pluginhost.Process, string) {
	t.Helper()
	spec, dir := fixtureSpec(t, behaviour, extraEnv...)
	p, err := pluginhost.Start(context.Background(), spec)
	if err != nil {
		t.Fatalf("Start(%s): %v", behaviour, err)
	}
	t.Cleanup(func() { _ = p.Stop(context.Background()) })
	return p, dir
}

func tool(t *testing.T, p *pluginhost.Process, name string, args map[string]any) json.RawMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	res, err := p.Client().MCPCallTool(ctx, subprocess.MCPCallRequest{ToolName: name, Arguments: args})
	if err != nil {
		t.Fatalf("tool %s: %v", name, err)
	}
	return res.Content
}

func toolAs[T any](t *testing.T, p *pluginhost.Process, name string, args map[string]any) T {
	t.Helper()
	var out T
	if err := json.Unmarshal(tool(t, p, name, args), &out); err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
	return out
}

// exists reports whether pid names a process, zombies included: a child that
// was killed but never waited for still counts, which is how an unreaped
// child is told from a reaped one.
func exists(pid int) bool { return syscall.Kill(pid, 0) == nil }

func readPID(t *testing.T, dir, name string) int {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

func eventually(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", within, what)
}

func TestStartRunsInitThenLoadAndReturnsWhatThePluginSaid(t *testing.T) {
	p, _ := startFixture(t, pluginhosttest.BehaviourEcho)
	if info := p.Info(); info.ID != "fixture" || info.Protocol != 1 {
		t.Fatalf("Info = %+v", info)
	}
	if got := toolAs[[]string](t, p, "trace", nil); !slices.Equal(got, []string{"plugin/init", "plugin/load"}) {
		t.Fatalf("trace = %v", got)
	}
	if got := toolAs[map[string]string](t, p, "echo", map[string]any{"message": "hi"}); got["echo"] != "hi" {
		t.Fatalf("echo = %v", got)
	}
	if p.Pid() <= 0 {
		t.Fatalf("Pid = %d", p.Pid())
	}
}

func TestInitParamsDefaultsAreFilledOnlyWhereZero(t *testing.T) {
	spec, dir := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	spec.Dir = dir
	spec.Init = subprocess.InitParams{}
	p, err := pluginhost.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Stop(context.Background()) })
	got := toolAs[subprocess.InitParams](t, p, "init", nil)
	if got.HostInfo.Protocol != 1 || got.LogLevel != "info" || got.PluginDir != dir || got.Config == nil {
		t.Fatalf("defaults = %+v", got)
	}
	raw := tool(t, p, "init", nil)
	if !strings.Contains(string(raw), `"config":{}`) {
		t.Fatalf("config must travel as {}, never null: %s", raw)
	}
}

func TestInitParamsHostOwnedFieldsArriveVerbatim(t *testing.T) {
	spec, dir := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	spec.Init = subprocess.InitParams{
		PluginDir: "/plugins/x", DataDir: dir, CacheDir: "/cache/x", LogLevel: "debug",
		Config:   map[string]string{"token": "s3cret"},
		Granted:  []string{"net.http", "fs.read"},
		HostInfo: subprocess.HostInfo{Version: "9.9.9", Protocol: 1},
	}
	p, err := pluginhost.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Stop(context.Background()) })
	got := toolAs[subprocess.InitParams](t, p, "init", nil)
	if got.PluginDir != "/plugins/x" || got.CacheDir != "/cache/x" || got.LogLevel != "debug" ||
		got.Config["token"] != "s3cret" || !slices.Equal(got.Granted, []string{"net.http", "fs.read"}) ||
		got.HostInfo.Version != "9.9.9" {
		t.Fatalf("init params = %+v", got)
	}
}

func TestSpecEnvIsExact(t *testing.T) {
	environ := func(p *pluginhost.Process) []string { return toolAs[[]string](t, p, "env", nil) }
	noise := func(env []string) []string { // Go adds PWD when Dir is set; nothing else may appear
		return slices.DeleteFunc(slices.Clone(env), func(e string) bool { return strings.HasPrefix(e, "PWD=") })
	}

	t.Setenv("PLUGINHOSTTEST_HOST_ONLY", "leak")
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho, "EXTRA=1")
	p, err := pluginhost.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	got := noise(environ(p))
	_ = p.Stop(context.Background())
	for _, e := range got {
		if strings.HasPrefix(e, "PLUGINHOSTTEST_HOST_ONLY") || strings.HasPrefix(e, "PATH=") {
			t.Fatalf("host environment leaked into the child: %q in %v", e, got)
		}
	}
	if !slices.Contains(got, "EXTRA=1") || len(got) != len(spec.Env) {
		t.Fatalf("child env = %v, want exactly %v", got, spec.Env)
	}

	// InheritEnv is the opt-in.
	spec.Env = pluginhost.InheritEnv(spec.Env...)
	p, err = pluginhost.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	got = environ(p)
	_ = p.Stop(context.Background())
	if !slices.Contains(got, "PLUGINHOSTTEST_HOST_ONLY=leak") {
		t.Fatalf("InheritEnv did not inherit: %v", got)
	}
}

func TestNilEnvMeansEmptyNotInherit(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	// A fixture with no behaviour variable would not act as a fixture, so use
	// the smallest environment that does and check nothing else is added.
	t.Setenv("PLUGINHOSTTEST_HOST_ONLY", "leak")
	p, err := pluginhost.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Stop(context.Background()) }()
	for _, e := range toolAs[[]string](t, p, "env", nil) {
		if strings.Contains(e, "HOST_ONLY") {
			t.Fatalf("leaked %q", e)
		}
	}
}

func TestStartFailuresAreErrorsThatKillAndReapTheChild(t *testing.T) {
	cases := []struct {
		behaviour string
		check     func(t *testing.T, err error)
	}{
		{pluginhosttest.BehaviourBadProtocol, func(t *testing.T, err error) {
			if !errors.Is(err, pluginhost.ErrProtocolMismatch) {
				t.Errorf("err = %v, want ErrProtocolMismatch", err)
			}
		}},
		{pluginhosttest.BehaviourNoID, func(t *testing.T, err error) {
			if !errors.Is(err, pluginhost.ErrNoPluginID) {
				t.Errorf("err = %v, want ErrNoPluginID", err)
			}
		}},
		{pluginhosttest.BehaviourInitError, func(t *testing.T, err error) {
			var rpc *subprocess.RPCError
			if !errors.As(err, &rpc) || !strings.Contains(err.Error(), pluginhosttest.InitFailedMarker) {
				t.Errorf("err = %v, want an RPCError carrying the stderr tail", err)
			}
		}},
		{pluginhosttest.BehaviourLoadError, func(t *testing.T, err error) {
			var rpc *subprocess.RPCError
			if !errors.As(err, &rpc) || !strings.Contains(err.Error(), pluginhosttest.LoadFailedMarker) {
				t.Errorf("err = %v, want an RPCError carrying the stderr tail", err)
			}
		}},
	}
	for _, c := range cases {
		t.Run(c.behaviour, func(t *testing.T) {
			spec, dir := fixtureSpec(t, c.behaviour)
			p, err := pluginhost.Start(context.Background(), spec)
			if err == nil {
				_ = p.Stop(context.Background())
				t.Fatal("Start succeeded")
			}
			c.check(t, err)
			pid := readPID(t, dir, "pid")
			if exists(pid) {
				t.Fatalf("child %d still exists after a failed Start (running or unreaped)", pid)
			}
		})
	}
}

func TestHandshakeIsBoundedEvenWithoutAContextDeadline(t *testing.T) {
	spec, dir := fixtureSpec(t, pluginhosttest.BehaviourHangOnInit)
	spec.HandshakeTimeout = 400 * time.Millisecond
	start := time.Now()
	_, err := pluginhost.Start(context.Background(), spec)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if took := time.Since(start); took > 6*time.Second {
		t.Fatalf("took %s, want about HandshakeTimeout + Reap", took)
	}
	if exists(readPID(t, dir, "pid")) {
		t.Fatal("child left behind")
	}
}

func TestStartRefusesAnEmptyCommandAndACancelledContext(t *testing.T) {
	if _, err := pluginhost.Start(context.Background(), pluginhost.Spec{ID: "x"}); err == nil {
		t.Fatal("empty command accepted")
	}
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := pluginhost.Spawn(ctx, spec); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	spec.Command = filepath.Join(t.TempDir(), "does-not-exist")
	if _, err := pluginhost.Start(context.Background(), spec); err == nil {
		t.Fatal("missing binary accepted")
	}
}

func TestCancellingTheStartContextDoesNotKillThePlugin(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	ctx, cancel := context.WithCancel(context.Background())
	p, err := pluginhost.Start(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Stop(context.Background()) })
	cancel()
	time.Sleep(200 * time.Millisecond)
	select {
	case <-p.Exited():
		t.Fatal("the plugin died with its boot context")
	default:
	}
	if got := toolAs[map[string]string](t, p, "echo", map[string]any{"message": "still here"}); got["echo"] != "still here" {
		t.Fatalf("echo = %v", got)
	}
}

func TestSpawnLeavesTheHandshakeToTheHost(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	p, err := pluginhost.Spawn(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Stop(context.Background()) })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	res, err := p.Client().Init(ctx, subprocess.InitParams{
		Config:   map[string]string{"api_key": "resolved-by-the-host"},
		HostInfo: subprocess.HostInfo{Version: "1", Protocol: 1},
	})
	if err != nil || res.ID != "fixture" {
		t.Fatalf("Init = %+v, %v", res, err)
	}
	if _, err := p.Client().Load(ctx); err != nil {
		t.Fatal(err)
	}
	got := toolAs[subprocess.InitParams](t, p, "init", nil)
	if got.Config["api_key"] != "resolved-by-the-host" {
		t.Fatalf("host-built init did not arrive: %+v", got)
	}
}

func TestGracefulStopEndsThroughStdinEOFWithoutSIGKILL(t *testing.T) {
	p, dir := startFixture(t, pluginhosttest.BehaviourEcho)
	start := time.Now()
	if err := p.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 900*time.Millisecond {
		t.Fatalf("Stop took %s, want well inside the 1s unload budget", took)
	}
	if _, err := os.Stat(filepath.Join(dir, "serve-returned")); err != nil {
		t.Fatalf("Serve never returned, so the plugin was killed rather than told to stop: %v", err)
	}
	info, ok := p.ExitInfo()
	if !ok || info.Code != 0 || info.Signal != "" {
		t.Fatalf("ExitInfo = %+v, %v; want a clean exit", info, ok)
	}
	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop = %v", err)
	}
	if _, err := p.Client().Health(context.Background()); !errors.Is(err, pluginhost.ErrGone) {
		t.Fatalf("call after Stop: %v, want ErrGone", err)
	}
}

func wedgeSpec(t *testing.T) (*pluginhost.Process, string) {
	t.Helper()
	spec, dir := fixtureSpec(t, pluginhosttest.BehaviourWedge)
	spec.UnloadTimeout = 300 * time.Millisecond
	spec.ReapTimeout = time.Second
	p, err := pluginhost.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	return p, dir
}

func TestStopKillsAWedgedPluginAndItsGrandchildWithinTheBudgets(t *testing.T) {
	p, dir := wedgeSpec(t)
	leader, grandchild := readPID(t, dir, "pid"), readPID(t, dir, "grandchild.pid")
	t.Cleanup(func() { _ = syscall.Kill(grandchild, syscall.SIGKILL) })
	if !exists(leader) || !exists(grandchild) {
		t.Fatal("fixture did not come up")
	}
	start := time.Now()
	if err := p.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 300*time.Millisecond+time.Second+2*time.Second {
		t.Fatalf("Stop took %s, want <= unload + reap + slack", took)
	}
	if exists(leader) {
		t.Fatalf("leader %d survived Stop", leader)
	}
	eventually(t, 3*time.Second, "the grandchild to die", func() bool { return !exists(grandchild) })
	if info, _ := p.ExitInfo(); info.Signal == "" {
		t.Fatalf("ExitInfo = %+v, want a signal", info)
	}
}

func TestStopContextCancellationShortensButNeverLengthens(t *testing.T) {
	t.Run("cancel shortens", func(t *testing.T) {
		spec, dir := fixtureSpec(t, pluginhosttest.BehaviourWedge)
		spec.UnloadTimeout = 30 * time.Second
		spec.ReapTimeout = time.Second
		p, err := pluginhost.Start(context.Background(), spec)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = syscall.Kill(readPID(t, dir, "grandchild.pid"), syscall.SIGKILL) })
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		start := time.Now()
		if err := p.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		if took := time.Since(start); took > 5*time.Second {
			t.Fatalf("Stop took %s despite a 200ms ctx", took)
		}
	})
	t.Run("a generous ctx does not lengthen", func(t *testing.T) {
		p, dir := wedgeSpec(t)
		t.Cleanup(func() { _ = syscall.Kill(readPID(t, dir, "grandchild.pid"), syscall.SIGKILL) })
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		start := time.Now()
		if err := p.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		if took := time.Since(start); took > 5*time.Second {
			t.Fatalf("Stop took %s", took)
		}
	})
}

func TestStopIsIdempotentAndConcurrencySafe(t *testing.T) {
	p, _ := startFixture(t, pluginhosttest.BehaviourEcho)
	errs := make(chan error, 5)
	for range 5 {
		go func() { errs <- p.Stop(context.Background()) }()
	}
	for range 5 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func TestACrashFailsInFlightAndLaterCallsWithErrGoneNotATimeout(t *testing.T) {
	p, _ := startFixture(t, pluginhosttest.BehaviourCrashOnCall)
	start := time.Now()
	_, err := p.Client().MCPCallTool(context.Background(), subprocess.MCPCallRequest{ToolName: "anything"})
	if !errors.Is(err, pluginhost.ErrGone) {
		t.Fatalf("in-flight call: %v, want ErrGone", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("the crash was reported as a timeout, not as gone")
	}
	select {
	case <-p.Exited():
	case <-time.After(5 * time.Second):
		t.Fatal("Exited never closed")
	}
	if _, err := p.Client().Health(context.Background()); !errors.Is(err, pluginhost.ErrGone) {
		t.Fatalf("later call: %v, want ErrGone", err)
	}
	if info, ok := p.ExitInfo(); !ok || info.Code != 3 {
		t.Fatalf("ExitInfo = %+v, %v; want code 3", info, ok)
	}
}

func TestAFinalFrameWrittenBeforeExitIsDelivered(t *testing.T) {
	for i := range 15 {
		p, _ := startFixture(t, pluginhosttest.BehaviourExitAfterResponse)
		res, err := p.Client().MCPCallTool(context.Background(), subprocess.MCPCallRequest{ToolName: "x"})
		if err != nil {
			t.Fatalf("iteration %d: the last frame was lost: %v", i, err)
		}
		_ = res
		<-p.Exited()
	}
}

func TestGarbageOnStdoutIsDroppedAndTheNextCallSucceeds(t *testing.T) {
	p, _ := startFixture(t, pluginhosttest.BehaviourGarbage)
	for i := range 3 {
		raw, err := p.Client().Conn().Call(context.Background(), "anything", nil)
		if err != nil || !strings.Contains(string(raw), "garbage") {
			t.Fatalf("call %d = %s, %v", i, raw, err)
		}
	}
}

func TestUnknownMethodAndApplicationErrorCodesArePreserved(t *testing.T) {
	p, _ := startFixture(t, pluginhosttest.BehaviourEcho)
	code := func(err error) int {
		var rpc *subprocess.RPCError
		if !errors.As(err, &rpc) {
			t.Fatalf("not an RPCError: %v", err)
		}
		return rpc.Code
	}
	_, err := p.Client().Conn().Call(context.Background(), "no/such/method", nil)
	if got := code(err); got != subprocess.ErrCodeMethodNotFound {
		t.Fatalf("code = %d", got)
	}
	for _, want := range []int{-32000, -32001, -32002, -32003} {
		_, err := p.Client().MCPCallTool(context.Background(), subprocess.MCPCallRequest{
			ToolName: "error", Arguments: map[string]any{"code": want},
		})
		if got := code(err); got != want {
			t.Errorf("code = %d, want %d", got, want)
		}
	}
}

func TestBigResponsesAreDeliveredAndBigRequestsRefusedClientSide(t *testing.T) {
	p, _ := startFixture(t, pluginhosttest.BehaviourEcho)
	big := toolAs[map[string]string](t, p, "big", map[string]any{"bytes": 12 << 20})
	if len(big["data"]) != 12<<20 {
		t.Fatalf("got %d bytes", len(big["data"]))
	}
	_, err := p.Client().MCPCallTool(context.Background(), subprocess.MCPCallRequest{
		ToolName: "echo", Arguments: map[string]any{"message": strings.Repeat("a", 9<<20)},
	})
	if !errors.Is(err, pluginhost.ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
	if got := toolAs[map[string]string](t, p, "echo", map[string]any{"message": "alive"}); got["echo"] != "alive" {
		t.Fatal("the plugin did not survive the refused request")
	}
}

func TestNotifyReachesThePluginWithoutAnAnswer(t *testing.T) {
	p, _ := startFixture(t, pluginhosttest.BehaviourEcho)
	if err := p.Client().Conn().Notify(subprocess.MethodEventHandle, subprocess.EventHandleParams{Type: "x"}); err != nil {
		t.Fatal(err)
	}
	if got := toolAs[map[string]string](t, p, "echo", map[string]any{"message": "after"}); got["echo"] != "after" {
		t.Fatal(got)
	}
}

func TestStderrIsBoundedAndRedacted(t *testing.T) {
	const secret = "tok-9f8e7d6c5b4a-SECRET"
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourStderrFlood, pluginhosttest.EnvSecret+"="+secret)
	spec.Secrets = []string{secret}
	spec.StderrBytes = 2048
	spec.Redact = func(s string) string { return strings.ReplaceAll(s, "flood-complete", "FLOOD-DONE") }
	p, err := pluginhost.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	check := func(when string) {
		d := p.Diagnostics()
		if len(d) > 2048+64 {
			t.Errorf("%s: Diagnostics is %d bytes", when, len(d))
		}
		if strings.Contains(d, secret) {
			t.Errorf("%s: Diagnostics leaks the secret", when)
		}
	}
	check("before Stop")
	if err := p.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	check("after Stop")
	if d := p.Diagnostics(); !strings.Contains(d, "FLOOD-DONE") || strings.Contains(d, "flood-complete") {
		t.Errorf("Spec.Redact was not applied: %q", d[max(0, len(d)-80):])
	}
}

func TestKillReapsAndFailsPendingCalls(t *testing.T) {
	p, dir := startFixture(t, pluginhosttest.BehaviourEcho)
	done := make(chan error, 1)
	go func() {
		_, err := p.Client().MCPCallTool(context.Background(), subprocess.MCPCallRequest{
			ToolName: "sleep", Arguments: map[string]any{"ms": 30000},
		})
		done <- err
	}()
	time.Sleep(200 * time.Millisecond)
	if err := p.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, pluginhost.ErrGone) {
		t.Fatalf("err = %v, want ErrGone", err)
	}
	if exists(readPID(t, dir, "pid")) {
		t.Fatal("killed child was not reaped")
	}
	if err := p.Kill(); err != nil {
		t.Fatalf("second Kill: %v", err)
	}
}

func TestNoGoroutinesLeakAcrossStartAndStop(t *testing.T) {
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
	// One warm-up cycle so lazily started runtime goroutines are counted in
	// the baseline rather than blamed on the library.
	warm, _ := startFixture(t, pluginhosttest.BehaviourEcho)
	_ = warm.Stop(context.Background())
	before := settle(0)

	for range 5 {
		p, _ := startFixture(t, pluginhosttest.BehaviourEcho)
		tool(t, p, "echo", map[string]any{"message": "x"})
		if err := p.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	crashed, _ := startFixture(t, pluginhosttest.BehaviourCrashOnCall)
	_, _ = crashed.Client().Conn().Call(context.Background(), "x", nil)
	<-crashed.Exited()
	_ = crashed.Stop(context.Background())

	if after := settle(before); after > before {
		buf := make([]byte, 1<<16)
		buf = buf[:runtime.Stack(buf, true)]
		t.Fatalf("goroutines: %d before, %d after\n%s", before, after, buf)
	}
}

func TestDefaultBudgets(t *testing.T) {
	if testing.Short() {
		t.Skip("uses the real default budgets (10s handshake, 2s+2s stop)")
	}
	spec, dir := fixtureSpec(t, pluginhosttest.BehaviourHangOnInit)
	spec.HandshakeTimeout, spec.UnloadTimeout, spec.ReapTimeout = 0, 0, 0
	start := time.Now()
	_, err := pluginhost.Start(context.Background(), spec)
	took := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) || took < 9*time.Second || took > 14*time.Second {
		t.Fatalf("hang-on-init with defaults: %v after %s, want DeadlineExceeded after ~10s", err, took)
	}
	if exists(readPID(t, dir, "pid")) {
		t.Fatal("child left behind")
	}

	spec, dir = fixtureSpec(t, pluginhosttest.BehaviourWedge)
	spec.HandshakeTimeout, spec.UnloadTimeout, spec.ReapTimeout = 0, 0, 0
	p, err := pluginhost.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(readPID(t, dir, "grandchild.pid"), syscall.SIGKILL) })
	start = time.Now()
	if err := p.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < 1500*time.Millisecond || took > 5*time.Second {
		t.Fatalf("wedge Stop with defaults took %s, want ~2s (unload budget) plus a kill", took)
	}
}
