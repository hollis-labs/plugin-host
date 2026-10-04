package pluginhosttest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Harness is the adapter a host writes over its own client. Start must spawn
// c.Command with exactly c.Env and c.Args, run the host's own handshake, and
// return the live plugin.
type Harness interface {
	Start(ctx context.Context, c Case) (Instance, error)
}

// Case is one spawn request.
type Case struct {
	Command string
	Args    []string
	// Env is the environment the child must get. A host that inherits its
	// own environment may add to it.
	Env []string
	// DataDir and CacheDir must reach the plugin as plugin/init's data_dir
	// and cache_dir.
	DataDir, CacheDir string
	// Secrets are values that must never appear in the instance's
	// Diagnostics. A host with no way to take them waives R17.
	Secrets []string
}

// Instance is one started plugin, seen through the host's own client.
type Instance interface {
	// Call sends method with params and decodes the result into result
	// (which may be nil to discard it).
	Call(ctx context.Context, method string, params, result any) error
	// Stop ends the plugin the way the host ends plugins.
	Stop(ctx context.Context) error
	// Exited is closed when the plugin process has exited.
	Exited() <-chan struct{}
	// Pid is the plugin's OS process id.
	Pid() int
	// Diagnostics is the host's retained, redacted stderr tail.
	Diagnostics() string
	// IsGone reports whether err says "the plugin's pipe closed", as
	// opposed to a timeout or a plugin-reported error.
	IsGone(err error) bool
	// RPCCode extracts the JSON-RPC error code of a plugin-reported error.
	RPCCode(err error) (code int, ok bool)
}

type config struct {
	waived      map[string]string
	startBound  time.Duration
	stopBound   time.Duration
	diagnostics int
}

// Option configures [Run].
type Option func(*config)

// Waive records that the host does not meet requirement id (for example
// "R17") and why. The waiver is printed as a skipped subtest that names the
// reason; a requirement is never skipped silently. An unknown id fails the
// run, so a typo cannot waive the wrong thing.
func Waive(id, reason string) Option {
	return func(c *config) { c.waived[id] = reason }
}

// WithStartBound sets how long a Start (or a failing Start) may take before
// the suite calls it unbounded. The default is 15s: a 10s handshake budget
// plus reap, with slack.
func WithStartBound(d time.Duration) Option { return func(c *config) { c.startBound = d } }

// WithStopBound sets how long a Stop may take, and how long a killed child
// may take to disappear. The default is 15s.
func WithStopBound(d time.Duration) Option { return func(c *config) { c.stopBound = d } }

// WithDiagnosticsCap sets the largest Diagnostics text R17 accepts. The
// default is 64 KiB: a bounded tail, not the 8 MiB the plugin wrote.
func WithDiagnosticsCap(n int) Option { return func(c *config) { c.diagnostics = n } }

type requirement struct {
	id, name string
	run      func(*env)
}

var requirements = []requirement{
	{"R01", "handshake sends init then load", (*env).r01},
	{"R02", "init params: protocol 2, explicit grants, config {}, dirs verbatim", (*env).r02},
	{"R03", "protocol mismatch fails start and the child is gone", (*env).r03},
	{"R04", "empty plugin id fails start", (*env).r04},
	{"R05", "init and load errors fail start with the stderr tail, child reaped", (*env).r05},
	{"R06", "hang on init is bounded without a context deadline", (*env).r06},
	{"R07", "concurrent calls correlate", (*env).r07},
	{"R08", "a timed-out call leaves the connection usable", (*env).r08},
	{"R09", "a crash fails calls as gone, promptly", (*env).r09},
	{"R10", "garbage frames are dropped", (*env).r10},
	{"R11", "the final frame before exit is delivered", (*env).r11},
	{"R12", "7 MiB response delivered, oversized frames refused", (*env).r12},
	{"R13", "graceful stop ends without SIGKILL", (*env).r13},
	{"R14", "stop of a wedged plugin is bounded and kills it", (*env).r14},
	{"R15", "stop kills the plugin's process group", (*env).r15},
	{"R16", "canceling the start context leaves the plugin callable", (*env).r16},
	{"R17", "stderr is bounded and never leaks secrets", (*env).r17},
	{"R18", "unknown method is -32601, application codes preserved", (*env).r18},
}

// Run runs requirements R01-R18 against the host behind h, one subtest each.
// The test binary's TestMain must call [MaybeRunFixture] first, because the
// fixture plugin is the test binary re-executing itself.
func Run(t *testing.T, h Harness, opts ...Option) {
	t.Helper()
	cfg := &config{
		waived:      map[string]string{},
		startBound:  15 * time.Second,
		stopBound:   15 * time.Second,
		diagnostics: 64 << 10,
	}
	for _, o := range opts {
		o(cfg)
	}
	known := map[string]bool{}
	for _, r := range requirements {
		known[r.id] = true
	}
	for id := range cfg.waived {
		if !known[id] {
			t.Fatalf("pluginhosttest: Waive(%q): no such requirement", id)
		}
	}
	for _, r := range requirements {
		t.Run(r.id+"_"+slug(r.name), func(t *testing.T) {
			if reason, waived := cfg.waived[r.id]; waived {
				t.Skipf("WAIVED %s (%s): %s", r.id, r.name, reason)
			}
			r.run(&env{t: t, h: h, cfg: cfg})
		})
	}
}

func slug(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return strings.Trim(b.String(), "_")
}

// env is one requirement's working state.
type env struct {
	t   *testing.T
	h   Harness
	cfg *config
}

// spawned is one attempted start.
type spawned struct {
	c    Case
	inst Instance
	err  error
	took time.Duration
}

func (e *env) needPIDs() {
	e.t.Helper()
	if !havePIDs {
		e.t.Skip("SKIPPED: this requirement inspects process ids and needs a unix platform")
	}
}

func (e *env) newCase(behavior string, secrets []string, extra ...string) Case {
	e.t.Helper()
	base := e.t.TempDir()
	c := Case{DataDir: filepath.Join(base, "data"), CacheDir: filepath.Join(base, "cache"), Secrets: secrets}
	for _, d := range []string{c.DataDir, c.CacheDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			e.t.Fatal(err)
		}
	}
	c.Command, c.Env = FixtureCommand(behavior, c.DataDir, extra...)
	e.t.Cleanup(func() { // registered first, so it runs after the graceful stops below
		for _, name := range []string{"pid", "grandchild.pid"} {
			if pid, err := readPIDFile(filepath.Join(c.DataDir, name)); err == nil {
				killPID(pid)
			}
		}
	})
	return c
}

// spawn runs the harness's Start with a bound on how long it may take.
func (e *env) spawn(ctx context.Context, c Case) *spawned {
	e.t.Helper()
	s := &spawned{c: c}
	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				s.inst, s.err = nil, fmt.Errorf("the harness's Start panicked: %v", r)
			}
		}()
		s.inst, s.err = e.h.Start(ctx, c)
	}()
	select {
	case <-done:
		s.took = time.Since(start)
	case <-time.After(e.cfg.startBound):
		e.t.Errorf("Start did not return within %s: the handshake is not bounded", e.cfg.startBound)
		e.t.FailNow()
	}
	if s.inst != nil {
		e.t.Cleanup(func() { e.stopBounded(s.inst) })
	}
	return s
}

// start spawns behavior and requires it to come up.
func (e *env) start(behavior string) (Instance, Case) {
	e.t.Helper()
	c := e.newCase(behavior, nil)
	s := e.spawn(context.Background(), c)
	if s.err != nil {
		e.t.Fatalf("Start(%s): %v", behavior, s.err)
	}
	return s.inst, c
}

// startFails spawns behavior and requires the start to fail.
func (e *env) startFails(behavior string) (*spawned, Case) {
	e.t.Helper()
	c := e.newCase(behavior, nil)
	s := e.spawn(context.Background(), c)
	if s.err == nil {
		e.t.Fatalf("Start(%s) succeeded; it must fail", behavior)
	}
	return s, c
}

// stopBounded stops inst without ever hanging the suite.
func (e *env) stopBounded(inst Instance) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), e.cfg.stopBound)
		defer cancel()
		_ = inst.Stop(ctx)
	}()
	select {
	case <-done:
	case <-time.After(e.cfg.stopBound + 2*time.Second):
	}
}

func readPIDFile(path string) (int, error) {
	b, err := os.ReadFile(path) //nolint:gosec // a pid file under the case's own temp dir
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(b)))
}

// requireGone fails unless the child recorded in dir/name disappears (is
// reaped, not merely killed) within the stop bound.
func (e *env) requireGone(c Case, name, what string) {
	e.t.Helper()
	e.needPIDs()
	pid, err := readPIDFile(filepath.Join(c.DataDir, name))
	if err != nil {
		e.t.Fatalf("%s: the fixture recorded no pid (%v)", what, err)
	}
	deadline := time.Now().Add(e.cfg.stopBound)
	for pidExists(pid) {
		if time.Now().After(deadline) {
			e.t.Fatalf("%s: process %d still exists %s later (running, or killed and never reaped)", what, pid, e.cfg.stopBound)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// --- calling helpers ---

type toolResult struct {
	Content json.RawMessage `json:"content"`
}

// callTool sends mcp/call_tool for a fixture tool and returns its content.
func (e *env) callTool(ctx context.Context, inst Instance, tool string, args map[string]any) (json.RawMessage, error) {
	var res toolResult
	err := inst.Call(ctx, "mcp/call_tool", map[string]any{"tool_name": tool, "arguments": args}, &res)
	return res.Content, err
}

func (e *env) mustTool(inst Instance, tool string, args map[string]any, out any) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), e.cfg.startBound)
	defer cancel()
	content, err := e.callTool(ctx, inst, tool, args)
	if err != nil {
		e.t.Fatalf("tool %s: %v", tool, err)
	}
	if out != nil {
		if err := json.Unmarshal(content, out); err != nil {
			e.t.Fatalf("tool %s: decode %s: %v", tool, content, err)
		}
	}
}

func (e *env) echo(ctx context.Context, inst Instance, msg string) error {
	content, err := e.callTool(ctx, inst, "echo", map[string]any{"message": msg})
	if err != nil {
		return err
	}
	var got struct{ Echo string }
	if err := json.Unmarshal(content, &got); err != nil {
		return fmt.Errorf("decode echo %s: %w", content, err)
	}
	if got.Echo != msg {
		return fmt.Errorf("crossed reply: sent %q, got %q", msg, got.Echo)
	}
	return nil
}

func (e *env) mustEcho(inst Instance, msg string, within time.Duration) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	if err := e.echo(ctx, inst, msg); err != nil {
		e.t.Fatalf("echo %q: %v", msg, err)
	}
}

// --- R01-R18 ---

func (e *env) r01() {
	inst, _ := e.start(BehaviourEcho)
	var trace []string
	e.mustTool(inst, "trace", nil, &trace)
	if !slices.Equal(trace, []string{"plugin/init", "plugin/load"}) {
		e.t.Fatalf("the plugin saw %v, want [plugin/init plugin/load]", trace)
	}
}

func (e *env) r02() {
	inst, c := e.start(BehaviourEcho)
	var got struct {
		Config             json.RawMessage `json:"config"`
		DataDir            string          `json:"data_dir"`
		CacheDir           string          `json:"cache_dir"`
		Grants             json.RawMessage `json:"grants"`
		CapabilityContract int             `json:"capability_contract"`
		HostInfo           struct {
			Protocol int `json:"protocol"`
		} `json:"host_info"`
	}
	e.mustTool(inst, "init", nil, &got)
	if got.HostInfo.Protocol != 2 {
		e.t.Errorf("host_info.protocol = %d, want 2", got.HostInfo.Protocol)
	}
	if got.CapabilityContract != 1 || strings.TrimSpace(string(got.Grants)) != "[]" {
		e.t.Errorf("capability contract/grants = %d/%s", got.CapabilityContract, got.Grants)
	}
	if strings.TrimSpace(string(got.Config)) != "{}" {
		e.t.Errorf("config = %s, want {} (never null)", got.Config)
	}
	if got.DataDir != c.DataDir || got.CacheDir != c.CacheDir {
		e.t.Errorf("dirs = %q, %q; want %q, %q verbatim", got.DataDir, got.CacheDir, c.DataDir, c.CacheDir)
	}
}

func (e *env) r03() {
	_, c := e.startFails(BehaviourBadProtocol)
	e.requireGone(c, "pid", "after a protocol mismatch")
}

func (e *env) r04() {
	_, c := e.startFails(BehaviourNoID)
	e.requireGone(c, "pid", "after an empty plugin id")
}

func (e *env) r05() {
	for _, tc := range []struct{ behavior, marker string }{
		{BehaviourInitError, InitFailedMarker},
		{BehaviourLoadError, LoadFailedMarker},
	} {
		s, c := e.startFails(tc.behavior)
		if !strings.Contains(s.err.Error(), tc.marker) {
			e.t.Errorf("%s: the error does not carry the plugin's stderr tail (%s): %v", tc.behavior, tc.marker, s.err)
		}
		e.requireGone(c, "pid", "after "+tc.behavior)
	}
}

func (e *env) r06() {
	s, c := e.startFails(BehaviourHangOnInit) // spawn uses a context with no deadline
	e.t.Logf("hang-on-init failed after %s", s.took)
	e.requireGone(c, "pid", "after a handshake timeout")
}

func (e *env) r07() {
	inst, _ := e.start(BehaviourEcho)
	const slowMS = 2000
	type outcome struct {
		slept float64
		err   error
	}
	slow := make(chan outcome, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		content, err := e.callTool(ctx, inst, "sleep", map[string]any{"ms": slowMS})
		var got struct{ Slept float64 }
		if err == nil {
			err = json.Unmarshal(content, &got)
		}
		slow <- outcome{got.Slept, err}
	}()
	time.Sleep(200 * time.Millisecond) // let the slow call get in flight

	fastBudget := slowMS*time.Millisecond - 500*time.Millisecond
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), fastBudget)
			defer cancel()
			if err := e.echo(ctx, inst, fmt.Sprintf("fast-%d", i)); err != nil {
				errs <- fmt.Errorf("fast call %d: %w", i, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		e.t.Error(err)
	}
	var got outcome
	select {
	case got = <-slow:
		e.t.Fatal("the slow call finished before the fast ones: calls were serialized, not correlated")
	default:
		got = <-slow
	}
	if got.err != nil || got.slept != slowMS {
		e.t.Errorf("slow call = %v, %v; want its own result", got.slept, got.err)
	}
}

func (e *env) r08() {
	inst, _ := e.start(BehaviourEcho)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	start := time.Now()
	_, err := e.callTool(ctx, inst, "sleep", map[string]any{"ms": 1500})
	cancel()
	if err == nil {
		e.t.Fatal("a call with a 300ms deadline to a 1500ms tool succeeded")
	}
	if took := time.Since(start); took > 1200*time.Millisecond {
		e.t.Errorf("the call took %s: its context was ignored", took)
	}
	if inst.IsGone(err) {
		e.t.Fatalf("a timeout must not look like gone: %v", err)
	}
	e.mustEcho(inst, "right after the timeout", 5*time.Second)
	time.Sleep(1400 * time.Millisecond) // the abandoned call's reply arrives meanwhile
	e.mustEcho(inst, "after the late reply", 5*time.Second)
	select {
	case <-inst.Exited():
		e.t.Fatal("the plugin exited")
	default:
	}
}

func (e *env) r09() {
	inst, _ := e.start(BehaviourCrashOnCall)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), e.cfg.startBound)
	defer cancel()
	_, err := e.callTool(ctx, inst, "anything", nil)
	if err == nil {
		e.t.Fatal("a call to a crashing plugin succeeded")
	}
	if !inst.IsGone(err) {
		e.t.Errorf("the in-flight call failed with %v, which IsGone does not recognize (a timeout is not an answer)", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		e.t.Errorf("the in-flight call took %s to fail", took)
	}
	select {
	case <-inst.Exited():
	case <-time.After(5 * time.Second):
		e.t.Error("Exited() did not close")
	}
	later := time.Now()
	_, err = e.callTool(ctx, inst, "anything", nil)
	if err == nil || !inst.IsGone(err) {
		e.t.Errorf("a later call failed with %v, want a gone error", err)
	}
	if took := time.Since(later); took > 5*time.Second {
		e.t.Errorf("the later call took %s to fail", took)
	}
}

func (e *env) r10() {
	inst, _ := e.start(BehaviourGarbage)
	for i := range 3 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		content, err := e.callTool(ctx, inst, "anything", nil)
		cancel()
		if err != nil {
			e.t.Fatalf("call %d after garbage: %v", i, err)
		}
		if !strings.Contains(string(content), "garbage") {
			e.t.Fatalf("call %d got %s, not its own reply", i, content)
		}
	}
}

func (e *env) r11() {
	for i := range 10 {
		inst, _ := e.start(BehaviourExitAfterResponse)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		content, err := e.callTool(ctx, inst, "anything", nil)
		cancel()
		if err != nil {
			e.t.Fatalf("iteration %d: the final frame written before the plugin exited was lost: %v", i, err)
		}
		if !strings.Contains(string(content), "word") {
			e.t.Fatalf("iteration %d: got %s", i, content)
		}
	}
}

func (e *env) r12() {
	inst, _ := e.start(BehaviourEcho)
	const size = 7 << 20
	var big struct{ Data string }
	e.mustTool(inst, "big", map[string]any{"bytes": size}, &big)
	if len(big.Data) != size {
		e.t.Fatalf("got %d bytes of a %d byte response", len(big.Data), size)
	}
	overCtx, overCancel := context.WithTimeout(context.Background(), time.Second)
	_, overErr := e.callTool(overCtx, inst, "big", map[string]any{"bytes": 9 << 20})
	overCancel()
	if overErr == nil {
		e.t.Fatal("default inbound cap accepted a 9 MiB response")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = e.callTool(ctx, inst, "echo", map[string]any{"message": strings.Repeat("a", 9<<20)})
	// The plugin must still be there afterwards. A host that sends the frame
	// kills it: Serve ends its read loop on a line over 8 MiB, and the
	// plugin exits. So the request has to be refused before it is written.
	if err := e.echo(ctx, inst, "still alive"); err != nil {
		e.t.Fatalf("the plugin did not survive the 9 MiB request: %v (frames over 8 MiB must be refused client-side)", err)
	}
}

func (e *env) r13() {
	inst, c := e.start(BehaviourEcho)
	start := time.Now()
	e.stopWithin(inst)
	if _, err := os.Stat(filepath.Join(c.DataDir, "serve-returned")); err != nil {
		e.t.Fatalf("Serve never returned: the plugin was killed instead of being told to stop (%v)", err)
	}
	e.t.Logf("graceful stop took %s", time.Since(start))
}

// stopWithin stops inst and requires it to return within the stop bound.
func (e *env) stopWithin(inst Instance) {
	e.t.Helper()
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), e.cfg.stopBound)
		defer cancel()
		done <- inst.Stop(ctx)
	}()
	select {
	case err := <-done:
		if err != nil {
			e.t.Logf("Stop returned %v", err)
		}
	case <-time.After(e.cfg.stopBound):
		e.t.Fatalf("Stop did not return within %s", e.cfg.stopBound)
	}
}

func (e *env) r14() {
	e.needPIDs()
	inst, c := e.start(BehaviourWedge)
	e.stopWithin(inst)
	e.requireGone(c, "pid", "after Stop of a wedged plugin")
}

func (e *env) r15() {
	e.needPIDs()
	inst, c := e.start(BehaviourWedge)
	if _, err := readPIDFile(filepath.Join(c.DataDir, "grandchild.pid")); err != nil {
		e.t.Fatalf("the fixture recorded no grandchild: %v", err)
	}
	e.stopWithin(inst)
	e.requireGone(c, "grandchild.pid", "the wedged plugin's grandchild after Stop")
}

func (e *env) r16() {
	c := e.newCase(BehaviourEcho, nil)
	ctx, cancel := context.WithCancel(context.Background())
	s := e.spawn(ctx, c)
	if s.err != nil {
		e.t.Fatal(s.err)
	}
	cancel()
	time.Sleep(200 * time.Millisecond)
	select {
	case <-s.inst.Exited():
		e.t.Fatal("the plugin died when the context given to Start was canceled")
	default:
	}
	e.mustEcho(s.inst, "after the start context ended", 10*time.Second)
}

func (e *env) r17() {
	const secret = "tok-9f8e7d6c5b4a-SECRET-VALUE" //nolint:gosec // a made-up value the suite plants to prove it is redacted
	c := e.newCase(BehaviourStderrFlood, []string{secret}, EnvSecret+"="+secret)
	s := e.spawn(context.Background(), c)
	if s.err != nil {
		e.t.Fatalf("Start: %v", s.err)
	}
	check := func(when string) {
		d := s.inst.Diagnostics()
		if len(d) > e.cfg.diagnostics {
			e.t.Errorf("%s: Diagnostics is %d bytes, cap %d: stderr is not bounded", when, len(d), e.cfg.diagnostics)
		}
		if strings.Contains(d, secret) {
			e.t.Errorf("%s: Diagnostics contains the secret", when)
		}
	}
	check("while running")
	e.stopWithin(s.inst)
	check("after Stop")
	if strings.TrimSpace(s.inst.Diagnostics()) == "" {
		e.t.Error("Diagnostics is empty after the plugin wrote 8 MiB to stderr: the host keeps no stderr tail " +
			"(a host that cannot should waive R17 and say so)")
	}
}

func (e *env) r18() {
	inst, _ := e.start(BehaviourEcho)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := inst.Call(ctx, "no/such/method", nil, nil)
	if code, ok := inst.RPCCode(err); !ok || code != -32601 {
		e.t.Errorf("unknown method: code %d (ok=%v) from %v, want -32601", code, ok, err)
	}
	for _, want := range []int{-32000, -32001, -32002, -32003} {
		_, err := e.callTool(ctx, inst, "error", map[string]any{"code": want})
		if code, ok := inst.RPCCode(err); !ok || code != want {
			e.t.Errorf("application error: code %d (ok=%v) from %v, want %d", code, ok, err, want)
		}
	}
}
