package pluginhosttest

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	plugin "github.com/hollis-labs/plugin-sdk"
	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

// Environment variables the fixture plugin reads. [FixtureCommand] sets them.
const (
	// EnvBehaviour selects the fixture behavior; MaybeRunFixture is a no-op
	// when it is unset.
	EnvBehaviour = "PLUGINHOSTTEST_BEHAVIOUR" //nolint:misspell // the variable name is fixed by the design brief
	// EnvDir is a directory the fixture writes into: "pid" (its own pid),
	// "grandchild.pid" (wedge), and "serve-returned" (echo, written after
	// subprocess.Serve returns, so its presence proves a graceful stop).
	EnvDir = "PLUGINHOSTTEST_DIR"
	// EnvSecret is a value the stderr-flood behavior embeds in its output.
	EnvSecret = "PLUGINHOSTTEST_SECRET"
)

// Fixture behaviors. Echo runs the real subprocess.Serve; every other one
// is a raw stdio loop, because a hostile plugin is exactly one that does not
// go through Serve.
const (
	// BehaviourEcho is a compliant plugin (plugin-sdk Serve with an
	// MCPHandler and a HealthChecker). Its tools: echo {message}, sleep {ms},
	// pid, env, init (the InitParams it received), trace (the lifecycle
	// methods it saw, in order), exit {code}, error {code}, big {bytes},
	// set_health {ok}.
	BehaviourEcho = "echo"
	// BehaviourHangOnInit reads its stdin and never answers.
	BehaviourHangOnInit = "hang-on-init"
	// BehaviourBadProtocol answers with a legacy protocol-1 result.
	BehaviourBadProtocol = "bad-protocol"
	// BehaviourBadContract acknowledges an unsupported capability contract.
	BehaviourBadContract = "bad-contract"
	// BehaviourProfileAck claims reverse services the driver cannot provide.
	BehaviourProfileAck = "profile-ack"
	// BehaviourDuplicateInit returns a duplicate security field.
	BehaviourDuplicateInit = "duplicate-init"
	// BehaviourNoID answers init with an empty plugin id.
	BehaviourNoID = "no-id"
	// BehaviourInitError answers init with an error, after printing
	// [InitFailedMarker] on stderr.
	BehaviourInitError = "init-error"
	// BehaviourLoadError answers load with an error, after printing
	// [LoadFailedMarker] on stderr.
	BehaviourLoadError = "load-error"
	// BehaviourCrashOnCall completes the handshake, then exits with status 3
	// on the first non-lifecycle request without answering it.
	BehaviourCrashOnCall = "crash-on-call"
	// BehaviourGarbage completes the handshake, then answers each further
	// request preceded by junk lines: not JSON, a wrong id, id 0, null,
	// invalid UTF-8.
	BehaviourGarbage = "garbage"
	// BehaviourExitAfterResponse answers the first non-lifecycle request and
	// exits immediately after writing the answer.
	BehaviourExitAfterResponse = "exit-after-response"
	// BehaviourWedge completes the handshake and then ignores everything:
	// stdin EOF, SIGTERM and unload. It first forks a grandchild that
	// inherits its stdio and sleeps forever.
	BehaviourWedge = "wedge"
	// BehaviourStderrFlood writes 8 MiB to stderr, with the secret from
	// [EnvSecret] scattered through it, before behaving like echo.
	BehaviourStderrFlood = "stderr-flood"

	// BehaviourHangOnRestart behaves like echo the first time it starts in a
	// given EnvDir and like hang-on-init every time after, which is what a
	// supervisor's restart-in-flight looks like from outside.
	BehaviourHangOnRestart = "hang-on-restart"

	// BehaviourDeaf completes the handshake and then stops reading stdin
	// forever, so a host's writes eventually block on a full pipe.
	BehaviourDeaf = "deaf"

	// BehaviourLoadSkips acknowledges load with one skipped declaration.
	BehaviourLoadSkips = "load-skips"
	// BehaviourUnloadError completes load but refuses unload. The host must
	// still close stdin, reap the child and dispose its scope.
	BehaviourUnloadError = "unload-error"
	// BehaviourWrongID announces a different canonical plugin identity.
	BehaviourWrongID = "wrong-id"

	behaviorSleeper = "sleeper" // the wedge's grandchild
)

// Markers the failing-handshake behaviors print on stderr, for tests that
// assert the stderr tail reaches the error.
const (
	InitFailedMarker = "FIXTURE-INIT-FAILED-MARKER"
	LoadFailedMarker = "FIXTURE-LOAD-FAILED-MARKER"
)

const (
	floodBytes = 8 << 20
	// maxRequestLine mirrors the limit in plugin-sdk's Serve; the raw loop
	// exits on a longer line the way Serve does.
	maxRequestLine = 8 << 20
)

// FixtureCommand returns the executable and exact environment that make a
// re-execution of the current test binary act as the named fixture behavior.
// dir becomes [EnvDir]; extra is appended to the environment. The test
// binary's TestMain must call [MaybeRunFixture] first.
func FixtureCommand(behavior, dir string, extra ...string) (command string, env []string) {
	exe, err := os.Executable()
	if err != nil {
		exe = os.Args[0]
	}
	// GORACE: a race-instrumented binary sleeps one second at exit by
	// default, which would make every graceful stop look like a slow one.
	env = []string{EnvBehaviour + "=" + behavior, "GORACE=atexit_sleep_ms=0"}
	if dir != "" {
		env = append(env, EnvDir+"="+dir)
	}
	return exe, append(env, extra...)
}

// MaybeRunFixture makes the test binary act as a fixture plugin when
// [EnvBehaviour] is set, and returns immediately when it is not. It must be
// the first line of TestMain:
//
//	func TestMain(m *testing.M) {
//		pluginhosttest.MaybeRunFixture()
//		os.Exit(m.Run())
//	}
//
// The fixture is the test binary re-executing itself, so no go build is
// involved. When it acts as a fixture it never returns.
func MaybeRunFixture() {
	behavior := os.Getenv(EnvBehaviour)
	if behavior == "" {
		return
	}
	writePID("pid")
	code := 0
	switch behavior {
	case BehaviourEcho, BehaviourLoadSkips:
		code = runEcho()
	case BehaviourStderrFlood:
		flood()
		code = runEcho()
	case BehaviourHangOnRestart:
		if startCount() > 1 {
			code = runRaw(BehaviourHangOnInit)
		} else {
			code = runEcho()
		}
	case behaviorSleeper:
		ignoreSignals()
		sleepForever()
	default:
		code = runRaw(behavior)
	}
	os.Exit(code)
}

// sleepForever blocks without tripping the runtime's deadlock detector, which
// a bare select{} would: that kills the process, and a wedge must not die.
func sleepForever() {
	for {
		time.Sleep(time.Hour)
	}
}

func writePID(name string) { writeDirFile(name, strconv.Itoa(os.Getpid())) }

// writeDirFile writes name into EnvDir; without one it does nothing. The
// directory comes from the test that spawned this fixture, never from input.
func writeDirFile(name, content string) {
	if dir := os.Getenv(EnvDir); dir != "" {
		// Publish a complete record atomically: a restart can write its pid while
		// the parent is polling the previous one. Truncating the destination
		// exposes an empty/intermediate record and makes the race fixture flaky.
		f, err := os.CreateTemp(dir, ".fixture-record-*")
		if err != nil {
			return
		}
		path := f.Name()
		defer func() { _ = os.Remove(path) }() //nolint:gosec // path is from CreateTemp under the spawning test's private directory
		_, err = f.WriteString(content)
		closeErr := f.Close()
		if err == nil && closeErr == nil {
			_ = os.Rename(path, filepath.Join(dir, name)) //nolint:gosec // fixed fixture record under the spawning test's private directory
		}
	}
}

// startCount records this start in EnvDir/starts and returns how many starts
// that directory has seen, this one included.
func startCount() int {
	dir := os.Getenv(EnvDir)
	if dir == "" {
		return 1
	}
	n := 0
	if b, err := os.ReadFile(filepath.Join(dir, "starts")); err == nil { //nolint:gosec // test-owned temp dir named by the spawning test
		n, _ = strconv.Atoi(strings.TrimSpace(string(b)))
	}
	n++
	writeDirFile("starts", strconv.Itoa(n))
	return n
}

func flood() {
	secret := os.Getenv(EnvSecret)
	chunk := strings.Repeat("flood-line-of-diagnostic-noise ", 100) // ~3 KB
	w := bufio.NewWriterSize(os.Stderr, 64*1024)
	written := 0
	for i := 0; written < floodBytes; i++ {
		line := chunk
		if secret != "" && i%3 == 0 {
			// Offsets drift, so retained-window edges land inside the secret.
			line = chunk[:i%97] + secret + chunk[i%97:]
		}
		n, _ := w.WriteString(line + "\n")
		written += n
	}
	if secret != "" {
		_, _ = w.WriteString("final: " + secret)
	}
	_ = w.Flush()
	fmt.Fprintln(os.Stderr, "\nflood-complete")
}

// --- compliant fixture: the real subprocess.Serve ---

func runEcho() int {
	p := &echoPlugin{}
	p.healthy.Store(true)
	err := subprocess.Serve(p)
	// Written only after Serve returned: its presence proves the host ended
	// the plugin through stdin EOF or SIGTERM, not SIGKILL.
	writeDirFile("serve-returned", "1")
	if err != nil {
		return 1
	}
	return 0
}

type echoPlugin struct {
	mu      sync.Mutex
	trace   []string
	init    subprocess.InitParams
	healthy atomic.Bool
}

func (p *echoPlugin) record(method string) {
	p.mu.Lock()
	p.trace = append(p.trace, method)
	p.mu.Unlock()
}

func (p *echoPlugin) Init(_ context.Context, params subprocess.InitParams) (subprocess.InitResult, error) {
	p.record(subprocess.MethodInit)
	p.mu.Lock()
	p.init = params
	p.mu.Unlock()
	return subprocess.InitResult{ID: "fixture", Name: "Fixture", Version: "1.0.0", Protocol: subprocess.ProtocolVersion, CapabilityContract: capability.ContractVersion}, nil
}

func (p *echoPlugin) Load(context.Context) (subprocess.LoadResult, error) {
	p.record(subprocess.MethodLoad)
	if os.Getenv(EnvBehaviour) == BehaviourLoadSkips {
		return subprocess.LoadResult{SkippedRegistrations: []subprocess.SkippedRegistration{{Kind: "command", ID: "example", Reason: "fixture opt-out"}}}, nil
	}
	return subprocess.LoadResult{}, nil
}

func (p *echoPlugin) Unload(context.Context) error {
	p.record(subprocess.MethodUnload)
	return nil
}

func (p *echoPlugin) Health(context.Context) (subprocess.HealthStatus, error) {
	if !p.healthy.Load() {
		return subprocess.HealthStatus{OK: false, Message: "forced unhealthy"}, nil
	}
	return subprocess.HealthStatus{OK: true}, nil
}

func (p *echoPlugin) MCPCallTool(ctx context.Context, req subprocess.MCPCallRequest) (subprocess.MCPCallResult, error) {
	arg := func(key string) float64 { v, _ := req.Arguments[key].(float64); return v }
	var out any
	switch req.ToolName {
	case "echo":
		msg, _ := req.Arguments["message"].(string)
		out = map[string]any{"echo": msg}
	case "sleep":
		select {
		case <-time.After(time.Duration(arg("ms")) * time.Millisecond):
		case <-ctx.Done():
		}
		out = map[string]any{"slept": arg("ms")}
	case "pid":
		out = map[string]any{"pid": os.Getpid()}
	case "env":
		out = os.Environ()
	case "init":
		p.mu.Lock()
		out = p.init
		p.mu.Unlock()
	case "trace":
		p.mu.Lock()
		out = append([]string(nil), p.trace...)
		p.mu.Unlock()
	case "exit":
		os.Exit(int(arg("code")))
	case "set_health":
		ok, _ := req.Arguments["ok"].(bool)
		p.healthy.Store(ok)
		out = map[string]any{"ok": ok}
	case "big":
		out = map[string]any{"data": strings.Repeat("x", int(arg("bytes")))}
	case "error":
		switch int(arg("code")) {
		case subprocess.ErrCodeNotFound:
			return subprocess.MCPCallResult{}, &plugin.Error{Code: 404, Message: "not found on purpose"}
		case subprocess.ErrCodeConflict:
			return subprocess.MCPCallResult{}, &plugin.Error{Code: 409, Message: "conflict on purpose"}
		case subprocess.ErrCodeValidation:
			return subprocess.MCPCallResult{}, &plugin.Error{Code: 422, Message: "invalid on purpose"}
		case subprocess.ErrCodeCancelled:
			return subprocess.MCPCallResult{}, plugin.ErrCancelled
		default:
			return subprocess.MCPCallResult{}, errors.New("internal on purpose")
		}
	default:
		return subprocess.MCPCallResult{}, fmt.Errorf("unknown tool %q", req.ToolName)
	}
	content, err := json.Marshal(out)
	if err != nil {
		return subprocess.MCPCallResult{}, err
	}
	return subprocess.MCPCallResult{Content: content}, nil
}

// --- hostile fixtures: a raw stdio loop ---

type rawRequest struct {
	ID     int64  `json:"id"`
	Method string `json:"method"`
}

// rawWriter serializes frames onto stdout.
type rawWriter struct{ mu sync.Mutex }

func (w *rawWriter) line(b []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, _ = os.Stdout.Write(append(b, '\n'))
}

func (w *rawWriter) result(id int64, result any) {
	payload, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
	w.line(payload)
}

func (w *rawWriter) fail(id int64, code int, message string) {
	payload, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": code, "message": message},
	})
	w.line(payload)
}

func runRaw(behavior string) int {
	switch behavior {
	case BehaviourHangOnInit, BehaviourBadProtocol, BehaviourBadContract, BehaviourProfileAck, BehaviourDuplicateInit, BehaviourNoID, BehaviourInitError, BehaviourLoadError,
		BehaviourCrashOnCall, BehaviourGarbage, BehaviourExitAfterResponse, BehaviourWedge, BehaviourDeaf, BehaviourUnloadError, BehaviourWrongID:
	default:
		fmt.Fprintf(os.Stderr, "pluginhosttest: unknown fixture behavior %q\n", behavior)
		return 2
	}
	if behavior == BehaviourWedge {
		ignoreSignals()
		startGrandchild()
	}

	w := &rawWriter{}
	in := bufio.NewReaderSize(os.Stdin, 64*1024)
	for {
		line, err := in.ReadBytes('\n')
		if len(line) > maxRequestLine {
			return 1 // Serve's scanner gives up on a longer line; so does this
		}
		var req rawRequest
		if len(line) > 0 && json.Unmarshal(line, &req) == nil {
			if code, exit := handleRaw(behavior, w, req); exit {
				return code
			}
		}
		if err != nil {
			if behavior == BehaviourWedge {
				sleepForever() // stdin EOF is ignored
			}
			return 0
		}
	}
}

// handleRaw answers one request. It reports whether the fixture exits.
func handleRaw(behavior string, w *rawWriter, req rawRequest) (int, bool) {
	if behavior == BehaviourHangOnInit {
		return 0, false
	}
	switch req.Method {
	case subprocess.MethodInit:
		switch behavior {
		case BehaviourInitError:
			fmt.Fprintln(os.Stderr, "fixture: init failed on purpose ("+InitFailedMarker+")")
			w.fail(req.ID, subprocess.ErrCodeInternal, "init failed on purpose")
			return 0, false
		case BehaviourBadProtocol:
			w.result(req.ID, map[string]any{"id": "fixture", "name": "Fixture", "version": "1.0.0", "protocol": 1})
			return 0, false
		case BehaviourBadContract:
			w.result(req.ID, map[string]any{"id": "fixture", "name": "Fixture", "version": "1.0.0", "description": "", "protocol": 2, "capability_contract": 2})
			return 0, false
		case BehaviourProfileAck:
			w.result(req.ID, map[string]any{"id": "fixture", "name": "Fixture", "version": "1.0.0", "description": "", "protocol": 2, "capability_contract": 1, "reverse_rpc_version": 1})
			return 0, false
		case BehaviourDuplicateInit:
			w.line([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":{"id":"fixture","name":"Fixture","version":"1.0.0","description":"","protocol":2,"protocol":2,"capability_contract":1}}`, req.ID)))
			return 0, false
		case BehaviourWrongID:
			w.result(req.ID, subprocess.InitResult{ID: "other", Name: "Other", Version: "1.0.0", Protocol: subprocess.ProtocolVersion, CapabilityContract: capability.ContractVersion})
			return 0, false
		case BehaviourNoID:
			w.result(req.ID, map[string]any{"id": "", "name": "Fixture", "version": "1.0.0", "description": "", "protocol": subprocess.ProtocolVersion, "capability_contract": capability.ContractVersion})
			return 0, false
		default:
			w.result(req.ID, subprocess.InitResult{ID: "fixture", Name: "Fixture", Version: "1.0.0", Protocol: subprocess.ProtocolVersion, CapabilityContract: capability.ContractVersion})
			return 0, false
		}
	case subprocess.MethodLoad:
		if behavior == BehaviourLoadError {
			fmt.Fprintln(os.Stderr, "fixture: load failed on purpose ("+LoadFailedMarker+")")
			w.fail(req.ID, subprocess.ErrCodeInternal, "load failed on purpose")
			return 0, false
		}
		w.result(req.ID, subprocess.LoadResult{})
		if behavior == BehaviourDeaf {
			sleepForever()
		}
		return 0, false
	}
	if behavior == BehaviourWedge {
		return 0, false // unload included: the wedge answers nothing
	}
	if req.Method == subprocess.MethodUnload {
		if behavior == BehaviourUnloadError {
			w.fail(req.ID, subprocess.ErrCodeInternal, "unload refused")
			return 0, false
		}
		w.result(req.ID, map[string]bool{"ok": true})
		return 0, false
	}
	switch behavior {
	case BehaviourCrashOnCall:
		return 3, true
	case BehaviourGarbage:
		for _, junk := range [][]byte{
			[]byte("this is not json"),
			[]byte(`{"jsonrpc":"2.0","id":987654,"result":"wrong id"}`),
			[]byte(`{"jsonrpc":"2.0","id":0,"result":"id zero"}`),
			[]byte("null"),
			{0xff, 0xfe, 0xfd, 0xfc},
			[]byte(`{"jsonrpc":"2.0","id":`),
		} {
			w.line(junk)
		}
		w.result(req.ID, map[string]any{"content": map[string]any{"after": "garbage"}})
		return 0, false
	case BehaviourExitAfterResponse:
		w.result(req.ID, map[string]any{"content": map[string]any{"last": "word"}})
		return 0, true
	}
	w.result(req.ID, map[string]any{"content": map[string]any{"ok": true}})
	return 0, false
}

// startGrandchild forks a sleeper that inherits this process's stdio, so it
// pins the same pipes a real plugin runtime's helper would.
func startGrandchild() {
	exe, err := os.Executable()
	if err != nil {
		return
	}
	cmd := exec.Command(exe) // #nosec G204 -- re-executes this test binary.
	cmd.Env = []string{EnvBehaviour + "=" + behaviorSleeper}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		return
	}
	writeDirFile("grandchild.pid", strconv.Itoa(cmd.Process.Pid))
}

// FixtureInit is a complete protocol-2 payload for the test fixture. Its tuple
// is test-only; real hosts supply their own persisted incarnation and grants.
func FixtureInit(dataDir, cacheDir string) subprocess.InitParams {
	return subprocess.InitParams{PluginDir: dataDir, DataDir: dataDir, CacheDir: cacheDir,
		Config: map[string]string{}, LogLevel: "info", HostInfo: subprocess.HostInfo{Version: "1.0.0", Protocol: subprocess.ProtocolVersion},
		CapabilityContract: capability.ContractVersion, Incarnation: capability.RuntimeIdentity{HostInstance: "fixture-host", OwnerID: "fixture", OwnerGeneration: 1}, Grants: capability.GrantSet{}}
}
