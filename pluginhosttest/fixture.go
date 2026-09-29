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
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

// Environment variables the fixture plugin reads. [FixtureCommand] sets them.
const (
	// EnvBehaviour selects the fixture behaviour; MaybeRunFixture is a no-op
	// when it is unset.
	EnvBehaviour = "PLUGINHOSTTEST_BEHAVIOUR"
	// EnvDir is a directory the fixture writes into: "pid" (its own pid),
	// "grandchild.pid" (wedge), and "serve-returned" (echo, written after
	// subprocess.Serve returns, so its presence proves a graceful stop).
	EnvDir = "PLUGINHOSTTEST_DIR"
	// EnvSecret is a value the stderr-flood behaviour embeds in its output.
	EnvSecret = "PLUGINHOSTTEST_SECRET"
)

// Fixture behaviours. Echo runs the real subprocess.Serve; every other one
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
	// BehaviourBadProtocol answers init with protocol 2.
	BehaviourBadProtocol = "bad-protocol"
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

	behaviourSleeper = "sleeper" // the wedge's grandchild
)

// Markers the failing-handshake behaviours print on stderr, for tests that
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
// re-execution of the current test binary act as the named fixture behaviour.
// dir becomes [EnvDir]; extra is appended to the environment. The test
// binary's TestMain must call [MaybeRunFixture] first.
func FixtureCommand(behaviour, dir string, extra ...string) (command string, env []string) {
	exe, err := os.Executable()
	if err != nil {
		exe = os.Args[0]
	}
	// GORACE: a race-instrumented binary sleeps one second at exit by
	// default, which would make every graceful stop look like a slow one.
	env = []string{EnvBehaviour + "=" + behaviour, "GORACE=atexit_sleep_ms=0"}
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
	behaviour := os.Getenv(EnvBehaviour)
	if behaviour == "" {
		return
	}
	writePID("pid")
	code := 0
	switch behaviour {
	case BehaviourEcho:
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
	case behaviourSleeper:
		ignoreSignals()
		select {}
	default:
		code = runRaw(behaviour)
	}
	os.Exit(code)
}

func writePID(name string) {
	if dir := os.Getenv(EnvDir); dir != "" {
		_ = os.WriteFile(filepath.Join(dir, name), []byte(strconv.Itoa(os.Getpid())), 0o600)
	}
}

// startCount records this start in EnvDir/starts and returns how many starts
// that directory has seen, this one included.
func startCount() int {
	dir := os.Getenv(EnvDir)
	if dir == "" {
		return 1
	}
	path := filepath.Join(dir, "starts")
	n := 0
	if b, err := os.ReadFile(path); err == nil {
		n, _ = strconv.Atoi(strings.TrimSpace(string(b)))
	}
	n++
	_ = os.WriteFile(path, []byte(strconv.Itoa(n)), 0o600)
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
	if dir := os.Getenv(EnvDir); dir != "" {
		_ = os.WriteFile(filepath.Join(dir, "serve-returned"), []byte("1"), 0o600)
	}
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
	return subprocess.InitResult{ID: "fixture", Name: "Fixture", Version: "test", Protocol: subprocess.ProtocolVersion}, nil
}

func (p *echoPlugin) Load(context.Context) (subprocess.LoadResult, error) {
	p.record(subprocess.MethodLoad)
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

func runRaw(behaviour string) int {
	switch behaviour {
	case BehaviourHangOnInit, BehaviourBadProtocol, BehaviourNoID, BehaviourInitError, BehaviourLoadError,
		BehaviourCrashOnCall, BehaviourGarbage, BehaviourExitAfterResponse, BehaviourWedge:
	default:
		fmt.Fprintf(os.Stderr, "pluginhosttest: unknown fixture behaviour %q\n", behaviour)
		return 2
	}
	if behaviour == BehaviourWedge {
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
			if code, exit := handleRaw(behaviour, w, req); exit {
				return code
			}
		}
		if err != nil {
			if behaviour == BehaviourWedge {
				select {} // stdin EOF is ignored
			}
			return 0
		}
	}
}

// handleRaw answers one request. It reports whether the fixture exits.
func handleRaw(behaviour string, w *rawWriter, req rawRequest) (int, bool) {
	if behaviour == BehaviourHangOnInit {
		return 0, false
	}
	switch req.Method {
	case subprocess.MethodInit:
		switch behaviour {
		case BehaviourInitError:
			fmt.Fprintln(os.Stderr, "fixture: init failed on purpose ("+InitFailedMarker+")")
			w.fail(req.ID, subprocess.ErrCodeInternal, "init failed on purpose")
			return 0, false
		case BehaviourBadProtocol:
			w.result(req.ID, subprocess.InitResult{ID: "fixture", Name: "Fixture", Version: "test", Protocol: 2})
			return 0, false
		case BehaviourNoID:
			w.result(req.ID, subprocess.InitResult{Name: "Fixture", Version: "test", Protocol: subprocess.ProtocolVersion})
			return 0, false
		default:
			w.result(req.ID, subprocess.InitResult{ID: "fixture", Name: "Fixture", Version: "test", Protocol: subprocess.ProtocolVersion})
			return 0, false
		}
	case subprocess.MethodLoad:
		if behaviour == BehaviourLoadError {
			fmt.Fprintln(os.Stderr, "fixture: load failed on purpose ("+LoadFailedMarker+")")
			w.fail(req.ID, subprocess.ErrCodeInternal, "load failed on purpose")
			return 0, false
		}
		w.result(req.ID, subprocess.LoadResult{})
		return 0, false
	}
	if behaviour == BehaviourWedge {
		return 0, false // unload included: the wedge answers nothing
	}
	if req.Method == subprocess.MethodUnload {
		w.result(req.ID, map[string]bool{"ok": true})
		return 0, false
	}
	switch behaviour {
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
		w.result(req.ID, map[string]any{"after": "garbage"})
		return 0, false
	case BehaviourExitAfterResponse:
		w.result(req.ID, map[string]any{"last": "word"})
		return 0, true
	}
	w.result(req.ID, map[string]any{"ok": true})
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
	cmd.Env = []string{EnvBehaviour + "=" + behaviourSleeper}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		return
	}
	if dir := os.Getenv(EnvDir); dir != "" {
		_ = os.WriteFile(filepath.Join(dir, "grandchild.pid"), []byte(strconv.Itoa(cmd.Process.Pid)), 0o600)
	}
}
