//go:build unix

package pluginhost

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-sdk/subprocess"
)

const interopEventLimit = 4096

// The bridge is a test child in Process's own process group. Worker stdout and
// stdin remain the actual protocol pipes; only fixture controls use the socket.
func TestInteropControlBridge(t *testing.T) {
	if os.Getenv("INTEROP_BRIDGE") != "1" {
		return
	}
	if err := runInteropBridge(); err != nil {
		if childExit, ok := err.(*interopWorkerExit); ok { //nolint:errorlint // Only a sole worker exit is natural; joined observer failures must remain fatal.
			os.Exit(childExit.code)
		}
		fmt.Fprintln(os.Stderr, "interop harness failure:", err)
		os.Exit(125)
	}
	os.Exit(0)
}

type interopWorkerExit struct{ code int }

func (e *interopWorkerExit) Error() string { return fmt.Sprintf("fixture worker exit %d", e.code) }

func runInteropBridge() error {
	socket, err := net.DialTimeout("unix", os.Getenv("INTEROP_SOCKET"), time.Second) //nolint:gosec // Explicit opt-in test-owned fixture, control or receipt path; never plugin-provided.
	if err != nil {
		return err
	}
	defer socket.Close()
	// A duplicated nonblocking pipe gets its own Go poller registration so Close
	// interrupts a read. Inherited os.Stdin can be a blocking descriptor whose
	// Close waits for a read, creating a child-exit/parent-EOF cycle.
	inputFD, err := syscall.Dup(0)
	if err != nil {
		return err
	}
	if err = syscall.SetNonblock(inputFD, true); err != nil {
		_ = syscall.Close(inputFD)
		return err
	}
	inputSource := os.NewFile(uintptr(inputFD), "interop-protocol-input") //nolint:gosec // Successful Dup returns a nonnegative kernel fd, representable as uintptr.
	defer inputSource.Close()
	_ = os.Stdin.Close()
	var args []string
	if err = json.Unmarshal([]byte(os.Getenv("INTEROP_ARGS")), &args); err != nil {
		return err
	}
	cmd := exec.Command(os.Getenv("INTEROP_COMMAND"), args...) //nolint:gosec // Explicit opt-in test-owned fixture, control or receipt path; never plugin-provided.
	cmd.WaitDelay = 2 * time.Second
	cmd.Env = []string{"SDK_FIXTURE_CHILD=" + os.Getenv("INTEROP_PROFILE"), "GORACE=atexit_sleep_ms=0", "GOMAXPROCS=4"}
	inputR, inputW, err := os.Pipe()
	if err != nil {
		return err
	}
	defer inputR.Close()
	defer inputW.Close()
	cmd.Stdin = inputR
	controlR, controlW, err := os.Pipe()
	if err != nil {
		return err
	}
	defer controlR.Close()
	defer controlW.Close()
	path := os.Getenv("INTEROP_PATH_CONTROL")
	if path != "" {
		cmd.Env = append(cmd.Env, "SDK_FIXTURE_CONTROL_PATH="+path)
	} else {
		cmd.ExtraFiles = []*os.File{controlR}
	}
	events := make(chan []byte, 128)
	failures := make(chan error, 3)
	eventsDone := make(chan struct{})
	controlDone := make(chan struct{})
	var controlClosing atomic.Bool
	var inputClosed atomic.Bool
	inputDone := make(chan struct{})
	observer := &interopEventWriter{events: events, diagnostics: os.Stderr, onFinished: func() { inputClosed.Store(true); _ = controlW.Close(); _ = inputW.Close(); _ = inputSource.Close() }}
	cmd.Stderr = observer
	wire := &interopWireWriter{output: os.Stdout, observer: observer}
	cmd.Stdout = wire
	if err := cmd.Start(); err != nil {
		return err
	}
	_ = controlR.Close()
	_ = inputR.Close()
	go func() {
		defer close(inputDone)
		_, err := io.Copy(&interopWireWriter{output: inputW, observer: observer, direction: "host-to-worker"}, inputSource)
		_ = inputW.Close()
		if err != nil && !inputClosed.Load() {
			failures <- err
		}
	}()
	go func() {
		defer close(eventsDone)
		for raw := range events {
			_ = socket.SetWriteDeadline(time.Now().Add(time.Second))
			if _, err := socket.Write(raw); err != nil {
				failures <- err
				return
			}
			observer.delivered(len(raw))
		}
	}()
	go func() {
		defer close(controlDone)
		reader := bufio.NewReaderSize(socket, interopEventLimit)
		for {
			raw, err := reader.ReadSlice('\n')
			if err != nil {
				if !controlClosing.Load() || (!errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF)) {
					failures <- err
				}
				return
			}
			if len(raw) > interopEventLimit {
				failures <- errors.New("control limit")
				return
			}
			if path != "" {
				err = os.WriteFile(path+".next", raw, 0600) //nolint:gosec // Explicit opt-in test-owned fixture, control or receipt path; never plugin-provided.
				if err == nil {
					err = os.Rename(path+".next", path) //nolint:gosec // Explicit opt-in test-owned fixture, control or receipt path; never plugin-provided.
				}
			} else {
				_, err = controlW.Write(raw)
			}
			if err != nil {
				failures <- err
				return
			}
		}
	}()
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
	var result error
	select {
	case err := <-failures:
		_ = cmd.Process.Kill()
		<-wait
		result = err
	case err := <-wait:
		result = errors.Join(observer.failure(), wire.finish())
		if result == nil && err != nil {
			var exit *exec.ExitError
			if errors.As(err, &exit) && exit.ExitCode() > 0 {
				result = &interopWorkerExit{code: exit.ExitCode()}
			} else {
				result = err
			}
		}
	}
	// Join the input observer before closing its shared event queue. The owned
	// stdin reader must be interrupted here: waiting for parent EOF would make
	// graceful Process.Stop depend on its own child-exit wait.
	inputClosed.Store(true)
	_ = inputSource.Close()
	_ = inputW.Close()
	select {
	case <-inputDone:
	case <-time.After(time.Second):
		return errors.New("harness input observer drain timeout")
	}
	if cmd.ProcessState != nil {
		raw, err := json.Marshal(map[string]any{"fixture_event": map[string]any{"kind": "worker_exit", "exit_code": cmd.ProcessState.ExitCode(), "state": cmd.ProcessState.String()}})
		if err == nil {
			_, err = observer.Write(append(raw, '\n'))
		}
		if err != nil {
			result = err
		}
	}
	// Stop control reads without closing the event write half. Required control
	// failures discovered after worker exit must reach the observer before EOF.
	controlClosing.Store(true)
	if err := socket.(*net.UnixConn).CloseRead(); err != nil {
		result = errors.Join(result, err)
	}
	select {
	case <-controlDone:
	case <-time.After(time.Second):
		return errors.New("harness control observer drain timeout")
	}
	select {
	case late := <-failures:
		result = errors.Join(result, late)
	default:
	}
	if result != nil {
		if _, natural := result.(*interopWorkerExit); !natural { //nolint:errorlint // A wrapped/joined worker exit also carries a harness failure.
			raw, err := json.Marshal(map[string]any{"fixture_event": map[string]any{"kind": "control_failure", "error": result.Error()}})
			if err == nil {
				_, err = observer.Write(append(raw, '\n'))
			}
			result = errors.Join(result, err)
		}
	}
	// Wait joins the stderr copier; the input and control observers are joined.
	// Flush every final event before closing the write half and publishing exit.
	close(events)
	select {
	case <-eventsDone:
	case <-time.After(2 * time.Second):
		return errors.New("event drain timeout")
	}
	select {
	case late := <-failures:
		result = errors.Join(result, late)
	default:
	}
	return result
}

type interopEventWriter struct {
	mu           sync.Mutex
	pending      []byte
	total        int
	queuedBytes  int
	err          error
	events       chan []byte
	diagnostics  io.Writer
	onFinished   func()
	finishedOnce sync.Once
}

func (w *interopEventWriter) Write(raw []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return 0, w.err
	}
	w.total += len(raw)
	if w.total > 1<<20 {
		w.err = errors.New("stderr total limit")
		return 0, w.err
	}
	if _, err := w.diagnostics.Write(raw); err != nil {
		w.err = err
		return 0, err
	}
	for _, b := range raw {
		if len(w.pending) >= interopEventLimit {
			w.err = errors.New("event line limit")
			return 0, w.err
		}
		w.pending = append(w.pending, b)
		if b != '\n' {
			continue
		}
		if bytes.HasPrefix(w.pending, []byte(`{"fixture_event":`)) {
			line := append([]byte(nil), w.pending...)
			var v struct {
				Event struct {
					Kind string `json:"kind"`
				} `json:"fixture_event"`
			}
			if err := json.Unmarshal(line, &v); err != nil {
				w.err = err
				return 0, err
			}
			if len(line) > 64<<10-w.queuedBytes {
				w.err = errors.New("event queue byte limit")
				return 0, w.err
			}
			select {
			case w.events <- line:
				w.queuedBytes += len(line)
			default:
				w.err = errors.New("event queue limit")
				return 0, w.err
			}
			if v.Event.Kind == "finished" && w.onFinished != nil {
				w.finishedOnce.Do(w.onFinished)
			}
		}
		w.pending = w.pending[:0]
	}
	return len(raw), nil
}
func (w *interopEventWriter) delivered(n int) { w.mu.Lock(); defer w.mu.Unlock(); w.queuedBytes -= n }
func (w *interopEventWriter) failure() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err == nil && len(w.pending) > 0 && bytes.HasPrefix(w.pending, []byte(`{"fixture_event":`)) {
		return errors.New("harness partial stderr event at EOF")
	}
	return w.err
}

type interopControls struct {
	queueProof           *interopQueueProof
	hungProof            *interopHungProof
	privatePlan          *interopPrivatePlan
	privateWrites        []interopWriteWitness
	hostCancelEvidence   map[uint64]interopHostCancelEvidence
	expandedReverseLimit uint32
	socket               net.Conn
	events               chan map[string]json.RawMessage
	failure              chan error
	seq                  int
	pending              []map[string]json.RawMessage
	trace                []string
	observed             []map[string]json.RawMessage
	observedBytes        int
	eventBytes           atomic.Int64
	eventCount           atomic.Int64
}

func (c *interopControls) record(e map[string]json.RawMessage) error {
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if len(c.observed) >= 256 || c.observedBytes+len(raw) > 256<<10 {
		return errors.New("harness receipt history limit")
	}
	c.observedBytes += len(raw)
	c.observed = append(c.observed, e)
	return nil
}
func (c *interopControls) consumed(e map[string]json.RawMessage) {
	raw, _ := json.Marshal(e)
	c.eventBytes.Add(-int64(len(raw)))
	c.eventCount.Add(-1)
}
func (c *interopControls) enqueue(e map[string]json.RawMessage) error {
	raw, _ := json.Marshal(e)
	if c.eventBytes.Add(int64(len(raw))) > 64<<10 || c.eventCount.Add(1) > 128 {
		return errors.New("harness combined event mailbox limit")
	}
	select {
	case c.events <- e:
		return nil
	default:
		return errors.New("harness event mailbox limit")
	}
}

func (c *interopControls) event(kind string) (map[string]json.RawMessage, error) {
	return c.eventWhere(kind, func(map[string]json.RawMessage) bool { return true })
}
func (c *interopControls) eventWhere(kind string, match func(map[string]json.RawMessage) bool) (map[string]json.RawMessage, error) {
	for i, e := range c.pending {
		var k string
		_ = json.Unmarshal(e["kind"], &k)
		if k == kind && match(e) {
			c.consumed(e)
			c.pending = append(c.pending[:i], c.pending[i+1:]...)
			if err := c.record(e); err != nil {
				return nil, err
			}
			return e, nil
		}
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case err := <-c.failure:
			if len(c.events) != 0 {
				c.failure <- err
				continue
			}
			return nil, fmt.Errorf("harness observer: %w", err)
		case e := <-c.events:
			var k string
			_ = json.Unmarshal(e["kind"], &k)
			if k == "control_failure" {
				return nil, errors.New("harness control failure")
			}
			if k == kind && match(e) {
				c.consumed(e)
				if err := c.record(e); err != nil {
					return nil, err
				}
				return e, nil
			}
			if len(c.pending) >= 128 {
				return nil, errors.New("harness unmatched event limit")
			}
			c.pending = append(c.pending, e)
		case <-timer.C:
			return nil, fmt.Errorf("harness event timeout: %s", kind)
		}
	}
}

// A successful case owns the observer through EOF. A finished event alone
// cannot hide a subsequent control failure or a truncated socket record.
func (c *interopControls) finish(expectedCode int) error {
	consume := func(e map[string]json.RawMessage) error {
		var kind string
		if err := json.Unmarshal(e["kind"], &kind); err != nil {
			return err
		}
		if kind == "control_failure" {
			return errors.New("harness terminal control failure")
		}
		c.consumed(e)
		return c.record(e)
	}
	for _, e := range c.pending {
		if err := consume(e); err != nil {
			return err
		}
	}
	c.pending = nil
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case e := <-c.events:
			if err := consume(e); err != nil {
				return err
			}
		case err := <-c.failure:
			for len(c.events) != 0 {
				if e := <-c.events; e != nil {
					if consumeErr := consume(e); consumeErr != nil {
						return consumeErr
					}
				}
			}
			if !errors.Is(err, io.EOF) || (c.hungProof != nil && err != io.EOF) { //nolint:errorlint // A joined EOF also carries a fatal selected-case observer failure.
				return fmt.Errorf("harness terminal observer: %w", err)
			}
			exits, finished := 0, 0
			for _, e := range c.observed {
				switch rawEventString(e, "kind") {
				case "finished":
					finished++
				case "worker_exit":
					exits++
					var code int
					if json.Unmarshal(e["exit_code"], &code) != nil || code != expectedCode {
						return errors.New("harness worker exit differs from owned Process receipt")
					}
				}
			}
			if exits != 1 || finished != 1 {
				return fmt.Errorf("harness terminal receipts exits=%d finished=%d", exits, finished)
			}
			wireEvents := c.observed
			if c.privatePlan != nil {
				var guardErr error
				wireEvents, guardErr = c.privatePlan.guard(c.observed, c.privateWrites)
				if guardErr != nil {
					return guardErr
				}
			}
			terminalExit := expectedCode
			if c.hungProof != nil {
				var guardErr error
				wireEvents, guardErr = c.hungProof.guard(wireEvents, expectedCode)
				if guardErr != nil {
					return guardErr
				}
				terminalExit = 0 // Only the selected hung input was removed; lifecycle terminals are mandatory.
			}
			if err := interopWireTerminals(wireEvents, terminalExit, c.hostCancelEvidence); err != nil {
				return err
			}
			if c.expandedReverseLimit != 0 {
				return interopExpandedHelpers(c.observed, c.expandedReverseLimit, c.queueProof)
			}
			return nil
		case <-timer.C:
			return errors.New("harness observer EOF timeout")
		}
	}
}

func (c *interopControls) release(gate string) error {
	if c.seq >= 64 || strings.TrimSpace(gate) == "" || len(gate) > 256 {
		return errors.New("harness control bound")
	}
	c.seq++
	raw, _ := json.Marshal(map[string]any{"seq": c.seq, "op": "release", "gate": gate})
	_ = c.socket.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err := c.socket.Write(append(raw, '\n')); err != nil {
		return err
	}
	e, err := c.event("control_received")
	if err != nil {
		return err
	}
	var seq int
	if err := json.Unmarshal(e["seq"], &seq); err != nil {
		return err
	}
	if seq != c.seq {
		return fmt.Errorf("harness control seq got%d want%d", seq, c.seq)
	}
	c.trace = append(c.trace, fmt.Sprintf("release:%s/ack:%d", gate, seq))
	return nil
}

func spawnInteropChild(t *testing.T, runtime, mode string, services HostServices, configured ...Spec) (*Process, *interopControls) {
	t.Helper()
	source := os.Getenv("INTEROP_SDK_SOURCE")
	if source == "" {
		t.Fatal("harness: exact SDK assets required")
	}
	root, err := os.MkdirTemp(os.Getenv("TMPDIR"), "ctl-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cleanupErr := os.RemoveAll(root); cleanupErr != nil { //nolint:gosec // Explicit opt-in test-owned fixture, control or receipt path; never plugin-provided.
			t.Error(cleanupErr)
		}
	})
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(root, "events"), Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	worker := filepath.Join(source, "ts/packages/plugin-sdk/test/negotiated-worker.js")
	command := os.Getenv("INTEROP_GO_CHILD")
	args := []string{"-test.run=^TestNegotiatedFixtureChild$"}
	controlPath := ""
	switch runtime {
	case "go":
		if command == "" {
			t.Fatal("harness: Go child required")
		}
	case "node":
		command, err = exec.LookPath("node")
		args = []string{worker, "negotiated:" + mode}
	case "deno":
		command, err = exec.LookPath("deno")
		controlPath = filepath.Join(root, "control.json")
		if err == nil {
			err = os.WriteFile(controlPath, nil, 0600) //nolint:gosec // Explicit opt-in test-owned fixture, control or receipt path; never plugin-provided.
		}
		args = []string{"run", "--cached-only", "--no-npm", "--no-check", "--allow-env", "--allow-read=" + root, worker, "negotiated:" + mode}
	default:
		t.Fatal("unknown fixture runtime")
	}
	if err != nil {
		t.Fatal("harness runtime:", err)
	}
	encoded, _ := json.Marshal(args)
	s := reverseTestSpec(t, services)
	if len(configured) != 0 {
		s = configured[0]
	}
	s.Command, err = os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	s.Args = []string{"-test.run=^TestInteropControlBridge$"}
	s.Env = []string{"INTEROP_BRIDGE=1", "INTEROP_SOCKET=" + listener.Addr().String(), "INTEROP_COMMAND=" + command, "INTEROP_ARGS=" + string(encoded), "INTEROP_PROFILE=negotiated:" + mode, "INTEROP_PATH_CONTROL=" + controlPath, "GORACE=atexit_sleep_ms=0", "GOMAXPROCS=4"}
	s.StderrBytes = 64 << 10
	p, err := Spawn(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Kill() })
	_ = listener.SetDeadline(time.Now().Add(5 * time.Second))
	socket, err := listener.Accept()
	if err != nil {
		t.Fatal("harness socket:", err)
	}
	t.Cleanup(func() { _ = socket.Close() })
	c := &interopControls{socket: socket, events: make(chan map[string]json.RawMessage, 128), failure: make(chan error, 1)}
	go func() {
		reader := bufio.NewReaderSize(socket, interopEventLimit)
		for {
			raw, err := reader.ReadSlice('\n')
			if err != nil {
				c.failure <- err
				return
			}
			var v struct {
				Event map[string]json.RawMessage `json:"fixture_event"`
			}
			if err = json.Unmarshal(raw, &v); err != nil {
				c.failure <- err
				return
			}
			var kind string
			if err = json.Unmarshal(v.Event["kind"], &kind); err != nil || kind == "" {
				c.failure <- errors.New("invalid fixture event kind")
				return
			}
			if err := c.enqueue(v.Event); err != nil {
				c.failure <- err
				return
			}
		}
	}()
	return p, c
}

func TestSDKManifestControlFeasibility(t *testing.T) {
	if os.Getenv("INTEROP_SDK_SOURCE") == "" {
		t.Skip("isolated interop script supplies exact assets")
	}
	var receipts []map[string]any
	defer func() {
		if path := os.Getenv("INTEROP_CONTROL_REPORT"); path != "" {
			report := interopProvenance(t)
			report["kind"] = "control-feasibility-not-manifest-replay"
			report["observations"] = receipts
			report["harness_failed"] = t.Failed()
			raw, err := json.MarshalIndent(report, "", "  ")
			if err == nil {
				err = os.WriteFile(path, append(raw, '\n'), 0600) //nolint:gosec // Explicit opt-in test-owned fixture, control or receipt path; never plugin-provided.
			}
			if err != nil {
				t.Error("write required control receipt:", err)
			}
		}
	}()
	for _, runtime := range []string{"go", "node", "deno"} {
		t.Run(runtime, func(t *testing.T) {
			logs := 0
			p, c := spawnInteropChild(t, runtime, "lifecycle", HostServices{Log: func(_ context.Context, call *HostCall, _ subprocess.LogParams) (subprocess.LogResult, error) {
				logs++
				return subprocess.LogResult{Accepted: true}, call.CheckCommit()
			}})
			if _, err := c.event("ready"); err != nil {
				t.Fatal(err)
			}
			for i := 1; i <= 2; i++ {
				if err := c.release("snapshot"); err != nil {
					t.Fatal(err)
				}
				if _, err := c.event("snapshot"); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := p.Handshake(context.Background()); err != nil {
				t.Fatal(err, p.Diagnostics())
			}
			if err := p.conn.ActivateHostServices(); err != nil {
				t.Fatal(err)
			}
			if err := p.Stop(context.Background()); err != nil {
				t.Fatal(err, p.Diagnostics())
			}
			if _, err := c.event("finished"); err != nil {
				t.Fatal(err)
			}
			if err := c.finish(0); err != nil {
				t.Fatal(err)
			}
			if logs != 3 {
				t.Fatal("expected actual Init/Load/Unload log callbacks, got " + strconv.Itoa(logs))
			}
			select {
			case <-p.Exited():
			default:
				t.Fatal("harness child not reaped")
			}
			exit, ok := p.ExitInfo()
			if !ok || exit.Code != 0 || exit.Signal != "" {
				t.Fatal("harness abnormal exit", exit)
			}
			assets := filepath.Dir(os.Getenv("INTEROP_GO_CHILD"))
			build, err := os.ReadFile(filepath.Join(assets, "build-receipt")) //nolint:gosec // Explicit opt-in test-owned fixture, control or receipt path; never plugin-provided.
			if err != nil {
				t.Fatal(err)
			}
			tsBuild, err := os.ReadFile(filepath.Join(assets, "ts-build-receipt")) //nolint:gosec // Explicit opt-in test-owned fixture, control or receipt path; never plugin-provided.
			if err != nil {
				t.Fatal(err)
			}
			manifest, err := os.ReadFile(filepath.Join(os.Getenv("INTEROP_SDK_SOURCE"), "protocol/v2/fixtures/duplex-child.json")) //nolint:gosec // Explicit opt-in test-owned fixture, control or receipt path; never plugin-provided.
			if err != nil {
				t.Fatal(err)
			}
			hash := sha256.Sum256(manifest)
			receipts = append(receipts, map[string]any{"runtime": runtime, "version": os.Getenv("INTEROP_" + strings.ToUpper(runtime) + "_VERSION"), "controls": c.trace, "backend_logs": logs, "finished": true, "reaped": true, "exit_code": exit.Code, "manifest_sha256": hex.EncodeToString(hash[:]), "corpus_version": 1, "selector_version": 1, "go_build_receipt": string(build), "ts_build_receipt": string(tsBuild), "bridge_command": p.spec.Command, "bridge_args": p.spec.Args, "worker_command_env": p.spec.Env, "diagnostic_tail": p.Diagnostics(), "wire_control_events": c.observed, "observer_eof": true})
			t.Log("actual control seq1+2 acknowledged; lifecycle3 backend logs; finished+reaped")
		})
	}
}
