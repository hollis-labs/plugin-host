//go:build unix

package pluginhost

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-host/internal/interopfixture"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

func newInteropDisconnectRef(source, runtime string, recipe interopRecipeRow) (interopQueueRef, error) {
	var ref interopQueueRef
	if runtime != "go" && runtime != "node" && runtime != "deno" {
		return ref, errors.New("disconnect runtime")
	}
	marker, err := os.ReadFile(filepath.Join(source, ".interop-source-commit")) //nolint:gosec // Test-owned pinned asset path.
	if err != nil || string(marker) != "ea8ec0dca862d0c7284cc6a130a4b27fb812ed21\n" {
		return ref, errors.New("disconnect source")
	}
	raw, err := os.ReadFile(filepath.Join(source, "protocol/v2/fixtures/duplex-child.json")) //nolint:gosec // Test-owned pinned asset path.
	if err != nil {
		return ref, err
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(raw))
	if hash != "30cb07b87060dc88e9cb6ac5414f7b9cb7bb19c7a97c058e284d96e1e59ff4a9" {
		return ref, errors.New("disconnect manifest")
	}
	rows, err := interopfixture.Select(raw, "normal-serve-negotiated", []string{recipe.Name})
	if err != nil || len(rows) != 1 {
		return ref, errors.New("disconnect selector")
	}
	var actual interopRecipeRow
	if json.Unmarshal(rows[0], &actual) != nil || actual.Name != recipe.Name || actual.Profile != recipe.Profile || actual.Scenario != recipe.Scenario || (actual.Status != "" && actual.Status != "observed") {
		return ref, errors.New("disconnect selected recipe")
	}
	validProfile := recipe.Scenario == "disconnect" && recipe.Profile == "expanded"
	if !validProfile {
		return ref, errors.New("disconnect handler profile")
	}
	var id [16]byte
	if _, err = rand.Read(id[:]); err != nil {
		return ref, err
	}
	return interopQueueRef{string(marker[:len(marker)-1]), hash, recipe.Name, recipe.Scenario, recipe.Profile, runtime, fmt.Sprintf("%x", id), 1, 1}, nil
}

// This tap preserves the actual file's native write deadline and Close. It only
// copies bounded physical results after Write returns, outside Conn locks.
type interopDisconnectWrite struct {
	Raw        string
	Bytes      int
	Error      string
	ClosedPipe bool
	At         time.Time
}
type interopDisconnectWriter struct {
	file     *os.File
	mu       sync.Mutex
	attempts []interopDisconnectWrite
	overflow bool
}

func (w *interopDisconnectWriter) Write(raw []byte) (int, error) {
	n, err := w.file.Write(raw)
	v := interopDisconnectWrite{Raw: string(raw), Bytes: n, At: time.Now()}
	if err != nil {
		v.Error = err.Error()
		v.ClosedPipe = errors.Is(err, os.ErrClosed) || errors.Is(err, syscall.EPIPE)
	}
	w.mu.Lock()
	if len(w.attempts) >= 16 || len(raw) > 64<<10 {
		w.overflow = true
	} else {
		w.attempts = append(w.attempts, v)
	}
	w.mu.Unlock()
	return n, err
}
func (w *interopDisconnectWriter) Close() error { return w.file.Close() }
func (w *interopDisconnectWriter) SetWriteDeadline(d time.Time) error {
	return w.file.SetWriteDeadline(d)
}
func (w *interopDisconnectWriter) snapshot() ([]interopDisconnectWrite, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]interopDisconnectWrite{}, w.attempts...), w.overflow
}

type interopDisconnectProof struct {
	Ref                                                  interopQueueRef
	expected                                             interopQueueRef
	CommandID, ReverseID                                 uint64
	Binding                                              string
	Lifecycle                                            map[string]uint64
	Writes                                               []interopDisconnectWrite
	WriterOverflow                                       bool
	HalfcloseAt, BackendEnteredAt, BackendReturnedAt     time.Time
	Authority                                            HostAuthority
	BackendEntries, BackendReturns                       int
	BeforePermit, AfterPermit, AfterPool                 int
	BackendCause, CommitRefusal                          string
	ParentRetired, SessionFenced, CustodyRetired         bool
	PublicDelivery, PublicError, ConnError               string
	PublicTransportCause                                 bool
	ControlTrace                                         []string
	CancellationParentRetired, CancellationSessionFenced bool
	BackendKey, ReplyDisposition                         string
	PublicErrorChain                                     []string
	PhysicalResult                                       json.RawMessage
}

func replayInteropDisconnect(t *testing.T, runtime string, recipe interopRecipeRow, b *interopBackend, row map[string]any) (*Process, *interopControls) {
	t.Helper()
	ref, err := newInteropDisconnectRef(os.Getenv("INTEROP_SDK_SOURCE"), runtime, recipe)
	if err != nil {
		t.Fatal(err)
	}
	proof := &interopDisconnectProof{Ref: ref, expected: ref, Lifecycle: map[string]uint64{}}
	tap := &interopDisconnectWriter{}
	spec := interopSourceSpec(t, b)
	entered := make(chan *HostCall, 1)
	returned := make(chan struct{}, 1)
	var backendMu sync.Mutex
	services := b.services()
	services.StorageGet = func(ctx context.Context, call *HostCall, params subprocess.StorageGetParams) (subprocess.StorageGetResult, error) {
		backendMu.Lock()
		proof.BackendEntries++
		proof.BackendEnteredAt = time.Now()
		proof.Authority = call.Authority()
		proof.BackendKey = params.Key
		backendMu.Unlock()
		b.mu.Lock()
		b.calls = append(b.calls, call.Authority())
		b.mu.Unlock()
		entered <- call
		<-ctx.Done()
		backendMu.Lock()
		proof.BackendReturns++
		proof.BackendReturnedAt = time.Now()
		proof.BackendCause = ctx.Err().Error()
		call.session.mu.Lock()
		_, live := call.session.parents[call.authority.Parent.ID]
		proof.CancellationParentRetired = !live
		proof.CancellationSessionFenced = call.session.closed
		call.session.mu.Unlock()
		if e := call.CheckCommit(); e != nil {
			proof.CommitRefusal = e.Error()
		}
		backendMu.Unlock()
		returned <- struct{}{}
		return subprocess.StorageGetResult{}, ctx.Err()
	}
	spec.Reverse.Runtime = NewHostServiceRuntime(services, func(ctx context.Context, _ HostAuthority) error { return ctx.Err() })
	spec.ConnOptions = append(spec.ConnOptions, func(conn *Conn) {
		file, ok := conn.w.(*os.File)
		if !ok {
			panic("disconnect tap requires native process pipe")
		}
		tap.file = file
		conn.w = tap
	})
	p, c := spawnInteropChild(t, runtime, recipe.Profile, services, spec)
	c.childStream.Store(true) // Assign physical worker-output order at observer ingress.
	c.disconnectProof = proof
	c.expandedReverseLimit = spec.Init.HostServices.Limits.PluginToHostInflight
	if _, err = c.event("ready"); err != nil {
		t.Fatal(err)
	}
	if err = c.release("request-2"); err != nil {
		t.Fatal(err)
	}
	if _, _, err = p.Handshake(context.Background()); err != nil {
		t.Fatal(err, p.Diagnostics())
	}
	if err = p.conn.ActivateHostServices(); err != nil {
		t.Fatal(err)
	}
	observer, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type outcome struct {
		Value subprocess.CommandExecResult
		Err   error
	}
	done := make(chan outcome, 1)
	go func() {
		v, e := interopCommand(observer, p, "get", map[string]any{"n": 1, "key": "read"}, "g-StorageGet")
		done <- outcome{v, e}
	}()
	input, err := c.eventWhere("wire", func(e map[string]json.RawMessage) bool {
		return rawEventString(e, "direction") == "host-to-worker" && rawEventString(e, "method") == subprocess.MethodCommandExecute
	})
	if err != nil || json.Unmarshal(input["id"], &proof.CommandID) != nil || proof.CommandID == 0 {
		t.Fatal("disconnect command input", err)
	}
	event, err := c.eventWhere("entered", func(e map[string]json.RawMessage) bool {
		var id uint64
		_ = json.Unmarshal(e["id"], &id)
		return id == proof.CommandID
	})
	var deadline bool
	if err != nil || rawEventString(event, "name") != "get" || json.Unmarshal(event["deadline"], &deadline) != nil || !deadline {
		t.Fatal("disconnect entered", err, event)
	}
	reverse, err := c.eventWhere("wire", func(e map[string]json.RawMessage) bool {
		return rawEventString(e, "direction") == "worker-to-host" && rawEventString(e, "method") == string(HostStorageGet)
	})
	if err != nil || json.Unmarshal(reverse["id"], &proof.ReverseID) != nil || proof.ReverseID == 0 {
		t.Fatal("disconnect reverse input", err)
	}
	var call *HostCall
	select {
	case call = <-entered:
	case <-observer.Done():
		t.Fatal("disconnect backend admission", observer.Err())
	}
	backendMu.Lock()
	proof.Binding = string(proof.Authority.BindingID)
	backendMu.Unlock()
	call.session.admission.pool.mu.Lock()
	proof.BeforePermit = call.session.admission.active
	call.session.admission.pool.mu.Unlock()
	if proof.BeforePermit != 1 {
		t.Fatal("disconnect actual permit", proof.BeforePermit)
	}
	proof.HalfcloseAt = time.Now()
	if err = p.stdin.Close(); err != nil {
		t.Fatal("disconnect halfclose", err)
	}
	terminal, err := c.eventWhere("wire", func(e map[string]json.RawMessage) bool {
		var id uint64
		_ = json.Unmarshal(e["id"], &id)
		return rawEventString(e, "direction") == "worker-to-host" && rawEventString(e, "frame_type") == "response" && id == proof.CommandID
	})
	if err != nil {
		t.Fatal("disconnect physical result", err, p.Diagnostics())
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
	}
	if json.Unmarshal([]byte(rawEventString(terminal, "raw")), &envelope) != nil {
		t.Fatal("disconnect result envelope")
	}
	proof.PhysicalResult = envelope.Result
	select {
	case <-p.exited:
	case <-observer.Done():
		t.Fatal("disconnect natural process exit", observer.Err(), p.Diagnostics())
	}
	exit, ok := p.ExitInfo()
	if !ok || exit.Code != 0 || exit.Signal != "" {
		t.Fatal("disconnect natural exit", exit, p.Diagnostics())
	}
	var public outcome
	select {
	case public = <-done:
	case <-observer.Done():
		t.Fatal("disconnect public waiter", observer.Err())
	}
	if public.Err == nil {
		proof.PublicDelivery = "SUCCEEDED"
		physical, _ := json.Marshal(public.Value)
		var observed subprocess.CommandExecResult
		if json.Unmarshal(proof.PhysicalResult, &observed) != nil || string(physical) != string(mustInteropDisconnectJSON(observed)) {
			t.Fatal("disconnect public result differs from physical", public.Value, string(proof.PhysicalResult))
		}
	} else {
		proof.PublicDelivery = "FAILED"
		proof.PublicError = public.Err.Error()
		proof.PublicTransportCause, proof.PublicErrorChain = interopDisconnectTransportError(public.Err)
		if !errors.Is(public.Err, ErrGone) || errors.Is(public.Err, context.DeadlineExceeded) || errors.Is(public.Err, context.Canceled) {
			t.Fatal("disconnect public failure unrelated to transport", public.Err)
		}
	}
	select {
	case <-returned:
	case <-observer.Done():
		t.Fatal("disconnect backend return", observer.Err())
	}
	select {
	case <-p.conn.Done():
	case <-observer.Done():
		t.Fatal("disconnect Conn EOF", observer.Err())
	}
	// Actual callback return precedes permit release; wait the real independent
	// bookkeeping owners, never treat context cancellation as refund.
	for {
		call.session.admission.pool.mu.Lock()
		proof.AfterPermit = call.session.admission.active
		proof.AfterPool = call.session.admission.pool.active
		call.session.admission.pool.mu.Unlock()
		p.conn.mu.Lock()
		p.conn.queue.mu.Lock()
		proof.CustodyRetired = len(p.conn.pending) == 0 && len(p.conn.correlations) == 0 && p.conn.ordinaryCalls == 0 && p.conn.lifecycleCalls == 0 && len(p.conn.inboundActive) == 0 && p.conn.activeReceipt == nil && p.conn.queue.reserved == 0 && p.conn.queue.active == nil && len(p.conn.outbound) == 0 && p.conn.reverse.workers == 0 && len(p.conn.reverse.active) == 0
		if p.conn.closedBy != nil {
			proof.ConnError = p.conn.closedBy.Error()
		}
		p.conn.queue.mu.Unlock()
		p.conn.mu.Unlock()
		call.session.mu.Lock()
		_, live := call.session.parents[proof.CommandID]
		proof.ParentRetired = !live
		proof.SessionFenced = call.session.closed
		active := len(call.session.active)
		call.session.mu.Unlock()
		if proof.CustodyRetired && proof.AfterPermit == 0 && proof.AfterPool == 0 && active == 0 {
			break
		}
		select {
		case <-observer.Done():
			t.Fatal("disconnect real retirement/permit", proof)
		case <-time.After(time.Millisecond):
		}
	}
	proof.Writes, proof.WriterOverflow = tap.snapshot()
	proof.ControlTrace = append([]string{}, c.trace...)
	proof.ReplyDisposition = "NO_ATTEMPT_FENCED"
	for _, write := range proof.Writes {
		var v struct {
			ID     uint64 `json:"id"`
			Method string `json:"method"`
		}
		if json.Unmarshal([]byte(write.Raw), &v) != nil {
			t.Fatal("disconnect physical write metadata")
		}
		if v.Method == "" {
			proof.ReplyDisposition = "ZERO_BYTE_NATIVE_FAILURE"
		}
		if v.Method == subprocess.MethodInit || v.Method == subprocess.MethodLoad {
			proof.Lifecycle[v.Method] = v.ID
		}
	}
	finished, err := c.event("finished")
	if err != nil {
		t.Fatal(err)
	}
	row["disconnect_proof"] = proof
	row["wire_control_events"] = c.observed
	if err = c.finish(0); err != nil {
		t.Log("disconnect diagnostic", string(mustInteropDisconnectJSON(proof)), string(mustInteropDisconnectJSON(c.observed)))
		t.Fatal(err)
	}
	auditInteropDisconnectCopies(t, c)
	row["disconnect_proof"] = proof
	row["finished"] = finished
	row["physical_child_conformance"] = true
	row["public_delivery"] = proof.PublicDelivery
	row["public_error"] = proof.PublicError
	row["effect_state_provenance"] = "unknown is stronger source-derived published-read EOF classification; authored driver asserts code target_unavailable"
	row["projection"] = "Genuine Init+Load and finite10000ms host-minted StorageGet authority; actual child result retained independently of public delivery; no literal binding-example authority"
	row["no_fallback_before_proof"] = true
	return p, c
}
func mustInteropDisconnectJSON(v any) []byte { raw, _ := json.Marshal(v); return raw }

func auditInteropDisconnectCopies(t *testing.T, c *interopControls) {
	t.Helper()
	for _, variant := range []string{"extra-pair", "unowned-cancel", "missing-result", "missing-helper", "duplicate-helper", "partial-reply", "full-reply-error", "unreturned", "permit", "credit", "wrong-parent", "wrong-stream", "late-error", "foreign-run", "extra-control"} {
		var proof interopDisconnectProof
		_ = json.Unmarshal(mustInteropDisconnectJSON(c.disconnectProof), &proof)
		proof.expected = c.disconnectProof.expected
		var events []map[string]json.RawMessage
		_ = json.Unmarshal(mustInteropDisconnectJSON(c.observed), &events)
		failure := io.EOF
		id := proof.CommandID + 100
		switch variant {
		case "extra-pair":
			events = append(events, queueGuardWire("host-to-worker", "request", id, subprocess.MethodHealth, map[string]any{}, nil), queueGuardWire("worker-to-host", "response", id, "", nil, map[string]any{"ok": true}))
		case "unowned-cancel":
			//nolint:misspell // Exact SDK cancellation reason.
			raw := append(mustInteropDisconnectJSON(map[string]any{"jsonrpc": "2.0", "method": "rpc/cancel", "params": map[string]any{"request_owner": "host", "id": proof.CommandID, "reason": "caller_cancelled"}}), '\n')
			events = append(events, queueGuardEvent(map[string]any{"kind": "wire", "direction": "host-to-worker", "frame_type": "notification", "method": "rpc/cancel", "raw": string(raw), "bytes": len(raw)})) //nolint:misspell // Exact SDK reason.
		case "missing-result":
			for i, e := range events {
				var actual uint64
				_ = json.Unmarshal(e["id"], &actual)
				if rawEventString(e, "kind") == "wire" && rawEventString(e, "direction") == "worker-to-host" && rawEventString(e, "frame_type") == "response" && actual == proof.CommandID {
					events = append(events[:i], events[i+1:]...)
					break
				}
			}
		case "missing-helper":
			for i, e := range events {
				if rawEventString(e, "kind") == "helper_done" {
					events = append(events[:i], events[i+1:]...)
					break
				}
			}
		case "duplicate-helper":
			for _, e := range events {
				if rawEventString(e, "kind") == "helper_done" {
					events = append(events, e)
					break
				}
			}
		case "partial-reply", "full-reply-error":
			raw := string(mustInteropDisconnectJSON(map[string]any{"jsonrpc": "2.0", "id": proof.ReverseID, "error": map[string]any{"code": -32010}})) + "\n"
			n := 1
			if variant == "full-reply-error" {
				n = len(raw)
			}
			proof.Writes = append(proof.Writes, interopDisconnectWrite{Raw: raw, Bytes: n, Error: "observer error after physical bytes", ClosedPipe: true, At: proof.BackendReturnedAt})
			proof.ReplyDisposition = "ZERO_BYTE_NATIVE_FAILURE"
		case "unreturned":
			proof.BackendReturns = 0
		case "permit":
			proof.AfterPermit = 1
		case "credit":
			proof.CustodyRetired = false
		case "wrong-parent":
			proof.Authority.Parent.ID++
		case "wrong-stream":
			for _, e := range events {
				if string(e["child_output_sequence"]) == "3" {
					e["child_output_sequence"] = json.RawMessage(`4`)
					break
				}
			}
		case "late-error":
			failure = errors.Join(io.EOF, errors.New("late control failure"))
		case "foreign-run":
			proof.Ref.Run = "foreign"
		case "extra-control":
			events = append(events, queueGuardEvent(map[string]any{"kind": "control_received", "seq": 2}))
		}
		copied := &interopControls{disconnectProof: &proof, observed: events, events: make(chan map[string]json.RawMessage), failure: make(chan error, 1), expandedReverseLimit: c.expandedReverseLimit}
		copied.failure <- failure
		if copied.finish(0) == nil {
			t.Fatal("disconnect copied actual trace accepted", variant)
		}
	}
}

// Enumerate the complete real error graph; an expected leaf never hides a joined
// harness/cancellation error. ErrGone plus the native EOF/closed-pipe cause only.
func interopDisconnectTransportError(err error) (bool, []string) {
	var chain []string
	var walk func(error, int) bool
	native := false
	walk = func(e error, depth int) bool {
		if e == nil || depth > 16 || len(chain) >= 32 {
			return false
		}
		chain = append(chain, e.Error())
		if many, ok := e.(interface{ Unwrap() []error }); ok {
			for _, child := range many.Unwrap() {
				if !walk(child, depth+1) {
					return false
				}
			}
			return true
		}
		if one, ok := e.(interface{ Unwrap() error }); ok {
			return walk(one.Unwrap(), depth+1)
		}
		//nolint:errorlint // Exact leaf comparison after walking every unwrap edge.
		if e == io.EOF || e == os.ErrClosed || e == syscall.EPIPE {
			native = true
			return true
		} //nolint:errorlint // Exact allowed leaves; joined failures are walked above.
		return e == ErrGone //nolint:errorlint // Exact library sentinel leaf.
	}
	good := walk(err, 0)
	return good && native && errors.Is(err, ErrGone), chain
}
