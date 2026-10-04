//go:build unix

package pluginhost

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-sdk/subprocess"
)

type interopWriteWitness struct {
	Ref                                           interopScenarioRef
	Sequence, Intent, Bytes, UnderlyingN          int
	Lane, Kind, SHA256, UnderlyingError, AckError string
	ID                                            subprocess.RPCID
	OrdinaryEligible                              bool
	ControlBurst                                  int
	Deadline                                      time.Time
}

// The decorator is installed as a ConnOption before either transport goroutine.
// It never allocates IDs, rewrites bytes, responds, or retires a receipt.
type interopPrivateWriter struct {
	mu       sync.Mutex
	output   io.Writer
	conn     *Conn
	observer context.Context
	store    *interopPrivateStore
	plan     *interopPrivatePlan
	deadline time.Time
	changed  chan struct{}
	closed   chan struct{}
	once     sync.Once
	gate     chan struct{}
	entered  chan struct{}
	armed    bool
	gateKind string
	writes   []interopWriteWitness
	frames   map[*queuedFrame]int
}

func (w *interopPrivateWriter) option(c *Conn) {
	w.conn = c
	w.output = c.w
	w.changed = make(chan struct{}, 1)
	w.closed = make(chan struct{})
	w.entered = make(chan struct{}, 1)
	w.frames = map[*queuedFrame]int{}
	if _, ok := w.output.(io.Closer); !ok {
		c.configError = errors.New("private decorator requires interruptible owned stream")
	}
	c.w = w
}

func (w *interopPrivateWriter) Close() error {
	w.once.Do(func() { close(w.closed) })
	if closer, ok := w.output.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (w *interopPrivateWriter) SetWriteDeadline(deadline time.Time) error {
	w.mu.Lock()
	w.deadline = deadline
	w.mu.Unlock()
	select {
	case w.changed <- struct{}{}:
	default:
	}
	if d, ok := w.output.(interface{ SetWriteDeadline(time.Time) error }); ok {
		return d.SetWriteDeadline(deadline)
	}
	return errors.ErrUnsupported
}

func (w *interopPrivateWriter) wait(gate <-chan struct{}) error {
	for {
		deadline, ok := w.observer.Deadline()
		if !ok {
			return errors.New("private observer is unbounded")
		}
		w.mu.Lock()
		active := w.deadline
		w.mu.Unlock()
		if !active.IsZero() && active.Before(deadline) {
			deadline = active
		}
		timer := time.NewTimer(max(time.Until(deadline), 0))
		select {
		case <-w.observer.Done():
			timer.Stop()
			return w.observer.Err()
		case <-w.conn.done:
			timer.Stop()
			return w.conn.gone()
		case <-w.closed:
			timer.Stop()
			return io.ErrClosedPipe
		case <-timer.C:
			return context.DeadlineExceeded
		case <-w.changed:
			timer.Stop()
		case <-gate:
			timer.Stop()
			return nil
		}
	}
}

// Only bounded categorical errors are retained; arbitrary stream error text may
// contain sensitive content and is not part of a semantic receipt.
func interopPrivateError(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, io.ErrShortWrite) {
		return "short_write"
	}
	return "stream_or_harness_failure"
}

func (w *interopPrivateWriter) Write(raw []byte) (int, error) {
	w.mu.Lock()
	plan := w.plan
	w.mu.Unlock()
	w.conn.mu.Lock()
	w.conn.queue.mu.Lock()
	active := w.conn.queue.active
	eligible := len(w.conn.queue.lanes[ordinaryLane].frames) > 0
	burst := w.conn.queue.burst
	w.conn.queue.mu.Unlock()
	receipt := w.conn.activeReceipt
	var call *pendingCall
	for _, candidate := range w.conn.pending {
		if candidate.frame == active {
			call = candidate
			break
		}
	}
	w.conn.mu.Unlock()
	witness := interopWriteWitness{Ref: w.store.ref, Bytes: len(raw), SHA256: fmt.Sprintf("%x", sha256.Sum256(raw)), OrdinaryEligible: eligible, ControlBurst: burst}
	if receipt != nil {
		witness.Lane = "control"
		witness.Kind = "reverse_terminal"
		witness.ID = receipt.id
	} else if call != nil {
		witness.Lane = "ordinary"
		witness.Kind = "forward"
		witness.ID = call.id
		if call.lifecycle {
			witness.Lane = "control"
			witness.Kind = "lifecycle"
		}
	} else if plan != nil {
		witness.Intent = plan.intentFor(active)
		if witness.Intent > 0 {
			witness.Lane = "control"
			witness.Kind = "authored_cancel"
		}
	}
	if active == nil || witness.Kind == "" {
		return 0, errors.New("private selected frame lacks owned metadata")
	}
	w.mu.Lock()
	gate := w.gate
	block := w.armed && witness.Kind == w.gateKind
	if block {
		w.armed = false
	}
	witness.Sequence = len(w.writes) + 1
	witness.Deadline = w.deadline
	if len(w.frames) >= 256 {
		w.mu.Unlock()
		return 0, errors.New("private selected frame metadata overflow")
	}
	w.frames[active] = witness.Sequence
	w.mu.Unlock()
	if block {
		select {
		case w.entered <- struct{}{}:
		default:
		}
		if err := w.wait(gate); err != nil {
			return 0, err
		}
	}
	n, err := w.output.Write(raw)
	witness.UnderlyingN = n
	witness.UnderlyingError = interopPrivateError(err)
	var ack error
	if err == nil && n == len(raw) && plan != nil && (witness.Kind == "reverse_terminal" || witness.Kind == "authored_cancel") {
		// Admission only. No wait for a later write by this same writer.
		ack = plan.afterWholeWrite(witness)
	}
	witness.AckError = interopPrivateError(ack)
	if recordErr := w.store.add(witness); recordErr != nil {
		ack = errors.Join(ack, recordErr)
		witness.AckError = interopPrivateError(ack)
	}
	w.mu.Lock()
	if len(w.writes) >= 256 {
		ack = errors.Join(ack, errors.New("private writer witness overflow"))
	} else {
		w.writes = append(w.writes, witness)
	}
	w.mu.Unlock()
	// Full underlying bytes followed by ACK failure remain a FULL physical
	// witness. Conn receives the error and owns fencing/uncertain effects.
	return n, errors.Join(err, ack)
}

func replayInteropPrivateHostFairness(t *testing.T, runtime string, recipe interopRecipeRow, b *interopBackend, observation map[string]any) (*Process, *interopControls) {
	t.Helper()
	ref, err := interopPrivateRef(os.Getenv("INTEROP_SDK_SOURCE"), runtime)
	if err != nil {
		t.Fatal(err)
	}
	if recipe.Name != ref.Recipe || recipe.Scenario != ref.Scenario || recipe.Profile != "expanded" || (recipe.Status != "" && recipe.Status != "observed") {
		t.Fatal("private passed recipe differs from selected authored source")
	}
	observer, end := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(end)
	store := &interopPrivateStore{ref: ref}
	plan, err := newInteropPrivatePlan(observer, ref, store)
	if err != nil {
		t.Fatal(err)
	}
	w := &interopPrivateWriter{observer: observer, store: store, gate: make(chan struct{})}
	spec := interopSourceSpec(t, b)
	spec.ConnOptions = append(spec.ConnOptions, w.option)
	b.getGate = make(chan struct{})
	b.getEntered = make(chan *HostCall, 8)
	t.Cleanup(func() {
		select {
		case <-b.getGate:
		default:
			close(b.getGate)
		}
	})
	p, c := startInteropExpanded(t, runtime, recipe, b, spec)
	plan.conn = p.conn
	p.conn.mu.Lock()
	plan.incarnation = p.conn.reverse
	p.conn.mu.Unlock()
	commandResult := make(chan subprocess.CommandExecResult, 1)
	commandFailure := make(chan error, 1)
	go func() {
		result, callErr := interopCommand(observer, p, "get", map[string]any{"n": 8, "key": "read"}, "g-StorageGet")
		commandResult <- result
		commandFailure <- callErr
	}()
	var commandID uint64
	for range 8 {
		select {
		case entered := <-b.getEntered:
			authority := entered.Authority()
			if authority.Method != HostStorageGet || authority.Parent.RequestOwner != subprocess.HostRPCOwnerHost || authority.Parent.ID == 0 || authority.Parent.ID > uint64(maxRequestID) || (commandID != 0 && commandID != authority.Parent.ID) {
				t.Fatal("private seed lacks genuine shared command authority")
			}
			commandID = authority.Parent.ID
		case <-observer.Done():
			t.Fatal("private helper barrier", observer.Err())
		}
	}
	// These IDs are copied from real authenticated execution/terminal custody,
	// before any reply is released; no observed notification makes a seed.
	p.conn.mu.Lock()
	if len(p.conn.inboundActive) != 8 {
		p.conn.mu.Unlock()
		t.Fatal("private genuine seed admission")
	}
	for id := range p.conn.inboundActive {
		plan.seedIDs[id] = true
	}
	p.conn.mu.Unlock()
	w.mu.Lock()
	w.plan = plan
	w.armed = true
	w.gateKind = "forward"
	w.mu.Unlock()
	var calls []*pendingCall
	for range 9 {
		call, callErr := p.conn.queueCallClass(observer, subprocess.MethodHealth, map[string]any{}, true)
		if callErr != nil {
			t.Fatal(callErr)
		}
		calls = append(calls, call)
	}
	select {
	case <-w.entered:
	case <-observer.Done():
		t.Fatal("private physical gate", observer.Err())
	}
	// Match the authored ordering: first ordinary output is physically blocked,
	// remaining ordinary calls and all eight real reply seeds queue behind it.
	close(b.getGate)
	var blocked interopAdmissionSnapshot
	for {
		blocked, err = interopSnapshotAdmission(p.conn, ref, w)
		if err != nil {
			t.Fatal(err)
		}
		if blocked.ControlFrames == 8 && blocked.OrdinaryFrames == 8 {
			break
		}
		select {
		case <-observer.Done():
			t.Fatal("private blocked queue", observer.Err())
		case <-p.conn.done:
			t.Fatal(p.conn.gone())
		case <-time.After(time.Millisecond):
		}
	}
	if !blocked.Active || blocked.Inbound != 8 || blocked.Ordinary != 10 {
		t.Fatal("private blockage/admission", blocked)
	}
	if err := store.add(blocked); err != nil {
		t.Fatal(err)
	}
	close(w.gate)
	ids := map[subprocess.RPCID]bool{}
	for _, call := range calls {
		raw, callErr := p.conn.awaitCall(call)
		var result subprocess.HealthStatus
		if callErr != nil || json.Unmarshal(raw, &result) != nil || !result.OK {
			t.Fatal("private actual health response", callErr, string(raw))
		}
		if ids[call.id] || call.id == (subprocess.RPCID{}) {
			t.Fatal("private duplicate/unselected ordinary ID")
		}
		ids[call.id] = true
	}
	var command subprocess.CommandExecResult
	select {
	case command = <-commandResult:
	case <-observer.Done():
		t.Fatal(observer.Err())
	}
	if callErr := <-commandFailure; callErr != nil {
		t.Fatal(callErr)
	}
	outcomes := interopHelperResults(t, command)
	if len(outcomes) != 8 {
		t.Fatal("private command lost authored helper outcomes", outcomes)
	}
	for _, outcome := range outcomes {
		if outcome.Code != "ok" {
			t.Fatal("private genuine helper failed", outcome)
		}
	}
	parentID := subprocess.NumberID(int64(commandID)) //nolint:gosec // Actual verified parent bounded to positive JS-safe ID above.
	if ids[parentID] {
		t.Fatal("private command/Health ID collision")
	}
	var responseIDs []subprocess.RPCID
	for id := range ids {
		responseIDs = append(responseIDs, id)
	}
	responseIDs = append(responseIDs, parentID)
	// Wait for the sole physical writer's last completion before detaching the
	// scenario ACK port. Shutdown is ordinary finite lifecycle, not this port.
	for {
		snapshot, snapshotErr := interopSnapshotAdmission(p.conn, ref, w)
		if snapshotErr != nil {
			t.Fatal(snapshotErr)
		}
		if !snapshot.Active && snapshot.ControlFrames == 0 && snapshot.OrdinaryFrames == 0 {
			if snapshot.Inbound != 0 || snapshot.Reserved != 0 {
				t.Fatal("private physical ownership not retired", snapshot)
			}
			if err := store.add(snapshot); err != nil {
				t.Fatal(err)
			}
			break
		}
		select {
		case <-observer.Done():
			t.Fatal(observer.Err())
		case <-p.conn.done:
			t.Fatal(p.conn.gone())
		case <-time.After(time.Millisecond):
		}
	}
	w.mu.Lock()
	writes := append([]interopWriteWitness(nil), w.writes...)
	w.plan = nil
	w.mu.Unlock()
	if err := plan.complete(writes); err != nil {
		t.Fatal(err)
	}
	ordinary, controls, burst, seeds := 0, 0, 0, 0
	for _, write := range writes {
		switch write.Kind {
		case "forward":
			// Derive ordinary membership from the actual nine queued Health calls,
			// rather than any static command ID or post-hoc method classification.
			if ids[write.ID] {
				ordinary++
				burst = 0
			}
		case "reverse_terminal", "authored_cancel":
			controls++
			burst++
			if write.Kind == "reverse_terminal" {
				seeds++
			}
			if ordinary < 9 && burst > 4 {
				t.Fatal("actual Conn control starvation", writes)
			}
			if write.OrdinaryEligible && write.ControlBurst > 4 {
				t.Fatal("actual eligible queue burst exceeded", write)
			}
			if write.UnderlyingN != write.Bytes || write.UnderlyingError != "" || write.AckError != "" {
				t.Fatal("private incomplete physical control", write)
			}
		}
	}
	if ordinary != 9 || controls < 32 || controls > 39 || seeds != 8 {
		t.Fatal("private authored fairness", ordinary, controls, seeds)
	}
	b.mu.Lock()
	effects, commits := len(b.calls), b.commits
	b.mu.Unlock()
	if effects != 8 || commits != 0 {
		t.Fatal("private control gained effects", effects, commits)
	}
	c.privatePlan = plan
	c.privateWrites = writes
	observation["scenario_ref"] = ref
	observation["actual_writer_witnesses"] = writes
	observation["private_metadata"] = store.records
	observation["authored_control_writes"] = controls
	observation["actual_ordinary_responses"] = ordinary
	observation["actual_reverse_seed_replies"] = seeds
	observation["actual_distinct_response_ids"] = responseIDs
	observation["projection"] = "Actual genuine Init+Load and finite bound helper command; closed nonauthority Health; preowned authored9999 refill intents through actual Conn control lane, no parent/credit invented"
	return p, c
}

func TestPrivateInteropFullUnderlyingBytesWithMetadataFailureFence(t *testing.T) {
	observer, end := context.WithTimeout(context.Background(), time.Second)
	defer end()
	store := &interopPrivateStore{records: make([]json.RawMessage, 256)}
	w := &interopPrivateWriter{observer: observer, store: store}
	p := newPeer(t, w.option)
	done := make(chan error, 1)
	go func() { _, err := p.conn.Call(observer, subprocess.MethodHealth, nil); done <- err }()
	p.request() // Original peer receives a whole frame before decorator failure.
	select {
	case err := <-done:
		if !errors.Is(err, ErrGone) {
			t.Fatal("full-byte failure normalized", err)
		}
	case <-observer.Done():
		t.Fatal("harness failure did not fence")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.writes) != 1 || w.writes[0].UnderlyingN != w.writes[0].Bytes || w.writes[0].UnderlyingError != "" || w.writes[0].AckError == "" {
		t.Fatal("full underlying receipt misclassified", w.writes)
	}
}

func TestPrivateInteropGateCloseInterruptsOwnedWait(t *testing.T) {
	observer, end := context.WithTimeout(context.Background(), time.Second)
	defer end()
	w := &interopPrivateWriter{observer: observer, store: &interopPrivateStore{}, closed: make(chan struct{}), changed: make(chan struct{}, 1), conn: &Conn{done: make(chan struct{})}}
	done := make(chan error, 1)
	go func() { done <- w.wait(make(chan struct{})) }()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatal(err)
		}
	case <-observer.Done():
		t.Fatal("closed gate leaked")
	}
}

func TestPrivateInteropGateUsesActiveWriteBoundAndConnFence(t *testing.T) {
	for _, fenced := range []bool{false, true} {
		t.Run(fmt.Sprint(fenced), func(t *testing.T) {
			observer, end := context.WithTimeout(context.Background(), time.Second)
			defer end()
			w := &interopPrivateWriter{observer: observer, store: &interopPrivateStore{}, closed: make(chan struct{}), changed: make(chan struct{}, 1), conn: &Conn{done: make(chan struct{}), closedBy: io.EOF}, deadline: time.Now().Add(20 * time.Millisecond)}
			done := make(chan error, 1)
			go func() { done <- w.wait(make(chan struct{})) }()
			if fenced {
				close(w.conn.done)
			}
			select {
			case err := <-done:
				if fenced && !errors.Is(err, io.EOF) {
					t.Fatal(err)
				}
				if !fenced && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
			case <-time.After(200 * time.Millisecond):
				_ = w.Close()
				t.Fatal("gate extended active deadline or ignored Conn fence")
			}
		})
	}
}
