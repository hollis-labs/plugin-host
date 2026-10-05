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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-host/internal/interopfixture"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

func newInteropChildRef(source, runtime string, recipe interopRecipeRow) (interopQueueRef, error) {
	var ref interopQueueRef
	if runtime != "go" && runtime != "node" && runtime != "deno" {
		return ref, errors.New("child runtime")
	}
	marker, err := os.ReadFile(filepath.Join(source, ".interop-source-commit")) //nolint:gosec // Test-owned pinned asset path.
	if err != nil || string(marker) != "ea8ec0dca862d0c7284cc6a130a4b27fb812ed21\n" {
		return ref, errors.New("child source")
	}
	raw, err := os.ReadFile(filepath.Join(source, "protocol/v2/fixtures/duplex-child.json")) //nolint:gosec // Test-owned pinned asset path.
	if err != nil {
		return ref, err
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(raw))
	if hash != "30cb07b87060dc88e9cb6ac5414f7b9cb7bb19c7a97c058e284d96e1e59ff4a9" {
		return ref, errors.New("child manifest")
	}
	rows, err := interopfixture.Select(raw, "normal-serve-negotiated", []string{recipe.Name})
	if err != nil || len(rows) != 1 {
		return ref, errors.New("child selector")
	}
	var actual interopRecipeRow
	if json.Unmarshal(rows[0], &actual) != nil || actual.Name != recipe.Name || actual.Profile != recipe.Profile || actual.Scenario != recipe.Scenario || (actual.Status != "" && actual.Status != "observed") {
		return ref, errors.New("child selected recipe")
	}
	validProfile := recipe.Scenario == "child-fairness" && recipe.Profile == "expanded"
	if !validProfile {
		return ref, errors.New("child handler profile")
	}
	var id [16]byte
	if _, err = rand.Read(id[:]); err != nil {
		return ref, err
	}
	return interopQueueRef{string(marker[:len(marker)-1]), hash, recipe.Name, recipe.Scenario, recipe.Profile, runtime, fmt.Sprintf("%x", id), 1, 1}, nil
}

type interopChildCredit struct {
	ID                                           uint64
	InputBytes                                   int
	WholeInput, LocalOK, Released, CreditRetired bool
}
type interopChildRefill struct {
	Index                             int
	TriggerID, TriggerSequence, NewID uint64
	Credit                            interopChildCredit
}
type interopChildOccupancy struct{ Ordinary, Reverse, Credits int }
type interopChildProof struct {
	Ref                                        interopQueueRef
	expected                                   interopQueueRef
	CommandID, BarrierID                       uint64
	Health                                     map[uint64]interopChildCredit
	Initial                                    []uint64
	Refills                                    []interopChildRefill
	Binding, Args                              string
	SnapshotOrdinary, SnapshotControl          map[string]int
	ArmSequence, ReleaseSequence, WaitingBytes int
	Occupancy                                  []interopChildOccupancy
	Backend                                    []HostAuthority
	EOF, CustodyRetired                        bool
	ControlTrace                               []string
	Lifecycle                                  map[string]uint64
	Intents                                    [8]int
	mu                                         sync.Mutex
}

func (p *interopChildProof) sample(c *Conn) {
	c.mu.Lock()
	c.queue.mu.Lock()
	v := interopChildOccupancy{c.ordinaryCalls, c.reverse.workers, c.queue.reserved}
	c.queue.mu.Unlock()
	c.mu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.Occupancy) >= 64 {
		panic("harness child occupancy bound")
	}
	p.Occupancy = append(p.Occupancy, v)
}

type interopChildHealth struct {
	call *pendingCall
	done chan error
}

// Retain the actual pending-call object from CallCorrelation's transport path.
// This closed test producer uses Health only, finite observer and empty wire
// context; awaitCall and the sole Conn writer own publication/retirement.
func launchInteropChildHealth(ctx context.Context, t *testing.T, p *Process, c *interopControls, proof *interopChildProof) (uint64, *interopChildHealth) {
	t.Helper()
	call, err := p.conn.queueCallClass(ctx, subprocess.MethodHealth, map[string]any{}, true)
	if err != nil {
		t.Fatal("child Health admission", err)
	}
	h := &interopChildHealth{call: call, done: make(chan error, 1)}
	go func() {
		raw, e := p.conn.awaitCall(call)
		if e == nil {
			var result struct {
				OK bool `json:"ok"`
			}
			if json.Unmarshal(raw, &result) != nil || !result.OK {
				e = errors.New("child Health typed result")
			}
		}
		h.done <- e
	}()
	limit := time.NewTimer(time.Second)
	defer limit.Stop()
	var id uint64
	for id == 0 {
		p.conn.mu.Lock()
		n, ok := call.id.Integer()
		p.conn.mu.Unlock()
		if ok && n > 0 {
			id = uint64(n)
			break
		}
		select {
		case <-limit.C:
			t.Fatal("child selected Health ID timeout")
		case <-time.After(time.Millisecond):
		}
	}
	_, err = c.eventWhere("wire", func(e map[string]json.RawMessage) bool {
		var n uint64
		_ = json.Unmarshal(e["id"], &n)
		return rawEventString(e, "direction") == "host-to-worker" && rawEventString(e, "method") == subprocess.MethodHealth && n == id
	})
	if err != nil {
		t.Fatal(err)
	}
	proof.sample(p.conn)
	return id, h
}
func retireInteropChildHealth(t *testing.T, p *Process, id uint64, h *interopChildHealth) interopChildCredit {
	t.Helper()
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatal("child Health local completion", err)
		}
	case <-time.After(time.Second):
		t.Fatal("child Health local completion timeout")
	}
	deadline := time.Now().Add(time.Second)
	for {
		p.conn.mu.Lock()
		p.conn.queue.mu.Lock()
		v := interopChildCredit{ID: id, InputBytes: h.call.publication.bytes, WholeInput: h.call.publication.complete && h.call.publication.err == nil && h.call.publication.bytes > 0, LocalOK: true, Released: h.call.released, CreditRetired: !h.call.credit.held && h.call.frame == nil}
		p.conn.queue.mu.Unlock()
		p.conn.mu.Unlock()
		if v.WholeInput && v.Released && v.CreditRetired {
			return v
		}
		if time.Now().After(deadline) {
			t.Fatal("child physical per-call credit still held", v)
		}
		time.Sleep(time.Millisecond)
	}
}
func replayInteropChildFairness(t *testing.T, runtime string, recipe interopRecipeRow, b *interopBackend, row map[string]any) (*Process, *interopControls) {
	t.Helper()
	ref, err := newInteropChildRef(os.Getenv("INTEROP_SDK_SOURCE"), runtime, recipe)
	if err != nil {
		t.Fatal(err)
	}
	proof := &interopChildProof{Ref: ref, expected: ref, Health: map[uint64]interopChildCredit{}}
	spec := interopSourceSpec(t, b)
	original := spec.Reverse.Runtime.services.StorageGet
	var p *Process
	spec.Reverse.Runtime.services.StorageGet = func(ctx context.Context, call *HostCall, v subprocess.StorageGetParams) (subprocess.StorageGetResult, error) {
		proof.sample(p.conn)
		return original(ctx, call, v)
	}
	p, c := startInteropExpanded(t, runtime, recipe, b, spec)
	defer func() {
		if !t.Failed() {
			return
		}
		row["interim_events"] = append(append([]map[string]json.RawMessage{}, c.observed...), c.pending...)
		row["interim_diagnostics"] = p.Diagnostics()
	}()
	c.childProof = proof
	proof.Lifecycle = map[string]uint64{}
	for i := range proof.Intents {
		proof.Intents[i] = i
	}
	// Drain physical startup inputs/terminals before enabling the scoped ingress
	// sequence, so fairness begins with the actually blocked Health output.
	for _, method := range []string{subprocess.MethodInit, subprocess.MethodLoad} {
		e, eerr := c.eventWhere("wire", func(e map[string]json.RawMessage) bool {
			return rawEventString(e, "direction") == "host-to-worker" && rawEventString(e, "method") == method
		})
		if eerr != nil {
			t.Fatal("child startup ", method, eerr)
		}
		var id uint64
		_ = json.Unmarshal(e["id"], &id)
		proof.Lifecycle[method] = id
		if _, eerr = c.eventWhere("wire", func(e map[string]json.RawMessage) bool {
			var n uint64
			_ = json.Unmarshal(e["id"], &n)
			return rawEventString(e, "direction") == "worker-to-host" && rawEventString(e, "frame_type") == "response" && n == id
		}); eerr != nil {
			t.Fatal("child startup ", method, eerr)
		}
	}
	c.childStream.Store(true)
	observer, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = c.release("arm-writer"); err != nil {
		t.Fatal("arm writer", err)
	}
	proof.ArmSequence = c.seq
	health := map[uint64]*interopChildHealth{}
	id, h := launchInteropChildHealth(observer, t, p, c, proof)
	proof.BarrierID = id
	proof.Initial = append(proof.Initial, id)
	health[id] = h
	waiting, err := c.event("writer_waiting")
	if err != nil || json.Unmarshal(waiting["bytes"], &proof.WaitingBytes) != nil || proof.WaitingBytes <= 0 {
		t.Fatal("child writer barrier", err)
	}
	type commandOutcome struct {
		value subprocess.CommandExecResult
		err   error
	}
	command := make(chan commandOutcome, 1)
	go func() {
		value, e := interopCommand(observer, p, "get", map[string]any{"n": 8, "key": "read"}, "g-StorageGet")
		command <- commandOutcome{value, e}
	}()
	proof.CommandID, _ = interopEnteredCommand(t, c, "get")
	var input map[string]json.RawMessage
	for _, e := range c.observed {
		var n uint64
		_ = json.Unmarshal(e["id"], &n)
		if rawEventString(e, "direction") == "host-to-worker" && rawEventString(e, "method") == subprocess.MethodCommandExecute && n == proof.CommandID {
			input = e
		}
	}
	if input == nil {
		t.Fatal("child actual input missing")
	}

	var fields struct {
		Params struct {
			Args    string `json:"args"`
			Context struct {
				Binding string `json:"binding_id"`
			} `json:"context"`
		} `json:"params"`
	}
	if json.Unmarshal([]byte(rawEventString(input, "raw")), &fields) != nil {
		t.Fatal("child command input")
	}
	proof.Binding, proof.Args = fields.Params.Context.Binding, fields.Params.Args
	until := func(match func(map[string]int) bool) map[string]int {
		for i := 0; i < 40; i++ {
			v := interopSnapshot(t, c)
			if match(v) {
				return v
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatal("child source snapshot bound exhausted")
		return nil
	}
	proof.SnapshotOrdinary = until(func(v map[string]int) bool { return v["ordinary_queued"] == 8 })
	for i := 0; i < 12; i++ {
		id, h = launchInteropChildHealth(observer, t, p, c, proof)
		proof.Initial = append(proof.Initial, id)
		health[id] = h
	}
	proof.SnapshotControl = until(func(v map[string]int) bool { return v["control_queued"] == 12 })
	if proof.SnapshotControl["reserved_frames"]+proof.SnapshotControl["control_queued"] > 32 || proof.SnapshotControl["reserved_bytes"] != proof.SnapshotControl["reserved_frames"]*1024 {
		t.Fatal("child reserved source counters", proof.SnapshotControl)
	}
	proof.sample(p.conn)
	b.mu.Lock()
	before := len(b.calls)
	b.mu.Unlock()
	if before != 0 {
		t.Fatal("child backend before writer release", before)
	}
	if err = c.release("writer"); err != nil {
		t.Fatal(err)
	}
	proof.ReleaseSequence = c.seq
	ordinary, burst, refills := 0, 0, 0
	doneCommand := false
	for sequence := uint64(1); ordinary < 8 || len(health) > 0 || !doneCommand; sequence++ {
		frame, eerr := c.eventWhere("wire", func(e map[string]json.RawMessage) bool {
			var n uint64
			_ = json.Unmarshal(e["child_output_sequence"], &n)
			return n == sequence
		})
		if eerr != nil {
			t.Fatal("child physical stream", eerr)
		}
		var actual uint64
		_ = json.Unmarshal(frame["id"], &actual)
		if rawEventString(frame, "frame_type") == "request" {
			if rawEventString(frame, "method") != "host/storage/get" {
				t.Fatal("child unexpected ordinary")
			}
			ordinary++
			burst = 0
		} else if actual == proof.CommandID {
			if doneCommand {
				t.Fatal("child duplicate command terminal")
			}
			doneCommand = true
		} else {
			h, exists := health[actual]
			if !exists {
				t.Fatal("child unowned control response", actual)
			}
			credit := retireInteropChildHealth(t, p, actual, h)
			delete(health, actual)
			proof.Health[actual] = credit
			burst++
			if ordinary < 8 && burst > 4 {
				t.Fatal("child ordinary starvation burst", burst)
			}
			if refills < 8 {
				newID, next := launchInteropChildHealth(observer, t, p, c, proof)
				proof.Refills = append(proof.Refills, interopChildRefill{refills, actual, sequence, newID, credit})
				health[newID] = next
				refills++
			}
		}
	}
	select {
	case result := <-command:
		if result.err != nil {
			t.Fatal(result.err)
		}
		values := interopHelperResults(t, result.value)
		if len(values) != 8 {
			t.Fatal("child typed helper count")
		}
		for _, v := range values {
			if v.Code != "ok" {
				t.Fatal("child helper outcome", v)
			}
		}
		row["raw_command_result"] = result.value
	case <-observer.Done():
		t.Fatal("child command local completion", observer.Err())
	}
	b.mu.Lock()
	proof.Backend = append([]HostAuthority{}, b.calls...)
	commits := b.commits
	b.mu.Unlock()
	if len(proof.Backend) != 8 || commits != 0 {
		t.Fatal("child backend source result", len(proof.Backend), commits)
	}
	if err = p.stdin.Close(); err != nil {
		t.Fatal(err)
	}
	proof.EOF = true
	select {
	case <-p.exited:
	case <-observer.Done():
		t.Fatal("child natural EOF exit missing")
	}
	exit, ok := p.ExitInfo()
	if !ok || exit.Code != 0 || exit.Signal != "" {
		t.Fatal("child natural exit", exit)
	}
	select {
	case <-p.conn.Done():
	case <-observer.Done():
		t.Fatal("child Conn EOF missing")
	}
	p.conn.mu.Lock()
	p.conn.queue.mu.Lock()
	row["local_custody_before_fallback"] = map[string]any{"pending": len(p.conn.pending), "correlations": len(p.conn.correlations), "ordinary_calls": p.conn.ordinaryCalls, "lifecycle_calls": p.conn.lifecycleCalls, "inbound_active": len(p.conn.inboundActive), "reverse_workers": p.conn.reverse.workers, "active_reply_receipt": p.conn.activeReceipt != nil, "reserved_control_credits": p.conn.queue.reserved, "active_write": p.conn.queue.active != nil, "outbound": len(p.conn.outbound)}
	proof.CustodyRetired = len(p.conn.pending) == 0 && len(p.conn.correlations) == 0 && p.conn.ordinaryCalls == 0 && p.conn.lifecycleCalls == 0 && len(p.conn.inboundActive) == 0 && p.conn.reverse.workers == 0 && p.conn.activeReceipt == nil && p.conn.queue.reserved == 0 && p.conn.queue.active == nil && len(p.conn.outbound) == 0
	p.conn.queue.mu.Unlock()
	p.conn.mu.Unlock()
	if !proof.CustodyRetired {
		t.Fatal("child custody before fallback")
	}
	finished, err := c.event("finished")
	if err != nil {
		t.Fatal(err)
	}
	proof.ControlTrace = append([]string{}, c.trace...)
	if err = c.finish(0); err != nil {
		t.Fatal(err)
	}
	auditInteropChildCopies(t, c)
	row["copied_trace_adverse_audit"] = true
	row["finished"] = finished
	row["child_fairness_proof"] = proof
	row["no_fallback_before_proof"] = true
	row["projection"] = "Genuine InitLoad and finite actual host binding; absent Health, actual Conn paths; source EOF cleanup without Unload input"
	return p, c
}

func auditInteropChildCopies(t *testing.T, c *interopControls) {
	t.Helper()
	for _, variant := range []string{"extra-pair", "unowned-cancel", "duplicate-helper", "missing-load", "missing-terminal", "identity", "credit", "custody", "intent", "waiting-bytes", "backend", "sequence", "burst", "early-refill", "late-error", "missing-ack", "false-snapshot"} {
		original := c.childProof
		proof := interopChildProof{Ref: original.Ref, expected: original.expected, CommandID: original.CommandID, BarrierID: original.BarrierID, Health: map[uint64]interopChildCredit{}, Initial: append([]uint64{}, original.Initial...), Refills: append([]interopChildRefill{}, original.Refills...), Binding: original.Binding, Args: original.Args, SnapshotOrdinary: original.SnapshotOrdinary, SnapshotControl: original.SnapshotControl, ArmSequence: original.ArmSequence, ReleaseSequence: original.ReleaseSequence, WaitingBytes: original.WaitingBytes, Occupancy: original.Occupancy, Backend: append([]HostAuthority{}, original.Backend...), EOF: original.EOF, CustodyRetired: original.CustodyRetired, ControlTrace: original.ControlTrace, Lifecycle: original.Lifecycle, Intents: original.Intents}
		for k, v := range original.Health {
			proof.Health[k] = v
		}
		raw, _ := json.Marshal(c.observed)
		var events []map[string]json.RawMessage
		_ = json.Unmarshal(raw, &events)

		failure := io.EOF
		switch variant {
		case "extra-pair":
			events = append(events, queueGuardWire("host-to-worker", "request", proof.CommandID+100, "plugin/health", map[string]any{}, nil), queueGuardWire("worker-to-host", "response", proof.CommandID+100, "", nil, map[string]any{"ok": true}))
		case "unowned-cancel":
			events = append(events, queueGuardEvent(map[string]any{"kind": "wire", "direction": "host-to-worker", "frame_type": "notification", "method": "rpc/cancel", "raw": fmt.Sprintf("{\"jsonrpc\":\"2.0\",\"method\":\"rpc/cancel\",\"params\":{\"request_owner\":\"host\",\"id\":%d,\"reason\":\"caller_cancelled\"}}\n", proof.BarrierID)})) //nolint:misspell // SDK wire reason.
		case "duplicate-helper":
			for _, e := range events {
				if rawEventString(e, "kind") == "helper_done" {
					events = append(events, e)
					break
				}
			}
		case "missing-load", "missing-terminal":
			for i, e := range events {
				var id uint64
				_ = json.Unmarshal(e["id"], &id)
				if (variant == "missing-load" && rawEventString(e, "method") == subprocess.MethodLoad) || (variant == "missing-terminal" && rawEventString(e, "direction") == "worker-to-host" && rawEventString(e, "frame_type") == "response" && id == proof.BarrierID) {
					events = append(events[:i], events[i+1:]...)
					break
				}
			}
		case "identity":
			proof.Ref.Run = "foreign"
		case "credit":
			v := proof.Health[proof.BarrierID]
			v.CreditRetired = false
			proof.Health[proof.BarrierID] = v
		case "custody":
			proof.CustodyRetired = false
		case "intent":
			proof.Intents[0] = 7
		case "waiting-bytes":
			proof.WaitingBytes++
		case "backend":
			proof.Backend[0].BindingID = "foreign"
		case "sequence":
			for _, e := range events {
				if _, ok := e["child_output_sequence"]; ok {
					e["child_output_sequence"] = json.RawMessage(`99`)
					break
				}
			}
		case "burst":
			var ordered []map[string]json.RawMessage
			for _, e := range events {
				if _, ok := e["child_output_sequence"]; ok {
					ordered = append(ordered, e)
				}
			}
			sort.Slice(ordered, func(i, j int) bool {
				var a, b uint64
				_ = json.Unmarshal(ordered[i]["child_output_sequence"], &a)
				_ = json.Unmarshal(ordered[j]["child_output_sequence"], &b)
				return a < b
			})
			var moved, remaining []map[string]json.RawMessage
			for _, e := range ordered {
				var id uint64
				_ = json.Unmarshal(e["id"], &id)
				_, ok := proof.Health[id]
				if ok && rawEventString(e, "frame_type") == "response" && len(moved) < 5 {
					moved = append(moved, e)
				} else {
					remaining = append(remaining, e)
				}
			}
			ordered = append(moved, remaining...)
			next := 0
			for i, e := range ordered {
				e["child_output_sequence"], _ = json.Marshal(i + 1)
				var id uint64
				_ = json.Unmarshal(e["id"], &id)
				if _, ok := proof.Health[id]; ok && rawEventString(e, "frame_type") == "response" && next < 8 {
					proof.Refills[next].TriggerID = id
					proof.Refills[next].TriggerSequence = uint64(i + 1)
					proof.Refills[next].Credit = proof.Health[id]
					next++
				}
			}

		case "early-refill":
			proof.Refills[0].TriggerSequence++
		case "missing-ack":
			for i, e := range events {
				if rawEventString(e, "kind") == "control_received" {
					events = append(events[:i], events[i+1:]...)
					break
				}
			}
		case "false-snapshot":
			for _, e := range events {
				if rawEventString(e, "kind") == "snapshot" {
					e["effects"] = json.RawMessage(`{}`)
				}
			}
		case "late-error":
			failure = errors.Join(io.EOF, errors.New("late control failure"))
		}
		copied := &interopControls{childProof: &proof, observed: events, events: make(chan map[string]json.RawMessage), failure: make(chan error, 1), expandedReverseLimit: c.expandedReverseLimit}
		copied.failure <- failure
		if copied.finish(0) == nil {
			t.Fatal("child copied trace accepted", variant)
		}
	}
}
