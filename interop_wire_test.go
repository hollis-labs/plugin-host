//go:build unix

package pluginhost

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/hollis-labs/plugin-host/internal/strictjson"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

// This selected contract has no cancellation trigger. Every host request must
// belong to the closed actual input plan, and every underlying host write must
// have exactly one observer witness. Shared notification rules remain unchanged.
func (p *interopLifecycleProof) guard(events []map[string]json.RawMessage, exit int) error {
	fail := func() error {
		_, _, line, _ := runtime.Caller(1)
		return fmt.Errorf("harness lifecycle capacity ownership/custody proof at line %d", line)
	}
	if p == nil || p.Ref != p.expected || len(p.Ref.Source) != 40 || len(p.Ref.Manifest) != 64 || p.Ref.Run == "" || p.Ref.Corpus != 1 || p.Ref.Selector != 1 || p.Ref.Profile != "expanded" || exit != 0 || p.Overflow || !p.CustodyRetired || !p.Halfclosed || !p.LocalOrdinaryRefused || !p.LocalLifecycleRefused || len(p.Inputs) != 20 {
		return fail()
	}
	if p.Ref.Runtime != "go" && p.Ref.Runtime != "node" && p.Ref.Runtime != "deno" {
		return fail()
	}
	reverseCount, holds := 0, 16
	if p.Ref.Scenario == "credits-v2" {
		reverseCount, holds = 8, 15
	} else if p.Ref.Scenario != "forward-v2" {
		return fail()
	}
	if p.BeforeRefusal != p.AfterRefusal || p.BeforeRefusal.Ordinary != 16 || p.BeforeRefusal.Lifecycle != 2 || p.BeforeRefusal.Pending != 18 || p.BeforeRefusal.Correlations != holds || p.BeforeRefusal.Inbound != reverseCount || p.BeforeRefusal.Workers != reverseCount || p.BeforeRefusal.Reserved != 18+reverseCount || p.BackendPermitBefore != reverseCount || p.BackendPermitAfter != 0 {
		return fail()
	}
	if p.AuthorityBefore.Parents != 2+reverseCount/8 || p.AuthorityBefore.LiveBindings != reverseCount/8 || p.AuthorityBefore.Active != reverseCount || p.AuthorityAfter.Parents != 0 || p.AuthorityAfter.LiveBindings != 0 || p.AuthorityAfter.Active != 0 {
		return fail()
	}
	if p.Startup["entered"] != 1 || p.Startup["load"] != 1 || p.Startup["returned"] != 0 || p.Startup["reserved_frames"] != 0 || p.Startup["reserved_bytes"] != 0 || p.Startup["reverse_pending"] != 0 || p.Saturated["entered"] != 19 || p.Saturated["load"] != 3 || p.Saturated["hold"] != holds || p.Saturated["returned"] != 0 || p.Saturated["reverse_pending"] != reverseCount || p.Saturated["reserved_frames"] != 18 || p.Saturated["reserved_bytes"] != 18*1024 || p.Released["returned"] != 16 || p.Released["reserved_frames"] != 0 || p.Released["reserved_bytes"] != 0 || p.Released["reverse_pending"] != 0 || p.BackendEntries != reverseCount || p.BackendReturns != reverseCount || len(p.Authorities) != reverseCount {
		return fail()
	}
	type envelope struct {
		JSONRPC string
		ID      uint64
		Method  string
		Params  json.RawMessage
		Result  json.RawMessage
		Error   json.RawMessage
	}
	if len(p.ControlTrace) > 64 {
		return fail()
	}
	releaseIDs := map[uint64]bool{}
	snapshotControls := 0
	for index, trace := range p.ControlTrace {
		suffix := fmt.Sprintf("/ack:%d", index+1)
		if !strings.HasPrefix(trace, "release:") || !strings.HasSuffix(trace, suffix) {
			return fail()
		}
		gate := strings.TrimSuffix(strings.TrimPrefix(trace, "release:"), suffix)
		if gate == "snapshot" {
			snapshotControls++
			continue
		}
		var id uint64
		if _, err := fmt.Sscanf(gate, "request-%d", &id); err != nil || gate != fmt.Sprintf("request-%d", id) || releaseIDs[id] {
			return fail()
		}
		owned, ok := p.Inputs[id]
		if !ok || (owned.Method != subprocess.MethodLoad && owned.Name != "hold") {
			return fail()
		}
		releaseIDs[id] = true
	}
	if len(releaseIDs) != holds+3 || snapshotControls < 4 || snapshotControls > 43 {
		return fail()
	}
	controlACKs := map[int]bool{}
	snapshotEvents := 0
	returnedSequence, completionAbort := map[uint64]uint64{}, map[uint64]uint64{}
	for _, field := range []string{"entered", "load", "hold", "get", "returned", "reverse_pending", "reserved_frames", "reserved_bytes", "commits", "unload_attempts"} {
		if p.Saturated[field] != p.AfterOverflow[field] {
			return fail()
		}
	}
	if p.Startup["unload_attempts"] != 0 || p.Saturated["unload_attempts"] != 0 || p.Released["unload_attempts"] != 0 || p.Startup["commits"] != 0 || p.Saturated["commits"] != 0 || p.Released["commits"] != 0 || p.Saturated["get"] != reverseCount/8 {
		return fail()
	}
	writes := map[string]string{}
	writeTimes := map[string]time.Time{}
	for _, w := range p.Writes {
		if w.Error != "" || w.Bytes != len(w.Raw) || w.Bytes <= 0 || w.Bytes > 64<<10 || !w.At.Before(p.Deadline) || strictjson.Validate([]byte(w.Raw)) != nil {
			return fail()
		}
		hash := fmt.Sprintf("%x", sha256.Sum256([]byte(w.Raw)))
		if _, exists := writes[hash]; exists {
			return fail()
		}
		writes[hash] = w.Raw
		writeTimes[hash] = w.At
	}
	inputs, terminals, entered, returned := map[uint64]bool{}, map[uint64]bool{}, map[uint64]bool{}, map[uint64]bool{}
	reverses := map[uint64]bool{}
	helperIndices := map[int]bool{}
	init, load, hold, get, finished, workerExit, cleanup := 0, 0, 0, 0, 0, 0, 0
	var lastSequence uint64
	var previousCounters map[string]int
	// Consumption order can differ from physical child order; ingress ordinals
	// are independently unique and retained for all physical worker frames.
	sequences := map[uint64]bool{}
	for _, e := range events {
		switch rawEventString(e, "kind") {
		case "control_received":
			var seq int
			if json.Unmarshal(e["seq"], &seq) != nil || seq <= 0 || seq > len(p.ControlTrace) || controlACKs[seq] {
				return fail()
			}
			controlACKs[seq] = true
		case "snapshot":
			snapshotEvents++
			effects, err := lifecyclePhysicalCounters(e["effects"], holds, reverseCount/8, false, previousCounters)
			if err != nil {
				return fail()
			}
			previousCounters = effects
			expected := p.Released
			switch snapshotEvents {
			case 1:
				expected = p.Startup
			case 2:
				expected = p.Saturated
			case 3:
				expected = p.AfterOverflow
			}
			for _, field := range []string{"entered", "load", "hold", "get", "returned"} {
				if effects[field] != expected[field] {
					return fail()
				}
			}
			if snapshotEvents <= 3 {
				for _, field := range []string{"reverse_pending", "reserved_frames", "reserved_bytes", "ordinary_queued", "control_queued"} {
					if effects[field] != expected[field] {
						return fail()
					}
				}
			} else if effects["reverse_pending"] != 0 || effects["reserved_frames"] < 0 || effects["reserved_frames"] > 18 || effects["reserved_bytes"] != effects["reserved_frames"]*1024 || effects["ordinary_queued"] != 0 || effects["control_queued"] < 0 || effects["control_queued"] > 18 {
				return fail()
			}
		case "wire":
			direction, kind := rawEventString(e, "direction"), rawEventString(e, "frame_type")
			if kind == "notification" {
				return fail()
			}
			raw := rawEventString(e, "raw")
			switch direction {
			case "host-to-worker":
				hash := rawEventString(e, "sha256")
				physical, ok := writes[hash]
				if !ok {
					return fail()
				}
				delete(writes, hash)
				if raw != "" && raw != physical {
					return fail()
				}
				raw = physical
			case "worker-to-host":
				var sequence uint64
				if json.Unmarshal(e["child_output_sequence"], &sequence) != nil || sequence == 0 || sequences[sequence] {
					return fail()
				}
				sequences[sequence] = true
				if sequence > lastSequence {
					lastSequence = sequence
				}
			default:
				return fail()
			}
			var v envelope
			if _, err := strictjson.ObjectFields([]byte(raw), []string{"jsonrpc", "id"}, []string{"method", "params", "result", "error"}); err != nil {
				return fail()
			}
			if strictjson.Validate([]byte(raw)) != nil || json.Unmarshal([]byte(raw), &v) != nil || v.ID == 0 || v.JSONRPC != "2.0" {
				return fail()
			}
			if direction == "host-to-worker" && kind == "request" {
				if len(v.Result) != 0 || len(v.Error) != 0 || len(v.Params) == 0 {
					return fail()
				}
				owned, ok := p.Inputs[v.ID]
				if !ok || inputs[v.ID] || owned.ID != v.ID || v.Method != owned.Method {
					return fail()
				}
				inputs[v.ID] = true
				var params map[string]json.RawMessage
				if json.Unmarshal(v.Params, &params) != nil {
					return fail()
				}
				contextRaw, hasContext := params["context"]
				if owned.Finite != hasContext {
					return fail()
				}
				if hasContext {
					contextFields, err := strictjson.ObjectFields(contextRaw, []string{"timeout_ms"}, []string{"binding_id"})
					if err != nil {
						return fail()
					}
					_, hasBinding := contextFields["binding_id"]
					if hasBinding != (owned.Binding != "") {
						return fail()
					}
					var context struct {
						Timeout int64  `json:"timeout_ms"`
						Binding string `json:"binding_id"`
					}
					if json.Unmarshal(contextRaw, &context) != nil || context.Timeout <= 0 || context.Timeout > 10000 || context.Binding != owned.Binding || context.Timeout > p.Deadline.Sub(writeTimes[rawEventString(e, "sha256")]).Milliseconds()+1 {
						return fail()
					}
				}
				delete(params, "context")
				actualParams, marshalErr := json.Marshal(params)
				if marshalErr != nil {
					return fail()
				}
				expectedParams := json.RawMessage(`{}`)
				switch owned.Method {
				case subprocess.MethodInit:
					expectedParams = p.InitParams
				case subprocess.MethodCommandExecute:
					expectedParams, _ = json.Marshal(subprocess.CommandExecParams{Name: owned.Name, Args: owned.Args})
				}
				var expectedFields map[string]json.RawMessage
				if json.Unmarshal(expectedParams, &expectedFields) != nil || expectedFields == nil {
					return fail()
				}
				delete(expectedFields, "context")
				expectedParams, marshalErr = json.Marshal(expectedFields)
				if marshalErr != nil || !bytes.Equal(actualParams, expectedParams) {
					return fail()
				}
				switch owned.Method {
				case subprocess.MethodInit:
					init++
				case subprocess.MethodLoad:
					load++
				case subprocess.MethodCommandExecute:
					var command struct{ Name, Args string }
					if json.Unmarshal(v.Params, &command) != nil || command.Name != owned.Name || command.Args != owned.Args {
						return fail()
					}
					if owned.Name == "hold" {
						hold++
					} else if owned.Name == "get" && reverseCount == 8 && owned.Binding != "" {
						get++
					} else {
						return fail()
					}
				default:
					return fail()
				}
				if owned.Method != subprocess.MethodInit && (!owned.Credit.WholeInput || !owned.Credit.LocalOK || !owned.Credit.Released || !owned.Credit.CreditRetired || owned.Credit.ID != v.ID) {
					return fail()
				}
			} else if direction == "worker-to-host" && kind == "response" {
				owned, ok := p.Inputs[v.ID]
				if !ok || terminals[v.ID] || len(v.Result) == 0 || len(v.Error) != 0 || v.Method != "" || len(v.Params) != 0 {
					return fail()
				}
				if owned.Method == subprocess.MethodInit {
					result, err := decodeInitResult(v.Result)
					if err != nil || result.ID != "fixture" || result.Version != "1.0.0" || result.Protocol != 2 || result.CapabilityContract != 1 || result.ReverseRPCVersion == nil || *result.ReverseRPCVersion != 1 {
						return fail()
					}
				} else if validatePendingResult(owned.Method, v.Result) != nil {
					return fail()
				}
				if owned.Method == subprocess.MethodCommandExecute {
					var result subprocess.CommandExecResult
					if json.Unmarshal(v.Result, &result) != nil {
						return fail()
					}
					if owned.Name == "hold" && result.Action != "noop" {
						return fail()
					}
					if owned.Name == "get" {
						var outcomes []interopHelperOutcome
						if result.Action != "message" || json.Unmarshal([]byte(result.Content), &outcomes) != nil || len(outcomes) != 8 {
							return fail()
						}
						for _, outcome := range outcomes {
							if outcome.Code != "ok" {
								return fail()
							}
						}
					}
				}
				terminals[v.ID] = true
			} else if direction == "worker-to-host" && kind == "request" {
				if reverseCount == 0 || v.Method != string(HostStorageGet) || reverses[v.ID] {
					return fail()
				}
				reverses[v.ID] = true
			} else if direction == "host-to-worker" && kind == "response" {
				var result subprocess.StorageGetResult
				if reverseCount == 0 || len(v.Result) == 0 || len(v.Error) != 0 || validatePendingResult(string(HostStorageGet), v.Result) != nil || json.Unmarshal(v.Result, &result) != nil || result.Found {
					return fail()
				}
			} else {
				return fail()
			}
		case "entered", "returned":
			var id uint64
			if json.Unmarshal(e["id"], &id) != nil {
				return fail()
			}
			owned, ok := p.Inputs[id]
			if !ok || owned.Method == subprocess.MethodInit {
				return fail()
			}
			if rawEventString(e, "kind") == "entered" {
				var deadline bool
				if entered[id] || json.Unmarshal(e["deadline"], &deadline) != nil || deadline != owned.Finite {
					return fail()
				}
				want := owned.Name
				if owned.Method == subprocess.MethodLoad {
					want = "load"
				}
				if rawEventString(e, "name") != want {
					return fail()
				}
				entered[id] = true
			} else {
				if returned[id] {
					return fail()
				}
				returned[id] = true
				var sequence uint64
				if json.Unmarshal(e["lifecycle_stderr_sequence"], &sequence) != nil || sequence == 0 {
					return fail()
				}
				returnedSequence[id] = sequence
			}
		case "helper_done":
			var id uint64
			var index int
			var outcome interopHelperOutcome
			if json.Unmarshal(e["id"], &id) != nil || p.Inputs[id].Name != "get" || json.Unmarshal(e["index"], &index) != nil || index < 0 || index >= 8 || helperIndices[index] || json.Unmarshal(e["failure"], &outcome) != nil || outcome.Code != "ok" {
				return fail()
			}
			helperIndices[index] = true
		case "aborted":
			var id, sequence uint64
			var deadline bool
			if p.Ref.Runtime == "go" || json.Unmarshal(e["id"], &id) != nil || json.Unmarshal(e["deadline"], &deadline) != nil || deadline || json.Unmarshal(e["lifecycle_stderr_sequence"], &sequence) != nil || sequence == 0 || completionAbort[id] != 0 {
				return fail()
			}
			if _, ok := p.Inputs[id]; !ok || p.Inputs[id].Method == subprocess.MethodInit {
				return fail()
			}
			completionAbort[id] = sequence
		case "control_failure":
			return fail()
		case "unload_started":
			cleanup++
		case "finished":
			finished++
			effects, err := lifecyclePhysicalCounters(e["effects"], holds, reverseCount/8, true, previousCounters)
			if err != nil || effects["returned"] != 16 || effects["entered"] != 19 || effects["load"] != 3 || effects["unload_attempts"] != 1 || effects["commits"] != 0 || effects["reverse_pending"] != 0 || effects["reserved_frames"] != 0 || effects["reserved_bytes"] != 0 {
				return fail()
			}
		case "worker_exit":
			workerExit++
		}
	}
	if len(writes) != 0 || len(inputs) != 20 || len(terminals) != 20 || len(entered) != 19 || len(returned) != 19 || init != 1 || load != 3 || hold != holds || get != reverseCount/8 || len(reverses) != reverseCount || len(helperIndices) != reverseCount || finished != 1 || workerExit != 1 || cleanup != 1 || uint64(len(sequences)) != lastSequence || len(controlACKs) != len(p.ControlTrace) || snapshotEvents != snapshotControls {
		return fail()
	}
	for _, a := range p.Authorities {
		owned, ok := p.Inputs[a.Parent.ID]
		if !ok || owned.Name != "get" || string(a.BindingID) != owned.Binding || a.Grant.GrantID != "g-StorageGet" || a.Method != HostStorageGet || a.Deadline.IsZero() || a.Deadline.After(p.Deadline) || a.Parent.RequestOwner != subprocess.HostRPCOwnerHost {
			return fail()
		}
	}
	if p.Ref.Runtime != "go" && len(completionAbort) != 19 {
		return fail()
	}
	for id, sequence := range completionAbort {
		if returnedSequence[id] == 0 || sequence <= returnedSequence[id] {
			return fail()
		}
	}
	return nil
}

// The observer forwards every worker stdout byte unchanged. It records bounded
// frame witnesses separately, never emits protocol messages or returns replies.
type interopWireWriter struct {
	output    io.Writer
	observer  *interopEventWriter
	pending   []byte
	direction string
}

func (w *interopWireWriter) Write(raw []byte) (int, error) {
	n, err := w.output.Write(raw)
	if err != nil {
		return n, err
	}
	for _, b := range raw {
		if len(w.pending) >= 8<<20 {
			return n, errors.New("harness stdout frame limit")
		}
		w.pending = append(w.pending, b)
		if b != '\n' {
			continue
		}
		hash := sha256.Sum256(w.pending)
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(w.pending, &envelope); err != nil {
			return n, err
		}
		direction := w.direction
		if direction == "" {
			direction = "worker-to-host"
		}
		frameType := "response"
		if _, method := envelope["method"]; method {
			frameType = "notification"
			if id := envelope["id"]; len(id) != 0 && string(id) != "null" {
				frameType = "request"
			}
		}
		event := map[string]any{"frame_type": frameType, "kind": "wire", "direction": direction, "bytes": len(w.pending), "sha256": hex.EncodeToString(hash[:]), "observed_at": time.Now().UTC().Format(time.RFC3339Nano), "id": envelope["id"], "method": envelope["method"]}
		if len(w.pending) <= 3000 {
			event["raw"] = string(w.pending)
		}
		line, err := json.Marshal(map[string]any{"fixture_event": event})
		if err != nil {
			return n, err
		}
		if _, err := w.observer.Write(append(line, '\n')); err != nil {
			return n, err
		}
		w.pending = w.pending[:0]
	}
	return n, nil
}

func (w *interopWireWriter) finish() error {
	if len(w.pending) != 0 {
		return errors.New("harness partial stdout frame at EOF")
	}
	return nil
}

// Reconcile both directions after observer EOF, rather than trusting the Conn
// reader to reject stray replies. Input/output observers can run in either
// order; every terminal must still belong to exactly one published request.
func interopWireTerminals(events []map[string]json.RawMessage, expectedExit int, hostEvidence ...map[uint64]interopHostCancelEvidence) error {
	type counts struct{ requests, terminals int }
	ledger := map[string]*counts{}
	for _, event := range events {
		if rawEventString(event, "kind") != "wire" {
			continue
		}
		direction := rawEventString(event, "direction")
		if direction != "host-to-worker" && direction != "worker-to-host" {
			return errors.New("harness unknown wire direction")
		}
		kind := rawEventString(event, "frame_type")
		if kind == "notification" {
			continue
		}
		if kind != "request" && kind != "response" {
			return errors.New("harness missing wire envelope classification")
		}
		var id uint64
		if err := json.Unmarshal(event["id"], &id); err != nil || id == 0 {
			return errors.New("harness invalid wire correlation id")
		}
		if kind == "response" {
			if direction == "host-to-worker" {
				direction = "worker-to-host"
			} else {
				direction = "host-to-worker"
			}
		}
		key := fmt.Sprintf("%s/%d", direction, id)
		entry := ledger[key]
		if entry == nil {
			entry = &counts{}
			ledger[key] = entry
		}
		if kind == "request" {
			entry.requests++
		} else {
			entry.terminals++
		}
	}
	for key, entry := range ledger {
		if entry.requests != 1 || entry.terminals > 1 || (expectedExit == 0 && entry.terminals != 1) {
			return fmt.Errorf("harness unmatched wire terminal %s: requests=%d terminals=%d", key, entry.requests, entry.terminals)
		}
	}
	return interopWireNotifications(events, hostEvidence...)
}

// Expanded fixture helpers originate only in actual get/put commands, not in
// Health or lifecycle calls. Derive method, argument and parent expectations
// from published command frames; a refused extra request is still extra traffic.
func interopExpandedHelpers(events []map[string]json.RawMessage, reverseLimit uint32, proofs ...*interopQueueProof) error {
	reductions := map[uint64]int{}
	if len(proofs) > 1 {
		return errors.New("harness duplicate queue proof")
	}
	if len(proofs) == 1 && proofs[0] != nil {
		id, n, err := proofs[0].reduction(events)
		if err != nil {
			return err
		}
		reductions[id] = n
	}

	type helper struct {
		method, binding, key, operation, value string
		remaining                              int
	}
	expected := map[uint64]*helper{}
	for _, e := range events {
		if rawEventString(e, "kind") != "wire" || rawEventString(e, "direction") != "host-to-worker" || rawEventString(e, "method") != "command/execute" {
			continue
		}
		var command struct {
			ID     uint64 `json:"id"`
			Params struct {
				Name    string `json:"name"`
				Args    string `json:"args"`
				Context struct {
					Binding string `json:"binding_id"`
				} `json:"context"`
			} `json:"params"`
		}
		if err := json.Unmarshal([]byte(rawEventString(e, "raw")), &command); err != nil {
			return err
		}
		if command.Params.Name != "get" && command.Params.Name != "put" {
			continue
		}
		var args struct {
			N          int    `json:"n"`
			Key        string `json:"key"`
			Operation  string `json:"operation_key"`
			ValueBytes int    `json:"value_bytes"`
		}
		if err := json.Unmarshal([]byte(command.Params.Args), &args); err != nil {
			return err
		}
		if args.N < 1 || args.N > 9 || args.ValueBytes < 0 || args.ValueBytes > 65536 {
			return errors.New("harness invalid published fixture helper args")
		}
		method := "host/storage/" + command.Params.Name
		expected[command.ID] = &helper{method: method, binding: command.Params.Context.Binding, key: args.Key, operation: args.Operation, value: strings.Repeat("v", args.ValueBytes), remaining: min(args.N, int(reverseLimit)) - reductions[command.ID]}
	}
	for _, e := range events {
		if rawEventString(e, "kind") != "wire" || rawEventString(e, "direction") != "worker-to-host" || rawEventString(e, "frame_type") != "request" {
			continue
		}
		var request struct {
			Method string `json:"method"`
			Params struct {
				Grant     string          `json:"grant_id"`
				Key       string          `json:"key"`
				Operation string          `json:"operation_key"`
				Value     string          `json:"value"`
				Expected  json.RawMessage `json:"expected_revision"`
				Context   struct {
					Binding string `json:"binding_id"`
					Parent  struct {
						Owner string `json:"request_owner"`
						ID    uint64 `json:"id"`
					} `json:"parent_call"`
				} `json:"context"`
			} `json:"params"`
		}
		if err := json.Unmarshal([]byte(rawEventString(e, "raw")), &request); err != nil {
			return err
		}
		want := expected[request.Params.Context.Parent.ID]
		if want == nil || want.remaining == 0 || request.Method != want.method || request.Params.Context.Parent.Owner != "host" || want.binding == "" || request.Params.Context.Binding != want.binding || request.Params.Key != want.key {
			return fmt.Errorf("harness unexpected expanded helper method/parent/args: %s parent=%d", request.Method, request.Params.Context.Parent.ID)
		}
		grant := "g-StorageGet"
		if want.method == "host/storage/put" {
			grant = "g-StoragePut"
			if request.Params.Operation != want.operation || request.Params.Value != want.value || string(request.Params.Expected) != "null" {
				return errors.New("harness unexpected put effect arguments")
			}
		}
		if request.Params.Grant != grant {
			return errors.New("harness unexpected helper grant")
		}
		want.remaining--
	}
	for parent, want := range expected {
		if want.remaining != 0 {
			return fmt.Errorf("harness missing expanded helpers parent=%d remaining=%d", parent, want.remaining)
		}
	}
	return nil
}

// Cancellation is the only authored notification in this implemented subset.
// Account for it against real directional requests and terminal/parent evidence
// after EOF, preserving observer reorder and opposite-direction ID collisions.
// This evidence is supplied only by host test code from actual Conn call/write
// receipts. Fixture observer events cannot populate it or assert its authority.
type interopHostCancelEvidence struct {
	Reason                                      string
	Published, ControlComplete, ObserverExpired bool
}

func interopWireNotifications(events []map[string]json.RawMessage, hostEvidence ...map[uint64]interopHostCancelEvidence) error {
	type request struct {
		parent      uint64
		parentOwner string
	}
	type cancellation struct {
		direction, owner, reason string
		id                       uint64
	}
	requests := map[string]request{}
	terminalCodes := map[string]string{}
	cancels := map[string]cancellation{}
	for _, event := range events {
		if rawEventString(event, "kind") != "wire" {
			continue
		}
		direction, kind := rawEventString(event, "direction"), rawEventString(event, "frame_type")
		var frame struct {
			ID     uint64 `json:"id"`
			Method string `json:"method"`
			Params struct {
				ID      uint64 `json:"id"`
				Owner   string `json:"request_owner"`
				Reason  string `json:"reason"`
				Context struct {
					Parent struct {
						ID    uint64 `json:"id"`
						Owner string `json:"request_owner"`
					} `json:"parent_call"`
				} `json:"context"`
			} `json:"params"`
			Error struct {
				Data struct {
					Code string `json:"code"`
				} `json:"data"`
			} `json:"error"`
		}
		// Large ordinary frames can have hash-only witnesses. Only cancellation
		// and its small correlation/effect metadata require a raw witness here.
		raw := rawEventString(event, "raw")
		if raw != "" {
			if err := json.Unmarshal([]byte(raw), &frame); err != nil {
				return err
			}
		}
		var id uint64
		_ = json.Unmarshal(event["id"], &id)
		switch kind {
		case "request":
			requests[fmt.Sprintf("%s/%d", direction, id)] = request{parent: frame.Params.Context.Parent.ID, parentOwner: frame.Params.Context.Parent.Owner}
		case "response":
			requestDirection := "host-to-worker"
			if direction == "host-to-worker" {
				requestDirection = "worker-to-host"
			}
			terminalCodes[fmt.Sprintf("%s/%d", requestDirection, id)] = frame.Error.Data.Code
		case "notification":
			envelope, err := strictjson.Object([]byte(raw), "jsonrpc", "method", "params")
			if err != nil || string(envelope["jsonrpc"]) != `"2.0"` {
				return errors.New("harness invalid cancellation envelope")
			}
			if _, err := strictjson.Object(envelope["params"], "request_owner", "id", "reason"); err != nil {
				return err
			}
			owner := "host"
			if direction == "worker-to-host" {
				owner = "plugin"
			}
			if raw == "" || frame.Method != "rpc/cancel" || frame.ID != 0 || frame.Params.ID == 0 || frame.Params.Owner != owner {
				return errors.New("harness unexpected wire notification")
			}
			key := fmt.Sprintf("%s/%d", direction, frame.Params.ID)
			if _, duplicate := cancels[key]; duplicate {
				return errors.New("harness duplicate cancellation notification")
			}
			cancels[key] = cancellation{direction: direction, owner: owner, reason: frame.Params.Reason, id: frame.Params.ID}
		}
	}
	for key, cancel := range cancels {
		published, ok := requests[key]
		if !ok {
			return errors.New("harness cancellation has no directional published request")
		}
		if cancel.owner == "host" {
			if cancel.reason == "deadline_exceeded" {
				var evidence interopHostCancelEvidence
				if len(hostEvidence) == 1 {
					evidence = hostEvidence[0][cancel.id]
				}
				if evidence.Reason != cancel.reason || !evidence.Published || !evidence.ControlComplete || !evidence.ObserverExpired {
					return errors.New("harness observer cancellation lacks actual host call/write provenance")
				}
			} else if cancel.reason != "caller_cancelled" { //nolint:misspell // Preserve exact SDK wire spelling.
				return errors.New("harness unexpected host cancellation reason")
			}
			continue
		}
		switch cancel.reason {
		case "parent_cancelled": //nolint:misspell // Preserve exact SDK wire spelling.
			parent, canceled := cancels[fmt.Sprintf("host-to-worker/%d", published.parent)]
			if published.parentOwner != "host" || !canceled || (parent.reason != "caller_cancelled" && parent.reason != "deadline_exceeded") { //nolint:misspell // Preserve exact SDK wire spelling; host deadline control separately authenticated above.
				return errors.New("harness descendant cancellation has no actual parent cancellation")
			}
		case "deadline_exceeded":
			if terminalCodes[key] != "deadline_exceeded" {
				return errors.New("harness deadline cancellation lacks physical deadline terminal")
			}
		default:
			return errors.New("harness unexpected plugin cancellation reason")
		}
	}
	return nil
}

func (p *interopHungProof) guard(events []map[string]json.RawMessage, exit int) ([]map[string]json.RawMessage, error) {
	fail := func() ([]map[string]json.RawMessage, error) {
		return nil, errors.New("harness hung owned input/exit proof")
	}
	if p == nil || p.Ref != p.expected || p.Ref.Run == "" || p.Ref.Scenario != "hung-callback" || p.Ref.Profile != "expanded-hung" || p.CommandID == 0 || !p.HalfClosed || !p.LocalCompleted || !p.CustodyRetired || exit != 1 {
		return fail()
	}
	counts := map[string]int{}
	entered, finished := 0, 0
	var filtered []map[string]json.RawMessage
	for _, e := range events {
		kind := rawEventString(e, "kind")
		var id uint64
		_ = json.Unmarshal(e["id"], &id)
		if kind == "entered" && id == p.CommandID {
			var deadline bool
			if rawEventString(e, "name") != "hung" || json.Unmarshal(e["deadline"], &deadline) != nil || deadline {
				return fail()
			}
			entered++
		}
		if (kind == "returned" && id == p.CommandID) || kind == "helper_done" {
			return fail()
		}
		if kind == "finished" {
			var effects map[string]int
			if json.Unmarshal(e["effects"], &effects) != nil || effects["entered"] != 2 || effects["load"] != 1 || effects["hung"] != 1 || effects["unload_attempts"] != 0 || effects["returned"] != 0 || effects["commits"] != 0 || effects["health"] != 0 || rawEventString(e, "transport_error") == "" {
				return fail()
			}
			finished++
		}
		if kind == "wire" {
			typ, dir := rawEventString(e, "frame_type"), rawEventString(e, "direction")
			if typ == "notification" || (typ == "request" && dir == "worker-to-host") {
				return fail()
			}
			if typ == "request" && dir == "host-to-worker" {
				method := rawEventString(e, "method")
				switch method {
				case subprocess.MethodInit, subprocess.MethodLoad:
				case subprocess.MethodCommandExecute:
					var frame struct {
						ID     uint64                     `json:"id"`
						Params map[string]json.RawMessage `json:"params"`
					}
					if id != p.CommandID || json.Unmarshal([]byte(rawEventString(e, "raw")), &frame) != nil || frame.ID != id {
						return fail()
					}
					var name, args, session string
					if json.Unmarshal(frame.Params["name"], &name) != nil || json.Unmarshal(frame.Params["args"], &args) != nil || json.Unmarshal(frame.Params["session_id"], &session) != nil || name != "hung" || args != "{}" || session != "" || len(frame.Params) != 3 {
						return fail()
					}
				default:
					return fail()
				}
				counts[method]++
			}
			if id == p.CommandID && dir == "worker-to-host" && typ == "response" {
				return fail()
			}
			if id == p.CommandID && dir == "host-to-worker" && typ == "request" {
				continue
			}
		}
		filtered = append(filtered, e)
	}
	if entered != 1 || finished != 1 {
		return fail()
	}
	for _, m := range []string{subprocess.MethodInit, subprocess.MethodLoad, subprocess.MethodCommandExecute} {
		if counts[m] != 1 {
			return fail()
		}
	}
	return filtered, nil
}

// Selected child fairness owns every input and no cancellation action. Worker
// stdout order is assigned at socket ingress, before consumers can reorder it.
func (p *interopChildProof) guard(events []map[string]json.RawMessage, exit int) error {
	fail := func() error { return errors.New("harness child fairness ownership/physical receipt") }
	if p == nil || p.Ref != p.expected || p.Ref.Run == "" || p.Ref.Scenario != "child-fairness" || p.Ref.Profile != "expanded" || exit != 0 || !p.EOF || !p.CustodyRetired || p.CommandID == 0 || p.Binding == "" || len(p.Initial) != 13 || len(p.Health) != 21 || len(p.Refills) != 8 || p.Initial[0] != p.BarrierID || p.ArmSequence <= 0 || p.ReleaseSequence <= p.ArmSequence || p.WaitingBytes <= 0 {
		return fail()
	}
	if p.SnapshotOrdinary["ordinary_queued"] != 8 || p.SnapshotControl["control_queued"] != 12 || p.SnapshotControl["reserved_frames"]+12 > 32 || p.SnapshotControl["reserved_bytes"] != p.SnapshotControl["reserved_frames"]*1024 {
		return fail()
	}
	peak := 0
	for _, v := range p.Occupancy {
		if v.Ordinary < 0 || v.Ordinary > 14 || v.Reverse < 0 || v.Reverse > 8 || v.Credits < 0 || v.Credits > 32 {
			return fail()
		}
		peak = max(peak, v.Ordinary)
	}
	if peak != 14 || len(p.Backend) != 8 {
		return fail()
	}
	for _, a := range p.Backend {
		if a.Parent.ID != p.CommandID || a.Parent.RequestOwner != "host" || string(a.BindingID) != p.Binding || a.Grant.GrantID != "g-StorageGet" || a.Method != HostStorageGet || a.Deadline.IsZero() {
			return fail()
		}
	}
	owned := map[uint64]bool{}
	for _, id := range p.Initial {
		if id == 0 || id == p.CommandID || owned[id] {
			return fail()
		}
		owned[id] = true
	}
	triggers := map[uint64]interopChildRefill{}
	for i, r := range p.Refills {
		if p.Intents[i] != i || r.Index != i || r.NewID == 0 || r.NewID == p.CommandID || owned[r.NewID] || !owned[r.TriggerID] || triggers[r.TriggerSequence].NewID != 0 || r.Credit != p.Health[r.TriggerID] || r.Credit.ID != r.TriggerID {
			return fail()
		}
		owned[r.NewID] = true
		triggers[r.TriggerSequence] = r
	}
	for id, credit := range p.Health {
		if !owned[id] || credit.ID != id || !credit.WholeInput || !credit.LocalOK || !credit.Released || !credit.CreditRetired || credit.InputBytes <= 0 {
			return fail()
		}
	}
	acks := map[int]int{}
	snapshots := map[string]bool{}
	counts := map[string]int{}
	healthInputs := map[uint64]int{}
	var outputs []map[string]json.RawMessage
	loadEntered, loadReturned, cleanup := 0, 0, 0
	entered, returned, helpers, finished, waiting := 0, 0, map[int]bool{}, 0, 0
	for _, e := range events {
		kind := rawEventString(e, "kind")
		var id uint64
		_ = json.Unmarshal(e["id"], &id)
		switch kind {
		case "control_received":
			var seq int
			if json.Unmarshal(e["seq"], &seq) != nil || seq < 1 || seq > 64 {
				return fail()
			}
			acks[seq]++
		case "snapshot":
			var v map[string]int
			if json.Unmarshal(e["effects"], &v) != nil {
				return fail()
			}
			if fmt.Sprint(v) == fmt.Sprint(p.SnapshotOrdinary) {
				snapshots["ordinary"] = true
			}
			if fmt.Sprint(v) == fmt.Sprint(p.SnapshotControl) {
				snapshots["control"] = true
			}
		case "writer_waiting":
			var n int
			if json.Unmarshal(e["bytes"], &n) != nil || n != p.WaitingBytes {
				return fail()
			}
			waiting++
		case "entered":
			if id == p.Lifecycle[subprocess.MethodLoad] {
				var d bool
				if rawEventString(e, "name") != "load" || json.Unmarshal(e["deadline"], &d) != nil || !d {
					return fail()
				}
				loadEntered++
			}
			if id != p.CommandID && id != p.Lifecycle[subprocess.MethodLoad] {
				return fail()
			}
			if id == p.CommandID {
				var deadline bool
				if rawEventString(e, "name") != "get" || json.Unmarshal(e["deadline"], &deadline) != nil || !deadline {
					return fail()
				}
				entered++
			}
		case "returned":
			if id == p.Lifecycle[subprocess.MethodLoad] {
				loadReturned++
			}
			if id != p.CommandID && id != p.Lifecycle[subprocess.MethodLoad] {
				return fail()
			}
			if id == p.CommandID {
				returned++
			}
		case "helper_done":
			var index int
			var result interopHelperOutcome
			if id != p.CommandID || json.Unmarshal(e["index"], &index) != nil || index < 0 || index >= 8 || helpers[index] || json.Unmarshal(e["failure"], &result) != nil || result.Code != "ok" {
				return fail()
			}
			helpers[index] = true
		case "unload_started":
			cleanup++
		case "finished":
			var v map[string]int
			if json.Unmarshal(e["effects"], &v) != nil || v["entered"] != 2 || v["load"] != 1 || v["get"] != 1 || v["returned"] != 1 || v["health"] != 21 || v["unload_attempts"] != 1 || v["commits"] != 0 || rawEventString(e, "transport_error") != "" {
				return fail()
			}
			finished++
		case "wire":
			dir, typ, method := rawEventString(e, "direction"), rawEventString(e, "frame_type"), rawEventString(e, "method")
			if typ == "notification" {
				return fail()
			}
			var frame struct {
				Version string                     `json:"jsonrpc"`
				ID      uint64                     `json:"id"`
				Params  map[string]json.RawMessage `json:"params"`
				Result  json.RawMessage            `json:"result"`
				Error   json.RawMessage            `json:"error"`
			}
			raw := []byte(rawEventString(e, "raw"))
			if dir == "host-to-worker" && typ == "request" && method == subprocess.MethodInit {
				if id != p.Lifecycle[method] {
					return fail()
				}
				counts[method]++
				continue
			}
			var bytes int
			if json.Unmarshal(raw, &frame) != nil || frame.ID != id || frame.Version != "2.0" || json.Unmarshal(e["bytes"], &bytes) != nil || bytes != len(raw) {
				return fail()
			}
			if dir == "host-to-worker" && typ == "response" {
				var v map[string]json.RawMessage
				var found bool
				if len(frame.Error) > 0 || json.Unmarshal(frame.Result, &v) != nil || json.Unmarshal(v["found"], &found) != nil || found || len(v) != 1 {
					return fail()
				}
			}
			if dir == "host-to-worker" && typ == "request" {
				counts[method]++
				switch method {
				case subprocess.MethodInit, subprocess.MethodLoad:
					if id != p.Lifecycle[method] {
						return fail()
					}
				case subprocess.MethodHealth:
					if !owned[id] || string(frame.Params["context"]) != "" || len(frame.Params) != 0 || bytes != p.Health[id].InputBytes {
						return fail()
					}
					healthInputs[id]++
				case subprocess.MethodCommandExecute:
					var name, args string
					var ctx struct {
						Binding string `json:"binding_id"`
						Timeout uint64 `json:"timeout_ms"`
					}
					if id != p.CommandID || json.Unmarshal(frame.Params["name"], &name) != nil || name != "get" || json.Unmarshal(frame.Params["args"], &args) != nil || args != p.Args || json.Unmarshal(frame.Params["context"], &ctx) != nil || ctx.Binding != p.Binding || ctx.Timeout == 0 || ctx.Timeout > 10000 {
						return fail()
					}
					var a struct {
						N   int    `json:"n"`
						Key string `json:"key"`
					}
					if json.Unmarshal([]byte(args), &a) != nil || a.N != 8 || a.Key != "read" {
						return fail()
					}
				default:
					return fail()
				}
			}
			if dir == "worker-to-host" {
				var sequence uint64
				_ = json.Unmarshal(e["child_output_sequence"], &sequence)
				if sequence > 0 {
					outputs = append(outputs, e)
				} else if typ == "request" || id == p.CommandID || owned[id] {
					return fail()
				}
				if typ == "response" && id == p.CommandID {
					var v subprocess.CommandExecResult
					if len(frame.Error) > 0 || json.Unmarshal(frame.Result, &v) != nil {
						return fail()
					}
					var outcomes []interopHelperOutcome
					if json.Unmarshal([]byte(v.Content), &outcomes) != nil || len(outcomes) != 8 {
						return fail()
					}
					for _, v := range outcomes {
						if v.Code != "ok" {
							return fail()
						}
					}
				}
				if typ == "response" && owned[id] {
					var r struct {
						OK bool `json:"ok"`
					}
					if len(frame.Error) > 0 || json.Unmarshal(frame.Result, &r) != nil || !r.OK {
						return fail()
					}
				}
			}
		}
	}
	if loadEntered != 1 || loadReturned != 1 || cleanup != 1 {
		return fail()
	}
	if len(p.ControlTrace) < 5 || len(p.ControlTrace) > 64 || len(acks) != len(p.ControlTrace) || !snapshots["ordinary"] || !snapshots["control"] {
		return fail()
	}
	for i, line := range p.ControlTrace {
		parts := strings.Split(line, "/ack:")
		var seq int
		if len(parts) != 2 || json.Unmarshal([]byte(parts[1]), &seq) != nil || seq != i+1 || acks[seq] != 1 {
			return fail()
		}
		if i == 0 && parts[0] != "release:request-2" {
			return fail()
		}
		if p.ArmSequence != 2 || p.ReleaseSequence != len(p.ControlTrace) {
			return fail()
		}
		if seq > p.ArmSequence && seq < p.ReleaseSequence && parts[0] != "release:snapshot" {
			return fail()
		}
		if seq == p.ArmSequence && parts[0] != "release:arm-writer" {
			return fail()
		}
		if seq == p.ReleaseSequence && parts[0] != "release:writer" {
			return fail()
		}
	}
	if entered != 1 || returned != 1 || len(helpers) != 8 || finished != 1 || waiting != 1 || counts[subprocess.MethodInit] != 1 || counts[subprocess.MethodLoad] != 1 || counts[subprocess.MethodCommandExecute] != 1 || counts[subprocess.MethodHealth] != 21 {
		return fail()
	}
	for id := range owned {
		if healthInputs[id] != 1 {
			return fail()
		}
	}
	sequenceOf := func(e map[string]json.RawMessage) uint64 {
		var n uint64
		_ = json.Unmarshal(e["child_output_sequence"], &n)
		return n
	}
	sort.Slice(outputs, func(i, j int) bool { return sequenceOf(outputs[i]) < sequenceOf(outputs[j]) })
	if len(outputs) != 30 {
		return fail()
	}
	ordinary, burst, command, refills := 0, 0, 0, 0
	completed := map[uint64]bool{}
	for i, e := range outputs {
		seq := sequenceOf(e)
		if seq != uint64(i+1) {
			return fail()
		}
		var id uint64
		_ = json.Unmarshal(e["id"], &id)
		switch rawEventString(e, "frame_type") {
		case "request":
			var v struct {
				Params struct {
					Context struct {
						Timeout uint64 `json:"timeout_ms"`
					} `json:"context"`
				} `json:"params"`
			}
			if json.Unmarshal([]byte(rawEventString(e, "raw")), &v) != nil || v.Params.Context.Timeout == 0 || v.Params.Context.Timeout > 10000 {
				return fail()
			}
			if rawEventString(e, "method") != "host/storage/get" {
				return fail()
			}
			ordinary++
			burst = 0
		case "response":
			if id == p.CommandID {
				command++
				continue
			}
			if !owned[id] || completed[id] {
				return fail()
			}
			if id == p.BarrierID {
				var n int
				_ = json.Unmarshal(e["bytes"], &n)
				if n != p.WaitingBytes || seq != 1 {
					return fail()
				}
			}
			completed[id] = true
			burst++
			if ordinary < 8 && burst > 4 {
				return fail()
			}
			if refills < 8 {
				r, ok := triggers[seq]
				if !ok || r.TriggerID != id || r.Index != refills {
					return fail()
				}
				refills++
			}
		default:
			return fail()
		}
	}
	if ordinary != 8 || command != 1 || len(completed) != 21 || refills != 8 {
		return fail()
	}
	return interopExpandedHelpers(events, 8)
}

// A selected disconnect owns complete actual inputs and no control action.
// Its one missing terminal is an admitted read, not a global EOF waiver.
func (p *interopDisconnectProof) guard(events []map[string]json.RawMessage, exit int) ([]map[string]json.RawMessage, error) {
	fail := func() ([]map[string]json.RawMessage, error) {
		return nil, errors.New("harness disconnect physical ownership/cause/custody")
	}
	if p == nil || p.Ref != p.expected || p.Ref.Run == "" || p.Ref.Scenario != "disconnect" || p.Ref.Profile != "expanded" || exit != 0 || p.CommandID == 0 || p.ReverseID == 0 || p.Binding == "" || p.WriterOverflow || !p.CustodyRetired || !p.ParentRetired || !p.SessionFenced || p.BackendEntries != 1 || p.BackendReturns != 1 || p.BeforePermit != 1 || p.AfterPermit != 0 || p.AfterPool != 0 || p.BackendCause != "context canceled" || p.BackendKey != "read" || (!p.CancellationParentRetired && !p.CancellationSessionFenced) || p.CommitRefusal == "" || p.BackendEnteredAt.IsZero() || !p.HalfcloseAt.After(p.BackendEnteredAt) || !p.BackendReturnedAt.After(p.HalfcloseAt) {
		return fail()
	}
	a := p.Authority
	if a.Parent.ID != p.CommandID || a.Parent.RequestOwner != "host" || string(a.BindingID) != p.Binding || a.Grant.GrantID != "g-StorageGet" || a.Method != HostStorageGet || !a.Deadline.After(p.BackendReturnedAt) {
		return fail()
	}
	if p.PublicDelivery != "SUCCEEDED" && (p.PublicDelivery != "FAILED" || p.PublicError == "" || !p.PublicTransportCause || p.ConnError == "") {
		return fail()
	}
	inputs := map[uint64]interopDisconnectWrite{}
	replyAttempts := 0
	for _, w := range p.Writes {
		fields, err := strictjson.ObjectFields([]byte(w.Raw), []string{"jsonrpc", "id"}, []string{"method", "params", "result", "error"})
		if err != nil || string(fields["jsonrpc"]) != `"2.0"` {
			return fail()
		}
		var id uint64
		var method string
		if json.Unmarshal(fields["id"], &id) != nil || id == 0 {
			return fail()
		}
		_ = json.Unmarshal(fields["method"], &method)
		if method == "" {
			if id != p.ReverseID || w.Bytes != 0 || w.Error == "" || !w.ClosedPipe || w.At.Before(p.HalfcloseAt) {
				return fail()
			}
			replyAttempts++
			continue
		}
		if _, err := strictjson.Object([]byte(w.Raw), "jsonrpc", "id", "method", "params"); err != nil {
			return fail()
		}
		if inputs[id].Raw != "" || w.Bytes != len(w.Raw) || w.Error != "" || w.At.After(p.HalfcloseAt) {
			return fail()
		}
		if method != subprocess.MethodInit && method != subprocess.MethodLoad && method != subprocess.MethodCommandExecute {
			return fail()
		}
		inputs[id] = w
	}
	if (replyAttempts == 0 && p.ReplyDisposition != "NO_ATTEMPT_FENCED") || (replyAttempts > 0 && p.ReplyDisposition != "ZERO_BYTE_NATIVE_FAILURE") || len(inputs) != 3 || len(p.Writes) != 3+replyAttempts {
		return fail()
	}
	// No attempt is different from a proved zero-byte closed-pipe attempt. Either
	// must be reconciled with actual closed/fenced once-only custody above.
	if p.PublicDelivery == "FAILED" && replyAttempts == 0 && p.ConnError == "" {
		return fail()
	}
	counts := map[string]int{}
	loadEntered, loadReturned, getEntered, getReturned, helpers, cleanup, finished, loadCompletedAbort, ready, initClient, ack := 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
	inputSeen := map[uint64]bool{}
	outputSequences := map[uint64]string{}
	var filtered []map[string]json.RawMessage
	for _, e := range events {
		kind := rawEventString(e, "kind")
		var id uint64
		_ = json.Unmarshal(e["id"], &id)
		switch kind {
		case "ready":
			ready++
		case "init_client":
			var client bool
			if json.Unmarshal(e["client"], &client) != nil || !client {
				return fail()
			}
			initClient++
		case "control_received":
			var seq int
			if json.Unmarshal(e["seq"], &seq) != nil || seq != 1 {
				return fail()
			}
			ack++
		case "worker_exit":
		case "entered":
			var d bool
			if json.Unmarshal(e["deadline"], &d) != nil || !d {
				return fail()
			}
			if id == p.Lifecycle[subprocess.MethodLoad] && rawEventString(e, "name") == "load" {
				loadEntered++
			} else if id == p.CommandID && rawEventString(e, "name") == "get" {
				getEntered++
			} else {
				return fail()
			}
		case "returned":
			switch id {
			case p.Lifecycle[subprocess.MethodLoad]:
				loadReturned++
			case p.CommandID:
				getReturned++
			default:
				return fail()
			}
		case "helper_done":
			var index int
			var result interopHelperOutcome
			if id != p.CommandID || json.Unmarshal(e["index"], &index) != nil || index != 0 || json.Unmarshal(e["failure"], &result) != nil || result.Code != "target_unavailable" || result.EffectState != "unknown" {
				return fail()
			}
			helpers++
		case "unload_started":
			cleanup++
		case "finished":
			var v map[string]int
			if json.Unmarshal(e["effects"], &v) != nil || v["entered"] != 2 || v["load"] != 1 || v["get"] != 1 || v["returned"] != 1 || v["unload_attempts"] != 1 || v["health"] != 0 || v["commits"] != 0 || v["reverse_pending"] != 0 || v["ordinary_queued"] != 0 || v["control_queued"] != 0 || v["reserved_frames"] != 0 || v["reserved_bytes"] != 0 || rawEventString(e, "transport_error") != "" {
				return fail()
			}
			finished++
		case "aborted":
			var deadline bool
			if id != p.Lifecycle[subprocess.MethodLoad] || json.Unmarshal(e["deadline"], &deadline) != nil || deadline {
				return fail()
			}
			loadCompletedAbort++
		case "control_failure", "writer_waiting", "snapshot":
			return fail()
		case "wire":
			dir, typ, method := rawEventString(e, "direction"), rawEventString(e, "frame_type"), rawEventString(e, "method")
			if typ == "notification" || (dir != "host-to-worker" && dir != "worker-to-host") {
				return fail()
			}
			var n int
			if json.Unmarshal(e["bytes"], &n) != nil || n <= 0 {
				return fail()
			}
			if dir == "host-to-worker" {
				if typ != "request" || inputSeen[id] || inputs[id].Raw == "" {
					return fail()
				}
				w := inputs[id]
				hash := fmt.Sprintf("%x", sha256.Sum256([]byte(w.Raw)))
				if n != len(w.Raw) || rawEventString(e, "sha256") != hash {
					return fail()
				}
				var frame struct {
					Method string          `json:"method"`
					Params json.RawMessage `json:"params"`
				}
				if json.Unmarshal([]byte(w.Raw), &frame) != nil || frame.Method != method {
					return fail()
				}
				switch method {
				case subprocess.MethodInit, subprocess.MethodLoad:
					if id != p.Lifecycle[method] || id == p.CommandID {
						return fail()
					}
				case subprocess.MethodCommandExecute:
					fields, err := strictjson.Object(frame.Params, "name", "args", "session_id", "context")
					if err != nil || id != p.CommandID || string(fields["name"]) != `"get"` || string(fields["session_id"]) != `""` {
						return fail()
					}
					var args string
					var ctx struct {
						Binding string `json:"binding_id"`
						Timeout uint32 `json:"timeout_ms"`
					}
					if json.Unmarshal(fields["args"], &args) != nil || json.Unmarshal(fields["context"], &ctx) != nil || ctx.Binding != p.Binding || ctx.Timeout == 0 || ctx.Timeout > 10000 {
						return fail()
					}
					values, err := strictjson.Object([]byte(args), "n", "key")
					if err != nil || string(values["n"]) != "1" || string(values["key"]) != `"read"` {
						return fail()
					}
				default:
					return fail()
				}
				counts[method]++
				inputSeen[id] = true
			} else {
				raw := []byte(rawEventString(e, "raw"))
				hash := fmt.Sprintf("%x", sha256.Sum256(raw))
				if len(raw) != n || rawEventString(e, "sha256") != hash {
					return fail()
				}
				var seq uint64
				if json.Unmarshal(e["child_output_sequence"], &seq) != nil || seq == 0 || outputSequences[seq] != "" {
					return fail()
				}
				if typ == "request" {
					if method != string(HostStorageGet) || id != p.ReverseID {
						return fail()
					}
					fields, err := strictjson.Object(raw, "jsonrpc", "id", "method", "params")
					if err != nil || string(fields["jsonrpc"]) != `"2.0"` || string(fields["method"]) != `"host/storage/get"` {
						return fail()
					}
					var requestID uint64
					_ = json.Unmarshal(fields["id"], &requestID)
					if requestID != id {
						return fail()
					}
					values, err := strictjson.Object(fields["params"], "grant_id", "key", "context")
					if err != nil || string(values["grant_id"]) != `"g-StorageGet"` || string(values["key"]) != `"read"` {
						return fail()
					}
					var ctx struct {
						Binding string                `json:"binding_id"`
						Timeout uint32                `json:"timeout_ms"`
						Parent  subprocess.ParentCall `json:"parent_call"`
					}
					if json.Unmarshal(values["context"], &ctx) != nil || ctx.Binding != p.Binding || ctx.Timeout == 0 || ctx.Timeout > 10000 || ctx.Parent != a.Parent {
						return fail()
					}
					outputSequences[seq] = "read"
					continue // The sole selected unanswered terminal; original helper audit follows.
				}
				if typ != "response" {
					return fail()
				}
				fields, err := strictjson.Object(raw, "jsonrpc", "id", "result")
				var actualID uint64
				if err != nil || string(fields["jsonrpc"]) != `"2.0"` || json.Unmarshal(fields["id"], &actualID) != nil || actualID != id {
					return fail()
				}
				switch id {
				case p.Lifecycle[subprocess.MethodInit]:
					outputSequences[seq] = "init"
				case p.Lifecycle[subprocess.MethodLoad]:
					outputSequences[seq] = "load"
				case p.CommandID:
					if _, err := strictjson.Object(fields["result"], "action", "content"); err != nil {
						return fail()
					}
					var result subprocess.CommandExecResult
					var outcomes []interopHelperOutcome
					if strictjson.Validate(fields["result"]) != nil || json.Unmarshal(fields["result"], &result) != nil || result.Action != "message" || json.Unmarshal([]byte(result.Content), &outcomes) != nil || len(outcomes) != 1 || outcomes[0].Code != "target_unavailable" || outcomes[0].EffectState != "unknown" || string(fields["result"]) != string(p.PhysicalResult) {
						return fail()
					}
					outputSequences[seq] = "command"
				default:
					return fail()
				}
			}
		default:
			return fail()
		}
		filtered = append(filtered, e)
	}
	if ready != 1 || initClient != 1 || ack != 1 || len(p.ControlTrace) != 1 || p.ControlTrace[0] != "release:request-2/ack:1" || counts[subprocess.MethodInit] != 1 || counts[subprocess.MethodLoad] != 1 || counts[subprocess.MethodCommandExecute] != 1 || len(inputSeen) != 3 || loadEntered != 1 || loadReturned != 1 || getEntered != 1 || getReturned != 1 || helpers != 1 || cleanup != 1 || finished != 1 || loadCompletedAbort > 1 || len(outputSequences) != 4 || outputSequences[1] != "init" || outputSequences[2] != "load" || outputSequences[3] != "read" || outputSequences[4] != "command" {
		return fail()
	}
	return filtered, nil
}

// Validate physical counters before reduction: omitted cumulative zeros are
// genuine worker output, while explicit nulls and unknown effects are not zero.
func lifecyclePhysicalCounters(raw json.RawMessage, holds, gets int, cleaned bool, previous map[string]int) (map[string]int, error) {
	if err := strictjson.Validate(raw); err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, errors.New("invalid physical counter object")
	}
	for _, key := range []string{"entered", "load", "reverse_pending", "ordinary_queued", "control_queued", "reserved_frames", "reserved_bytes"} {
		if _, ok := fields[key]; !ok {
			return nil, fmt.Errorf("missing physical counter %s", key)
		}
	}
	limits := map[string]uint64{"entered": 19, "load": 3, "hold": uint64(holds), "get": uint64(gets), "returned": 16, "unload_attempts": 1, "reverse_pending": uint64(gets * 8), "ordinary_queued": 21, "control_queued": 21, "reserved_frames": 18, "reserved_bytes": 18432, "commits": 0, "health": 0, "put": 0}
	counters := make(map[string]int, len(fields))
	for key, value := range fields {
		var n uint64
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) || json.Unmarshal(value, &n) != nil || n > 9007199254740991 {
			return nil, fmt.Errorf("invalid physical counter %s", key)
		}
		// An unauthored counter may only report zero; it cannot hide an effect.
		if n > limits[key] {
			return nil, fmt.Errorf("physical counter exceeds domain %s", key)
		}
		counters[key] = int(n) // Domain bounds above fit every supported Go int.
	}
	for _, key := range []string{"entered", "load", "hold", "get", "returned"} {
		if counters[key] < previous[key] {
			return nil, fmt.Errorf("decreasing physical counter %s", key)
		}
	}
	cleanup := 0
	if cleaned {
		cleanup = 1
	}
	if counters["entered"] != counters["load"]+counters["hold"]+counters["get"] || counters["returned"] > counters["hold"]+counters["get"] || counters["unload_attempts"] != cleanup || counters["reserved_bytes"] != counters["reserved_frames"]*1024 {
		return nil, errors.New("inconsistent physical counter ledger")
	}
	return counters, nil
}
