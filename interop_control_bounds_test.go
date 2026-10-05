//go:build unix

package pluginhost

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestInteropObserverBoundsAndTerminalEventBeforeEOF(t *testing.T) {
	w := &interopEventWriter{events: make(chan []byte, 32), diagnostics: io.Discard}
	if _, err := w.Write(bytes.Repeat([]byte("x"), interopEventLimit+1)); err == nil {
		t.Fatal("unbounded event line accepted")
	}
	w = &interopEventWriter{events: make(chan []byte, 32), diagnostics: io.Discard}
	raw := append(append([]byte(`{"fixture_event":{"kind":"padding","padding":"`), bytes.Repeat([]byte("x"), 3900)...), []byte("\"}}\n")...)
	for i := 0; i < 16; i++ {
		if _, err := w.Write(raw); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Write(raw); err == nil {
		t.Fatal("event byte budget ignored")
	}
	c := &interopControls{events: make(chan map[string]json.RawMessage, 1), failure: make(chan error, 1)}
	if err := c.enqueue(map[string]json.RawMessage{"kind": json.RawMessage(`"finished"`)}); err != nil {
		t.Fatal(err)
	}
	c.failure <- io.EOF
	if _, err := c.event("finished"); err != nil {
		t.Fatal("buffered final event lost to EOF", err)
	}
	if _, err := c.event("missing"); !errors.Is(err, io.EOF) {
		t.Fatal("required missing event did not fail", err)
	}
}

func TestInteropTerminalOwnershipRejectsLateFailureAndIncompleteExit(t *testing.T) {
	for _, scenario := range []string{"late-control-failure", "missing-worker-exit", "wrong-worker-exit", "duplicate-finished"} {
		t.Run(scenario, func(t *testing.T) {
			c := &interopControls{events: make(chan map[string]json.RawMessage, 8), failure: make(chan error, 1)}
			events := []map[string]json.RawMessage{{"kind": json.RawMessage(`"finished"`)}}
			switch scenario {
			case "late-control-failure":
				events = append(events, map[string]json.RawMessage{"kind": json.RawMessage(`"control_failure"`)}, map[string]json.RawMessage{"kind": json.RawMessage(`"worker_exit"`), "exit_code": json.RawMessage(`0`)})
			case "wrong-worker-exit":
				events = append(events, map[string]json.RawMessage{"kind": json.RawMessage(`"worker_exit"`), "exit_code": json.RawMessage(`1`)})
			case "duplicate-finished":
				events = append(events, events[0], map[string]json.RawMessage{"kind": json.RawMessage(`"worker_exit"`), "exit_code": json.RawMessage(`0`)})
			}
			for _, e := range events {
				if err := c.enqueue(e); err != nil {
					t.Fatal(err)
				}
			}
			c.failure <- io.EOF
			if err := c.finish(0); err == nil {
				t.Fatal("incomplete or failed observer accepted")
			}
		})
	}
}

func TestInteropWireForwardsBytesAndRefusesPartialEOF(t *testing.T) {
	var forwarded bytes.Buffer
	events := make(chan []byte, 4)
	observer := &interopEventWriter{events: events, diagnostics: io.Discard}
	w := &interopWireWriter{output: &forwarded, observer: observer}
	raw := []byte("{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{}}\n")
	for _, chunk := range [][]byte{raw[:9], raw[9:]} {
		if _, err := w.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(forwarded.Bytes(), raw) || w.finish() != nil {
		t.Fatal("observer changed actual wire")
	}
	var witness struct {
		Event struct {
			Raw       string `json:"raw"`
			Bytes     int    `json:"bytes"`
			Direction string `json:"direction"`
		} `json:"fixture_event"`
	}
	if err := json.Unmarshal(<-events, &witness); err != nil {
		t.Fatal(err)
	}
	if witness.Event.Raw != string(raw) || witness.Event.Bytes != len(raw) || witness.Event.Direction != "worker-to-host" {
		t.Fatal("physical witness differs", witness)
	}
	if _, err := w.Write([]byte(`{"id":8`)); err != nil {
		t.Fatal(err)
	}
	if err := w.finish(); err == nil {
		t.Fatal("partial wire at EOF accepted")
	}
}

func TestInteropControlRequiresMatchingAcknowledgment(t *testing.T) {
	parent, child := net.Pipe()
	defer parent.Close()
	defer child.Close()
	c := &interopControls{socket: parent, events: make(chan map[string]json.RawMessage, 2), failure: make(chan error, 1)}
	done := make(chan error, 1)
	go func() {
		_, err := bufio.NewReader(child).ReadString('\n')
		if err == nil {
			err = c.enqueue(map[string]json.RawMessage{"kind": json.RawMessage(`"control_received"`), "seq": json.RawMessage(`2`)})
		}
		done <- err
	}()
	if err := c.release("snapshot"); err == nil || !strings.Contains(err.Error(), "control seq got2 want1") {
		t.Fatal("mismatched required acknowledgment accepted", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestInteropPartialRequiredEventAtEOFIsHarnessFailure(t *testing.T) {
	w := &interopEventWriter{events: make(chan []byte, 1), diagnostics: io.Discard}
	if _, err := w.Write([]byte(`{"fixture_event":{"kind":"finished"`)); err != nil {
		t.Fatal(err)
	}
	if err := w.failure(); err == nil {
		t.Fatal("partial required terminal event accepted")
	}
}

func TestInteropTerminalOwnershipRequiresPublishedRequest(t *testing.T) {
	wire := func(direction, kind string, id int) map[string]json.RawMessage {
		raw, err := json.Marshal(map[string]any{"kind": "wire", "direction": direction, "frame_type": kind, "id": id})
		if err != nil {
			t.Fatal(err)
		}
		var event map[string]json.RawMessage
		if err := json.Unmarshal(raw, &event); err != nil {
			t.Fatal(err)
		}
		return event
	}
	request := wire("host-to-worker", "request", 7)
	reply := wire("worker-to-host", "response", 7)
	for _, scenario := range []string{"paired", "orphan", "duplicate", "missing", "opposite-direction-same-id"} {
		t.Run(scenario, func(t *testing.T) {
			c := &interopControls{events: make(chan map[string]json.RawMessage, 8), failure: make(chan error, 1)}
			events := []map[string]json.RawMessage{request, reply}
			valid := false
			switch scenario {
			case "paired":
				valid = true
			case "orphan":
				events = append(events, wire("worker-to-host", "response", 99999))
			case "duplicate":
				events = append(events, reply)
			case "missing":
				events = events[:1]
			case "opposite-direction-same-id":
				valid = true
				events = append(events, wire("worker-to-host", "request", 7), wire("host-to-worker", "response", 7))
			}
			events = append(events, map[string]json.RawMessage{"kind": json.RawMessage(`"finished"`)}, map[string]json.RawMessage{"kind": json.RawMessage(`"worker_exit"`), "exit_code": json.RawMessage(`0`)})
			for _, e := range events {
				if err := c.enqueue(e); err != nil {
					t.Fatal(err)
				}
			}
			c.failure <- io.EOF
			if err := c.finish(0); (err == nil) != valid {
				t.Fatalf("terminal ownership valid=%v: %v", valid, err)
			}
		})
	}
}

func TestInteropExpandedTrafficRequiresCommandedHelper(t *testing.T) {
	frame := func(direction, kind, method string, id int, params any) map[string]json.RawMessage {
		raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
		if err != nil {
			t.Fatal(err)
		}
		event, err := json.Marshal(map[string]any{"kind": "wire", "direction": direction, "frame_type": kind, "method": method, "id": id, "raw": string(raw)})
		if err != nil {
			t.Fatal(err)
		}
		var e map[string]json.RawMessage
		if err := json.Unmarshal(event, &e); err != nil {
			t.Fatal(err)
		}
		return e
	}
	for _, scenario := range []string{"commanded", "balanced-unadvertised", "wrong-parent", "wrong-binding", "wrong-key", "wrong-operation", "extra-helper", "missing-helper"} {
		t.Run(scenario, func(t *testing.T) {
			c := &interopControls{expandedReverseLimit: 8, events: make(chan map[string]json.RawMessage, 16), failure: make(chan error, 1)}
			command := map[string]any{"name": "put", "args": `{"n":1,"key":"write","operation_key":"stable"}`, "context": map[string]any{"binding_id": "minted-binding"}}
			parent, binding, key, operation := 73, "minted-binding", "write", "stable"
			switch scenario {
			case "wrong-parent":
				parent = 74
			case "wrong-binding":
				binding = "plugin-chosen"
			case "wrong-key":
				key = "other"
			case "wrong-operation":
				operation = "other"
			}
			helper := map[string]any{"grant_id": "g-StoragePut", "key": key, "operation_key": operation, "value": "", "expected_revision": nil, "context": map[string]any{"binding_id": binding, "parent_call": map[string]any{"request_owner": "host", "id": parent}}}
			events := []map[string]json.RawMessage{frame("host-to-worker", "request", "command/execute", 73, command), frame("worker-to-host", "response", "", 73, nil)}
			if scenario != "missing-helper" {
				events = append(events, frame("worker-to-host", "request", "host/storage/put", 73, helper), frame("host-to-worker", "response", "", 73, nil))
			}
			if scenario == "balanced-unadvertised" {
				events = append(events, frame("worker-to-host", "request", "host/unadvertised", 991, nil), frame("host-to-worker", "response", "", 991, nil))
			}
			if scenario == "extra-helper" {
				events = append(events, frame("worker-to-host", "request", "host/storage/put", 992, helper), frame("host-to-worker", "response", "", 992, nil))
			}
			events = append(events, map[string]json.RawMessage{"kind": json.RawMessage(`"finished"`)}, map[string]json.RawMessage{"kind": json.RawMessage(`"worker_exit"`), "exit_code": json.RawMessage(`0`)})
			for _, e := range events {
				if err := c.enqueue(e); err != nil {
					t.Fatal(err)
				}
			}
			c.failure <- io.EOF
			if err := c.finish(0); (err == nil) != (scenario == "commanded") {
				t.Fatalf("scenario %s: %v", scenario, err)
			}
		})
	}
}

func TestInteropNotificationsRequireAuthoredDirectionalCancellation(t *testing.T) {
	frame := func(direction, kind, method string, id int, params any, code string) map[string]json.RawMessage {
		body := map[string]any{"jsonrpc": "2.0", "params": params}
		if kind != "notification" {
			body["id"] = id
		}
		if method != "" {
			body["method"] = method
		}
		if code != "" {
			body["error"] = map[string]any{"data": map[string]any{"code": code}}
		}
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		witness, err := json.Marshal(map[string]any{"kind": "wire", "direction": direction, "frame_type": kind, "method": method, "id": id, "raw": string(raw)})
		if err != nil {
			t.Fatal(err)
		}
		var event map[string]json.RawMessage
		if err := json.Unmarshal(witness, &event); err != nil {
			t.Fatal(err)
		}
		return event
	}
	for _, scenario := range []string{"parent-cancel", "reordered", "opposite-direction-same-id", "deadline", "observer-deadline", "observer-without-receipt", "observer-without-cause", "unadvertised", "wrong-owner", "missing-target", "wrong-parent", "missing-parent-cancel", "duplicate", "unexpected-reason", "unproven-deadline", "malformed-envelope", "extra-params", "duplicate-field"} {
		t.Run(scenario, func(t *testing.T) {
			c := &interopControls{events: make(chan map[string]json.RawMessage, 16), failure: make(chan error, 1)}
			parent, child, childParent, owner, reason := 31, 7, 31, "plugin", "parent_cancelled" //nolint:misspell // Preserve exact SDK wire spelling.
			valid := scenario == "parent-cancel" || scenario == "reordered" || scenario == "opposite-direction-same-id" || scenario == "deadline" || scenario == "observer-deadline"
			if scenario == "opposite-direction-same-id" {
				parent = 7
				childParent = 7
			}
			if scenario == "wrong-parent" {
				childParent = 32
			}
			if scenario == "wrong-owner" {
				owner = "host"
			}
			if scenario == "unexpected-reason" {
				reason = "caller_cancelled" //nolint:misspell // Preserve exact SDK wire spelling.
			}
			if scenario == "deadline" || scenario == "unproven-deadline" {
				reason = "deadline_exceeded"
			}
			terminalCode := ""
			if scenario == "deadline" {
				terminalCode = "deadline_exceeded"
			}
			events := []map[string]json.RawMessage{
				frame("host-to-worker", "request", "command/execute", parent, nil, ""), frame("worker-to-host", "response", "", parent, nil, ""),
				frame("worker-to-host", "request", "host/storage/get", child, map[string]any{"context": map[string]any{"parent_call": map[string]any{"request_owner": "host", "id": childParent}}}, ""), frame("host-to-worker", "response", "", child, nil, terminalCode),
			}
			notifications := []map[string]json.RawMessage{}
			if scenario != "missing-parent-cancel" && scenario != "deadline" && scenario != "unproven-deadline" {
				hostReason := "caller_cancelled" //nolint:misspell // Preserve exact SDK wire spelling.
				if strings.HasPrefix(scenario, "observer-") {
					hostReason = "deadline_exceeded"
					c.hostCancelEvidence = map[uint64]interopHostCancelEvidence{uint64(parent): {Reason: hostReason, Published: true, ControlComplete: scenario != "observer-without-receipt", ObserverExpired: scenario != "observer-without-cause"}} //nolint:gosec // Positive small test-owned parent ID.
				}
				notifications = append(notifications, frame("host-to-worker", "notification", "rpc/cancel", 0, map[string]any{"request_owner": "host", "id": parent, "reason": hostReason}, ""))
			}
			if scenario == "missing-target" {
				child = 999
			}
			method := "rpc/cancel"
			if scenario == "unadvertised" {
				method = "host/unadvertised"
			}
			notify := frame("worker-to-host", "notification", method, 0, map[string]any{"request_owner": owner, "id": child, "reason": reason}, "")
			if scenario == "malformed-envelope" || scenario == "extra-params" || scenario == "duplicate-field" {
				raw := rawEventString(notify, "raw")
				if scenario == "duplicate-field" {
					raw = strings.Replace(raw, `"jsonrpc":"2.0"`, `"jsonrpc":"2.0","jsonrpc":"2.0"`, 1)
				} else {
					var body map[string]any
					if err := json.Unmarshal([]byte(raw), &body); err != nil {
						t.Fatal(err)
					}
					if scenario == "malformed-envelope" {
						body["id"] = nil
					} else {
						body["params"].(map[string]any)["extra"] = true
					}
					changed, err := json.Marshal(body)
					if err != nil {
						t.Fatal(err)
					}
					raw = string(changed)
				}
				notify["raw"], _ = json.Marshal(raw)
			}
			notifications = append(notifications, notify)
			if scenario == "duplicate" {
				notifications = append(notifications, notify)
			}
			if scenario == "reordered" {
				events = append(notifications, events...)
			} else {
				events = append(events, notifications...)
			}
			events = append(events, map[string]json.RawMessage{"kind": json.RawMessage(`"finished"`)}, map[string]json.RawMessage{"kind": json.RawMessage(`"worker_exit"`), "exit_code": json.RawMessage(`0`)})
			for _, event := range events {
				if err := c.enqueue(event); err != nil {
					t.Fatal(err)
				}
			}
			c.failure <- io.EOF
			if err := c.finish(0); (err == nil) != valid {
				t.Fatalf("authored notification valid=%v: %v", valid, err)
			}
		})
	}
}

func queueGuardEvent(value any) map[string]json.RawMessage {
	raw, _ := json.Marshal(value)
	var e map[string]json.RawMessage
	_ = json.Unmarshal(raw, &e)
	return e
}
func queueGuardWire(direction, kind string, id uint64, method string, params, result any) map[string]json.RawMessage {
	frame := map[string]any{"jsonrpc": "2.0", "id": id}
	if method != "" {
		frame["method"] = method
		frame["params"] = params
	} else {
		frame["result"] = result
	}
	raw, _ := json.Marshal(frame)
	raw = append(raw, '\n')
	return queueGuardEvent(map[string]any{"kind": "wire", "direction": direction, "frame_type": kind, "id": id, "method": method, "raw": string(raw), "bytes": len(raw)})
}
func queueGuardFixture(frames bool) (*interopQueueProof, []map[string]json.RawMessage) {
	ref := interopQueueRef{Source: "ea8ec0dca862d0c7284cc6a130a4b27fb812ed21", Manifest: "30cb07b87060dc88e9cb6ac5414f7b9cb7bb19c7a97c058e284d96e1e59ff4a9", Recipe: "writer-byte-queue-refuses-before-publication", Scenario: "queue", Profile: "expanded-queue", Runtime: "go", Run: "9bea71ef56ecc44a3a3c7ad65e6d6833", Corpus: 1, Selector: 1}
	p := &interopQueueProof{Ref: ref, expected: ref, HealthID: 19, CommandID: 41, Name: "put", Args: `{"key":"write","n":1,"operation_key":"queued","value_bytes":65536}`, Binding: "actual-test-binding", Grant: "g-StoragePut", ArmSequence: 2, ReleaseSequence: 4, RefusedIndex: 0, Snapshot: map[string]int{"ordinary_queued": 0, "reverse_pending": 0}, Outcomes: []interopHelperOutcome{{Code: "rate_limited", EffectState: "not_started"}}}
	if frames {
		p.Ref.Recipe = "writer-frame-queue-refuses-before-publication"
		p.Ref.Scenario = "queue-frames"
		p.Ref.Profile = "expanded-queue-frames"
		p.expected = p.Ref
		p.Name = "get"
		p.Args = `{"key":"read","n":4}`
		p.Grant = "g-StorageGet"
		p.Snapshot = map[string]int{"ordinary_queued": 3, "reserved_frames": 1}
		p.Outcomes = append(p.Outcomes, interopHelperOutcome{Code: "ok", EffectState: "committed"}, interopHelperOutcome{Code: "ok", EffectState: "committed"}, interopHelperOutcome{Code: "ok", EffectState: "committed"})
		p.BackendCalls = 3
	}
	p.ControlTrace = []string{"release:arm-writer/ack:2", "release:snapshot/ack:3", "release:writer/ack:4"}
	health := queueGuardWire("worker-to-host", "response", p.HealthID, "", nil, map[string]any{"ok": true})
	_ = json.Unmarshal(health["bytes"], &p.WaitingBytes)
	events := []map[string]json.RawMessage{
		queueGuardEvent(map[string]any{"kind": "control_received", "seq": 2}),
		queueGuardWire("host-to-worker", "request", p.HealthID, "plugin/health", map[string]any{}, nil),
		queueGuardEvent(map[string]any{"kind": "writer_waiting", "bytes": p.WaitingBytes}),
		queueGuardWire("host-to-worker", "request", p.CommandID, "command/execute", map[string]any{"name": p.Name, "args": p.Args, "context": map[string]any{"timeout_ms": 5000, "binding_id": p.Binding}}, nil),
		queueGuardEvent(map[string]any{"kind": "entered", "id": p.CommandID, "name": p.Name, "deadline": true}),
		queueGuardEvent(map[string]any{"kind": "helper_done", "id": p.CommandID, "index": 0, "failure": p.Outcomes[0]}),
		queueGuardEvent(map[string]any{"kind": "snapshot", "effects": p.Snapshot}),
		queueGuardEvent(map[string]any{"kind": "control_received", "seq": 4}), health,
	}
	if frames {
		for i := 1; i <= 3; i++ {
			id := uint64(40 + i)
			events = append(events, queueGuardWire("worker-to-host", "request", id, "host/storage/get", map[string]any{"grant_id": p.Grant, "key": "read", "context": map[string]any{"timeout_ms": 4000, "binding_id": p.Binding, "parent_call": map[string]any{"request_owner": "host", "id": p.CommandID}}}, nil), queueGuardWire("host-to-worker", "response", id, "", nil, map[string]any{"found": false}), queueGuardEvent(map[string]any{"kind": "helper_done", "id": p.CommandID, "index": i, "failure": p.Outcomes[i]}))
			p.Authorities = append(p.Authorities, HostAuthority{Parent: subprocess.ParentCall{RequestOwner: subprocess.HostRPCOwnerHost, ID: p.CommandID}, BindingID: subprocess.BindingID(p.Binding), Grant: capability.Grant{GrantID: p.Grant}, Method: HostStorageGet})
		}
	}
	content, _ := json.Marshal(p.Outcomes)
	events = append(events, queueGuardWire("worker-to-host", "response", p.CommandID, "", nil, map[string]any{"content": string(content)}))
	for i, method := range []string{subprocess.MethodInit, subprocess.MethodLoad, subprocess.MethodUnload} {
		id := uint64(900 + i)
		events = append(events, queueGuardWire("host-to-worker", "request", id, method, map[string]any{}, nil), queueGuardWire("worker-to-host", "response", id, "", nil, map[string]any{}))
	}
	return p, events
}
func TestInteropQueueScopedProof(t *testing.T) {
	for _, frames := range []bool{false, true} {
		name := "bytes"
		if frames {
			name = "frames"
		}
		t.Run(name, func(t *testing.T) {
			p, events := queueGuardFixture(frames)
			if err := interopWireTerminals(events, 0); err != nil {
				t.Fatal(err)
			}
			if err := interopExpandedHelpers(events, 8, p); err != nil {
				t.Fatal(err)
			}
			if err := interopExpandedHelpers(events, 8); err == nil {
				t.Fatal("missing local proof accepted")
			}
			if err := interopExpandedHelpers(events, 8, p, p); err == nil {
				t.Fatal("duplicate proof accepted")
			}
			mutations := map[string]func(*interopQueueProof, []map[string]json.RawMessage){
				"foreign-runtime":        func(p *interopQueueProof, _ []map[string]json.RawMessage) { p.Ref.Runtime = "node" },
				"foreign-run":            func(p *interopQueueProof, _ []map[string]json.RawMessage) { p.Ref.Run = "another-run" },
				"foreign-source":         func(p *interopQueueProof, _ []map[string]json.RawMessage) { p.Ref.Source = "unreviewed" },
				"foreign-recipe":         func(p *interopQueueProof, _ []map[string]json.RawMessage) { p.Ref.Recipe = "another-case" },
				"wrong-profile":          func(p *interopQueueProof, _ []map[string]json.RawMessage) { p.Ref.Profile = "expanded" },
				"wrong-binding":          func(p *interopQueueProof, _ []map[string]json.RawMessage) { p.Binding = "foreign" },
				"wrong-grant":            func(p *interopQueueProof, _ []map[string]json.RawMessage) { p.Grant = "g-StorageDelete" },
				"wrong-command":          func(p *interopQueueProof, _ []map[string]json.RawMessage) { p.CommandID++ },
				"helper-index":           func(p *interopQueueProof, _ []map[string]json.RawMessage) { p.RefusedIndex++ },
				"mutated-args":           func(p *interopQueueProof, _ []map[string]json.RawMessage) { p.Args = `{"n":9}` },
				"backend-effect":         func(p *interopQueueProof, _ []map[string]json.RawMessage) { p.Commits = 1 },
				"backend-count":          func(p *interopQueueProof, _ []map[string]json.RawMessage) { p.BackendCalls++ },
				"spoof-snapshot":         func(p *interopQueueProof, _ []map[string]json.RawMessage) { p.Snapshot["ordinary_queued"] = 9 },
				"wrong-byte-proof":       func(p *interopQueueProof, _ []map[string]json.RawMessage) { p.WaitingBytes++ },
				"late-refusal":           func(_ *interopQueueProof, e []map[string]json.RawMessage) { e[5], e[7] = e[7], e[5] },
				"duplicate-helper-index": func(_ *interopQueueProof, e []map[string]json.RawMessage) { e[6] = e[5] },
				"missing-observed-snapshot": func(_ *interopQueueProof, e []map[string]json.RawMessage) {
					e[6] = queueGuardEvent(map[string]any{"kind": "padding"})
				},
				"missing-refusal": func(_ *interopQueueProof, e []map[string]json.RawMessage) {
					e[5] = queueGuardEvent(map[string]any{"kind": "padding"})
				},
				"wrong-effect-state": func(p *interopQueueProof, _ []map[string]json.RawMessage) { p.Outcomes[0].EffectState = "unknown" },
				"notification-leftover": func(_ *interopQueueProof, e []map[string]json.RawMessage) {
					e[6] = queueGuardEvent(map[string]any{"kind": "wire", "direction": "worker-to-host", "frame_type": "notification", "method": "host/unadvertised", "raw": `{"jsonrpc":"2.0","method":"host/unadvertised","params":{}}`})
				},
			}
			for name, mutate := range mutations {
				t.Run(name, func(t *testing.T) {
					mutated, e := queueGuardFixture(frames)
					mutate(mutated, e)
					if interopExpandedHelpers(e, 8, mutated) == nil && interopWireTerminals(e, 0) == nil {
						t.Fatal("unowned/inconsistent queue evidence accepted")
					}
				})
			}
			// A refused mutation appearing on wire is forbidden even with a matching
			// terminal and zero backend effects: balance alone does not prove local refusal.
			extra := append([]map[string]json.RawMessage{}, events...)
			extra = append(extra, queueGuardWire("worker-to-host", "request", 99, "host/storage/put", map[string]any{"grant_id": p.Grant, "key": "write", "operation_key": "queued", "value": "v", "expected_revision": nil, "context": map[string]any{"binding_id": p.Binding, "parent_call": map[string]any{"request_owner": "host", "id": p.CommandID}}}, nil), queueGuardWire("host-to-worker", "response", 99, "", nil, map[string]any{}))
			if interopExpandedHelpers(extra, 8, p) == nil {
				t.Fatal("published local-refused mutation accepted")
			}
		})
	}
}

func TestInteropQueueRejectsUnownedInputsAtEOF(t *testing.T) {
	for _, frames := range []bool{false, true} {
		proof, events := queueGuardFixture(frames)
		finish := func(observed []map[string]json.RawMessage) error {
			c := &interopControls{queueProof: proof, expandedReverseLimit: 8, observed: observed, events: make(chan map[string]json.RawMessage), failure: make(chan error, 1)}
			c.failure <- io.EOF
			// Own worker exit/finished evidence accompanies the complete physical trace.
			c.observed = append(c.observed, queueGuardEvent(map[string]any{"kind": "finished"}), queueGuardEvent(map[string]any{"kind": "worker_exit", "exit_code": 0}))
			return c.finish(0)
		}
		if err := finish(events); err != nil {
			t.Fatal("canonical", err)
		}
		cancel := map[string]any{"jsonrpc": "2.0", "method": "rpc/cancel", "params": map[string]any{"request_owner": "host", "id": proof.HealthID, "reason": "caller_cancelled"}} //nolint:misspell // Exact protocol reason.
		raw, _ := json.Marshal(cancel)
		raw = append(raw, '\n')
		notification := queueGuardEvent(map[string]any{"kind": "wire", "direction": "host-to-worker", "frame_type": "notification", "method": "rpc/cancel", "raw": string(raw), "bytes": len(raw)})
		if finish(append(append([]map[string]json.RawMessage{}, events...), notification)) == nil {
			t.Fatal("completed published Health target authorized an unowned cancellation")
		}
		paired := append(append([]map[string]json.RawMessage{}, events...), queueGuardWire("host-to-worker", "request", proof.CommandID+100, "plugin/health", map[string]any{}, nil), queueGuardWire("worker-to-host", "response", proof.CommandID+100, "", nil, map[string]any{"ok": true}))
		if finish(paired) == nil {
			t.Fatal("balanced extra Health pair authorized an unowned input")
		}
	}
}

func TestInteropHungRequiresOwnedEOFAndNaturalExit(t *testing.T) {
	ref := interopQueueRef{Source: "ea8ec0dca862d0c7284cc6a130a4b27fb812ed21", Manifest: "30cb07b87060dc88e9cb6ac5414f7b9cb7bb19c7a97c058e284d96e1e59ff4a9", Recipe: "hung-callback", Scenario: "hung-callback", Profile: "expanded-hung", Runtime: "go", Run: "owned-run", Corpus: 1, Selector: 1}
	makeProof := func() *interopHungProof {
		return &interopHungProof{Ref: ref, expected: ref, CommandID: 73, HalfClosed: true, LocalCompleted: true, CustodyRetired: true}
	}
	canonical := []map[string]json.RawMessage{
		queueGuardWire("host-to-worker", "request", 61, "plugin/init", map[string]any{}, nil), queueGuardWire("worker-to-host", "response", 61, "", nil, map[string]any{}),
		queueGuardWire("host-to-worker", "request", 62, "plugin/load", map[string]any{}, nil), queueGuardWire("worker-to-host", "response", 62, "", nil, map[string]any{}),
		queueGuardWire("host-to-worker", "request", 73, "command/execute", map[string]any{"name": "hung", "args": "{}", "session_id": ""}, nil),
		queueGuardEvent(map[string]any{"kind": "entered", "id": 73, "name": "hung", "deadline": false}),
		queueGuardEvent(map[string]any{"kind": "finished", "effects": map[string]int{"entered": 2, "load": 1, "hung": 1}, "transport_error": "shutdown drain failed"}),
		queueGuardEvent(map[string]any{"kind": "worker_exit", "exit_code": 1}),
	}
	for _, name := range []string{"canonical", "missing-load", "extra-pair", "notification", "fabricated-terminal", "returned", "deadline", "foreign-ref", "unretired", "unload", "wrong-exit", "late-error"} {
		t.Run(name, func(t *testing.T) {
			p := makeProof()
			e := append([]map[string]json.RawMessage{}, canonical...)
			failure := io.EOF
			switch name {
			case "missing-load":
				e = append(e[:3], e[4:]...)
			case "extra-pair":
				e = append(e, queueGuardWire("host-to-worker", "request", 99, "plugin/health", map[string]any{}, nil), queueGuardWire("worker-to-host", "response", 99, "", nil, map[string]any{"ok": true}))
			case "notification":
				e = append(e, queueGuardEvent(map[string]any{"kind": "wire", "direction": "host-to-worker", "frame_type": "notification", "method": "rpc/cancel"}))
			case "fabricated-terminal":
				e = append(e, queueGuardWire("worker-to-host", "response", 73, "", nil, map[string]any{}))
			case "returned":
				e = append(e, queueGuardEvent(map[string]any{"kind": "returned", "id": 73}))
			case "deadline":
				e[5] = queueGuardEvent(map[string]any{"kind": "entered", "id": 73, "name": "hung", "deadline": true})
			case "foreign-ref":
				p.Ref.Run = "foreign"
			case "unretired":
				p.CustodyRetired = false
			case "unload":
				e[6] = queueGuardEvent(map[string]any{"kind": "finished", "effects": map[string]int{"unload_attempts": 1}, "transport_error": "failed"})
			case "wrong-exit":
				e[7] = queueGuardEvent(map[string]any{"kind": "worker_exit", "exit_code": 0})
			case "late-error":
				failure = errors.Join(io.EOF, errors.New("late control failure"))
			}
			c := &interopControls{hungProof: p, observed: e, events: make(chan map[string]json.RawMessage), failure: make(chan error, 1)}
			c.failure <- failure
			err := c.finish(1)
			if (err == nil) != (name == "canonical") {
				t.Fatalf("owned hung EOF %s: %v", name, err)
			}
		})
	}
}

func childGuardFixture() (*interopChildProof, []map[string]json.RawMessage) {
	ref := interopQueueRef{Source: "ea8ec0dca862d0c7284cc6a130a4b27fb812ed21", Manifest: "30cb07b87060dc88e9cb6ac5414f7b9cb7bb19c7a97c058e284d96e1e59ff4a9", Recipe: "child-writer-four-frame-fairness", Scenario: "child-fairness", Profile: "expanded", Runtime: "go", Run: "owned-semantic-test", Corpus: 1, Selector: 1}
	p := &interopChildProof{Ref: ref, expected: ref, CommandID: 73, BarrierID: 17, Health: map[uint64]interopChildCredit{}, Binding: "host-binding", Args: `{"key":"read","n":8}`, SnapshotOrdinary: map[string]int{"ordinary_queued": 8}, SnapshotControl: map[string]int{"control_queued": 12, "reserved_frames": 9, "reserved_bytes": 9216}, ArmSequence: 2, ReleaseSequence: 5, Occupancy: []interopChildOccupancy{{Ordinary: 14, Credits: 14}, {Reverse: 8, Credits: 8}}, EOF: true, CustodyRetired: true, Lifecycle: map[string]uint64{subprocess.MethodInit: 1, subprocess.MethodLoad: 2}}
	p.ControlTrace = []string{"release:request-2/ack:1", "release:arm-writer/ack:2", "release:snapshot/ack:3", "release:snapshot/ack:4", "release:writer/ack:5"}
	events := []map[string]json.RawMessage{queueGuardWire("host-to-worker", "request", 1, subprocess.MethodInit, map[string]any{}, nil), queueGuardWire("worker-to-host", "response", 1, "", nil, map[string]any{}), queueGuardWire("host-to-worker", "request", 2, subprocess.MethodLoad, map[string]any{}, nil), queueGuardWire("worker-to-host", "response", 2, "", nil, map[string]any{}), queueGuardEvent(map[string]any{"kind": "entered", "id": 2, "name": "load", "deadline": true}), queueGuardEvent(map[string]any{"kind": "returned", "id": 2}), queueGuardWire("host-to-worker", "request", p.CommandID, subprocess.MethodCommandExecute, map[string]any{"name": "get", "args": p.Args, "session_id": "", "context": map[string]any{"timeout_ms": 9000, "binding_id": p.Binding}}, nil), queueGuardEvent(map[string]any{"kind": "entered", "id": p.CommandID, "name": "get", "deadline": true})}
	for i := 1; i <= 5; i++ {
		events = append(events, queueGuardEvent(map[string]any{"kind": "control_received", "seq": i}))
	}
	events = append(events, queueGuardEvent(map[string]any{"kind": "snapshot", "effects": p.SnapshotOrdinary}), queueGuardEvent(map[string]any{"kind": "snapshot", "effects": p.SnapshotControl}))
	var outputs []map[string]json.RawMessage
	for i := 0; i < 21; i++ {
		id := uint64(17 + i)
		if i >= 13 {
			id = uint64(103 + i - 13)
		} else {
			p.Initial = append(p.Initial, id)
		}
		input := queueGuardWire("host-to-worker", "request", id, subprocess.MethodHealth, map[string]any{}, nil)
		var n int
		_ = json.Unmarshal(input["bytes"], &n)
		p.Health[id] = interopChildCredit{id, n, true, true, true, true}
		events = append(events, input)
		output := queueGuardWire("worker-to-host", "response", id, "", nil, map[string]any{"ok": true})
		outputs = append(outputs, output)
		if i == 0 {
			_ = json.Unmarshal(output["bytes"], &p.WaitingBytes)
		}
	}
	events = append(events, queueGuardEvent(map[string]any{"kind": "writer_waiting", "bytes": p.WaitingBytes}))
	// Four controls then one ordinary request, followed by alternating work. The
	// remainder follows all ordinary requests and cannot starve that lane.
	var ordered []map[string]json.RawMessage
	ordered = append(ordered, outputs[:4]...)
	for i := 0; i < 8; i++ {
		id := uint64(201 + i)
		request := queueGuardWire("worker-to-host", "request", id, "host/storage/get", map[string]any{"grant_id": "g-StorageGet", "key": "read", "context": map[string]any{"timeout_ms": 4000, "binding_id": p.Binding, "parent_call": map[string]any{"request_owner": "host", "id": p.CommandID}}}, nil)
		ordered = append(ordered, request, outputs[4+i])
		events = append(events, queueGuardWire("host-to-worker", "response", id, "", nil, map[string]any{"found": false}), queueGuardEvent(map[string]any{"kind": "helper_done", "id": p.CommandID, "index": i, "failure": map[string]any{"code": "ok"}}))
		p.Backend = append(p.Backend, HostAuthority{Parent: subprocess.ParentCall{RequestOwner: subprocess.HostRPCOwnerHost, ID: p.CommandID}, BindingID: subprocess.BindingID(p.Binding), Grant: capability.Grant{GrantID: "g-StorageGet"}, Method: HostStorageGet, Deadline: time.Now().Add(time.Minute)})
	}
	ordered = append(ordered, outputs[12:]...)
	var outcomes []interopHelperOutcome
	for i := 0; i < 8; i++ {
		outcomes = append(outcomes, interopHelperOutcome{Code: "ok"})
	}
	content, _ := json.Marshal(outcomes)
	ordered = append(ordered, queueGuardWire("worker-to-host", "response", p.CommandID, "", nil, subprocess.CommandExecResult{Content: string(content)}))
	refill := 0
	for i, e := range ordered {
		e["child_output_sequence"], _ = json.Marshal(i + 1)
		var id uint64
		_ = json.Unmarshal(e["id"], &id)
		if rawEventString(e, "frame_type") == "response" && id != p.CommandID && refill < 8 {
			p.Intents[refill] = refill
			p.Refills = append(p.Refills, interopChildRefill{refill, id, uint64(i + 1), uint64(103 + refill), p.Health[id]})
			refill++
		}
	}
	events = append(events, ordered...)
	events = append(events, queueGuardEvent(map[string]any{"kind": "returned", "id": p.CommandID}), queueGuardEvent(map[string]any{"kind": "unload_started"}), queueGuardEvent(map[string]any{"kind": "finished", "effects": map[string]int{"entered": 2, "load": 1, "get": 1, "returned": 1, "health": 21, "unload_attempts": 1}}), queueGuardEvent(map[string]any{"kind": "worker_exit", "exit_code": 0}))
	return p, events
}

func TestInteropChildFairnessCompleteOwnershipAndRetirement(t *testing.T) {
	proof, events := childGuardFixture()
	if err := proof.guard(events, 0); err != nil {
		t.Fatal("canonical", err)
	}
	// Consumer observation order is irrelevant to physical worker stream order.
	for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
		events[i], events[j] = events[j], events[i]
	}
	c := &interopControls{childProof: proof, observed: events, events: make(chan map[string]json.RawMessage), failure: make(chan error, 1), expandedReverseLimit: 8}
	auditInteropChildCopies(t, c)
	c.failure <- io.EOF
	if err := c.finish(0); err != nil {
		t.Fatal("legal reordered trace", err)
	}
}

// The fixture models semantic ownership, including opposite-direction ID
// collision, without pretending these constructed events are runtime receipts.
func disconnectGuardFixture() (*interopDisconnectProof, []map[string]json.RawMessage) {
	now := time.Now()
	ref := interopQueueRef{Source: "ea8ec0dca862d0c7284cc6a130a4b27fb812ed21", Manifest: "30cb07b87060dc88e9cb6ac5414f7b9cb7bb19c7a97c058e284d96e1e59ff4a9", Recipe: "disconnect-with-pending-host-read", Scenario: "disconnect", Profile: "expanded", Runtime: "go", Run: "test-owned-run", Corpus: 1, Selector: 1}
	p := &interopDisconnectProof{Ref: ref, expected: ref, CommandID: 17, ReverseID: 17, Binding: "actual-bound-read", Lifecycle: map[string]uint64{subprocess.MethodInit: 8, subprocess.MethodLoad: 9}, BackendEntries: 1, BackendReturns: 1, BeforePermit: 1, BackendCause: "context canceled", BackendKey: "read", CommitRefusal: "refused", BackendEnteredAt: now, HalfcloseAt: now.Add(time.Millisecond), BackendReturnedAt: now.Add(2 * time.Millisecond), ParentRetired: true, SessionFenced: true, CustodyRetired: true, CancellationParentRetired: true, PublicDelivery: "SUCCEEDED", ReplyDisposition: "NO_ATTEMPT_FENCED", ControlTrace: []string{"release:request-2/ack:1"}}
	p.Authority = HostAuthority{Method: HostStorageGet, Parent: subprocess.ParentCall{RequestOwner: subprocess.HostRPCOwnerHost, ID: p.CommandID}, BindingID: subprocess.BindingID(p.Binding), Grant: capability.Grant{GrantID: "g-StorageGet"}, Deadline: now.Add(10 * time.Second)}
	content := `[{"code":"target_unavailable","effect_state":"unknown"}]`
	result := map[string]any{"action": "message", "content": content}
	p.PhysicalResult = mustInteropDisconnectJSON(result)
	events := []map[string]json.RawMessage{
		queueGuardEvent(map[string]any{"kind": "ready"}), queueGuardEvent(map[string]any{"kind": "init_client", "client": true}), queueGuardEvent(map[string]any{"kind": "control_received", "seq": 1}),
		queueGuardWire("host-to-worker", "request", 8, subprocess.MethodInit, map[string]any{}, nil),
		queueGuardWire("worker-to-host", "response", 8, "", nil, map[string]any{}),
		queueGuardWire("host-to-worker", "request", 9, subprocess.MethodLoad, map[string]any{}, nil),
		queueGuardEvent(map[string]any{"kind": "entered", "id": 9, "name": "load", "deadline": true}), queueGuardEvent(map[string]any{"kind": "returned", "id": 9}),
		queueGuardWire("worker-to-host", "response", 9, "", nil, map[string]any{}),
		queueGuardWire("host-to-worker", "request", 17, subprocess.MethodCommandExecute, map[string]any{"name": "get", "args": `{"key":"read","n":1}`, "session_id": "", "context": map[string]any{"binding_id": p.Binding, "timeout_ms": 9999}}, nil),
		queueGuardEvent(map[string]any{"kind": "entered", "id": 17, "name": "get", "deadline": true}),
		queueGuardWire("worker-to-host", "request", 17, string(HostStorageGet), map[string]any{"grant_id": "g-StorageGet", "key": "read", "context": map[string]any{"binding_id": p.Binding, "timeout_ms": 9998, "parent_call": p.Authority.Parent}}, nil),
		queueGuardEvent(map[string]any{"kind": "helper_done", "id": 17, "index": 0, "failure": interopHelperOutcome{Code: "target_unavailable", EffectState: "unknown"}}),
		queueGuardWire("worker-to-host", "response", 17, "", nil, result), queueGuardEvent(map[string]any{"kind": "returned", "id": 17}), queueGuardEvent(map[string]any{"kind": "unload_started"}),
		queueGuardEvent(map[string]any{"kind": "finished", "effects": map[string]int{"entered": 2, "load": 1, "get": 1, "returned": 1, "unload_attempts": 1, "reverse_pending": 0}}), queueGuardEvent(map[string]any{"kind": "worker_exit", "exit_code": 0}),
	}
	var sequence uint64
	for _, e := range events {
		if rawEventString(e, "kind") != "wire" {
			continue
		}
		raw := rawEventString(e, "raw")
		e["sha256"], _ = json.Marshal(fmt.Sprintf("%x", sha256.Sum256([]byte(raw))))
		if rawEventString(e, "direction") == "host-to-worker" {
			p.Writes = append(p.Writes, interopDisconnectWrite{Raw: raw, Bytes: len(raw), At: now.Add(-time.Millisecond)})
		} else {
			sequence++
			e["child_output_sequence"], _ = json.Marshal(sequence)
		}
	}
	return p, events
}

func TestInteropDisconnectCompleteOwnership(t *testing.T) {
	variants := []string{"canonical", "reversed-consumption", "completed-load-abort", "failed-public-native", "extra-pair", "unowned-cancel", "missing-physical-result", "wrong-helper", "missing-load", "late-error", "partial-reply", "full-reply-with-error", "unavailable-writer", "unreturned-backend", "permit-held", "credit-held", "false-public-success", "wrong-run", "foreign-parent", "wrong-deadline", "wrong-stream-order", "extra-control", "zero-attempt-unattributed", "joined-public-error", "duplicate-helper", "extra-abort", "unattributed-cause", "swapped-stream", "missing-cleanup"}
	for _, variant := range variants {
		t.Run(variant, func(t *testing.T) {
			p, events := disconnectGuardFixture()
			pure := variant == "canonical" || variant == "reversed-consumption" || variant == "completed-load-abort" || variant == "failed-public-native"
			failure := io.EOF
			reply := interopDisconnectWrite{Raw: string(mustInteropDisconnectJSON(map[string]any{"jsonrpc": "2.0", "id": p.ReverseID, "error": map[string]any{"code": -32010}})) + "\n", At: p.BackendReturnedAt, Error: "file already closed", ClosedPipe: true}
			switch variant {
			case "reversed-consumption":
				for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
					events[i], events[j] = events[j], events[i]
				}
			case "completed-load-abort":
				events = append(events, queueGuardEvent(map[string]any{"kind": "aborted", "id": p.Lifecycle[subprocess.MethodLoad], "deadline": false}))
			case "failed-public-native":
				p.PublicDelivery = "FAILED"
				p.PublicError = "plugin is gone: file already closed"
				p.PublicTransportCause = true
				p.ConnError = p.PublicError
				p.Writes = append(p.Writes, reply)
				p.ReplyDisposition = "ZERO_BYTE_NATIVE_FAILURE"
			case "extra-pair":
				events = append(events, queueGuardWire("host-to-worker", "request", 99, subprocess.MethodHealth, map[string]any{}, nil), queueGuardWire("worker-to-host", "response", 99, "", nil, map[string]any{"ok": true}))
			case "unowned-cancel":
				//nolint:misspell // Exact SDK cancellation reason.
				raw := append(mustInteropDisconnectJSON(map[string]any{"jsonrpc": "2.0", "method": "rpc/cancel", "params": map[string]any{"request_owner": "host", "id": p.CommandID, "reason": "caller_cancelled"}}), '\n')
				events = append(events, queueGuardEvent(map[string]any{"kind": "wire", "direction": "host-to-worker", "frame_type": "notification", "method": "rpc/cancel", "raw": string(raw), "bytes": len(raw)})) //nolint:misspell // Wire reason.
			case "missing-physical-result":
				for i, e := range events {
					if rawEventString(e, "kind") == "wire" && rawEventString(e, "direction") == "worker-to-host" && rawEventString(e, "frame_type") == "response" && string(e["id"]) == "17" {
						events = append(events[:i], events[i+1:]...)
						break
					}
				}
			case "wrong-helper":
				for _, e := range events {
					if rawEventString(e, "kind") == "helper_done" {
						e["index"] = json.RawMessage(`1`)
					}
				}
			case "missing-load":
				for i, e := range events {
					if rawEventString(e, "method") == subprocess.MethodLoad {
						events = append(events[:i], events[i+1:]...)
						break
					}
				}
			case "late-error":
				failure = errors.Join(io.EOF, errors.New("late observer failure"))
			case "partial-reply":
				reply.Bytes = 1
				p.Writes = append(p.Writes, reply)
				p.ReplyDisposition = "ZERO_BYTE_NATIVE_FAILURE"
			case "full-reply-with-error":
				reply.Bytes = len(reply.Raw)
				p.Writes = append(p.Writes, reply)
				p.ReplyDisposition = "ZERO_BYTE_NATIVE_FAILURE"
			case "unavailable-writer":
				p.WriterOverflow = true
			case "unreturned-backend":
				p.BackendReturns = 0
			case "permit-held":
				p.AfterPermit = 1
			case "credit-held":
				p.CustodyRetired = false
			case "false-public-success":
				p.PhysicalResult = nil
			case "wrong-run":
				p.Ref.Run = "foreign"
			case "foreign-parent":
				p.Authority.Parent.ID++
			case "wrong-deadline":
				p.BackendCause = "context deadline exceeded"
			case "wrong-stream-order":
				for _, e := range events {
					if string(e["child_output_sequence"]) == "3" {
						e["child_output_sequence"] = json.RawMessage(`4`)
						break
					}
				}
			case "extra-control":
				events = append(events, queueGuardEvent(map[string]any{"kind": "control_received", "seq": 2}))
			case "zero-attempt-unattributed":
				p.PublicDelivery = "FAILED"
				p.PublicError = "gone"
				p.PublicTransportCause = false
			case "joined-public-error":
				p.PublicDelivery = "FAILED"
				p.PublicError = "gone plus failure"
				p.PublicTransportCause = false
				p.ConnError = "gone"
			case "duplicate-helper":
				for _, e := range events {
					if rawEventString(e, "kind") == "helper_done" {
						events = append(events, e)
						break
					}
				}
			case "unattributed-cause":
				p.CancellationParentRetired = false
				p.CancellationSessionFenced = false
			case "swapped-stream":
				for _, e := range events {
					if string(e["child_output_sequence"]) == "3" {
						e["child_output_sequence"] = json.RawMessage(`4`)
					} else if string(e["child_output_sequence"]) == "4" {
						e["child_output_sequence"] = json.RawMessage(`3`)
					}
				}
			case "missing-cleanup":
				for _, e := range events {
					if rawEventString(e, "kind") == "finished" {
						var v map[string]int
						_ = json.Unmarshal(e["effects"], &v)
						v["unload_attempts"] = 0
						e["effects"], _ = json.Marshal(v)
					}
				}
			case "extra-abort":
				events = append(events, queueGuardEvent(map[string]any{"kind": "aborted", "id": p.CommandID, "deadline": false}))
			}
			c := &interopControls{disconnectProof: p, expandedReverseLimit: 8, observed: events, events: make(chan map[string]json.RawMessage), failure: make(chan error, 1)}
			c.failure <- failure
			err := c.finish(0)
			if pure && err != nil {
				t.Fatal("legal owned disconnect refused", err)
			}
			if !pure && err == nil {
				t.Fatal("unowned or incomplete disconnect accepted", variant)
			}
		})
	}
}

func TestInteropDisconnectTransportErrorDoesNotMaskJoinedFailure(t *testing.T) {
	for _, leaf := range []error{io.EOF, os.ErrClosed, syscall.EPIPE} {
		native := fmt.Errorf("%w: %w", ErrGone, leaf)
		if ok, _ := interopDisconnectTransportError(native); !ok {
			t.Fatal("native transport attribution rejected", native)
		}
		if ok, _ := interopDisconnectTransportError(errors.Join(native, errors.New("late control failure"))); ok {
			t.Fatal("joined harness failure hidden")
		}
	}
}
