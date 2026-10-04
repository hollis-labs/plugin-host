//go:build unix

package pluginhost

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/hollis-labs/plugin-host/internal/strictjson"
)

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
func interopExpandedHelpers(events []map[string]json.RawMessage, reverseLimit uint32) error {
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
		expected[command.ID] = &helper{method: method, binding: command.Params.Context.Binding, key: args.Key, operation: args.Operation, value: strings.Repeat("v", args.ValueBytes), remaining: min(args.N, int(reverseLimit))}
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
