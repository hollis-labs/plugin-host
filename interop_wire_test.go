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
func interopWireTerminals(events []map[string]json.RawMessage, expectedExit int) error {
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
	return nil
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
