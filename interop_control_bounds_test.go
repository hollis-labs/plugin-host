//go:build unix

package pluginhost

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
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
