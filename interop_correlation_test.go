//go:build unix

package pluginhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

func interopSnapshot(t *testing.T, c *interopControls) map[string]int {
	t.Helper()
	if err := c.release("snapshot"); err != nil {
		t.Fatal(err)
	}
	e, err := c.event("snapshot")
	if err != nil {
		t.Fatal(err)
	}
	var effects map[string]int
	if err := json.Unmarshal(e["effects"], &effects); err != nil {
		t.Fatal(err)
	}
	return effects
}

func interopReleaseReturned(t *testing.T, c *interopControls, id uint64) {
	t.Helper()
	if err := c.release(fmt.Sprintf("request-%d", id)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.eventWhere("returned", func(e map[string]json.RawMessage) bool {
		var actual uint64
		return json.Unmarshal(e["id"], &actual) == nil && actual == id
	}); err != nil {
		t.Fatal(err)
	}
}

func interopEnteredCommand(t *testing.T, c *interopControls, name string) (uint64, map[string]json.RawMessage) {
	t.Helper()
	wire, err := c.eventWhere("wire", func(e map[string]json.RawMessage) bool {
		if rawEventString(e, "direction") != "host-to-worker" || rawEventString(e, "method") != subprocess.MethodCommandExecute {
			return false
		}
		var frame struct {
			Params struct {
				Name string `json:"name"`
			} `json:"params"`
		}
		return json.Unmarshal([]byte(rawEventString(e, "raw")), &frame) == nil && frame.Params.Name == name
	})
	if err != nil {
		t.Fatal(err)
	}
	var id uint64
	if json.Unmarshal(wire["id"], &id) != nil || id == 0 || id > 9007199254740991 {
		t.Fatal("invalid actual published ID", wire)
	}
	entered, err := c.eventWhere("entered", func(e map[string]json.RawMessage) bool {
		var actual uint64
		return json.Unmarshal(e["id"], &actual) == nil && actual == id && rawEventString(e, "name") == name
	})
	if err != nil {
		t.Fatal(err)
	}
	return id, entered
}

// This executes the exact authored deadline/absence parameters through the
// actual public Conn API. Local observation is finite but never wire context.
func replayInteropRawDeadline(t *testing.T, p *Process, c *interopControls, b *interopBackend, observation map[string]any) {
	t.Helper()
	observer, end := context.WithTimeout(context.Background(), 5*time.Second)
	defer end()
	observation["connection_default_timeout_ms"] = p.conn.defaultTimeout.Milliseconds()
	type outcome struct {
		raw json.RawMessage
		err error
	}
	start := func(params json.RawMessage) (chan outcome, uint64, *pendingCall, map[string]json.RawMessage) {
		done := make(chan outcome, 1)
		go func() {
			raw, err := p.conn.CallCorrelation(observer, subprocess.MethodCommandExecute, params)
			done <- outcome{raw, err}
		}()
		var input struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(params, &input); err != nil {
			t.Fatal(err)
		}
		id, e := interopEnteredCommand(t, c, input.Name)
		p.conn.mu.Lock()
		call := p.conn.pending[subprocess.NumberID(int64(id))] //nolint:gosec // interopEnteredCommand validates positive JS-safe actual publication ID.
		p.conn.mu.Unlock()                                     //nolint:gosec // Positive JS-safe ID validated immediately above.
		if call == nil || !call.correlation || call.session != nil || call.prepared != nil {
			t.Fatal("raw call is not non-authorizing actual Conn correlation")
		}
		return done, id, call, e
	}
	wait := func(done chan outcome) outcome {
		select {
		case result := <-done:
			return result
		case <-observer.Done():
			t.Fatal("harness observer expired before raw terminal")
			return outcome{}
		}
	}
	rawDeadline := json.RawMessage(`{"name":"uncooperative","args":"{}","session_id":"","context":{"binding_id":"binding-example","timeout_ms":300}}`)
	done, id, call, entered := start(rawDeadline)
	if string(entered["deadline"]) != "true" {
		t.Fatal("raw remote timer absent", entered)
	}
	result := wait(done)
	var rpc *subprocess.RPCError
	if !errors.As(result.err, &rpc) {
		t.Fatal("remote typed terminal missing", result.err)
	}
	var terminal subprocess.HostRPCErrorData
	data, _ := json.Marshal(rpc.Data)
	if json.Unmarshal(data, &terminal) != nil || terminal.Code != capability.UnknownOutcome || terminal.EffectState != capability.Unknown || terminal.Retryable {
		t.Fatal("remote expiry classification", result.err)
	}
	aborted, err := c.eventWhere("aborted", func(e map[string]json.RawMessage) bool {
		var actual uint64
		return json.Unmarshal(e["id"], &actual) == nil && actual == id
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(aborted["deadline"]) != "true" {
		t.Fatal("local cancellation substituted remote deadline", aborted)
	}
	if observer.Err() != nil {
		t.Fatal("remote budget ended observer", observer.Err())
	}
	snapshot := interopSnapshot(t, c)
	if snapshot["returned"] != 0 {
		t.Fatal("uncooperative permit released before explicit release", snapshot)
	}
	p.conn.mu.Lock()
	publication := call.publication
	p.conn.mu.Unlock()
	if !publication.complete || publication.err != nil || publication.cancelReason != "" {
		t.Fatalf("remote timer lacks complete uncancelled physical publication: %+v", publication)
	}
	interopReleaseReturned(t, c, id)
	observation["remote300"] = map[string]any{"caller_params": rawDeadline, "actual_id": id, "entered": entered, "aborted": aborted, "remote_error": rpc, "before_release_effects": snapshot, "publication_bytes": publication.bytes, "full_publication": publication.complete, "observer_canceled": false, "returned_before_release": 0}
	rawAbsent := json.RawMessage(`{"name":"hold","args":"{}","session_id":""}`)
	hold, holdID, holdCall, holdEntered := start(rawAbsent)
	if string(holdEntered["deadline"]) != "false" {
		t.Fatal("absent wire context invented SDK deadline", holdEntered)
	}
	select {
	case result := <-hold:
		t.Fatal("absent hold completed without release", result)
	case <-time.After(100 * time.Millisecond):
	}
	health, err := p.conn.CallCorrelation(observer, subprocess.MethodHealth, json.RawMessage(`{}`))
	if err != nil {
		t.Fatal("interleaved raw Health", err)
	}
	var healthy subprocess.HealthResult
	if json.Unmarshal(health, &healthy) != nil || !healthy.OK {
		t.Fatal("Health reply", string(health))
	}
	select {
	case result := <-hold:
		t.Fatal("Health retired held callback", result)
	default:
	}
	interopReleaseReturned(t, c, holdID)
	if result := wait(hold); result.err != nil || string(result.raw) != `{"action":"noop"}` {
		t.Fatal("released hold terminal", string(result.raw), result.err)
	}
	p.conn.mu.Lock()
	holdPublication := holdCall.publication
	p.conn.mu.Unlock()
	if !holdPublication.complete || holdPublication.err != nil || holdPublication.cancelReason != "" {
		t.Fatal("held callback missing physical receipt")
	}
	observation["absent_hold"] = map[string]any{"caller_params": rawAbsent, "actual_id": holdID, "entered": holdEntered, "held_ms": 100, "full_publication": true, "publication_bytes": holdPublication.bytes, "health_caller_params": json.RawMessage(`{}`), "health_result": healthy}
	b.mu.Lock()
	calls, commits := len(b.calls), b.commits
	b.mu.Unlock()
	if calls != 0 || commits != 0 {
		t.Fatal("correlation-only callbacks gained backend authority", calls, commits)
	}
	observation["backend_calls"] = calls
	observation["projection"] = "Exact authored remote300 binding-example and absent hold/Health parameters; actual public CallCorrelation observer5s independent; selected IDs follow genuine Init+Load; no authority projection"
}

func auditInteropRawDeadline(t *testing.T, events []map[string]json.RawMessage) {
	t.Helper()
	commands, health, remote := 0, 0, false
	for _, event := range events {
		if rawEventString(event, "kind") != "wire" {
			continue
		}
		direction, method := rawEventString(event, "direction"), rawEventString(event, "method")
		if method == "rpc/cancel" {
			t.Fatal("raw remote/absent vector manufactured cancellation", event)
		}
		if direction != "host-to-worker" || (method != subprocess.MethodHealth && method != subprocess.MethodCommandExecute) {
			continue
		}
		var frame struct {
			ID     uint64                     `json:"id"`
			Params map[string]json.RawMessage `json:"params"`
		}
		if json.Unmarshal([]byte(rawEventString(event, "raw")), &frame) != nil {
			t.Fatal("missing raw request witness")
		}
		if method == subprocess.MethodHealth {
			health++
			if len(frame.Params) != 0 {
				t.Fatal("raw Health params changed", frame.Params)
			}
			continue
		}
		commands++
		var name, args, session string
		_ = json.Unmarshal(frame.Params["name"], &name)
		_ = json.Unmarshal(frame.Params["args"], &args)
		_ = json.Unmarshal(frame.Params["session_id"], &session)
		if args != "{}" || session != "" {
			t.Fatal("raw command changed", frame.Params)
		}
		switch name {
		case "uncooperative":
			var context subprocess.ForwardContext
			if len(frame.Params) != 4 || json.Unmarshal(frame.Params["context"], &context) != nil || context.TimeoutMS == 0 || context.TimeoutMS > 300 || context.BindingID == nil || *context.BindingID != "binding-example" {
				t.Fatal("raw remote selector/budget changed", frame.Params)
			}
			remote = true
		case "hold":
			if len(frame.Params) != 3 || frame.Params["context"] != nil {
				t.Fatal("raw absent context changed", frame.Params)
			}
		default:
			t.Fatal("extra raw command", frame.Params)
		}
	}
	if commands != 2 || health != 1 || !remote {
		t.Fatalf("missing/extra raw traffic commands=%d health=%d remote=%v", commands, health, remote)
	}
}

func TestSDKManifestCorrelationObserverCancellation(t *testing.T) {
	if source := os.Getenv("INTEROP_SDK_SOURCE"); source == "" {
		t.Skip("isolated exact SDK assets required")
	}
	var receipts []map[string]any
	defer writeInteropGuardReport(t, "observer-expiry", os.Getenv("INTEROP_OBSERVER_REPORT"), &receipts)
	for _, runtime := range []string{"go", "node", "deno"} {
		t.Run(runtime, func(t *testing.T) {
			b := &interopBackend{receipts: map[string]subprocess.StoragePutResult{}, inputs: map[string][32]byte{}}
			p, c := startInteropExpanded(t, runtime, interopRecipeRow{Profile: "expanded"}, b)
			observer, end := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer end()
			done := make(chan error, 1)
			go func() {
				_, err := p.conn.CallCorrelation(observer, subprocess.MethodCommandExecute, json.RawMessage(`{"name":"uncooperative","args":"{}","session_id":""}`))
				done <- err
			}()
			id, entered := interopEnteredCommand(t, c, "uncooperative")
			p.conn.mu.Lock()
			call := p.conn.pending[subprocess.NumberID(int64(id))] //nolint:gosec // interopEnteredCommand validates positive JS-safe actual publication ID.
			p.conn.mu.Unlock()                                     //nolint:gosec // Positive JS-safe ID checked immediately above.
			if call == nil || string(entered["deadline"]) != "false" {
				t.Fatal("observer deadline leaked to wire")
			}
			if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("missing local observer expiry", err)
			}
			aborted, err := c.eventWhere("aborted", func(e map[string]json.RawMessage) bool {
				var actual uint64
				return json.Unmarshal(e["id"], &actual) == nil && actual == id
			})
			if err != nil {
				t.Fatal(err)
			}
			if string(aborted["deadline"]) != "false" {
				t.Fatal("observer cancellation relabeled remote SDK deadline", aborted)
			}
			until := time.Now().Add(time.Second)
			var publication forwardPublication
			for {
				p.conn.mu.Lock()
				publication = call.publication
				p.conn.mu.Unlock()
				if publication.cancelComplete {
					break
				}
				if time.Now().After(until) {
					t.Fatal("no physical observer control receipt")
				}
				time.Sleep(time.Millisecond)
			}
			if publication.cancelReason != subprocess.DeadlineExpired || !publication.complete || observer.Err() != context.DeadlineExceeded {
				t.Fatal("observer control lacks actual cause/publication")
			}
			c.hostCancelEvidence = map[uint64]interopHostCancelEvidence{id: {Reason: string(publication.cancelReason), Published: publication.complete, ControlComplete: publication.cancelComplete, ObserverExpired: errors.Is(observer.Err(), context.DeadlineExceeded)}}
			interopReleaseReturned(t, c, id)
			finished := finishInteropExpanded(t, p, c, 0)
			receipts = append(receipts, map[string]any{"runtime": runtime, "actual_id": id, "entered": entered, "aborted": aborted, "host_control_provenance": c.hostCancelEvidence, "wire_control_events": c.observed, "controls": c.trace, "finished": finished, "worker_env": p.spec.Env, "bridge_command": p.spec.Command, "bridge_args": p.spec.Args, "exit_code": 0, "reaped": true, "observer_eof": true})
		})
	}
}

func TestSDKManifestCorrelationInertSelectorRefusal(t *testing.T) {
	if os.Getenv("INTEROP_SDK_SOURCE") == "" {
		t.Skip("isolated exact SDK assets required")
	}
	var receipts []map[string]any
	defer writeInteropGuardReport(t, "inert-selector-no-authority", os.Getenv("INTEROP_AUTHORITY_REPORT"), &receipts)
	for _, runtime := range []string{"go", "node", "deno"} {
		t.Run(runtime, func(t *testing.T) {
			b := &interopBackend{receipts: map[string]subprocess.StoragePutResult{}, inputs: map[string][32]byte{}}
			p, c := startInteropExpanded(t, runtime, interopRecipeRow{Profile: "expanded"}, b)
			observer, end := context.WithTimeout(context.Background(), 2*time.Second)
			defer end()
			raw, err := p.conn.CallCorrelation(observer, subprocess.MethodCommandExecute, json.RawMessage(`{"name":"get","args":"{\"n\":1,\"key\":\"read\"}","session_id":"","context":{"timeout_ms":500,"binding_id":"binding-example"}}`))
			if err != nil {
				t.Fatal(err)
			}
			var command subprocess.CommandExecResult
			if json.Unmarshal(raw, &command) != nil {
				t.Fatal("typed command result missing")
			}
			outcomes := interopHelperResults(t, command)
			if len(outcomes) != 1 || outcomes[0].Code != "target_unavailable" || outcomes[0].EffectState != "not_started" {
				t.Fatal("inert selector helper classification", outcomes)
			}
			b.mu.Lock()
			effects, commits := len(b.calls), b.commits
			b.mu.Unlock()
			p.conn.reverse.business.mu.Lock()
			minted := p.conn.reverse.business.bindings["binding-example"] != nil
			parents := len(p.conn.reverse.business.parents)
			p.conn.reverse.business.mu.Unlock()
			if effects != 0 || commits != 0 || minted || parents != 0 {
				t.Fatal("inert selector gained host authority", effects, commits, minted, parents)
			}
			finished := finishInteropExpanded(t, p, c, 0)
			requests, refusals := 0, 0
			for _, event := range c.observed {
				if rawEventString(event, "method") == "host/storage/get" && rawEventString(event, "direction") == "worker-to-host" {
					requests++
				}
				if rawEventString(event, "direction") == "host-to-worker" && rawEventString(event, "frame_type") == "response" {
					var response struct {
						Error *subprocess.HostRPCError `json:"error"`
					}
					if json.Unmarshal([]byte(rawEventString(event, "raw")), &response) == nil && response.Error != nil && response.Error.Data.Code == capability.TargetUnavailable && response.Error.Data.EffectState == capability.NotStarted {
						refusals++
					}
				}
			}
			if requests != 1 || refusals != 1 {
				t.Fatal("no actual closed helper refusal wire", requests, refusals)
			}
			receipts = append(receipts, map[string]any{"runtime": runtime, "helper_results": outcomes, "helper_requests": requests, "typed_refusals": refusals, "backend_calls": effects, "commits": commits, "binding_minted": minted, "authority_parents": parents, "wire_control_events": c.observed, "controls": c.trace, "finished": finished, "worker_env": p.spec.Env, "bridge_command": p.spec.Command, "bridge_args": p.spec.Args, "exit_code": 0, "reaped": true, "observer_eof": true})
		})
	}
}

func writeInteropGuardReport(t *testing.T, kind, path string, receipts *[]map[string]any) {
	t.Helper()
	if path == "" {
		return
	}
	report := interopProvenance(t)
	report["kind"] = kind
	report["observations"] = *receipts
	report["harness_failed"] = t.Failed()
	report["manifest_pass_claim"] = false
	raw, err := json.MarshalIndent(report, "", "  ")
	if err == nil {
		err = os.WriteFile(path, append(raw, '\n'), 0600) //nolint:gosec // Explicit opt-in owned receipt path, never plugin input.
	} //nolint:gosec // Explicit opt-in owned receipt path, never plugin input.
	if err != nil {
		t.Error(err)
	}
}
