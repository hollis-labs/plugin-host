//go:build unix

package pluginhost

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-host/internal/interopfixture"
	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

type interopRecipeRow struct {
	Name     string `json:"name"`
	Profile  string `json:"profile"`
	Scenario string `json:"scenario"`
	Status   string `json:"status"`
	Owner    string `json:"owner"`
	Reason   string `json:"reason"`
}
type interopBackend struct {
	mu                sync.Mutex
	calls             []HostAuthority
	commits           int
	unknown           bool
	getGate           chan struct{}
	getEntered        chan *HostCall
	receipts          map[string]subprocess.StoragePutResult
	inputs            map[string][32]byte
	generation        uint64
	receiptGeneration map[string]uint64
}

func (b *interopBackend) services() HostServices {
	return HostServices{
		Log: func(_ context.Context, c *HostCall, _ subprocess.LogParams) (subprocess.LogResult, error) {
			return subprocess.LogResult{Accepted: true}, c.CheckCommit()
		},
		StorageGet: func(ctx context.Context, c *HostCall, _ subprocess.StorageGetParams) (subprocess.StorageGetResult, error) {
			b.mu.Lock()
			b.calls = append(b.calls, c.Authority())
			b.mu.Unlock()
			if b.getEntered != nil {
				b.getEntered <- c
			}
			if b.getGate != nil {
				select {
				case <-b.getGate:
				case <-ctx.Done():
					return subprocess.StorageGetResult{}, ctx.Err()
				}
			}
			return subprocess.StorageGetResult{Found: false}, c.CheckCommit()
		},
		StoragePut: func(_ context.Context, c *HostCall, p subprocess.StoragePutParams) (subprocess.StoragePutResult, error) {
			b.mu.Lock()
			defer b.mu.Unlock()
			b.calls = append(b.calls, c.Authority())
			if err := c.CheckCommit(); err != nil {
				return subprocess.StoragePutResult{}, err
			}
			key := p.Key + "/" + p.OperationKey
			input, _ := json.Marshal(struct {
				Value            any
				ExpectedRevision any
			}{p.Value, p.ExpectedRevision})
			digest := sha256.Sum256(input)
			if previous, exists := b.inputs[key]; exists && previous != digest {
				return subprocess.StoragePutResult{}, hostRefusal(capability.Conflict, "")
			}
			result, ok := b.receipts[key]
			if !ok {
				b.commits++
				result = subprocess.StoragePutResult{OperationKey: p.OperationKey, Revision: fmt.Sprintf("r%d", b.commits)}
				b.receipts[key] = result
				b.inputs[key] = digest
				if b.receiptGeneration == nil {
					b.receiptGeneration = map[string]uint64{}
				}
				generation := b.generation
				if generation == 0 {
					generation = 1
				}
				b.receiptGeneration[key] = generation
				if b.unknown {
					delete(b.receipts, key)
				}
			}
			if b.unknown {
				return subprocess.StoragePutResult{}, hostRefusal(capability.UnknownOutcome, "")
			}
			return result, nil
		},
		// The raw fixture offers these methods too; this host backend explicitly
		// refuses them. This is no claim of positive service interoperability.
		StorageDelete: func(context.Context, *HostCall, subprocess.StorageDeleteParams) (subprocess.StorageDeleteResult, error) {
			return subprocess.StorageDeleteResult{}, hostRefusal(capability.CapabilityDenied, "")
		},
		SecretsGet: func(context.Context, *HostCall, subprocess.SecretsGetParams) (subprocess.SecretsGetResult, error) {
			return subprocess.SecretsGetResult{}, hostRefusal(capability.CapabilityDenied, "")
		},
		EgressRequest: func(context.Context, *HostCall, subprocess.EgressRequestParams) (subprocess.EgressRequestResult, error) {
			return subprocess.EgressRequestResult{}, hostRefusal(capability.CapabilityDenied, "")
		},
		EventsPublish: func(context.Context, *HostCall, subprocess.EventsPublishParams) (subprocess.EventsPublishResult, error) {
			return subprocess.EventsPublishResult{}, hostRefusal(capability.CapabilityDenied, "")
		},
	}
}
func interopSourceSpec(t *testing.T, b *interopBackend) Spec {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(os.Getenv("INTEROP_SDK_SOURCE"), "protocol/v2/fixtures/host-storage.json")) //nolint:gosec // Explicit opt-in test-owned fixture, control or receipt path; never plugin-provided.
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Init subprocess.InitParams `json:"init"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	// The authored expanded driver sets every offered method ceiling to 10000ms.
	for _, method := range doc.Init.HostServices.Methods {
		doc.Init.HostServices.Limits.MethodTimeoutMS[method] = 10000
	}
	if b.generation > 1 {
		doc.Init.Incarnation.OwnerGeneration = b.generation
		doc.Init.HostServices.Incarnation.OwnerGeneration = b.generation
		for i := range doc.Init.Grants {
			doc.Init.Grants[i].OwnerGeneration = b.generation
		}
	}
	return Spec{ExpectedID: "fixture", ExpectedVersion: "1.0.0", Init: doc.Init, Reverse: &ReverseProfile{Runtime: NewHostServiceRuntime(b.services(), func(ctx context.Context, _ HostAuthority) error { return ctx.Err() }), Required: true, LifecycleBinding: &HostBinding{GrantID: "g-Log", Scope: json.RawMessage(`{}`)}}, HandshakeTimeout: 2 * time.Second, UnloadTimeout: time.Second, ReapTimeout: time.Second}
}
func startInteropExpanded(t *testing.T, runtime string, recipe interopRecipeRow, b *interopBackend, configured ...Spec) (*Process, *interopControls) {
	t.Helper()
	s := interopSourceSpec(t, b)
	if len(configured) != 0 {
		s = configured[0]
	}
	p, c := spawnInteropChild(t, runtime, recipe.Profile, b.services(), s)
	c.expandedReverseLimit = s.Init.HostServices.Limits.PluginToHostInflight
	if _, err := c.event("ready"); err != nil {
		t.Fatal(err)
	}
	// Genuine host lifecycle publishes Init1, Load2 before business readiness.
	if err := c.release("request-2"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.Handshake(context.Background()); err != nil {
		t.Fatal(err, p.Diagnostics())
	}
	if err := p.conn.ActivateHostServices(); err != nil {
		t.Fatal(err)
	}
	return p, c
}
func interopCommand(ctx context.Context, p *Process, name string, args any, grant string) (subprocess.CommandExecResult, error) {
	if _, bounded := ctx.Deadline(); !bounded {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}
	raw, _ := json.Marshal(args)
	if grant != "" {
		ctx = WithHostBinding(ctx, HostBinding{GrantID: grant, Scope: json.RawMessage(`{}`)})
	}
	return p.Client().CommandExecute(ctx, subprocess.CommandExecParams{Name: name, Args: string(raw), SessionID: ""})
}

type interopHelperOutcome struct {
	Code        string `json:"code"`
	EffectState string `json:"effect_state"`
}

func interopHelperResults(t *testing.T, result subprocess.CommandExecResult) []interopHelperOutcome {
	t.Helper()
	var outcomes []interopHelperOutcome
	if err := json.Unmarshal([]byte(result.Content), &outcomes); err != nil {
		t.Fatal("helper outcomes", err, result)
	}
	return outcomes
}
func finishInteropExpanded(t *testing.T, p *Process, c *interopControls, expectedCode int) map[string]json.RawMessage {
	t.Helper()
	if err := p.Stop(context.Background()); err != nil {
		t.Fatal("harness shutdown", err, p.Diagnostics())
	}
	e, err := c.event("finished")
	if err != nil {
		t.Fatal(err, p.Diagnostics())
	}
	exit, ok := p.ExitInfo()
	if !ok || exit.Code != expectedCode || exit.Signal != "" {
		t.Fatal("harness exit", exit, p.Diagnostics())
	}
	var effects map[string]int
	if err := json.Unmarshal(e["effects"], &effects); err != nil {
		t.Fatal(err)
	}
	if effects["unload_attempts"] != 1 {
		t.Fatal("not once-only cleanup", effects)
	}
	if err := c.finish(expectedCode); err != nil {
		t.Fatal(err)
	}
	return e
}
func replayInteropReverse(t *testing.T, p *Process, c *interopControls, b *interopBackend) {
	b.getGate = make(chan struct{})
	b.getEntered = make(chan *HostCall, 8)
	t.Cleanup(func() {
		select {
		case <-b.getGate:
		default:
			close(b.getGate)
		}
	})
	result := make(chan subprocess.CommandExecResult, 1)
	failure := make(chan error, 1)
	go func() {
		r, e := interopCommand(context.Background(), p, "get", map[string]any{"n": 9, "key": "read"}, "g-StorageGet")
		result <- r
		failure <- e
	}()
	for i := 0; i < 8; i++ {
		select {
		case <-b.getEntered:
		case <-time.After(time.Second):
			t.Fatal("harness backend admission wait")
		}
	}
	if _, err := c.eventWhere("helper_done", func(e map[string]json.RawMessage) bool {
		var outcome interopHelperOutcome
		_ = json.Unmarshal(e["failure"], &outcome)
		return outcome.Code == "rate_limited" && outcome.EffectState == "not_started"
	}); err != nil {
		t.Fatal(err)
	}
	close(b.getGate)
	r := <-result
	if err := <-failure; err != nil {
		t.Fatal(err)
	}
	ok, refused := 0, 0
	for _, v := range interopHelperResults(t, r) {
		switch v.Code {
		case "ok":
			ok++
		case "rate_limited":
			refused++
		default:
			t.Fatal("unexpected helper outcome", v)
		}
	}
	b.mu.Lock()
	calls := len(b.calls)
	b.mu.Unlock()
	if calls != 8 {
		t.Fatal("reverse backend admission count", calls)
	}
	if ok != 8 || refused != 1 {
		t.Fatal("reverse admission arithmetic", ok, refused)
	}
}
func replayInteropMutation(t *testing.T, p *Process, c *interopControls, b *interopBackend, unknown bool) {
	b.mu.Lock()
	b.unknown = unknown
	b.mu.Unlock()
	r, err := interopCommand(context.Background(), p, "put", map[string]any{"n": 1, "key": "write", "operation_key": "stable-key"}, "g-StoragePut")
	if err != nil {
		t.Fatal(err)
	}
	outcomes := interopHelperResults(t, r)
	expected := "ok"
	if unknown {
		expected = "unknown_outcome"
	}
	if len(outcomes) != 1 || outcomes[0].Code != expected {
		t.Fatal("mutation classification", outcomes)
	}
	if unknown && outcomes[0].EffectState != "unknown" {
		t.Fatal("lost effect classification", outcomes)
	}
	if _, err := p.Client().Health(context.Background()); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if unknown && len(b.receipts) != 0 {
		t.Fatal("lost receipt was retained")
	}
	if b.commits != 1 || len(b.calls) != 1 {
		t.Fatal("mutation retried", b.commits, len(b.calls))
	}
}

func replayInteropDescendants(t *testing.T, p *Process, c *interopControls, b *interopBackend) {
	b.getGate = make(chan struct{})
	b.getEntered = make(chan *HostCall, 4)
	t.Cleanup(func() {
		select {
		case <-b.getGate:
		default:
			close(b.getGate)
		}
	})
	type result struct {
		value subprocess.CommandExecResult
		err   error
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first := make(chan result, 1)
	second := make(chan result, 1)
	invoke := func(ctx context.Context, out chan result) {
		r, e := interopCommand(ctx, p, "get", map[string]any{"n": 2, "key": "read"}, "g-StorageGet")
		out <- result{r, e}
	}
	go invoke(ctx, first)
	var parents [2]uint64
	for i := 0; i < 2; i++ {
		select {
		case call := <-b.getEntered:
			parents[0] = call.Authority().Parent.ID
		case <-time.After(time.Second):
			t.Fatal("first descendants admission timeout")
		}
	}
	go invoke(context.Background(), second)
	for i := 0; i < 2; i++ {
		select {
		case call := <-b.getEntered:
			parents[1] = call.Authority().Parent.ID
		case <-time.After(time.Second):
			t.Fatal("second descendants admission timeout")
		}
	}
	if parents[0] == parents[1] {
		t.Fatal("parent namespaces collapsed", parents)
	}
	if parents[0] > uint64(math.MaxInt64) {
		t.Fatal("unsafe parent ID")
		return
	}
	if err := p.conn.Notify("rpc/cancel", subprocess.CancelParams{RequestOwner: subprocess.HostRPCOwnerHost, ID: subprocess.NumberID(int64(parents[0])), Reason: subprocess.CallerCancelled}); err != nil { //nolint:gosec // Actual host-issued parent ID checked against MaxInt64 above.
		t.Fatal(err)
	}
	select {
	case r := <-first:
		var rpc *subprocess.RPCError
		if !errors.As(r.err, &rpc) {
			t.Fatal("wire-cancel terminal missing", r.err)
		}
	case <-time.After(time.Second):
		t.Fatal("host cancellation blocked")
	}
	var cancelledIDs []uint64
	for i := 0; i < 2; i++ {
		event, err := c.eventWhere("wire", func(e map[string]json.RawMessage) bool {
			return rawEventString(e, "direction") == "worker-to-host" && rawEventString(e, "method") == "rpc/cancel"
		})
		if err != nil {
			t.Fatal(err)
		}
		var frame struct {
			Params struct {
				Owner  string `json:"request_owner"`
				ID     uint64 `json:"id"`
				Reason string `json:"reason"`
			} `json:"params"`
		}
		if err := json.Unmarshal([]byte(rawEventString(event, "raw")), &frame); err != nil {
			t.Fatal(err)
		}
		if frame.Params.Owner != "plugin" || frame.Params.Reason != string(subprocess.ParentCancelled) {
			t.Fatal("descendant cancel direction/reason", frame)
		}
		cancelledIDs = append(cancelledIDs, frame.Params.ID)
	}
	// The actual parent response must carry the authored uncertain effect,
	// independently of the local host Call having already returned cancellation.
	if _, err := c.eventWhere("wire", func(e map[string]json.RawMessage) bool {
		if rawEventString(e, "direction") != "worker-to-host" {
			return false
		}
		var frame struct {
			ID    uint64                   `json:"id"`
			Error *subprocess.HostRPCError `json:"error"`
		}
		return json.Unmarshal([]byte(rawEventString(e, "raw")), &frame) == nil && frame.ID == parents[0] && frame.Error != nil && frame.Error.Data.Code == capability.UnknownOutcome && frame.Error.Data.EffectState == capability.Unknown
	}); err != nil {
		t.Fatal("canceled parent terminal", err)
	}
	close(b.getGate)
	select {
	case r := <-second:
		if r.err != nil {
			t.Fatal(r.err)
		}
		outcomes := interopHelperResults(t, r.value)
		if len(outcomes) != 2 || outcomes[0].Code != "ok" || outcomes[1].Code != "ok" {
			t.Fatal("sibling parent was fenced", outcomes)
		}
	case <-time.After(time.Second):
		t.Fatal("sibling parent did not complete")
	}
	// Correlate both cancels to exactly the first parent's genuine reverse IDs.
	var firstIDs []uint64
	events := append(append([]map[string]json.RawMessage{}, c.observed...), c.pending...)
	for _, e := range events {
		if rawEventString(e, "direction") != "worker-to-host" || rawEventString(e, "method") != "host/storage/get" {
			continue
		}
		var frame struct {
			ID     uint64 `json:"id"`
			Params struct {
				Context subprocess.ReverseContext `json:"context"`
			} `json:"params"`
		}
		if err := json.Unmarshal([]byte(rawEventString(e, "raw")), &frame); err != nil {
			t.Fatal(err)
		}
		if frame.Params.Context.ParentCall.ID == parents[0] {
			firstIDs = append(firstIDs, frame.ID)
		}
	}
	slices.Sort(firstIDs)
	slices.Sort(cancelledIDs)
	if len(firstIDs) != 2 || len(cancelledIDs) != 2 || cancelledIDs[0] == cancelledIDs[1] || firstIDs[0] != cancelledIDs[0] || firstIDs[1] != cancelledIDs[1] {
		t.Fatal("wrong descendants canceled", firstIDs, cancelledIDs)
	}
}

func replayInteropReceiptRestart(t *testing.T, p *Process, c *interopControls, b *interopBackend, runtime string, recipe interopRecipeRow, observation map[string]any) (*Process, *interopControls) {
	replayInteropMutation(t, p, c, b, false)
	first := finishInteropExpanded(t, p, c, 0)
	if len(first) == 0 {
		t.Fatal("missing first-generation exit")
	}
	observation["generation1"] = map[string]any{"finished": first, "wire_control_events": c.observed, "controls": c.trace, "exit_code": 0, "reaped": true}
	b.generation = 2
	next, controls := startInteropExpanded(t, runtime, recipe, b)
	result, err := interopCommand(context.Background(), next, "put", map[string]any{"n": 1, "key": "write", "operation_key": "stable-key"}, "g-StoragePut")
	if err != nil {
		t.Fatal(err)
	}
	outcomes := interopHelperResults(t, result)
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(outcomes) != 1 || outcomes[0].Code != "ok" || b.commits != 1 || len(b.calls) != 2 || b.receiptGeneration["write/stable-key"] != 1 || b.calls[1].Owner.OwnerGeneration != 2 {
		t.Fatal("restart receipt ownership", outcomes, b.commits, b.calls, b.receiptGeneration)
	}
	return next, controls
}
func replayInteropCleanup(t *testing.T, p *Process, c *interopControls, scenario string) map[string]json.RawMessage {
	expected := 0
	if scenario == "cleanup-hung" {
		expected = 1
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		err := p.Client().Unload(ctx)
		cancel()
		if err == nil {
			t.Fatal("hung cleanup unexpectedly replied successfully")
		}
		select {
		case <-p.Exited():
		case <-time.After(time.Second):
			t.Fatal("harness hung-cleanup worker did not exit naturally", p.Diagnostics())
		}
	}
	e := finishInteropExpanded(t, p, c, expected)
	if scenario == "cleanup-hung" {
		if string(e["transport_error"]) == "null" || len(e["transport_error"]) == 0 {
			t.Fatal("hung cleanup lacked transport failure", e)
		}
	} else {
		var rpc *subprocess.RPCError
		if !errors.As(p.unloadErr, &rpc) || rpc.Code != -32603 {
			t.Fatal("cleanup classified error missing", p.unloadErr)
		}
	}
	return e
}
func TestSDKManifestExpandedReplay(t *testing.T) {
	source := os.Getenv("INTEROP_SDK_SOURCE")
	if source == "" {
		t.Skip("isolated interop script supplies assets")
	}
	raw, err := os.ReadFile(filepath.Join(source, "protocol/v2/fixtures/duplex-child.json")) //nolint:gosec // Explicit opt-in test-owned fixture, control or receipt path; never plugin-provided.
	if err != nil {
		t.Fatal(err)
	}
	selected, err := interopfixture.Select(raw, "normal-serve-negotiated", nil)
	if err != nil {
		t.Fatal("harness metadata", err)
	}
	var observations []map[string]any
	defer func() {
		if path := os.Getenv("INTEROP_REPORT"); path != "" {
			report := interopProvenance(t)
			report["mode"] = "normal-serve-negotiated"
			report["observations"] = observations
			report["harness_failed"] = t.Failed()
			raw, e := json.MarshalIndent(report, "", "  ")
			if e == nil {
				e = os.WriteFile(path, append(raw, '\n'), 0600) //nolint:gosec // Explicit opt-in test-owned fixture, control or receipt path; never plugin-provided.
			}
			if e != nil {
				t.Error(e)
			}
		}
	}()
	for _, runtime := range []string{"go", "node", "deno"} {
		for _, row := range selected {
			var recipe interopRecipeRow
			if err := json.Unmarshal(row, &recipe); err != nil {
				t.Fatal(err)
			}
			if recipe.Status == "" {
				recipe.Status = "observed"
			}
			observation := map[string]any{"authored_recipe": row, "runtime": runtime, "case": recipe.Name, "scenario": recipe.Scenario, "profile": recipe.Profile, "mode": "normal-serve-negotiated", "source_status": recipe.Status}
			if recipe.Status == "proposed" {
				observation["status"] = "unavailable"
				observation["owner"] = recipe.Owner
				observation["reason"] = recipe.Reason
				observations = append(observations, observation)
				continue
			}
			supported := recipe.Scenario == "clip" || recipe.Scenario == "queue" || recipe.Scenario == "queue-frames" || recipe.Scenario == "reverse" || recipe.Scenario == "effects" || recipe.Scenario == "unknown" || recipe.Scenario == "overflow" || recipe.Scenario == "cleanup-error" || recipe.Scenario == "cleanup-panic" || recipe.Scenario == "cleanup-hung" || recipe.Scenario == "receipt-restart" || recipe.Scenario == "descendants" || recipe.Scenario == "deadline" || recipe.Scenario == "host-fairness"
			if !supported {
				observation["status"] = "pending"
				observation["owner"] = "plugin-host interop adapter (slice5)"
				observation["reason"] = "Scenario handler pending; no execution claimed"
				if recipe.Scenario == "forward" || recipe.Scenario == "credits" {
					observation["reason"] = "Historical raw ea8 lifecycle/counter case PENDING/incompatible: actual finite Init+Load authority and cumulative startup counters preserved; no repeated/unbounded Load or counter projection"
				}
				observations = append(observations, observation)
				continue
			}
			executed := false
			passed := t.Run(runtime+"/"+recipe.Name, func(t *testing.T) {
				executed = true
				b := &interopBackend{receipts: map[string]subprocess.StoragePutResult{}, inputs: map[string][32]byte{}}
				var p *Process
				var c *interopControls
				if recipe.Scenario == "host-fairness" {
					p, c = replayInteropPrivateHostFairness(t, runtime, recipe, b, observation)
				} else {
					p, c = startInteropExpanded(t, runtime, recipe, b)
				}
				switch recipe.Scenario {
				case "clip", "queue", "queue-frames":
					replayInteropQueue(t, p, c, b, runtime, recipe, observation)
				case "deadline":
					replayInteropRawDeadline(t, p, c, b, observation)
				case "reverse":
					replayInteropReverse(t, p, c, b)
				case "descendants":
					replayInteropDescendants(t, p, c, b)
				case "receipt-restart":
					p, c = replayInteropReceiptRestart(t, p, c, b, runtime, recipe, observation)
				case "effects", "unknown":
					replayInteropMutation(t, p, c, b, recipe.Scenario == "unknown")
				case "cleanup-error", "cleanup-panic", "cleanup-hung":
					observation["finished"] = replayInteropCleanup(t, p, c, recipe.Scenario)
				case "overflow":
					_, err := interopCommand(context.Background(), p, "overflow", map[string]any{}, "")
					var rpc *subprocess.RPCError
					if !errors.As(err, &rpc) {
						t.Fatal("missing committed overflow", err)
					}
					raw, _ := json.Marshal(rpc.Data)
					var data map[string]any
					if decodeErr := json.Unmarshal(raw, &data); decodeErr != nil {
						t.Fatal(decodeErr)
					}
					if data["code"] != "budget_exceeded" || data["effect_state"] != "committed" {
						t.Fatal("overflow classification", data)
					}
					if releaseErr := c.release("snapshot"); releaseErr != nil {
						t.Fatal(releaseErr)
					}
					e, err := c.event("snapshot")
					if err != nil {
						t.Fatal(err)
					}
					var effects map[string]int
					if err := json.Unmarshal(e["effects"], &effects); err != nil || effects["commits"] != 1 {
						t.Fatal("committed overflow effects", e)
					}
				}
				if observation["finished"] == nil {
					observation["finished"] = finishInteropExpanded(t, p, c, 0)
				}
				if c.queueProof != nil {
					auditInteropQueueCopies(t, c)
					observation["queue_copied_trace_adverse_audit"] = true
				}
				if recipe.Scenario == "deadline" {
					auditInteropRawDeadline(t, c.observed)
				}
				if recipe.Scenario == "host-fairness" {
					auditInteropPrivateEOF(t, c)
					observation["private_eof_adverse_audit"] = true
				}
				observation["reaped"] = true
				exit, _ := p.ExitInfo()
				observation["exit_code"] = exit.Code
				observation["exit_signal"] = exit.Signal
				p.conn.mu.Lock()
				p.conn.queue.mu.Lock()
				retired := len(p.conn.inboundActive) == 0 && p.conn.activeReceipt == nil && p.conn.queue.reserved == 0 && p.conn.queue.active == nil
				p.conn.queue.mu.Unlock()
				p.conn.mu.Unlock()
				if !retired {
					t.Fatal("physical receipt remains active after reaped exit")
				}
				observation["terminal_receipts_retired"] = true
				observation["observer_eof"] = true
				if recipe.Scenario == "descendants" {
					observation["cancel_path"] = "Authored rpc/cancel notification through actual Conn.Notify; local-context-cancel Go race retained separately"
				}
				observation["worker_command_env"] = p.spec.Env
				observation["bridge_command"] = p.spec.Command
				observation["bridge_args"] = p.spec.Args
				if recipe.Scenario != "deadline" && recipe.Scenario != "host-fairness" {
					observation["projection"] = "Actual Init+Load lifecycle and host-minted binding; command IDs follow lifecycle; authored expanded 10000ms method ceilings"
				}
				if recipe.Scenario == "cleanup-hung" {
					observation["terminal_parent_timeout_ms"] = 300
				}
				observation["controls"] = c.trace
				observation["wire_control_events"] = append(c.observed, c.pending...)
				b.mu.Lock()
				observation["backend_calls"] = b.calls
				observation["commits"] = b.commits
				b.mu.Unlock()
			})
			if !executed {
				observation["status"] = "pending"
				observation["owner"] = "plugin-host interop runner"
				observation["reason"] = "Not selected by test invocation"
			} else if passed {
				observation["status"] = "passed"
			} else {
				observation["status"] = "failed"
			}
			observations = append(observations, observation)
		}
	}
}

func interopProvenance(t *testing.T) map[string]any {
	t.Helper()
	source := os.Getenv("INTEROP_SDK_SOURCE")
	raw, err := os.ReadFile(filepath.Join(source, "protocol/v2/fixtures/duplex-child.json")) //nolint:gosec // Explicit opt-in test-owned fixture, control or receipt path; never plugin-provided.
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	assets := filepath.Dir(os.Getenv("INTEROP_GO_CHILD"))
	build, err := os.ReadFile(filepath.Join(assets, "build-receipt")) //nolint:gosec // Explicit opt-in test-owned fixture, control or receipt path; never plugin-provided.
	if err != nil {
		t.Fatal(err)
	}
	tsBuild, err := os.ReadFile(filepath.Join(assets, "ts-build-receipt")) //nolint:gosec // Explicit opt-in test-owned fixture, control or receipt path; never plugin-provided.
	if err != nil {
		t.Fatal(err)
	}
	assetHashes := map[string]string{}
	for name, path := range map[string]string{"go_child": os.Getenv("INTEROP_GO_CHILD"), "worker": filepath.Join(source, "ts/packages/plugin-sdk/test/negotiated-worker.js"), "selector": filepath.Join(source, "ts/packages/plugin-sdk/test/expanded-selection.js"), "driver": filepath.Join(source, "ts/packages/plugin-sdk/test/child-cases-replay.js"), "compiled_sdk_index": filepath.Join(source, "ts/packages/plugin-sdk/dist/index.js")} {
		file, openErr := os.Open(path) //nolint:gosec // Exact opt-in source/build assets, never plugin-provided.
		if openErr != nil {
			t.Fatal(openErr)
		}
		h := sha256.New()
		_, copyErr := io.Copy(h, file)
		closeErr := file.Close()
		if copyErr != nil || closeErr != nil {
			t.Fatal(copyErr, closeErr)
		}
		assetHashes[name] = fmt.Sprintf("%x", h.Sum(nil))
	}
	goBuildCommand := fmt.Sprintf("heavytest go test -p 2 -c ./subprocess -o %s.building (cwd=%s); rename to duplex-child", os.Getenv("INTEROP_GO_CHILD"), source)
	tsBuildCommand := fmt.Sprintf("heavytest bash -c 'npm ci --ignore-scripts && npm run build' (cwd=%s)", filepath.Join(source, "ts"))
	if strings.Contains(string(build), "race=true") {
		goBuildCommand = fmt.Sprintf("GOWORK=off go test -p 2 -race -c ./subprocess -o %s (cwd=%s)", os.Getenv("INTEROP_GO_CHILD"), source)
		tsBuildCommand = fmt.Sprintf("npm ci --ignore-scripts && npm run build (cwd=%s)", filepath.Join(source, "ts"))
	}
	return map[string]any{"asset_sha256": assetHashes, "asset_build_commands": []string{goBuildCommand, tsBuildCommand}, "sdk_source": "ea8ec0dca862d0c7284cc6a130a4b27fb812ed21", "sdk_module": "5c663e7ce74c40ceceb95133c439396316850858", "sdk_base": "d04ab2149506a96e8c54b58829f58ee480e0de41", "host_base": "60d0b318c264c7ea0dd11bef0731199a3eedadd9", "host_commit": os.Getenv("INTEROP_HOST_COMMIT"), "host_dirty": os.Getenv("INTEROP_HOST_DIRTY") == "true", "manifest_sha256": fmt.Sprintf("%x", digest), "corpus_version": 1, "selector_version": 1, "go_build_receipt": string(build), "ts_build_receipt": string(tsBuild), "runtime_versions": map[string]string{"go": os.Getenv("INTEROP_GO_VERSION"), "node": os.Getenv("INTEROP_NODE_VERSION"), "deno": os.Getenv("INTEROP_DENO_VERSION")}, "scope": "OptionB implementation receipts for selected supported scenarios only; missing mandatory0189/0172/0163 evidence remains open; no unqualified profile or Phase1 acceptance", "transport_base": "98a79bdee1c44d8582c4bc1b8a4395f728057099", "historical_limitation": map[string]any{"case": "arrival-deadline-and-absent-context", "status_at_60d0b318": "pending_host_unsupported", "current_status": "See actual per-runtime raw case execution rows; no pass from implementation alone", "owner": "orch-pp0 host/protocol contract owners"}}
}

func rawEventString(event map[string]json.RawMessage, key string) string {
	var value string
	_ = json.Unmarshal(event[key], &value)
	return value
}

// This is a separate host receipt-clock witness, not a substituted manifest
// deadline vector: a finite parent remains live while a 20ms method ceiling
// expires during an 80ms authority-admission wait.
func TestSDKManifestReceivedBudgetWitness(t *testing.T) {
	if os.Getenv("INTEROP_SDK_SOURCE") == "" {
		t.Skip("isolated script supplies assets")
	}
	var rows []map[string]any
	defer func() {
		path := os.Getenv("INTEROP_RECEIVED_REPORT")
		if path == "" {
			return
		}
		report := interopProvenance(t)
		report["kind"] = "derived host received-budget witness, not shared-vector pass"
		report["observations"] = rows
		report["harness_failed"] = t.Failed()
		raw, err := json.MarshalIndent(report, "", "  ")
		if err == nil {
			err = os.WriteFile(path, append(raw, '\n'), 0600) //nolint:gosec // Explicit opt-in test-owned fixture, control or receipt path; never plugin-provided.
		}
		if err != nil {
			t.Error(err)
		}
	}()
	for _, runtime := range []string{"go", "node", "deno"} {
		for _, ceilingSource := range []string{"shared-offer", "host-only-ceiling"} {
			t.Run(runtime+"/"+ceilingSource, func(t *testing.T) {
				b := &interopBackend{receipts: map[string]subprocess.StoragePutResult{}, inputs: map[string][32]byte{}}
				spec := interopSourceSpec(t, b)
				if ceilingSource == "shared-offer" {
					spec.Init.HostServices.Limits.MethodTimeoutMS[string(HostStoragePut)] = 20
				}
				p, c := startInteropExpanded(t, runtime, interopRecipeRow{Profile: "expanded"}, b, spec)
				if ceilingSource == "host-only-ceiling" {
					// Test-only host policy narrowing leaves the authored wire offer intact.
					p.conn.reverse.business.mu.Lock()
					p.conn.reverse.business.ceilings[HostStoragePut] = 20 * time.Millisecond
					p.conn.reverse.business.mu.Unlock()
				}
				type result struct {
					value subprocess.CommandExecResult
					err   error
				}
				done := make(chan result, 1)
				go func() {
					value, err := interopCommand(context.Background(), p, "put", map[string]any{"n": 1, "key": "write", "operation_key": "receipt-expiry", "gate": true}, "g-StoragePut")
					done <- result{value, err}
				}()
				entered, err := c.eventWhere("entered", func(e map[string]json.RawMessage) bool { return rawEventString(e, "name") == "put" })
				if err != nil {
					t.Fatal(err)
				}
				var parent uint64
				if decodeErr := json.Unmarshal(entered["id"], &parent); decodeErr != nil {
					t.Fatal(decodeErr)
				}
				session := p.conn.reverse.business
				session.mu.Lock()
				if releaseErr := c.release(fmt.Sprintf("request-%d", parent)); releaseErr != nil {
					session.mu.Unlock()
					t.Fatal(releaseErr)
				}
				deadline := time.Now().Add(time.Second)
				for {
					p.conn.mu.Lock()
					admitted := p.conn.reverse.workers == 1
					p.conn.mu.Unlock()
					if admitted {
						break
					}
					if time.Now().After(deadline) {
						session.mu.Unlock()
						t.Fatal("received witness admission timeout")
					}
					time.Sleep(time.Millisecond)
				}
				time.Sleep(80 * time.Millisecond)
				session.mu.Unlock()
				var completed result
				select {
				case completed = <-done:
				case <-time.After(time.Second):
					t.Fatal("received witness terminal timeout")
				}
				if completed.err != nil {
					t.Fatal(completed.err)
				}
				outcomes := interopHelperResults(t, completed.value)
				expectedCode, expectedState := "unknown_outcome", "unknown"
				if ceilingSource == "host-only-ceiling" {
					expectedCode, expectedState = "deadline_exceeded", "not_started"
				}
				if len(outcomes) != 1 || outcomes[0].Code != expectedCode || outcomes[0].EffectState != expectedState {
					t.Fatal("receipt expiry classification", outcomes)
				}
				b.mu.Lock()
				calls := len(b.calls)
				b.mu.Unlock()
				if calls != 0 {
					t.Fatal("backend started after receipt deadline", calls)
				}
				terminal, err := c.eventWhere("wire", func(e map[string]json.RawMessage) bool {
					if rawEventString(e, "direction") != "host-to-worker" {
						return false
					}
					var frame struct {
						Error *subprocess.HostRPCError `json:"error"`
					}
					if json.Unmarshal([]byte(rawEventString(e, "raw")), &frame) != nil || frame.Error == nil {
						return false
					}
					return frame.Error.Data.Code == capability.DeadlineExceeded && frame.Error.Data.EffectState == capability.NotStarted
				})
				if err != nil {
					t.Fatal("missing physical pre-backend deadline refusal", err)
				}
				finished := finishInteropExpanded(t, p, c, 0)
				rows = append(rows, map[string]any{"runtime": runtime, "ceiling_source": ceilingSource, "test_only_policy_narrowing": ceilingSource == "host-only-ceiling", "method_ceiling_ms": 20, "admission_wait_ms": 80, "backend_count": calls, "raw_result": completed.value, "physical_host_terminal": terminal, "finished": finished, "exit_code": 0, "reaped": true, "controls": c.trace, "wire_control_events": c.observed})
			})
		}
	}
}
