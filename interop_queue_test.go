//go:build unix

package pluginhost

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-host/internal/interopfixture"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

// The launch identity is resolved from the authored selector before any command.
// It is independent of worker events, which cannot mint a local-refusal proof.
type interopQueueRef struct {
	Source, Manifest, Recipe, Scenario, Profile, Runtime, Run string
	Corpus, Selector                                          int
}

func newInteropQueueRef(source, runtime string, recipe interopRecipeRow) (interopQueueRef, error) {
	var ref interopQueueRef
	if runtime != "go" && runtime != "node" && runtime != "deno" {
		return ref, errors.New("queue runtime")
	}
	marker, err := os.ReadFile(filepath.Join(source, ".interop-source-commit")) //nolint:gosec // Test-owned pinned asset path.
	if err != nil || string(marker) != "ea8ec0dca862d0c7284cc6a130a4b27fb812ed21\n" {
		return ref, errors.New("queue source")
	}
	raw, err := os.ReadFile(filepath.Join(source, "protocol/v2/fixtures/duplex-child.json")) //nolint:gosec // Test-owned pinned asset path.
	if err != nil {
		return ref, err
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(raw))
	if hash != "30cb07b87060dc88e9cb6ac5414f7b9cb7bb19c7a97c058e284d96e1e59ff4a9" {
		return ref, errors.New("queue manifest")
	}
	rows, err := interopfixture.Select(raw, "normal-serve-negotiated", []string{recipe.Name})
	if err != nil || len(rows) != 1 {
		return ref, errors.New("queue selector")
	}
	var actual interopRecipeRow
	if json.Unmarshal(rows[0], &actual) != nil || actual.Name != recipe.Name || actual.Profile != recipe.Profile || actual.Scenario != recipe.Scenario || (actual.Status != "" && actual.Status != "observed") {
		return ref, errors.New("queue selected recipe")
	}
	validProfile := (recipe.Scenario == "clip" && recipe.Profile == "expanded") || (recipe.Scenario == "queue" && recipe.Profile == "expanded-queue") || (recipe.Scenario == "queue-frames" && recipe.Profile == "expanded-queue-frames")
	if !validProfile {
		return ref, errors.New("queue handler profile")
	}
	var id [16]byte
	if _, err = rand.Read(id[:]); err != nil {
		return ref, err
	}
	return interopQueueRef{string(marker[:len(marker)-1]), hash, recipe.Name, recipe.Scenario, recipe.Profile, runtime, fmt.Sprintf("%x", id), 1, 1}, nil
}

type interopQueueProof struct {
	Ref                                        interopQueueRef
	expected                                   interopQueueRef
	CommandID, HealthID                        uint64
	Name, Args, Binding, Grant                 string
	ArmSequence, ReleaseSequence, WaitingBytes int
	Snapshot                                   map[string]int
	RefusedIndex                               int
	Outcomes                                   []interopHelperOutcome
	BackendCalls, Commits                      int
	Authorities                                []HostAuthority
	HoldStarted, HoldEnded                     time.Time
	HoldMS                                     int64
	ControlTrace                               []string
}

// Only the two frozen producer-local policies can reduce expected publication.
// Physical EOF/terminal reconciliation is still performed by the shared guard.
func (p *interopQueueProof) reduction(events []map[string]json.RawMessage) (uint64, int, error) {
	fail := func() (uint64, int, error) { return 0, 0, errors.New("harness invalid queue local-refusal proof") }
	if p == nil || p.Ref != p.expected || p.Ref.Run == "" || p.Ref.Source != "ea8ec0dca862d0c7284cc6a130a4b27fb812ed21" || p.Ref.Manifest != "30cb07b87060dc88e9cb6ac5414f7b9cb7bb19c7a97c058e284d96e1e59ff4a9" || p.Ref.Corpus != 1 || p.Ref.Selector != 1 || (p.Ref.Runtime != "go" && p.Ref.Runtime != "node" && p.Ref.Runtime != "deno") {
		return fail()
	}
	n, published := 1, 0
	name, profile, grant, args := "put", "expanded-queue", "g-StoragePut", `{"key":"write","n":1,"operation_key":"queued","value_bytes":65536}`
	if p.Ref.Scenario == "queue-frames" {
		n, published = 4, 3
		name, profile, grant, args = "get", "expanded-queue-frames", "g-StorageGet", `{"key":"read","n":4}`
	} else if p.Ref.Scenario == "clip" {
		n, published = 1, 1
		name, profile, grant, args = "get", "expanded", "g-StorageGet", `{"key":"read","n":1}`
		if p.HoldMS < 250 || p.HoldEnded.Sub(p.HoldStarted) < 250*time.Millisecond {
			return fail()
		}
	} else if p.Ref.Scenario != "queue" {
		return fail()
	}
	if p.Ref.Profile != profile || p.Ref.Recipe == "" || p.CommandID == 0 || p.HealthID == 0 || p.HealthID == p.CommandID || p.Name != name || p.Args != args || p.Binding == "" || p.Grant != grant || (published != n && (p.RefusedIndex < 0 || p.RefusedIndex >= n)) || p.WaitingBytes <= 0 || p.ArmSequence <= 0 || p.ReleaseSequence <= p.ArmSequence || len(p.Outcomes) != n || p.BackendCalls != published || p.Commits != 0 {
		return fail()
	}
	if !slices.Contains(p.ControlTrace, fmt.Sprintf("release:arm-writer/ack:%d", p.ArmSequence)) || !slices.Contains(p.ControlTrace, fmt.Sprintf("release:writer/ack:%d", p.ReleaseSequence)) {
		return fail()
	}
	if published == 3 && (p.Snapshot["ordinary_queued"] != 3 || p.Snapshot["control_queued"]+p.Snapshot["reserved_frames"] > 3) {
		return fail()
	}
	good, refused := 0, 0
	for _, o := range p.Outcomes {
		if o.Code == "ok" {
			good++
		} else if o.Code == "rate_limited" && o.EffectState == "not_started" {
			refused++
		} else {
			return fail()
		}
	}
	if good != published || refused != n-published || (published != n && p.Outcomes[p.RefusedIndex].Code != "rate_limited") {
		return fail()
	}
	if len(p.Authorities) != published {
		return fail()
	}
	for _, a := range p.Authorities {
		if a.Parent.ID != p.CommandID || a.Parent.RequestOwner != subprocess.HostRPCOwnerHost || string(a.BindingID) != p.Binding || a.Grant.GrantID != p.Grant || string(a.Method) != "host/storage/"+p.Name {
			return fail()
		}
	}
	if p.Ref.Scenario == "clip" && (p.Snapshot["ordinary_queued"] != 1 || p.Snapshot["reverse_pending"] != 1) {
		return fail()
	}
	if p.Ref.Scenario == "queue" && (p.Snapshot["ordinary_queued"] != 0 || p.Snapshot["reverse_pending"] != 0) {
		return fail()
	}
	indices := map[int]bool{}
	snapshotPosition, healthPosition, waitingPosition, commandPosition := -1, -1, -1, -1
	healthTerminal, entered, armCount, releaseCount := 0, 0, 0, 0
	command, health, waiting, snapshot, terminal, done, arm, release := 0, 0, 0, 0, 0, 0, -1, -1
	refusalPosition := -1
	for pos, e := range events {
		kind := rawEventString(e, "kind")
		if kind == "control_received" {
			var seq int
			_ = json.Unmarshal(e["seq"], &seq)
			if seq == p.ArmSequence {
				arm = pos
				armCount++
			}
			if seq == p.ReleaseSequence {
				release = pos
				releaseCount++
			}
		}
		if kind == "writer_waiting" {
			var bytes int
			_ = json.Unmarshal(e["bytes"], &bytes)
			if bytes == p.WaitingBytes {
				waiting++
				waitingPosition = pos
			}
		}
		if kind == "snapshot" {
			var v map[string]int
			_ = json.Unmarshal(e["effects"], &v)
			if reflect.DeepEqual(v, p.Snapshot) {
				snapshot++
				snapshotPosition = pos
			}
		}
		var id uint64
		_ = json.Unmarshal(e["id"], &id)
		if kind == "entered" && id == p.CommandID {
			var deadline bool
			if rawEventString(e, "name") != p.Name || json.Unmarshal(e["deadline"], &deadline) != nil || !deadline {
				return fail()
			}
			entered++
		}
		if kind == "helper_done" && id == p.CommandID {
			var index int
			var o interopHelperOutcome
			if json.Unmarshal(e["index"], &index) != nil || json.Unmarshal(e["failure"], &o) != nil || index < 0 || index >= n {
				return fail()
			}
			if indices[index] || o != p.Outcomes[index] {
				return fail()
			}
			indices[index] = true
			if o.Code == "rate_limited" {
				if index != p.RefusedIndex || o.EffectState != "not_started" {
					return fail()
				}
				done++
				refusalPosition = pos
			}
		}
		if kind != "wire" {
			continue
		}
		direction := rawEventString(e, "direction")
		frameType := rawEventString(e, "frame_type")
		if p.Ref.Scenario == "clip" && direction == "worker-to-host" && rawEventString(e, "method") == "host/storage/get" {
			var helper struct {
				Params struct {
					Context struct {
						Timeout int64 `json:"timeout_ms"`
					} `json:"context"`
				} `json:"params"`
			}
			if json.Unmarshal([]byte(rawEventString(e, "raw")), &helper) != nil || helper.Params.Context.Timeout <= 0 || helper.Params.Context.Timeout > 4800 {
				return fail()
			}
		}
		ownedDirection := (direction == "host-to-worker" && frameType == "request") || (direction == "worker-to-host" && frameType == "response")
		if (id != p.CommandID && id != p.HealthID) || !ownedDirection {
			continue
		}
		var frame struct {
			Method string `json:"method"`
			Params struct {
				Name    string `json:"name"`
				Args    string `json:"args"`
				Context struct {
					Binding string `json:"binding_id"`
				} `json:"context"`
			} `json:"params"`
			Result subprocess.CommandExecResult `json:"result"`
		}
		if json.Unmarshal([]byte(rawEventString(e, "raw")), &frame) != nil {
			return fail()
		}
		if direction == "host-to-worker" && id == p.CommandID {
			if frame.Method != "command/execute" || frame.Params.Name != p.Name || frame.Params.Args != p.Args || frame.Params.Context.Binding != p.Binding {
				return fail()
			}
			command++
			commandPosition = pos
		}
		if direction == "host-to-worker" && id == p.HealthID {
			var envelope map[string]json.RawMessage
			_ = json.Unmarshal([]byte(rawEventString(e, "raw")), &envelope)
			if frame.Method != "plugin/health" || string(envelope["params"]) != "{}" {
				return fail()
			}
			health++
			healthPosition = pos
		}
		if direction == "worker-to-host" && rawEventString(e, "frame_type") == "response" && id == p.HealthID {
			var bytes int
			_ = json.Unmarshal(e["bytes"], &bytes)
			healthTerminal++
			var envelope struct {
				Result struct {
					OK bool `json:"ok"`
				} `json:"result"`
			}
			if json.Unmarshal([]byte(rawEventString(e, "raw")), &envelope) != nil || !envelope.Result.OK {
				return fail()
			}
			if bytes != p.WaitingBytes || bytes != len(rawEventString(e, "raw")) {
				return fail()
			}
		}
		if direction == "worker-to-host" && rawEventString(e, "frame_type") == "response" && id == p.CommandID {
			var outcomes []interopHelperOutcome
			if json.Unmarshal([]byte(frame.Result.Content), &outcomes) != nil || !reflect.DeepEqual(outcomes, p.Outcomes) {
				return fail()
			}
			terminal++
		}
	}
	if command != 1 || health != 1 || waiting != 1 || snapshot == 0 || terminal != 1 || done != n-published || entered != 1 || len(indices) != n || healthTerminal != 1 || armCount != 1 || releaseCount != 1 || arm < 0 || release <= arm || healthPosition <= arm || waitingPosition <= healthPosition || commandPosition <= waitingPosition || snapshotPosition <= commandPosition || snapshotPosition >= release || (published != n && (refusalPosition <= commandPosition || refusalPosition >= release)) {
		return fail()
	}
	return p.CommandID, n - published, nil
}

func replayInteropQueue(t *testing.T, p *Process, c *interopControls, b *interopBackend, runtime string, recipe interopRecipeRow, observation map[string]any) {
	t.Helper()
	ref, err := newInteropQueueRef(os.Getenv("INTEROP_SDK_SOURCE"), runtime, recipe)
	if err != nil {
		t.Fatal(err)
	}
	proof := &interopQueueProof{Ref: ref, expected: ref, RefusedIndex: -1}
	if err = c.release("arm-writer"); err != nil {
		t.Fatal(err)
	}
	proof.ArmSequence = c.seq
	observer, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type healthOutcome struct {
		raw json.RawMessage
		err error
	}
	health := make(chan healthOutcome, 1)
	go func() {
		raw, e := p.conn.CallCorrelation(observer, subprocess.MethodHealth, json.RawMessage(`{}`))
		health <- healthOutcome{raw, e}
	}()
	wire, err := c.eventWhere("wire", func(e map[string]json.RawMessage) bool {
		return rawEventString(e, "direction") == "host-to-worker" && rawEventString(e, "method") == "plugin/health"
	})
	if err != nil {
		t.Fatal(err)
	}
	if json.Unmarshal(wire["id"], &proof.HealthID) != nil || proof.HealthID == 0 {
		t.Fatal("Health ID")
	}
	waiting, err := c.event("writer_waiting")
	if err != nil {
		t.Fatal(err)
	}
	if json.Unmarshal(waiting["bytes"], &proof.WaitingBytes) != nil || proof.WaitingBytes <= 0 {
		t.Fatal("writer waiting bytes")
	}
	name, grant, args, n := "get", "g-StorageGet", map[string]any{"n": 1, "key": "read"}, 1
	switch recipe.Scenario {
	case "queue":
		name, grant, args = "put", "g-StoragePut", map[string]any{"n": 1, "key": "write", "operation_key": "queued", "value_bytes": 65536}
	case "queue-frames":
		n = 4
		args["n"] = 4
	}
	proof.Name, proof.Grant = name, grant
	type commandOutcome struct {
		value subprocess.CommandExecResult
		err   error
	}
	command := make(chan commandOutcome, 1)
	parentTimeout := 10 * time.Second
	if recipe.Scenario == "clip" {
		parentTimeout = 5 * time.Second
	}
	parent, end := context.WithTimeout(context.Background(), parentTimeout)
	defer end()
	go func() { r, e := interopCommand(parent, p, name, args, grant); command <- commandOutcome{r, e} }()
	proof.CommandID, _ = interopEnteredCommand(t, c, name)
	for _, e := range c.observed {
		if rawEventString(e, "kind") == "wire" && rawEventString(e, "direction") == "host-to-worker" {
			var id uint64
			_ = json.Unmarshal(e["id"], &id)
			if id == proof.CommandID {
				var frame struct {
					Params struct {
						Args    string `json:"args"`
						Context struct {
							Binding string `json:"binding_id"`
						} `json:"context"`
					} `json:"params"`
				}
				if json.Unmarshal([]byte(rawEventString(e, "raw")), &frame) != nil {
					t.Fatal("command wire")
				}
				proof.Args, proof.Binding = frame.Params.Args, frame.Params.Context.Binding
			}
		}
	}
	if recipe.Scenario != "clip" {
		e, eerr := c.eventWhere("helper_done", func(e map[string]json.RawMessage) bool {
			var id uint64
			var o interopHelperOutcome
			return json.Unmarshal(e["id"], &id) == nil && id == proof.CommandID && json.Unmarshal(e["failure"], &o) == nil && o.Code == "rate_limited" && o.EffectState == "not_started"
		})
		if eerr != nil {
			t.Fatal(eerr)
		}
		if json.Unmarshal(e["index"], &proof.RefusedIndex) != nil {
			t.Fatal("helper index")
		}
	}
	limit := time.Now().Add(time.Second)
	for {
		proof.Snapshot = interopSnapshot(t, c)
		if recipe.Scenario == "queue" || (recipe.Scenario == "clip" && proof.Snapshot["ordinary_queued"] == 1 && proof.Snapshot["reverse_pending"] == 1) || (recipe.Scenario == "queue-frames" && proof.Snapshot["ordinary_queued"] == 3 && proof.Snapshot["control_queued"]+proof.Snapshot["reserved_frames"] <= 3) {
			break
		}
		if time.Now().After(limit) {
			t.Fatal("blocked queue snapshot", proof.Snapshot)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if recipe.Scenario == "clip" {
		held := time.Now()
		proof.HoldStarted = held
		time.Sleep(250 * time.Millisecond)
		proof.HoldEnded = time.Now()
		proof.HoldMS = proof.HoldEnded.Sub(held).Milliseconds()
	}
	b.mu.Lock()
	before := len(b.calls)
	b.mu.Unlock()
	if before != 0 {
		t.Fatal("backend ran before blocked writer release", before)
	}
	if err = c.release("writer"); err != nil {
		t.Fatal(err)
	}
	proof.ReleaseSequence = c.seq
	proof.ControlTrace = append([]string{}, c.trace...)
	select {
	case h := <-health:
		if h.err != nil {
			t.Fatal(h.err)
		}
		var result struct {
			OK bool `json:"ok"`
		}
		if json.Unmarshal(h.raw, &result) != nil || !result.OK {
			t.Fatal("Health result", string(h.raw))
		}
	case <-observer.Done():
		t.Fatal("Health observer expired")
	}
	var result commandOutcome
	select {
	case result = <-command:
		if result.err != nil {
			t.Fatal(result.err)
		}
	case <-parent.Done():
		t.Fatal("command observer expired")
	}
	proof.Outcomes = interopHelperResults(t, result.value)
	b.mu.Lock()
	proof.BackendCalls, proof.Commits = len(b.calls), b.commits
	proof.Authorities = append([]HostAuthority{}, b.calls...)
	b.mu.Unlock()
	expected := n
	if recipe.Scenario != "clip" {
		expected--
	}
	if proof.BackendCalls != expected || proof.Commits != 0 || len(proof.Outcomes) != n {
		t.Fatal("queue backend/result counts", proof)
	}
	if recipe.Scenario == "clip" {
		if proof.Outcomes[0].Code != "ok" || proof.HoldMS < 250 {
			t.Fatal("clip result/hold", proof)
		}
		e, eerr := c.eventWhere("wire", func(e map[string]json.RawMessage) bool {
			return rawEventString(e, "direction") == "worker-to-host" && rawEventString(e, "method") == "host/storage/get"
		})
		if eerr != nil {
			t.Fatal(eerr)
		}
		var frame struct {
			Params struct {
				Context struct {
					Timeout int64 `json:"timeout_ms"`
				} `json:"context"`
			} `json:"params"`
		}
		if json.Unmarshal([]byte(rawEventString(e, "raw")), &frame) != nil || frame.Params.Context.Timeout <= 0 || frame.Params.Context.Timeout > 4800 {
			t.Fatal("queue debit wire budget", rawEventString(e, "raw"))
		}
		observation["actual_queued_wire_timeout_ms"] = frame.Params.Context.Timeout
	}
	c.queueProof = proof
	observation["queue_proof"] = proof
}

// Adversarial copies are guard probes, not additional child replay receipts.
func auditInteropQueueCopies(t *testing.T, c *interopControls) {
	t.Helper()
	original := c.queueProof
	if original == nil {
		t.Fatal("missing queue run proof")
	}
	for _, variant := range []string{"foreign-run", "foreign-source", "foreign-parent", "foreign-index", "foreign-binding", "missing-snapshot", "missing-ack", "unadvertised-request", "notification", "duplicate-helper", "unclipped-budget", "wrong-terminal"} {
		raw, _ := json.Marshal(original)
		var copied interopQueueProof
		if err := json.Unmarshal(raw, &copied); err != nil {
			t.Fatal(err)
		}
		copied.expected = original.expected
		encoded, _ := json.Marshal(c.observed)
		var events []map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &events); err != nil {
			t.Fatal(err)
		}
		switch variant {
		case "foreign-run":
			copied.Ref.Run = "foreign"
		case "foreign-source":
			copied.Ref.Source = "foreign"
		case "foreign-parent":
			copied.CommandID++
		case "foreign-index":
			if copied.Ref.Scenario == "clip" {
				continue
			}
			copied.RefusedIndex = -1
		case "foreign-binding":
			copied.Binding = "foreign"
		case "missing-snapshot":
			copied.Snapshot = map[string]int{}
		case "missing-ack":
			copied.ControlTrace = nil
		case "unadvertised-request":
			events = append(events, queueGuardWire("worker-to-host", "request", copied.CommandID+100000, "host/unadvertised", map[string]any{}, nil), queueGuardWire("host-to-worker", "response", copied.CommandID+100000, "", nil, map[string]any{}))
		case "notification":
			events = append(events, queueGuardEvent(map[string]any{"kind": "wire", "direction": "worker-to-host", "frame_type": "notification", "method": "host/unadvertised", "raw": `{"jsonrpc":"2.0","method":"host/unadvertised","params":{}}`}))
		case "unclipped-budget":
			if copied.Ref.Scenario != "clip" {
				continue
			}
			for _, e := range events {
				if rawEventString(e, "direction") == "worker-to-host" && rawEventString(e, "method") == "host/storage/get" {
					var frame map[string]any
					_ = json.Unmarshal([]byte(rawEventString(e, "raw")), &frame)
					frame["params"].(map[string]any)["context"].(map[string]any)["timeout_ms"] = 5000
					raw, _ := json.Marshal(frame)
					e["raw"], _ = json.Marshal(string(append(raw, '\n')))
				}
			}
		case "wrong-terminal":
			for _, e := range events {
				var id uint64
				_ = json.Unmarshal(e["id"], &id)
				if rawEventString(e, "direction") == "worker-to-host" && rawEventString(e, "frame_type") == "response" && id == copied.CommandID {
					var frame map[string]any
					_ = json.Unmarshal([]byte(rawEventString(e, "raw")), &frame)
					frame["result"] = map[string]any{"content": `[{"code":"unknown_outcome","effect_state":"unknown"}]`}
					raw, _ := json.Marshal(frame)
					e["raw"], _ = json.Marshal(string(append(raw, '\n')))
				}
			}
		case "duplicate-helper":
			for _, e := range events {
				var id uint64
				_ = json.Unmarshal(e["id"], &id)
				if rawEventString(e, "kind") == "helper_done" && id == copied.CommandID {
					events = append(events, e)
					break
				}
			}
		}
		if interopExpandedHelpers(events, c.expandedReverseLimit, &copied) == nil && interopWireTerminals(events, 0) == nil {
			t.Fatalf("queue copied-trace guard accepted %s", variant)
		}
	}
	if original.Ref.Scenario != "clip" && interopExpandedHelpers(c.observed, c.expandedReverseLimit) == nil {
		t.Fatal("absent scoped local-refusal proof accepted actual trace")
	}
}
