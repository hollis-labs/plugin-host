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
	"testing"
	"time"

	"github.com/hollis-labs/plugin-host/internal/interopfixture"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

func newInteropHungRef(source, runtime string, recipe interopRecipeRow) (interopQueueRef, error) {
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
	validProfile := recipe.Scenario == "hung-callback" && recipe.Profile == "expanded-hung"
	if !validProfile {
		return ref, errors.New("queue handler profile")
	}
	var id [16]byte
	if _, err = rand.Read(id[:]); err != nil {
		return ref, err
	}
	return interopQueueRef{string(marker[:len(marker)-1]), hash, recipe.Name, recipe.Scenario, recipe.Profile, runtime, fmt.Sprintf("%x", id), 1, 1}, nil
}

type interopHungProof struct {
	Ref                            interopQueueRef
	expected                       interopQueueRef
	CommandID                      uint64
	HalfClosed                     bool
	LocalCompleted, CustodyRetired bool
}

func replayInteropHung(t *testing.T, p *Process, c *interopControls, b *interopBackend, runtime string, recipe interopRecipeRow, row map[string]any) {
	t.Helper()
	ref, err := newInteropHungRef(os.Getenv("INTEROP_SDK_SOURCE"), runtime, recipe)
	if err != nil {
		t.Fatal(err)
	}
	proof := &interopHungProof{Ref: ref, expected: ref}
	c.hungProof = proof
	observer, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, e := p.conn.CallCorrelation(observer, subprocess.MethodCommandExecute, subprocess.CommandExecParams{Name: "hung", Args: "{}", SessionID: ""})
		done <- e
	}()
	input, err := c.eventWhere("wire", func(e map[string]json.RawMessage) bool {
		return rawEventString(e, "direction") == "host-to-worker" && rawEventString(e, "method") == subprocess.MethodCommandExecute
	})
	if err != nil || json.Unmarshal(input["id"], &proof.CommandID) != nil || proof.CommandID == 0 {
		t.Fatal("hung actual input", err)
	}
	entered, err := c.eventWhere("entered", func(e map[string]json.RawMessage) bool {
		var id uint64
		_ = json.Unmarshal(e["id"], &id)
		return id == proof.CommandID
	})
	var deadline bool
	if err != nil || rawEventString(entered, "name") != "hung" || json.Unmarshal(entered["deadline"], &deadline) != nil || deadline {
		t.Fatal("hung entered", err, entered)
	}
	halfcloseAt := time.Now()
	if err = p.stdin.Close(); err != nil {
		t.Fatal("hung halfclose", err)
	}
	proof.HalfClosed = true
	select {
	case <-p.exited:
	case <-observer.Done():
		t.Fatal("hung natural exit missing", observer.Err())
	}
	exit, ok := p.ExitInfo()
	if !ok || exit.Code != 1 || exit.Signal != "" {
		t.Fatal("hung natural Process exit", exit)
	}
	select {
	case err = <-done:
	case <-observer.Done():
		t.Fatal("hung local call completion missing")
	}
	if err == nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatal("hung local error not actual transport retirement", err)
	}
	localError := err.Error()
	proof.LocalCompleted = true
	select {
	case <-p.conn.Done():
	case <-observer.Done():
		t.Fatal("hung Conn reader EOF missing")
	}
	p.conn.mu.Lock()
	p.conn.queue.mu.Lock()
	row["local_custody_before_fallback"] = map[string]any{"pending": len(p.conn.pending), "correlations": len(p.conn.correlations), "ordinary_calls": p.conn.ordinaryCalls, "lifecycle_calls": p.conn.lifecycleCalls, "inbound_active": len(p.conn.inboundActive), "active_reply_receipt": p.conn.activeReceipt != nil, "reserved_control_credits": p.conn.queue.reserved, "active_write": p.conn.queue.active != nil, "outbound": len(p.conn.outbound)}
	proof.CustodyRetired = len(p.conn.pending) == 0 && len(p.conn.correlations) == 0 && p.conn.ordinaryCalls == 0 && p.conn.lifecycleCalls == 0 && len(p.conn.inboundActive) == 0 && p.conn.activeReceipt == nil && p.conn.queue.reserved == 0 && p.conn.queue.active == nil && len(p.conn.outbound) == 0
	p.conn.queue.mu.Unlock()
	p.conn.mu.Unlock()
	if !proof.CustodyRetired {
		t.Fatal("hung local call/credit custody remains before fallback")
	}
	finished, err := c.event("finished")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.finish(1); err != nil {
		t.Fatal(err)
	}
	auditInteropHungCopies(t, c)
	row["hung_copied_trace_adverse_audit"] = true
	b.mu.Lock()
	calls, commits := len(b.calls), b.commits
	b.mu.Unlock()
	if calls != 0 || commits != 0 {
		t.Fatal("hung backend effects", calls, commits)
	}
	row["finished"] = finished
	row["hung_proof"] = proof
	row["stdin_halfclose_at"] = halfcloseAt
	row["natural_exit_elapsed_ms"] = time.Since(halfcloseAt).Milliseconds()
	row["local_call_error"] = localError
	row["no_fallback_before_proof"] = true
	row["projection"] = "Genuine Init+Load; actual hung correlation-only command with absent wire context; stdin halfclose without Unload; no delegated authority"
}

func auditInteropHungCopies(t *testing.T, c *interopControls) {
	t.Helper()
	for _, variant := range []string{"extra-pair", "unowned-cancel", "missing-load", "fabricated-terminal", "returned", "late-error"} {
		events := append([]map[string]json.RawMessage{}, c.observed...)
		failure := io.EOF
		switch variant {
		case "extra-pair":
			events = append(events, queueGuardWire("host-to-worker", "request", c.hungProof.CommandID+100, "plugin/health", map[string]any{}, nil), queueGuardWire("worker-to-host", "response", c.hungProof.CommandID+100, "", nil, map[string]any{"ok": true}))
		case "unowned-cancel":
			raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "rpc/cancel", "params": map[string]any{"request_owner": "host", "id": c.hungProof.CommandID, "reason": "caller_cancelled"}}) //nolint:misspell // Exact SDK wire reason.
			raw = append(raw, '\n')
			events = append(events, queueGuardEvent(map[string]any{"kind": "wire", "direction": "host-to-worker", "frame_type": "notification", "method": "rpc/cancel", "raw": string(raw), "bytes": len(raw)}))
		case "missing-load":
			for i, e := range events {
				if rawEventString(e, "kind") == "wire" && rawEventString(e, "direction") == "host-to-worker" && rawEventString(e, "method") == subprocess.MethodLoad {
					events = append(events[:i], events[i+1:]...)
					break
				}
			}
		case "fabricated-terminal":
			events = append(events, queueGuardWire("worker-to-host", "response", c.hungProof.CommandID, "", nil, map[string]any{}))
		case "returned":
			events = append(events, queueGuardEvent(map[string]any{"kind": "returned", "id": c.hungProof.CommandID}))
		case "late-error":
			failure = errors.Join(io.EOF, errors.New("late control failure"))
		}
		copied := &interopControls{hungProof: c.hungProof, observed: events, events: make(chan map[string]json.RawMessage), failure: make(chan error, 1), expandedReverseLimit: c.expandedReverseLimit}
		copied.failure <- failure
		if copied.finish(1) == nil {
			t.Fatal("hung copied trace accepted", variant)
		}
	}
}
