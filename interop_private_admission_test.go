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
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-host/internal/interopfixture"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

// These identities are host-owned before launch. No wire event creates one.
type interopScenarioRef struct {
	Source, Manifest, Recipe, Scenario, Runtime, Run string
	Corpus, Selector                                 int
}

func (r interopScenarioRef) valid() bool {
	return r.Source == "ea8ec0dca862d0c7284cc6a130a4b27fb812ed21" && r.Manifest == "30cb07b87060dc88e9cb6ac5414f7b9cb7bb19c7a97c058e284d96e1e59ff4a9" && r.Corpus == 1 && r.Selector == 1 && r.Recipe == "host-writer-four-frame-fairness" && r.Scenario == "host-fairness" && (r.Runtime == "go" || r.Runtime == "node" || r.Runtime == "deno") && regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(r.Run)
}

func interopPrivateRef(source, runtime string) (interopScenarioRef, error) {
	var ref interopScenarioRef
	if runtime != "go" && runtime != "node" && runtime != "deno" {
		return ref, errors.New("private runtime")
	}
	marker, err := os.ReadFile(filepath.Join(source, ".interop-source-commit")) //nolint:gosec // Explicit test-owned source path.
	if err != nil || string(marker) != "ea8ec0dca862d0c7284cc6a130a4b27fb812ed21\n" {
		return ref, errors.New("private exact source")
	}
	raw, err := os.ReadFile(filepath.Join(source, "protocol/v2/fixtures/duplex-child.json")) //nolint:gosec // Explicit test-owned source path.
	if err != nil {
		return ref, err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(raw))
	if digest != "30cb07b87060dc88e9cb6ac5414f7b9cb7bb19c7a97c058e284d96e1e59ff4a9" {
		return ref, errors.New("private exact manifest")
	}
	rows, err := interopfixture.Select(raw, "normal-serve-negotiated", []string{"host-writer-four-frame-fairness"})
	if err != nil || len(rows) != 1 {
		return ref, errors.New("private selected recipe")
	}
	var row interopRecipeRow
	if err := json.Unmarshal(rows[0], &row); err != nil || row.Name != "host-writer-four-frame-fairness" || row.Scenario != "host-fairness" || row.Profile != "expanded" || (row.Status != "" && row.Status != "observed") {
		return ref, errors.New("private authored scenario")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return ref, err
	}
	id[6] = (id[6] & 15) | 64
	id[8] = (id[8] & 63) | 128
	return interopScenarioRef{Source: "ea8ec0dca862d0c7284cc6a130a4b27fb812ed21", Manifest: digest, Recipe: row.Name, Scenario: row.Scenario, Runtime: runtime, Run: fmt.Sprintf("%x-%x-%x-%x-%x", id[:4], id[4:6], id[6:8], id[8:10], id[10:]), Corpus: 1, Selector: 1}, nil
}

type interopPrivateStore struct {
	mu        sync.Mutex
	ref       interopScenarioRef
	records   []json.RawMessage
	bytes     int
	snapshots int
	failure   error
}

func (s *interopPrivateStore) add(v any) error {
	raw, err := json.Marshal(v)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failure != nil {
		return s.failure
	}
	if _, snapshot := v.(interopAdmissionSnapshot); snapshot {
		s.snapshots++
		if s.snapshots > 64 {
			s.failure = errors.New("private snapshot overflow")
			return s.failure
		}
	}
	if err != nil || len(raw) > 4096 || len(s.records) >= 256 || s.bytes+len(raw) > 256<<10 {
		s.failure = errors.New("private metadata overflow")
		return s.failure
	}
	s.records = append(s.records, raw)
	s.bytes += len(raw)
	return nil
}

// Conn -> queue, then separately Conn -> session. Never triple nesting.
type interopAdmissionSnapshot struct {
	Ref                                   interopScenarioRef
	Ordinary, Lifecycle, Pending, Inbound int
	IDs                                   []subprocess.RPCID
	// HighWater records the base/refusal stream, not negotiated reverse.high.
	HighWater                                                                                  int64
	OrdinaryFrames, OrdinaryBytes, ControlFrames, ControlBytes, Reserved, ReservedBytes, Burst int
	Active, OrdinaryEligible                                                                   bool
	ActiveFrameSequence                                                                        int
}

func interopSnapshotAdmission(c *Conn, ref interopScenarioRef, writer *interopPrivateWriter) (interopAdmissionSnapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := interopAdmissionSnapshot{Ref: ref, Ordinary: c.ordinaryCalls, Lifecycle: c.lifecycleCalls, Pending: len(c.pending), Inbound: len(c.inboundActive), HighWater: c.inboundHighWater}
	if len(c.pending) > 64 {
		return s, errors.New("private ID collection overflow")
	}
	for id := range c.pending {
		s.IDs = append(s.IDs, id)
	}
	c.queue.mu.Lock()
	s.OrdinaryFrames = len(c.queue.lanes[ordinaryLane].frames)
	s.OrdinaryBytes = c.queue.lanes[ordinaryLane].bytes
	s.ControlFrames = len(c.queue.lanes[controlLane].frames)
	s.ControlBytes = c.queue.lanes[controlLane].bytes
	s.Reserved = c.queue.reserved
	s.ReservedBytes = s.Reserved * terminalBytes
	s.Burst = c.queue.burst
	s.Active = c.queue.active != nil
	active := c.queue.active
	s.OrdinaryEligible = s.OrdinaryFrames > 0
	c.queue.mu.Unlock()
	writer.mu.Lock()
	s.ActiveFrameSequence = writer.frames[active]
	writer.mu.Unlock()
	return s, nil
}

func interopPrivateBound(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("private finite observer required")
	}
	return ctx.Err()
}

func TestPrivateInteropMetadataOverflowIsStickyFailure(t *testing.T) {
	store := &interopPrivateStore{}
	if err := store.add(strings.Repeat("x", 4097)); err == nil {
		t.Fatal("oversized metadata accepted")
	}
	if err := store.add("small"); err == nil {
		t.Fatal("overflow hidden by later valid record")
	}
	store = &interopPrivateStore{}
	for range 64 {
		if err := store.add(interopAdmissionSnapshot{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.add(interopAdmissionSnapshot{}); err == nil {
		t.Fatal("snapshot bound exceeded")
	}
	if len(store.records) != 64 {
		t.Fatal("overflow recorded as successful metadata")
	}
}

func TestPrivateInteropIdentityAndIntentCannotBorrowAnotherRun(t *testing.T) {
	ref := interopScenarioRef{Source: "ea8ec0dca862d0c7284cc6a130a4b27fb812ed21", Manifest: "30cb07b87060dc88e9cb6ac5414f7b9cb7bb19c7a97c058e284d96e1e59ff4a9", Corpus: 1, Selector: 1, Recipe: "host-writer-four-frame-fairness", Scenario: "host-fairness", Runtime: "go", Run: "12345678-1234-4234-8234-123456789abc"}
	observer, end := context.WithTimeout(context.Background(), time.Second)
	defer end()
	for _, alter := range []func(*interopScenarioRef){func(r *interopScenarioRef) { r.Runtime = "future" }, func(r *interopScenarioRef) { r.Selector = 2 }, func(r *interopScenarioRef) { r.Recipe = "other" }, func(r *interopScenarioRef) { r.Run = "not-a-run" }} {
		other := ref
		alter(&other)
		if _, err := newInteropPrivatePlan(observer, other, &interopPrivateStore{ref: other}); err == nil {
			t.Fatal("unresolved private identity accepted", other)
		}
	}
	plan, err := newInteropPrivatePlan(observer, ref, &interopPrivateStore{ref: ref})
	if err != nil {
		t.Fatal(err)
	}
	intent := plan.intents[0]
	intent.Ref.Run = "12345678-1234-4234-8234-123456789abd"
	if err := plan.publish(intent, 1); err == nil {
		t.Fatal("foreign run intent accepted")
	}
	if plan.receipts[0] != nil {
		t.Fatal("foreign intent acquired admission")
	}
	if err := plan.publish(plan.intents[0], 1); err == nil {
		t.Fatal("intent without actual write dependency accepted")
	}
	if plan.receipts[0] != nil {
		t.Fatal("premature intent acquired admission")
	}
}
