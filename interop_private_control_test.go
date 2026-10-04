//go:build unix

package pluginhost

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-sdk/subprocess"
)

type interopControlIntent struct {
	Ref                                      interopScenarioRef
	ID, Dependency                           int
	Origin, Direction, Method, Owner, Reason string
	Target                                   uint64
	MaxEnqueues                              int
}
type interopControlReceipt struct {
	Intent      interopControlIntent
	frame       *queuedFrame
	written     chan error
	Admission   string
	Completed   bool
	WriterError string
}
type interopPrivatePlan struct {
	mu              sync.Mutex
	ref             interopScenarioRef
	observer        context.Context
	conn            *Conn
	incarnation     *reverseConnection
	store           *interopPrivateStore
	intents         [31]interopControlIntent
	receipts        [31]*interopControlReceipt
	controls, seeds int
	seedIDs         map[subprocess.RPCID]bool
	seedWritten     map[subprocess.RPCID]bool
	dependencies    [31]time.Time
}

func newInteropPrivatePlan(observer context.Context, ref interopScenarioRef, store *interopPrivateStore) (*interopPrivatePlan, error) {
	if err := interopPrivateBound(observer); err != nil {
		return nil, err
	}
	if store.ref != ref || !ref.valid() {
		return nil, errors.New("private resolved identity mismatch")
	}
	p := &interopPrivatePlan{ref: ref, observer: observer, store: store, seedIDs: map[subprocess.RPCID]bool{}, seedWritten: map[subprocess.RPCID]bool{}}
	for i := range p.intents {
		p.intents[i] = interopControlIntent{Ref: ref, ID: i + 1, Dependency: i + 1, Origin: "authored-hostFairness-refill", Direction: "host-to-worker", Method: "rpc/cancel", Owner: "host", Target: 9999, Reason: "caller_cancelled", MaxEnqueues: 1} //nolint:misspell // Exact SDK caller_cancelled wire value.
	}
	return p, nil
}

func (p *interopPrivatePlan) intentFor(frame *queuedFrame) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, r := range p.receipts {
		if r != nil && r.frame == frame {
			return i + 1
		}
	}
	return 0
}

func (p *interopPrivatePlan) afterWholeWrite(w interopWriteWitness) error {
	if w.Ref != p.ref || w.UnderlyingN != w.Bytes || w.UnderlyingError != "" {
		return errors.New("private dependency lacks full underlying write")
	}
	if err := interopPrivateBound(p.observer); err != nil {
		return err
	}
	if w.Deadline.IsZero() || !time.Now().Before(w.Deadline) {
		return context.DeadlineExceeded
	}
	p.mu.Lock()
	if w.Kind == "reverse_terminal" {
		if !p.seedIDs[w.ID] || p.seedWritten[w.ID] {
			p.mu.Unlock()
			return errors.New("private unowned seed terminal")
		}
		p.seedWritten[w.ID] = true
		p.seeds++
		if p.seeds > 8 {
			p.mu.Unlock()
			return errors.New("private excess seed replies")
		}
	} else if w.Kind != "authored_cancel" || w.Intent < 1 || w.Intent > 31 || p.receipts[w.Intent-1] == nil {
		p.mu.Unlock()
		return errors.New("private foreign control write")
	}
	p.controls++
	ordinal := p.controls
	if ordinal < 32 {
		p.dependencies[ordinal-1] = w.Deadline
	}
	p.mu.Unlock()
	if ordinal > 39 {
		return errors.New("private excess control writes")
	}
	if ordinal < 32 {
		return p.publish(p.intents[ordinal-1], ordinal)
	}
	return nil
}

// Driver only: written is the actual Conn loop's completion, not an ACK. This
// runs after refills, never while a physical writer awaits enqueue admission.
func (p *interopPrivatePlan) complete(writes []interopWriteWitness) error {
	for i := range p.receipts {
		p.mu.Lock()
		receipt := p.receipts[i]
		p.mu.Unlock()
		if receipt == nil || receipt.Admission != "admitted" {
			return errors.New("private missing admitted intent")
		}
		var err error
		select {
		case err = <-receipt.written:
		case <-p.observer.Done():
			return p.observer.Err()
		case <-p.conn.done:
			return p.conn.gone()
		}
		p.mu.Lock()
		receipt.Completed = true
		receipt.WriterError = interopPrivateError(err)
		p.mu.Unlock()
		if err != nil {
			return err
		}
		found := 0
		for _, w := range writes {
			if w.Intent == receipt.Intent.ID {
				found++
				if w.Ref != p.ref || w.Kind != "authored_cancel" || w.UnderlyingN != w.Bytes || w.UnderlyingError != "" || w.AckError != "" {
					return errors.New("private control completion is not full physical publication")
				}
			}
		}
		if found != 1 {
			return errors.New("private missing or duplicate physical receipt")
		}
		if err := p.store.add(struct {
			Intent      interopControlIntent
			Completed   bool
			WriterError string
		}{receipt.Intent, true, receipt.WriterError}); err != nil {
			return err
		}
	}
	return nil
}

// The only EOF exemption is the exact pre-owned intent/whole-write ledger. A
// filtered copy is used by the ordinary strict guard; original events survive.
func (p *interopPrivatePlan) guard(events []map[string]json.RawMessage, writes []interopWriteWitness) ([]map[string]json.RawMessage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	owned := map[string]int{}
	for _, r := range p.receipts {
		if r == nil || !r.Completed || r.WriterError != "" {
			return nil, errors.New("private EOF intent incomplete")
		}
		for _, w := range writes {
			if w.Intent == r.Intent.ID {
				if w.Ref != p.ref || w.UnderlyingN != w.Bytes || w.UnderlyingError != "" || w.AckError != "" {
					return nil, errors.New("private EOF receipt failed")
				}
				owned[w.SHA256]++
			}
		}
	}
	if len(owned) == 0 {
		return nil, errors.New("private EOF lacks physical intent witnesses")
	}
	var filtered []map[string]json.RawMessage
	consumed := 0
	for _, event := range events {
		if rawEventString(event, "kind") == "wire" && rawEventString(event, "direction") == "host-to-worker" && rawEventString(event, "frame_type") == "notification" {
			raw := rawEventString(event, "raw")
			digest := fmt.Sprintf("%x", sha256.Sum256([]byte(raw)))
			if owned[digest] > 0 {
				owned[digest]--
				consumed++
				continue
			}
			// This selected scenario authors only the pre-owned refill controls.
			// A known live or completed request does not create a further intent,
			// driver cause or physical receipt. Other scenarios retain their own
			// caller/observer/descendant cancellation validation.
			return nil, errors.New("private EOF unowned host control")
		}
		filtered = append(filtered, event)
	}
	if consumed != 31 {
		return nil, errors.New("private EOF missing bounded authored controls")
	}
	for _, remaining := range owned {
		if remaining != 0 {
			return nil, errors.New("private EOF physical/wire mismatch")
		}
	}
	return filtered, nil
}

// Closed private port: no arbitrary method, ID, lease or raw-control API. A
// pre-owned intent is consumed once before enqueue. Metadata precedes visibility.
func (p *interopPrivatePlan) publish(intent interopControlIntent, dependency int) error {
	if err := interopPrivateBound(p.observer); err != nil {
		return err
	}
	if intent.ID < 1 || intent.ID > 31 || intent != p.intents[intent.ID-1] || intent.Dependency != dependency || intent.Ref != p.ref {
		return errors.New("private foreign intent")
	}
	raw, err := json.Marshal(subprocess.RPCRequest{JSONRPC: "2.0", Method: "rpc/cancel", Params: subprocess.CancelParams{RequestOwner: subprocess.HostRPCOwnerHost, ID: subprocess.NumberID(9999), Reason: subprocess.CallerCancelled}})
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	decoded, err := decodeWireEnvelope(raw)
	if err != nil || !decoded.request || decoded.id != (subprocess.RPCID{}) || decoded.method != "rpc/cancel" {
		return errors.New("private control envelope")
	}
	p.mu.Lock()
	if p.controls != dependency || p.receipts[intent.ID-1] != nil {
		p.mu.Unlock()
		return errors.New("private duplicate or premature intent")
	}
	deadline := p.dependencies[intent.ID-1]
	if deadline.IsZero() || !time.Now().Before(deadline) {
		p.mu.Unlock()
		return context.DeadlineExceeded
	}
	receipt := &interopControlReceipt{Intent: intent, written: make(chan error, 1), Admission: "refused"}
	p.receipts[intent.ID-1] = receipt
	p.mu.Unlock()
	c := p.conn
	if c == nil {
		return errors.New("private connection absent")
	}
	c.mu.Lock()
	err = p.observer.Err()
	if err == nil && (c.closedBy != nil || c.reverse != p.incarnation || c.reverse == nil || !c.reverse.ready || !c.reverse.initialized || !c.reverse.loaded || !c.reverse.selected || c.reverse.fenced) {
		err = errors.New("private retired generation")
	}
	if err == nil && !time.Now().Before(deadline) {
		err = context.DeadlineExceeded
	}
	if err == nil && c.nextID.Load()+int64(c.ordinaryCalls+c.lifecycleCalls) >= 9999 {
		err = errors.New("private target is selected or within admitted publication range")
	}
	if err == nil && c.pending[subprocess.NumberID(9999)] != nil {
		err = errors.New("private target is a genuine pending call")
	}
	// The authority snapshots are separate from queue locking.
	if err == nil {
		for _, session := range []*HostSession{c.reverse.business, c.reverse.cleanup} {
			if session == nil {
				continue
			}
			session.mu.Lock()
			exists := session.parents[9999] != nil
			session.mu.Unlock()
			if exists {
				err = errors.New("private target is an authority parent")
				break
			}
		}
	}
	if err == nil && len(raw) > c.maxFrame {
		err = ErrFrameTooLarge
	}
	if err == nil {
		frame, enqueueErr := c.queue.enqueueOwned(controlLane, raw)
		err = enqueueErr
		if err == nil {
			c.queue.mu.Lock()
			inControl := false
			for _, queued := range c.queue.lanes[controlLane].frames {
				if queued == frame {
					inControl = true
					break
				}
			}
			c.queue.mu.Unlock()
			if !inControl {
				c.queue.remove(frame)
				err = errors.New("private control lane custody lost")
			}
		}
		if err == nil {
			p.mu.Lock()
			receipt.frame = frame
			receipt.Admission = "admitted"
			p.mu.Unlock()
			c.outbound[frame] = &outboundFrame{ctx: p.observer, budget: -1, idSlot: -1, cancel: true, written: receipt.written}
		}
	}
	c.mu.Unlock()
	if recordErr := p.store.add(struct {
		Intent    interopControlIntent
		Admission string
	}{intent, receipt.Admission}); recordErr != nil {
		return recordErr
	}
	if err == nil {
		c.signalWriter()
	}
	return err
}

// Audit copies of genuine receipts/events after the original EOF verdict. No
// variant changes the live Conn, actual receipt custody or runtime pass record.
func auditInteropPrivateEOF(t *testing.T, c *interopControls) {
	t.Helper()
	check := func(plan *interopPrivatePlan, events []map[string]json.RawMessage, writes []interopWriteWitness) error {
		filtered, err := plan.guard(events, writes)
		if err != nil {
			return err
		}
		return interopWireTerminals(filtered, 0, c.hostCancelEvidence)
	}
	if err := check(c.privatePlan, c.observed, c.privateWrites); err != nil {
		t.Fatal("valid private EOF", err)
	}
	var owned map[string]json.RawMessage
	for _, event := range c.observed {
		if rawEventString(event, "kind") == "wire" && rawEventString(event, "direction") == "host-to-worker" && rawEventString(event, "frame_type") == "notification" {
			owned = event
			break
		}
	}
	if owned == nil {
		t.Fatal("actual authored notification absent")
	}
	duplicate := append(append([]map[string]json.RawMessage(nil), c.observed...), owned)
	if err := check(c.privatePlan, duplicate, c.privateWrites); err == nil {
		t.Fatal("extra identical control accepted")
	}
	// A real directional request is a correlation witness, not an authored
	// cancellation intent. Check both a live-position insertion and post-terminal
	// traffic using the actual published Health ID, never a fixed fixture ID.
	healthIndex := -1
	var healthID uint64
	for i, event := range c.observed {
		if rawEventString(event, "kind") == "wire" && rawEventString(event, "direction") == "host-to-worker" && rawEventString(event, "frame_type") == "request" && rawEventString(event, "method") == subprocess.MethodHealth {
			healthIndex = i
			if err := json.Unmarshal(event["id"], &healthID); err != nil || healthID == 0 {
				t.Fatal("actual Health correlation absent", err)
			}
			break
		}
	}
	if healthIndex < 0 {
		t.Fatal("actual Health request absent")
	}
	for _, reason := range []subprocess.CancelReason{subprocess.CallerCancelled, subprocess.DeadlineExpired} {
		raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "rpc/cancel", "params": map[string]any{"request_owner": "host", "id": healthID, "reason": reason}})
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(string(append(raw, '\n')))
		if err != nil {
			t.Fatal(err)
		}
		extra := map[string]json.RawMessage{"kind": json.RawMessage(`"wire"`), "direction": json.RawMessage(`"host-to-worker"`), "frame_type": json.RawMessage(`"notification"`), "raw": encoded}
		for _, at := range []int{healthIndex + 1, len(c.observed)} {
			events := append([]map[string]json.RawMessage(nil), c.observed[:at]...)
			events = append(events, extra)
			events = append(events, c.observed[at:]...)
			if err := check(c.privatePlan, events, c.privateWrites); err == nil {
				t.Fatal("known directional target accepted unowned control", reason, at)
			}
		}
	}
	unexpected := map[string]json.RawMessage{"kind": json.RawMessage(`"wire"`), "direction": json.RawMessage(`"worker-to-host"`), "frame_type": json.RawMessage(`"notification"`), "raw": json.RawMessage(`"{\"jsonrpc\":\"2.0\",\"method\":\"host/unadvertised\",\"params\":{}}\n"`)}
	if err := check(c.privatePlan, append(append([]map[string]json.RawMessage(nil), c.observed...), unexpected), c.privateWrites); err == nil {
		t.Fatal("unowned notification accepted")
	}
	for _, alter := range []func(*interopWriteWitness){func(w *interopWriteWitness) { w.Ref.Run = "foreign" }, func(w *interopWriteWitness) { w.UnderlyingN = 0 }, func(w *interopWriteWitness) { w.AckError = "deadline_exceeded" }} {
		writes := append([]interopWriteWitness(nil), c.privateWrites...)
		for i := range writes {
			if writes[i].Intent > 0 {
				alter(&writes[i])
				break
			}
		}
		if err := check(c.privatePlan, c.observed, writes); err == nil {
			t.Fatal("failed/foreign physical receipt authorized EOF")
		}
	}
	c.privatePlan.mu.Lock()
	copyPlan := &interopPrivatePlan{ref: c.privatePlan.ref}
	for i, receipt := range c.privatePlan.receipts {
		copyReceipt := *receipt
		copyPlan.receipts[i] = &copyReceipt
	}
	c.privatePlan.mu.Unlock()
	copyPlan.receipts[0].Completed = false
	if err := check(copyPlan, c.observed, c.privateWrites); err == nil {
		t.Fatal("unfinished writer receipt authorized EOF")
	}
}
