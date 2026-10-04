package pluginhost

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

func TestReverseReceivedBudgetIncludesAdmissionWait(t *testing.T) {
	for _, source := range []string{"request", "method"} {
		t.Run(source, func(t *testing.T) {
			var effects atomic.Int64
			s := reverseTestSpec(t, HostServices{StorageGet: func(context.Context, *HostCall, subprocess.StorageGetParams) (subprocess.StorageGetResult, error) {
				effects.Add(1)
				return subprocess.StorageGetResult{Found: false}, nil
			}})
			requestMS := uint32(20)
			if source == "method" {
				requestMS = 500
				s.Init.HostServices.Limits.MethodTimeoutMS[string(HostStorageGet)] = 20
			}
			p := reverseReadyPeer(t, s)
			parent := callAsync(WithHostBinding(context.Background(), HostBinding{GrantID: "g-StorageGet", Scope: json.RawMessage(`{}`)}), p.conn, subprocess.MethodHealth)
			request := p.request()
			id, ok := request.ID.Integer()
			if !ok || id <= 0 {
				t.Fatal("nonpositive parent ID")
				return
			}
			fc := reverseParams(t, request)
			raw, err := json.Marshal(map[string]any{
				"jsonrpc": "2.0", "id": 1, "method": HostStorageGet,
				"params": map[string]any{"grant_id": "g-StorageGet", "key": "read", "context": subprocess.ReverseContext{BindingID: *fc.BindingID, TimeoutMS: requestMS, ParentCall: subprocess.ParentCall{RequestOwner: subprocess.HostRPCOwnerHost, ID: uint64(id)}}},
			})
			if err != nil {
				t.Fatal(err)
			}
			// Force a bounded worker to wait before authority preparation. The reader
			// remains free, and the parent retains a much longer independent deadline.
			session := p.conn.reverse.business
			session.mu.Lock()
			p.raw(string(raw))
			limit := time.Now().Add(time.Second)
			for {
				p.conn.mu.Lock()
				admitted := p.conn.reverse.workers == 1
				p.conn.mu.Unlock()
				if admitted {
					break
				}
				if time.Now().After(limit) {
					session.mu.Unlock()
					t.Fatal("worker not admitted")
				}
				time.Sleep(time.Millisecond)
			}
			time.Sleep(80 * time.Millisecond)
			session.mu.Unlock()
			terminal := p.frame()
			if terminal.ID != subprocess.NumberID(1) || terminal.Method != "" {
				t.Fatal(terminal)
			}
			if effects.Load() != 0 {
				t.Fatal("backend started after the absolute received budget expired during admission wait")
			}
			p.reply(request.ID, `{"ok":true}`)
			if got := await(t, parent); got.err != nil {
				t.Fatal(got.err)
			}
		})
	}
}

// An expired mutating request is a definite pre-effect refusal, not an unknown
// outcome. Both the caller budget and method ceiling use the receipt clock.
func TestHostExpiredReceivedBudgetIsNotStarted(t *testing.T) {
	for _, source := range []string{"request", "method"} {
		t.Run(source, func(t *testing.T) {
			var effects atomic.Int64
			services := hostTestServices()
			services.StoragePut = func(context.Context, *HostCall, subprocess.StoragePutParams) (subprocess.StoragePutResult, error) {
				effects.Add(1)
				return subprocess.StoragePutResult{OperationKey: "op", Revision: "2"}, nil
			}
			s, _ := hostTestSession(t, services, nil)
			binding := hostTestBinding(t, s, HostStoragePut, HostBudgets{})
			raw := hostTestRaw(t, HostStoragePut, binding)
			if source == "method" {
				s.ceilings[HostStoragePut] = 20 * time.Millisecond
			}
			elapsed := 600 * time.Millisecond
			if source == "method" {
				elapsed = 80 * time.Millisecond
			}
			var q frameQueue
			credit, err := q.reserveTerminal()
			if err != nil {
				t.Fatal(err)
			}
			defer credit.release()
			frame, err := s.executeHostWithReply(context.Background(), 1, HostStoragePut, raw, time.Now().Add(-elapsed), false, credit.terminal)
			if err != nil {
				t.Fatal(err)
			}
			var reply subprocess.ApplicationErrorResponse
			if err := json.Unmarshal(frame.wire, &reply); err != nil {
				t.Fatal(err)
			}
			if reply.Error.Data.Code != capability.DeadlineExceeded || reply.Error.Data.EffectState != capability.NotStarted {
				t.Fatalf("expired request lost pre-effect classification: %+v", reply)
			}
			if effects.Load() != 0 {
				t.Fatal("expired mutation entered backend")
			}
		})
	}
}
