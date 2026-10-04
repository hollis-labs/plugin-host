package pluginhost

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
	"sync"
	"testing"
)

func hostReviewCall(ctx context.Context, t *testing.T, s *HostSession) *HostCall {
	t.Helper()
	b := hostTestBinding(t, s, HostStorageGet, HostBudgets{})
	a, err := s.prepareHost(HostStorageGet, "storage.read", subprocess.ReverseContext{BindingID: b, TimeoutMS: 500, ParentCall: subprocess.ParentCall{RequestOwner: subprocess.HostRPCOwnerHost, ID: 1}})
	if err != nil {
		t.Fatal(err)
	}
	return &HostCall{session: s, authority: a, ctx: ctx}
}

// Policy completed its lookup before cancellation, but delivery raced request
// cancellation. The guard must revalidate the live request after policy returns.
func TestHostReviewCommitCancelDuringPolicy(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	policy := func(ctx context.Context, _ HostAuthority) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		close(entered)
		<-release
		return nil
	}
	s, _ := hostTestSession(t, hostTestServices(), policy)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := hostReviewCall(ctx, t, s)
	done := make(chan error, 1)
	go func() { done <- c.CheckCommit() }()
	<-entered
	cancel()
	close(release)
	if err := <-done; err == nil {
		t.Fatal("CheckCommit approved a canceled request after policy return")
	}
}

func TestHostReviewCommitRevokeDuringPolicy(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	s, _ := hostTestSession(t, hostTestServices(), func(context.Context, HostAuthority) error { close(entered); <-release; return nil })
	c := hostReviewCall(context.Background(), t, s)
	done := make(chan error, 1)
	go func() { done <- c.CheckCommit() }()
	<-entered
	s.RevokeBinding(c.authority.BindingID)
	close(release)
	if err := <-done; err == nil {
		t.Fatal("policy return bypassed concurrent binding revoke")
	}
}

func TestHostReviewPolicySnapshotIsolation(t *testing.T) {
	s, _ := hostTestSession(t, hostTestServices(), func(_ context.Context, a HostAuthority) error { a.Scope[0] = 'x'; a.Grant.Scope[0] = 'x'; return nil })
	c := hostReviewCall(context.Background(), t, s)
	for range 3 {
		if err := c.CheckCommit(); err != nil {
			t.Fatal(err)
		}
	}
	if string(c.Authority().Scope) != "{}" {
		t.Fatal("policy mutated canonical binding")
	}
}

func TestHostReviewCompoundBudgetAtomic(t *testing.T) {
	s, _ := hostTestSession(t, hostTestServices(), nil)
	c := hostReviewCall(context.Background(), t, s)
	bytes, effects := uint64(4), uint64(1)
	s.mu.Lock()
	s.bindings[c.authority.BindingID].spec.Budgets = HostBudgets{Bytes: &bytes, Effects: &effects}
	s.mu.Unlock()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.Charge(1, 2, 0)
			var ce *capability.Error
			if !errors.As(err, &ce) || ce.Code != capability.BudgetExceeded {
				t.Error("overdraw did not refuse")
			}
		}()
	}
	wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	if bytes != 4 || effects != 1 {
		t.Fatal("failed compound reservation consumed budget")
	}
}

func TestHostReviewMutationTypedRefusalPreserved(t *testing.T) {
	services := hostTestServices()
	services.StoragePut = func(context.Context, *HostCall, subprocess.StoragePutParams) (subprocess.StoragePutResult, error) {
		return subprocess.StoragePutResult{}, &capability.Error{Code: capability.Conflict, EffectState: capability.NotCommitted}
	}
	s, _ := hostTestSession(t, services, nil)
	binding := hostTestBinding(t, s, HostStoragePut, HostBudgets{})
	reply := hostTestExecute(t, s, 1, HostStoragePut, hostTestRaw(t, HostStoragePut, binding))
	var err subprocess.HostRPCError
	if decodeErr := json.Unmarshal(reply["error"], &err); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if err.Data.Code != capability.Conflict || err.Data.EffectState != capability.NotCommitted {
		t.Fatalf("reliable typed refusal replaced with %s/%s", err.Data.Code, err.Data.EffectState)
	}
}

func TestHostReviewMutationInvalidResultUnknown(t *testing.T) {
	services := hostTestServices()
	services.StoragePut = func(_ context.Context, _ *HostCall, p subprocess.StoragePutParams) (subprocess.StoragePutResult, error) {
		return subprocess.StoragePutResult{OperationKey: "wrong", Revision: "2"}, nil
	}
	s, _ := hostTestSession(t, services, nil)
	binding := hostTestBinding(t, s, HostStoragePut, HostBudgets{})
	reply := hostTestExecute(t, s, 1, HostStoragePut, hostTestRaw(t, HostStoragePut, binding))
	if code := hostTestCode(t, reply); code != capability.UnknownOutcome {
		t.Fatalf("lost mutation result classified %s", code)
	}
}
