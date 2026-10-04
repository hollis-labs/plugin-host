package pluginhost

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

var hostTestMethods = []HostMethod{HostStorageGet, HostStoragePut, HostStorageDelete, HostSecretsGet, HostEgressRequest, HostEventsPublish, HostLog, HostReadonlyQuery, HostMCPListTools, HostMCPCallTool, HostMCPCancelCall, HostBindingsRenew}

func hostTestServices() HostServices {
	return HostServices{
		StorageGet: func(context.Context, *HostCall, subprocess.StorageGetParams) (subprocess.StorageGetResult, error) {
			return subprocess.StorageGetResult{Found: false}, nil
		},
		StoragePut: func(_ context.Context, _ *HostCall, p subprocess.StoragePutParams) (subprocess.StoragePutResult, error) {
			return subprocess.StoragePutResult{OperationKey: p.OperationKey, Revision: "2"}, nil
		},
		StorageDelete: func(_ context.Context, _ *HostCall, p subprocess.StorageDeleteParams) (subprocess.StorageDeleteResult, error) {
			return subprocess.StorageDeleteResult{OperationKey: p.OperationKey, Deleted: true, Revision: "3"}, nil
		},
		SecretsGet: func(context.Context, *HostCall, subprocess.SecretsGetParams) (subprocess.SecretsGetResult, error) {
			return subprocess.SecretsGetResult{ValueBase64: "", ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano)}, nil
		},
		EgressRequest: func(context.Context, *HostCall, subprocess.EgressRequestParams) (subprocess.EgressRequestResult, error) {
			return subprocess.EgressRequestResult{Status: 200, Headers: []subprocess.HostRPCHeader{}, BodyBase64: ""}, nil
		},
		EventsPublish: func(_ context.Context, _ *HostCall, p subprocess.EventsPublishParams) (subprocess.EventsPublishResult, error) {
			return subprocess.EventsPublishResult{OperationKey: p.OperationKey, Accepted: true, EventID: "event"}, nil
		},
		Log: func(context.Context, *HostCall, subprocess.LogParams) (subprocess.LogResult, error) {
			return subprocess.LogResult{Accepted: true}, nil
		},
		ReadonlyQuery: func(_ context.Context, _ *HostCall, p subprocess.ReadonlyQueryParams) (subprocess.ReadonlyQueryResult, error) {
			return subprocess.ReadonlyQueryResult{Resource: p.Resource, SchemaVersion: p.SchemaVersion, Data: json.RawMessage(`{}`)}, nil
		},
		MCPListTools: func(_ context.Context, _ *HostCall, p subprocess.MCPListToolsParams) (subprocess.MCPListToolsResult, error) {
			return subprocess.MCPListToolsResult{ServerID: p.ServerID, Tools: []subprocess.MCPTool{}}, nil
		},
		MCPCallTool: func(context.Context, *HostCall, subprocess.MCPCallToolParams) (subprocess.MCPCallToolResult, error) {
			return subprocess.MCPCallToolResult{Content: []json.RawMessage{}, IsError: false}, nil
		},
	}
}
func hostTestSession(t *testing.T, services HostServices, policy HostPolicy) (*HostSession, capability.GrantSet) {
	t.Helper()
	if policy == nil {
		policy = func(context.Context, HostAuthority) error { return nil }
	}
	owner := Owner{HostInstance: "host", OwnerID: "plugin", OwnerGeneration: 1}
	grants := capability.GrantSet{}
	names := map[string]bool{}
	ceilings := map[HostMethod]time.Duration{}
	for _, method := range hostTestMethods {
		ceilings[method] = time.Second
		name := methodDescriptor(method)
		if name == "binding" || names[name] {
			continue
		}
		names[name] = true
		grants = append(grants, capability.Grant{GrantID: name, Name: name, SchemaVersion: 1, Scope: json.RawMessage(`{}`), HostInstance: owner.HostInstance, OwnerID: owner.OwnerID, OwnerGeneration: owner.OwnerGeneration, Audience: "plugin", IssuedAt: time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano), ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano), PolicyRevision: "1"})
	}
	s, err := NewHostServiceRuntime(services, policy).OpenSession(owner, grants, ceilings)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.RegisterParent(1, "http/handle", time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := s.OwnerReady(); err != nil {
		t.Fatal(err)
	}
	return s, grants
}
func hostTestBinding(t *testing.T, s *HostSession, method HostMethod, budgets HostBudgets) subprocess.BindingID {
	t.Helper()
	name := methodDescriptor(method)
	if method == HostBindingsRenew {
		name = "storage.read"
	}
	id, err := s.IssueBinding(HostBinding{GrantID: name, Scope: json.RawMessage(`{}`), Parent: subprocess.ParentCall{RequestOwner: subprocess.HostRPCOwnerHost, ID: 1}, Deadline: time.Now().Add(time.Second), Budgets: budgets})
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func hostTestRaw(t *testing.T, method HostMethod, binding subprocess.BindingID) []byte {
	t.Helper()
	grant := methodDescriptor(method)
	if method == HostBindingsRenew {
		grant = "storage.read"
	}
	p := map[string]any{"grant_id": grant, "context": subprocess.ReverseContext{BindingID: binding, TimeoutMS: 500, ParentCall: subprocess.ParentCall{RequestOwner: subprocess.HostRPCOwnerHost, ID: 1}}}
	switch method {
	case HostStorageGet:
		p["key"] = "key"
	case HostStoragePut:
		p["key"] = "key"
		p["value"] = true
		p["expected_revision"] = nil
		p["operation_key"] = "op"
	case HostStorageDelete:
		p["key"] = "key"
		p["expected_revision"] = "1"
		p["operation_key"] = "op"
	case HostSecretsGet:
		p["secret_ref"] = "secret"
	case HostEgressRequest:
		p["method"] = "GET"
		p["url"] = "https://example.com/"
	case HostEventsPublish:
		p["event_name"] = "plugin/event"
		p["payload"] = true
		p["operation_key"] = "op"
	case HostLog:
		p["level"] = "info"
		p["message"] = "message"
	case HostReadonlyQuery:
		p["resource"] = "resource"
		p["schema_version"] = 1
		p["params"] = true
	case HostMCPListTools:
		p["server_id"] = "server"
	case HostMCPCallTool:
		p["server_id"] = "server"
		p["tool_name"] = "tool"
		p["tool_binding"] = "toolref"
		p["arguments"] = true
	case HostMCPCancelCall:
		p["target_call_id"] = 1
	case HostBindingsRenew:
		p["requested_lease_ms"] = 300000
	}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func hostTestExecute(t *testing.T, s *HostSession, id uint64, method HostMethod, raw []byte) map[string]json.RawMessage {
	t.Helper()
	var queue frameQueue
	frame, err := s.executeHost(context.Background(), id, method, raw, &queue)
	if err != nil {
		t.Fatal(err)
	}
	var reply map[string]json.RawMessage
	if err := json.Unmarshal(frame.wire, &reply); err != nil {
		t.Fatal(err)
	}
	return reply
}
func hostTestCode(t *testing.T, reply map[string]json.RawMessage) capability.Code {
	t.Helper()
	if _, ok := reply["result"]; ok {
		return ""
	}
	var err subprocess.HostRPCError
	if e := json.Unmarshal(reply["error"], &err); e != nil {
		t.Fatal(e)
	}
	return err.Data.Code
}
func TestHostClosedTypedServices(t *testing.T) {
	for _, method := range hostTestMethods {
		t.Run(string(method), func(t *testing.T) {
			s, _ := hostTestSession(t, hostTestServices(), nil)
			binding := hostTestBinding(t, s, method, HostBudgets{})
			id := uint64(1)
			if method == HostMCPCancelCall {
				// Cancellation can classify an own terminal tool call, never an actor/type
				// inferred from an arbitrary integer.
				hostTestExecute(t, s, 1, HostMCPCallTool, hostTestRaw(t, HostMCPCallTool, binding))
				id = 2
			}
			reply := hostTestExecute(t, s, id, method, hostTestRaw(t, method, binding))
			if code := hostTestCode(t, reply); code != "" {
				t.Fatalf("%s", code)
			}
		})
	}
}
func TestHostParentForgeryNeverRunsBackend(t *testing.T) {
	for _, kind := range []string{"unknown", "other_direction", "other_binding", "other_grant", "foreign_connection", "terminal", "expired", "revoked"} {
		t.Run(kind, func(t *testing.T) {
			var calls atomic.Int32
			services := hostTestServices()
			services.StorageGet = func(context.Context, *HostCall, subprocess.StorageGetParams) (subprocess.StorageGetResult, error) {
				calls.Add(1)
				return subprocess.StorageGetResult{Found: false}, nil
			}
			s, _ := hostTestSession(t, services, nil)
			binding := hostTestBinding(t, s, HostStorageGet, HostBudgets{})
			raw := hostTestRaw(t, HostStorageGet, binding)
			var p map[string]any
			if err := json.Unmarshal(raw, &p); err != nil {
				t.Fatal(err)
			}
			c := p["context"].(map[string]any)
			parent := c["parent_call"].(map[string]any)
			switch kind {
			case "unknown":
				parent["id"] = 99
			case "other_direction":
				parent["request_owner"] = "plugin"
			case "other_binding":
				if err := s.RegisterParent(2, "http/handle", time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				parent["id"] = 2
			case "other_grant":
				p["grant_id"] = "storage.write"
			case "foreign_connection":
				other, _ := hostTestSession(t, services, nil)
				c["binding_id"] = hostTestBinding(t, other, HostStorageGet, HostBudgets{})
			case "terminal":
				s.RetireParent(1)
			case "expired":
				s.mu.Lock()
				s.bindings[binding].expiry = time.Now().Add(-time.Second)
				s.mu.Unlock()
			case "revoked":
				s.Revoke()
			}
			raw, _ = json.Marshal(p)
			reply := hostTestExecute(t, s, 1, HostStorageGet, raw)
			if hostTestCode(t, reply) == "" || calls.Load() != 0 {
				t.Fatalf("authorized forged parent: %s calls%d", reply, calls.Load())
			}
			if kind == "unknown" || kind == "other_direction" || kind == "other_binding" {
				var e subprocess.HostRPCError
				if err := json.Unmarshal(reply["error"], &e); err != nil {
					t.Fatal(err)
				}
				if e.Data.Detail != capability.ParentInvalid {
					t.Fatalf("oracle detail: %+v", e.Data)
				}
			}
		})
	}
}
func TestHostLifecycleLogOnlyAndParentRetirement(t *testing.T) {
	for _, method := range []string{"plugin/init", "plugin/load", "plugin/unload"} {
		t.Run(method, func(t *testing.T) {
			s, _ := hostTestSession(t, hostTestServices(), nil)
			s.mu.Lock()
			s.parents[1].method = method
			s.ownerReady = false
			s.mu.Unlock()
			business := hostTestBinding(t, s, HostStorageGet, HostBudgets{})
			logBinding := hostTestBinding(t, s, HostLog, HostBudgets{})
			if hostTestCode(t, hostTestExecute(t, s, 1, HostStorageGet, hostTestRaw(t, HostStorageGet, business))) != capability.TargetUnavailable {
				t.Fatal("preactivation business allowed")
			}
			if hostTestCode(t, hostTestExecute(t, s, 2, HostLog, hostTestRaw(t, HostLog, logBinding))) != "" {
				t.Fatal("live log denied")
			}
			s.RetireParent(1)
			if hostTestCode(t, hostTestExecute(t, s, 3, HostLog, hostTestRaw(t, HostLog, logBinding))) != capability.TargetUnavailable {
				t.Fatal("completed lifecycle log allowed")
			}
			if err := s.RegisterParent(1, method, time.Now().Add(time.Second)); err == nil {
				t.Fatal("retired parent reused")
			}
		})
	}
}
func TestHostRenewPreservesSpentBudgetsAndCannotRevive(t *testing.T) {
	s, _ := hostTestSession(t, hostTestServices(), nil)
	n := uint64(10000)
	effects := uint64(2)
	binding := hostTestBinding(t, s, HostBindingsRenew, HostBudgets{Bytes: &n, Effects: &effects})
	a, err := s.prepareHost(HostStorageGet, "storage.read", subprocess.ReverseContext{BindingID: binding, TimeoutMS: 500, ParentCall: subprocess.ParentCall{RequestOwner: subprocess.HostRPCOwnerHost, ID: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if chargeErr := s.charge(a, 0, 2, 0); chargeErr != nil {
		t.Fatal(chargeErr)
	}
	reply := hostTestExecute(t, s, 1, HostBindingsRenew, hostTestRaw(t, HostBindingsRenew, binding))
	if code := hostTestCode(t, reply); code != "" {
		t.Fatal(code)
	}
	var result subprocess.BindingsRenewResult
	if decodeErr := json.Unmarshal(reply["result"], &result); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if result.BindingID != binding || result.RemainingBudgets.Effects == nil || *result.RemainingBudgets.Effects != 0 {
		t.Fatal("renew reset quota or reminted binding")
	}
	expiry, err := time.Parse(time.RFC3339Nano, result.ExpiresAt)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	parentDeadline := s.parents[1].deadline
	s.mu.Unlock()
	if expiry.After(parentDeadline) {
		t.Fatal("lease escaped parent")
	}
	s.RetireParent(1)
	if hostTestCode(t, hostTestExecute(t, s, 2, HostBindingsRenew, hostTestRaw(t, HostBindingsRenew, binding))) != capability.TargetUnavailable {
		t.Fatal("renew revived parent")
	}
}
func TestHostConcurrentBudgetReservationsAreAtomic(t *testing.T) {
	s, _ := hostTestSession(t, hostTestServices(), nil)
	n := uint64(1)
	binding := hostTestBinding(t, s, HostStorageGet, HostBudgets{Effects: &n})
	a, err := s.prepareHost(HostStorageGet, "storage.read", subprocess.ReverseContext{BindingID: binding, TimeoutMS: 500, ParentCall: subprocess.ParentCall{RequestOwner: subprocess.HostRPCOwnerHost, ID: 1}})
	if err != nil {
		t.Fatal(err)
	}
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.charge(a, 0, 1, 0) == nil {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 1 {
		t.Fatalf("spent parent quota %d times", accepted.Load())
	}
}
func TestHostGrantReplacementAndCommitRevalidation(t *testing.T) {
	s, grants := hostTestSession(t, hostTestServices(), nil)
	binding := hostTestBinding(t, s, HostStorageGet, HostBudgets{})
	a, err := s.prepareHost(HostStorageGet, "storage.read", subprocess.ReverseContext{BindingID: binding, TimeoutMS: 500, ParentCall: subprocess.ParentCall{RequestOwner: subprocess.HostRPCOwnerHost, ID: 1}})
	if err != nil {
		t.Fatal(err)
	}
	c := &HostCall{session: s, authority: a, ctx: context.Background()}
	snapshot := c.Authority()
	snapshot.Scope[0] = 'x'
	snapshot.Grant.Scope[0] = 'x'
	if err := c.CheckCommit(); err != nil {
		t.Fatalf("snapshot mutation changed authority: %v", err)
	}
	for i := range grants {
		if grants[i].GrantID == "storage.read" {
			grants[i].Scope = json.RawMessage(`{"changed":true}`)
		}
	}
	if err := s.ReplaceGrants(grants); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckCommit(); err == nil {
		t.Fatal("old same-revision scope survived")
	}
}
func TestHostIssuanceRejectsDepthCycleAndUnregisteredParent(t *testing.T) {
	for _, kind := range []string{"depth", "cycle", "unknown", "direction"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := hostTestSession(t, hostTestServices(), nil)
			spec := HostBinding{GrantID: "storage.read", Scope: json.RawMessage(`{}`), Parent: subprocess.ParentCall{RequestOwner: subprocess.HostRPCOwnerHost, ID: 1}, Deadline: time.Now().Add(time.Second)}
			switch kind {
			case "depth":
				spec.Depth = 8
			case "cycle":
				spec.Origin = []Owner{s.owner}
			case "unknown":
				spec.Parent.ID = 2
			case "direction":
				spec.Parent.RequestOwner = subprocess.HostRPCOwnerPlugin
			}
			if _, err := s.IssueBinding(spec); err == nil {
				t.Fatal("issued untrusted ancestry")
			}
		})
	}
}
func TestHostNoDefaultAuthority(t *testing.T) {
	owner := Owner{HostInstance: "host", OwnerID: "plugin", OwnerGeneration: 1}
	if _, err := NewHostServiceRuntime(HostServices{}, nil).OpenSession(owner, nil, nil); err == nil {
		t.Fatal("nil policy authenticated")
	}
	s, err := NewHostServiceRuntime(HostServices{}, func(context.Context, HostAuthority) error { return nil }).OpenSession(owner, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.RegisterParent(1, "http/handle", time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.IssueBinding(HostBinding{GrantID: "storage.read", Scope: json.RawMessage(`{}`), Parent: subprocess.ParentCall{RequestOwner: subprocess.HostRPCOwnerHost, ID: 1}, Deadline: time.Now().Add(time.Second)}); err == nil {
		t.Fatal("empty grants granted authority")
	}
}
func TestHostMalformedParamsAndReceiptMismatch(t *testing.T) {
	var calls atomic.Int32
	services := hostTestServices()
	services.StoragePut = func(context.Context, *HostCall, subprocess.StoragePutParams) (subprocess.StoragePutResult, error) {
		calls.Add(1)
		return subprocess.StoragePutResult{OperationKey: "other", Revision: "2"}, nil
	}
	s, _ := hostTestSession(t, services, nil)
	binding := hostTestBinding(t, s, HostStoragePut, HostBudgets{})
	raw := hostTestRaw(t, HostStoragePut, binding)
	duplicate := append([]byte(`{"grant_id":"forged",`), raw[1:]...)
	if hostTestCode(t, hostTestExecute(t, s, 1, HostStoragePut, duplicate)) != capability.InvalidRequest || calls.Load() != 0 {
		t.Fatal("ambiguous DTO invoked backend")
	}
	reply := hostTestExecute(t, s, 2, HostStoragePut, raw)
	if hostTestCode(t, reply) == "" {
		t.Fatal("wrong receipt echoed")
	}
	if calls.Load() != 1 {
		t.Fatalf("calls %d", calls.Load())
	}
}

func TestHostFencingCancelsAndBlocksCommit(t *testing.T) {
	for _, cause := range []string{"revoke", "disable", "reload", "crash", "close", "parent_complete", "binding_revoke", "grant_revoke"} {
		t.Run(cause, func(t *testing.T) {
			entered := make(chan struct{})
			release := make(chan struct{})
			returned := make(chan struct{})
			var committed atomic.Int32
			services := hostTestServices()
			services.StoragePut = func(ctx context.Context, c *HostCall, p subprocess.StoragePutParams) (subprocess.StoragePutResult, error) {
				defer close(returned)
				close(entered)
				<-release
				if err := c.CheckCommit(); err != nil {
					return subprocess.StoragePutResult{}, err
				}
				committed.Add(1)
				return subprocess.StoragePutResult{OperationKey: p.OperationKey, Revision: "2"}, nil
			}
			s, _ := hostTestSession(t, services, nil)
			binding := hostTestBinding(t, s, HostStoragePut, HostBudgets{})
			raw := hostTestRaw(t, HostStoragePut, binding)
			done := make(chan error, 1)
			go func() {
				var q frameQueue
				_, err := s.executeHost(context.Background(), 1, HostStoragePut, raw, &q)
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("callback did not start")
			}
			switch cause {
			case "close", "crash":
				s.Close()
			case "parent_complete":
				s.RetireParent(1)
			case "binding_revoke":
				s.RevokeBinding(binding)
			case "grant_revoke":
				if err := s.ReplaceGrants(nil); err != nil {
					t.Fatal(err)
				}
			default:
				s.Revoke()
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(100 * time.Millisecond):
				close(release)
				t.Fatal("fence waited for uncooperative callback")
			}
			close(release)
			select {
			case <-returned:
			case <-time.After(time.Second):
				t.Fatal("callback leaked")
			}
			if committed.Load() != 0 {
				t.Fatal("late fenced callback committed")
			}
			if _, err := s.IssueBinding(HostBinding{GrantID: "storage.write", Scope: json.RawMessage(`{}`), Parent: subprocess.ParentCall{RequestOwner: subprocess.HostRPCOwnerHost, ID: 1}, Deadline: time.Now().Add(time.Second)}); err == nil && cause != "binding_revoke" {
				t.Fatal("retired owner/parent issued binding")
			}
		})
	}
}
func TestHostCancellationKeepsPermitsUntilActualCompletion(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 8)
	returned := make(chan struct{}, 8)
	services := hostTestServices()
	services.StorageGet = func(context.Context, *HostCall, subprocess.StorageGetParams) (subprocess.StorageGetResult, error) {
		started <- struct{}{}
		<-release
		returned <- struct{}{}
		return subprocess.StorageGetResult{Found: false}, nil
	}
	s, _ := hostTestSession(t, services, nil)
	binding := hostTestBinding(t, s, HostStorageGet, HostBudgets{})
	raw := hostTestRaw(t, HostStorageGet, binding)
	for id := uint64(1); id <= 8; id++ {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { var q frameQueue; _, err := s.executeHost(ctx, id, HostStorageGet, raw, &q); done <- err }()
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("callback failed admission")
		}
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("logical cancel hung")
		}
	}
	reply := hostTestExecute(t, s, 9, HostStorageGet, raw)
	if hostTestCode(t, reply) != capability.RateLimited {
		t.Fatal("cancellation prematurely released permits")
	}
	close(release)
	for range 8 {
		select {
		case <-returned:
		case <-time.After(time.Second):
			t.Fatal("handler leaked")
		}
	}
	deadline := time.Now().Add(time.Second)
	for {
		s.admission.pool.mu.Lock()
		active := s.admission.pool.active
		s.admission.pool.mu.Unlock()
		if active == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("completed handlers kept permits")
		}
		time.Sleep(time.Millisecond)
	}
	if hostTestCode(t, hostTestExecute(t, s, 10, HostStorageGet, raw)) != "" {
		t.Fatal("capacity never recovered")
	}
}
func TestHostCancelDirectionAndSiblingIsolation(t *testing.T) {
	var started atomic.Int32
	entered := make(chan struct{}, 2)
	services := hostTestServices()
	services.StorageGet = func(ctx context.Context, _ *HostCall, _ subprocess.StorageGetParams) (subprocess.StorageGetResult, error) {
		started.Add(1)
		entered <- struct{}{}
		<-ctx.Done()
		return subprocess.StorageGetResult{}, ctx.Err()
	}
	s, _ := hostTestSession(t, services, nil)
	binding := hostTestBinding(t, s, HostStorageGet, HostBudgets{})
	raw := hostTestRaw(t, HostStorageGet, binding)
	done := []chan error{make(chan error, 1), make(chan error, 1)}
	for i, id := range []uint64{1, 2} {
		go func() {
			var q frameQueue
			_, err := s.executeHost(context.Background(), id, HostStorageGet, raw, &q)
			done[i] <- err
		}()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("callback did not enter")
		}
	}
	if err := s.cancelPluginRequest(subprocess.HostRPCOwnerHost, 1); err == nil {
		t.Fatal("wrong direction accepted")
	}
	select {
	case <-done[0]:
		t.Fatal("other-direction cancellation selected plugin call")
	default:
	}
	if err := s.cancelPluginRequest(subprocess.HostRPCOwnerPlugin, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done[0]:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("own cancel did not signal")
	}
	select {
	case <-done[1]:
		t.Fatal("child cancellation canceled sibling")
	default:
	}
	s.RetireParent(1)
	select {
	case err := <-done[1]:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("parent cancellation missed descendant")
	}
	if err := s.cancelPluginRequest(subprocess.HostRPCOwnerPlugin, 999); err != nil {
		t.Fatal("unknown cancel not idempotent")
	}
	if started.Load() != 2 {
		t.Fatal("backend count")
	}
}
func TestHostTerminalReservationPrecedesBackend(t *testing.T) {
	var calls atomic.Int32
	services := hostTestServices()
	services.Log = func(context.Context, *HostCall, subprocess.LogParams) (subprocess.LogResult, error) {
		calls.Add(1)
		return subprocess.LogResult{Accepted: true}, nil
	}
	s, _ := hostTestSession(t, services, nil)
	binding := hostTestBinding(t, s, HostLog, HostBudgets{})
	var q frameQueue
	credits := make([]*terminalCredit, 0, writerLaneFrames)
	for range writerLaneFrames {
		credit, err := q.reserveTerminal()
		if err != nil {
			t.Fatal(err)
		}
		credits = append(credits, credit)
	}
	_, err := s.executeHost(context.Background(), 1, HostLog, hostTestRaw(t, HostLog, binding), &q)
	if err == nil || calls.Load() != 0 {
		t.Fatal("backend ran with no terminal capacity")
	}
	for _, credit := range credits {
		credit.release()
	}
}
func TestHostBoundedLedgerAndSharedAdmission(t *testing.T) {
	s, _ := hostTestSession(t, hostTestServices(), nil)
	for range maxHostBindings {
		hostTestBinding(t, s, HostStorageGet, HostBudgets{})
	}
	if _, err := s.IssueBinding(HostBinding{GrantID: "storage.read", Scope: json.RawMessage(`{}`), Parent: subprocess.ParentCall{RequestOwner: subprocess.HostRPCOwnerHost, ID: 1}, Deadline: time.Now().Add(time.Second)}); err == nil {
		t.Fatal("ledger grew past limit")
	}
	// The same runtime pool is shared across independently authenticated sessions.
	var permits []*reversePermit
	for range 8 {
		a := &reverseAdmission{pool: &s.runtime.pool}
		for range 8 {
			p := a.acquire()
			if p == nil {
				t.Fatal("early shared admission refusal")
			}
			permits = append(permits, p)
		}
	}
	if p := s.admission.acquire(); p != nil {
		p.release()
		t.Fatal("global limit exceeded")
	}
	for _, p := range permits {
		p.release()
	}
}

func TestHostPolicyDenialAtAdmissionAndCommit(t *testing.T) {
	var denied atomic.Bool
	var calls atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	returned := make(chan struct{})
	policy := func(context.Context, HostAuthority) error {
		if denied.Load() {
			return hostRefusal(capability.ScopeDenied, "")
		}
		return nil
	}
	services := hostTestServices()
	services.StoragePut = func(_ context.Context, c *HostCall, p subprocess.StoragePutParams) (subprocess.StoragePutResult, error) {
		defer close(returned)
		close(entered)
		<-release
		if err := c.CheckCommit(); err != nil {
			return subprocess.StoragePutResult{}, err
		}
		calls.Add(1)
		return subprocess.StoragePutResult{OperationKey: p.OperationKey, Revision: "2"}, nil
	}
	s, _ := hostTestSession(t, services, policy)
	binding := hostTestBinding(t, s, HostStoragePut, HostBudgets{})
	raw := hostTestRaw(t, HostStoragePut, binding)
	denied.Store(true)
	if hostTestCode(t, hostTestExecute(t, s, 1, HostStoragePut, raw)) != capability.ScopeDenied {
		t.Fatal("current policy denial ignored")
	}
	select {
	case <-entered:
		t.Fatal("denied admission reached backend")
	default:
	}
	denied.Store(false)
	done := make(chan error, 1)
	go func() {
		var q frameQueue
		_, err := s.executeHost(context.Background(), 2, HostStoragePut, raw, &q)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("callback did not enter")
	}
	denied.Store(true)
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not return")
	}
	<-returned
	if calls.Load() != 0 {
		t.Fatal("commit ignored current policy")
	}
}
func TestHostFencedSessionCannotBecomeReadyOrRegisterParent(t *testing.T) {
	s, _ := hostTestSession(t, hostTestServices(), nil)
	s.Revoke()
	if err := s.OwnerReady(); err == nil {
		t.Fatal("revoked owner resurrected")
	}
	if err := s.RegisterParent(2, "http/handle", time.Now().Add(time.Second)); err == nil {
		t.Fatal("fenced session registered new invocation")
	}
	var zero HostSession
	if err := zero.RegisterParent(1, "http/handle", time.Now().Add(time.Second)); err == nil {
		t.Fatal("zero session created authority")
	}
	var call HostCall
	if err := call.CheckCommit(); err == nil {
		t.Fatal("zero callback created authority")
	}
}
