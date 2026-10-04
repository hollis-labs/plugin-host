package pluginhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

// executeHost is deliberately private and NOT reached by Conn. Slice4 must
// establish negotiated activation, bounded reader routing, actual parent
// publication/retirement and terminal write receipts before wiring this seam.
// Its caller is a bounded worker, never the reader. It reserves reply credit
// before running host code and retains execution admission until code returns.
func (s *HostSession) executeHost(ctx context.Context, id uint64, method HostMethod, raw []byte, queue *frameQueue) (*queuedFrame, error) {
	if id == 0 || id > capability.MaxSafeInteger {
		return nil, hostRefusal(capability.InvalidRequest, "")
	}
	if queue == nil {
		return nil, hostRefusal(capability.RateLimited, "")
	}
	credit, err := queue.reserveTerminal()
	if err != nil {
		return nil, err
	}
	defer credit.release()
	var authority HostAuthority
	reply := func(result any, err error) (*queuedFrame, error) {
		wire := hostWireReply(id, result, err)
		fallbackError := &capability.Error{Code: capability.InternalError, EffectState: capability.Unknown}
		if effectfulMethod(method) {
			fallbackError.Code = capability.UnknownOutcome
		}
		fallback := hostWireReply(id, nil, fallbackError)
		s.mu.Lock()
		defer s.mu.Unlock()
		if err == nil && result != nil {
			if fenced := s.checkAuthorityLocked(authority); fenced != nil {
				if effectfulMethod(method) {
					fenced = &capability.Error{Code: capability.UnknownOutcome, EffectState: capability.Unknown}
				}
				wire = hostWireReply(id, nil, fenced)
			}
		}
		return credit.terminal(wire, fallback)
	}
	if len(raw) > 1<<20 {
		return reply(nil, hostRefusal(capability.InvalidRequest, ""))
	}
	grant, reverse, err := decodeHostContext(method, raw)
	if err != nil {
		return reply(nil, hostRefusal(capability.InvalidRequest, ""))
	}
	authority, err = s.prepareHost(method, grant, reverse)
	if err != nil {
		return reply(nil, err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		authority.Deadline = earliest(authority.Deadline, deadline)
	}
	permit := s.admission.acquire()
	if permit == nil {
		return reply(nil, hostRefusal(capability.RateLimited, ""))
	}
	callCtx, cancel := context.WithDeadline(ctx, authority.Deadline)
	// Registration is host-held and precedes callbacks; received parent metadata
	// never creates a record. A duplicated active ID fences the whole session.
	s.mu.Lock()
	if _, exists := s.active[id]; exists {
		s.mu.Unlock()
		cancel()
		permit.release()
		s.Revoke()
		return nil, hostRefusal(capability.InvalidRequest, "")
	}
	if id <= s.reverseHighWater {
		s.mu.Unlock()
		cancel()
		permit.release()
		return reply(nil, hostRefusal(capability.InvalidRequest, ""))
	}
	if err := s.checkAuthorityLocked(authority); err != nil {
		s.mu.Unlock()
		cancel()
		permit.release()
		return reply(nil, err)
	}
	s.reverseHighWater = id
	s.active[id] = hostActiveCall{method: method, binding: authority.BindingID, cancel: cancel}
	s.mu.Unlock()
	call := &HostCall{session: s, authority: authority, ctx: callCtx}
	type outcome struct {
		result any
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		defer permit.release()
		defer cancel()
		defer func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if !s.closed {
				s.terminal[id] = s.active[id]
				s.terminalOrder = append(s.terminalOrder, id)
				if len(s.terminalOrder) > 32 {
					delete(s.terminal, s.terminalOrder[0])
					s.terminalOrder = s.terminalOrder[1:]
				}
			}
			delete(s.active, id)
		}()
		result, err := s.invokeHost(callCtx, call, method, raw)
		if err == nil {
			err = validateHostResult(method, raw, result)
			if err == nil {
				encoded, _ := json.Marshal(result)
				err = s.charge(authority, uint64(len(encoded)), 0, 0)
			}
		}
		if err != nil && effectfulMethod(method) && call.started.Load() && result != nil {
			// A valid backend result that cannot be validated/charged/published is not
			// a definite pre-effect refusal. The service's receipt remains authoritative.
			err = &capability.Error{Code: capability.UnknownOutcome, EffectState: capability.Unknown}
		}
		done <- outcome{result, err}
	}()
	select {
	case completed := <-done:
		if err := s.checkAuthority(authority); err != nil {
			if effectfulMethod(method) && call.started.Load() {
				return reply(nil, &capability.Error{Code: capability.UnknownOutcome, EffectState: capability.Unknown})
			}
			return reply(nil, err)
		}
		if completed.err != nil && !call.started.Load() {
			var refusal *capability.Error
			errors.As(safeHostError(completed.err, false), &refusal)
			refusal.EffectState = capability.NotStarted
			return reply(nil, refusal)
		}
		return reply(completed.result, safeHostError(completed.err, effectfulMethod(method)))
	case <-callCtx.Done():
		// An uncooperative handler retains its permit until its real return. The
		// service owns authoritative receipts; cancellation cannot promise rollback.
		if effectfulMethod(method) && call.started.Load() {
			return reply(nil, &capability.Error{Code: capability.UnknownOutcome, EffectState: capability.Unknown})
		}
		return reply(nil, hostContextError(callCtx.Err()))
	}
}

func (s *HostSession) prepareHost(method HostMethod, grantID string, reverse subprocess.ReverseContext) (HostAuthority, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.runtime == nil {
		return HostAuthority{}, hostRefusal(capability.TargetUnavailable, capability.StaleBinding)
	}
	binding := s.bindings[reverse.BindingID]
	if binding == nil {
		return HostAuthority{}, hostRefusal(capability.TargetUnavailable, capability.StaleBinding)
	}
	// Binding ownership is authenticated by this session before disclosing any
	// parent state. Grant selection proves nothing and cannot widen the binding.
	if binding.spec.GrantID != grantID {
		return HostAuthority{}, hostRefusal(capability.CapabilityDenied, "")
	}
	if reverse.ParentCall != binding.spec.Parent {
		return HostAuthority{}, hostRefusal(capability.TargetUnavailable, capability.ParentInvalid)
	}
	ceiling, exists := s.ceilings[method]
	if !exists {
		return HostAuthority{}, hostRefusal(capability.UnsupportedCapability, "")
	}
	if method != HostBindingsRenew && methodDescriptor(method) != binding.grant.Name {
		return HostAuthority{}, hostRefusal(capability.CapabilityDenied, "")
	}
	if (isLifecycleMethod(binding.parent.method) || !s.ownerReady) && method != HostLog {
		return HostAuthority{}, hostRefusal(capability.TargetUnavailable, "")
	}
	deadline := earliest(binding.expiry, binding.parent.deadline, time.Now().Add(ceiling), time.Now().Add(time.Duration(reverse.TimeoutMS)*time.Millisecond))
	a := HostAuthority{Owner: s.owner, ConnectionInstance: s.connection, BindingID: reverse.BindingID, Grant: binding.grant, Scope: binding.spec.Scope, Caller: binding.spec.Caller, Root: binding.spec.Root, Parent: reverse.ParentCall, Depth: binding.spec.Depth + 1, Origin: append([]Owner(nil), binding.spec.Origin...), Method: method, Deadline: deadline}
	if err := s.checkAuthorityLocked(a); err != nil {
		return HostAuthority{}, err
	}
	return copyAuthority(a), nil
}
func isLifecycleMethod(method string) bool {
	return method == "plugin/init" || method == "plugin/load" || method == "plugin/unload"
}
func safeHostError(err error, mutating bool) error {
	if err == nil {
		return nil
	}
	var classified *capability.Error
	if errors.As(err, &classified) && classified.Validate() == nil {
		snapshot := *classified
		snapshot.Capability = ""
		return &snapshot
	}
	if !mutating && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return hostContextError(err)
	}
	if mutating {
		return &capability.Error{Code: capability.UnknownOutcome, EffectState: capability.Unknown}
	}
	return &capability.Error{Code: capability.InternalError, EffectState: capability.Unknown}
}
func hostWireReply(id uint64, result any, err error) []byte {
	if err == nil {
		raw, encodeErr := json.Marshal(result)
		if encodeErr == nil && len(raw) <= 1<<20 && string(raw) != "null" {
			wire, encodeErr := json.Marshal(struct {
				JSONRPC string          `json:"jsonrpc"`
				ID      uint64          `json:"id"`
				Result  json.RawMessage `json:"result"`
			}{"2.0", id, raw})
			if encodeErr == nil {
				return append(wire, '\n')
			}
		}
		err = &capability.Error{Code: capability.InternalError, EffectState: capability.Unknown}
	}
	var classified *capability.Error
	if !errors.As(safeHostError(err, false), &classified) || classified == nil {
		classified = &capability.Error{Code: capability.InternalError, EffectState: capability.Unknown}
	}
	classified.RequestID = capability.RequestID(id)
	data, _ := classified.RPCData()
	response := subprocess.ApplicationErrorResponse{JSONRPC: "2.0", ID: capability.RequestID(id), Error: subprocess.HostRPCError{Code: capability.HostRPCErrorCode, Message: "Host service request failed", Data: subprocess.HostRPCErrorData(data)}}
	wire, _ := json.Marshal(response)
	return append(wire, '\n')
}
func effectfulMethod(method HostMethod) bool {
	return method == HostStoragePut || method == HostStorageDelete || method == HostEventsPublish || method == HostEgressRequest || method == HostMCPCallTool
}

func typedHostCall[P, R any](ctx context.Context, call *HostCall, raw []byte, callback func(context.Context, *HostCall, P) (R, error)) (any, error) {
	if callback == nil {
		return nil, hostRefusal(capability.UnsupportedCapability, "")
	}
	var params P
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, hostRefusal(capability.InvalidRequest, "")
	}
	call.started.Store(true)
	return callback(ctx, call, params)
}
func (s *HostSession) invokeHost(ctx context.Context, call *HostCall, method HostMethod, raw []byte) (result any, err error) {
	defer func() {
		if recover() != nil {
			result = nil
			err = &capability.Error{Code: capability.InternalError, EffectState: capability.Unknown}
		}
	}()
	if err := call.CheckCommit(); err != nil {
		return nil, err
	}
	if err := call.Charge(uint64(len(raw)), 0, 0); err != nil {
		return nil, err
	}
	services := s.runtime.services
	switch method {
	case HostStorageGet:
		return typedHostCall(ctx, call, raw, services.StorageGet)
	case HostStoragePut:
		return typedHostCall(ctx, call, raw, services.StoragePut)
	case HostStorageDelete:
		return typedHostCall(ctx, call, raw, services.StorageDelete)
	case HostSecretsGet:
		return typedHostCall(ctx, call, raw, services.SecretsGet)
	case HostEgressRequest:
		return typedHostCall(ctx, call, raw, services.EgressRequest)
	case HostEventsPublish:
		return typedHostCall(ctx, call, raw, services.EventsPublish)
	case HostLog:
		return typedHostCall(ctx, call, raw, services.Log)
	case HostReadonlyQuery:
		return typedHostCall(ctx, call, raw, services.ReadonlyQuery)
	case HostMCPListTools:
		return typedHostCall(ctx, call, raw, services.MCPListTools)
	case HostMCPCallTool:
		return typedHostCall(ctx, call, raw, services.MCPCallTool)
	case HostBindingsRenew:
		return s.renewHost(ctx, call, raw)
	case HostMCPCancelCall:
		return s.cancelHost(ctx, call, raw)
	default:
		return nil, hostRefusal(capability.UnsupportedCapability, "")
	}
}
func decodeHostContext(method HostMethod, raw []byte) (string, subprocess.ReverseContext, error) {
	var dto string
	switch method {
	case HostStorageGet:
		dto = "StorageGetParams"
	case HostStoragePut:
		dto = "StoragePutParams"
	case HostStorageDelete:
		dto = "StorageDeleteParams"
	case HostSecretsGet:
		dto = "SecretsGetParams"
	case HostEgressRequest:
		dto = "EgressRequestParams"
	case HostEventsPublish:
		dto = "EventsPublishParams"
	case HostLog:
		dto = "LogParams"
	case HostReadonlyQuery:
		dto = "ReadonlyQueryParams"
	case HostMCPListTools:
		dto = "MCPListToolsParams"
	case HostMCPCallTool:
		dto = "MCPCallToolParams"
	case HostBindingsRenew:
		dto = "BindingsRenewParams"
	case HostMCPCancelCall:
		dto = "MCPCancelCallParams"
	default:
		return "", subprocess.ReverseContext{}, fmt.Errorf("unknown host method")
	}
	if err := subprocess.ValidateHostRPCDTO(dto, raw); err != nil {
		return "", subprocess.ReverseContext{}, err
	}
	var header struct {
		GrantID string                    `json:"grant_id"`
		Context subprocess.ReverseContext `json:"context"`
	}
	err := json.Unmarshal(raw, &header)
	return header.GrantID, header.Context, err
}

func (s *HostSession) renewHost(ctx context.Context, call *HostCall, raw []byte) (any, error) {
	var params subprocess.BindingsRenewParams
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, hostRefusal(capability.InvalidRequest, "")
	}
	if callback := s.runtime.services.BindingsRenew; callback != nil {
		if err := callback(ctx, call, params); err != nil {
			return nil, err
		}
	}
	if err := call.CheckCommit(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthorityLocked(call.authority); err != nil {
		return nil, err
	}
	binding := s.bindings[call.authority.BindingID]
	grantExpiry, _ := time.Parse(time.RFC3339Nano, binding.grant.ExpiresAt)
	if params.RequestedLeaseMS == 0 || params.RequestedLeaseMS > 300000 {
		return nil, hostRefusal(capability.InvalidRequest, "")
	}
	expiry := earliest(time.Now().Add(time.Duration(params.RequestedLeaseMS)*time.Millisecond), binding.spec.Deadline, binding.parent.deadline, grantExpiry)
	// A finite request never extends the root or grant; already spent aggregate
	// dimensions survive renewal unchanged, including zero-valued counters.
	binding.expiry = expiry
	remaining := time.Until(expiry) / time.Millisecond
	if remaining < 1 || remaining > 300000 {
		return nil, hostRefusal(capability.DeadlineExceeded, "")
	}
	b := copyBudgets(binding.spec.Budgets)
	return subprocess.BindingsRenewResult{BindingID: call.authority.BindingID, ExpiresAt: expiry.UTC().Format(time.RFC3339Nano), RemainingBudgets: subprocess.HostRPCRemainingBudgets{TimeoutMS: uint32(remaining), Bytes: b.Bytes, Effects: b.Effects, Tokens: b.Tokens}}, nil
}
func (s *HostSession) cancelHost(ctx context.Context, call *HostCall, raw []byte) (any, error) {
	var params subprocess.MCPCancelCallParams
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, hostRefusal(capability.InvalidRequest, "")
	}
	s.mu.Lock()
	target, exists := s.active[params.TargetCallID]
	if !exists {
		retired, own := s.terminal[params.TargetCallID]
		s.mu.Unlock()
		if own && retired.method == HostMCPCallTool && retired.binding == call.authority.BindingID {
			return subprocess.MCPCancelCallResult{AlreadyTerminal: true}, nil
		}
		return nil, hostRefusal(capability.ScopeDenied, "")
	}
	if target.method != HostMCPCallTool || target.binding != call.authority.BindingID {
		s.mu.Unlock()
		return nil, hostRefusal(capability.ScopeDenied, "")
	}
	s.mu.Unlock()
	if callback := s.runtime.services.MCPCancelCall; callback != nil {
		if err := callback(ctx, call, params); err != nil {
			return nil, err
		}
	}
	if err := call.CheckCommit(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	target, exists = s.active[params.TargetCallID]
	if !exists {
		return subprocess.MCPCancelCallResult{AlreadyTerminal: true}, nil
	}
	if target.method != HostMCPCallTool || target.binding != call.authority.BindingID {
		return nil, hostRefusal(capability.ScopeDenied, "")
	}
	target.cancel()
	return subprocess.MCPCancelCallResult{Accepted: true}, nil
}

func validateHostResult(method HostMethod, params []byte, result any) error {
	raw, err := json.Marshal(result)
	if err != nil || len(raw) > 1<<20 {
		return &capability.Error{Code: capability.InternalError, EffectState: capability.Unknown}
	}
	dto := ""
	switch method {
	case HostStorageGet:
		dto = "StorageGetResult"
	case HostStoragePut:
		dto = "StoragePutResult"
	case HostStorageDelete:
		dto = "StorageDeleteResult"
	case HostSecretsGet:
		dto = "SecretsGetResult"
	case HostEgressRequest:
		dto = "EgressRequestResult"
	case HostEventsPublish:
		dto = "EventsPublishResult"
	case HostLog:
		dto = "LogResult"
	case HostReadonlyQuery:
		dto = "ReadonlyQueryResult"
	case HostMCPListTools:
		dto = "MCPListToolsResult"
	case HostMCPCallTool:
		dto = "MCPCallToolResult"
	case HostMCPCancelCall:
		dto = "MCPCancelCallResult"
	case HostBindingsRenew:
		dto = "BindingsRenewResult"
	}
	if err := subprocess.ValidateHostRPCDTO(dto, raw); err != nil {
		return &capability.Error{Code: capability.InternalError, EffectState: capability.Unknown}
	}
	var input, output map[string]json.RawMessage
	if json.Unmarshal(params, &input) != nil || json.Unmarshal(raw, &output) != nil {
		return hostRefusal(capability.InternalError, "")
	}
	equal := func(key string) bool {
		var a, b any
		if json.Unmarshal(input[key], &a) != nil || json.Unmarshal(output[key], &b) != nil {
			return false
		}
		return fmt.Sprint(a) == fmt.Sprint(b)
	}
	if _, exists := input["operation_key"]; exists && !equal("operation_key") {
		return &capability.Error{Code: capability.InternalError, EffectState: capability.Unknown}
	}
	if _, exists := input["operation_key"]; !exists {
		if _, exists := output["operation_key"]; exists {
			return &capability.Error{Code: capability.InternalError, EffectState: capability.Unknown}
		}
	}
	if method == HostReadonlyQuery && (!equal("resource") || !equal("schema_version")) {
		return &capability.Error{Code: capability.InternalError, EffectState: capability.Unknown}
	}
	return nil
}
