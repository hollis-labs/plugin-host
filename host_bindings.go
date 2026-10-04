package pluginhost

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/hollis-labs/plugin-host/internal/strictjson"
	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

const maxHostBindings = 256
const maxBindingLease = 5 * time.Minute

// HostServiceRuntime owns the shared host limit and fixed service/policy seam.
// Share one instance across a host's connections. It does not negotiate a
// profile, attach to Conn or authorize callbacks by itself.
type HostServiceRuntime struct {
	services HostServices
	policy   HostPolicy
	pool     reversePool
}

func NewHostServiceRuntime(services HostServices, policy HostPolicy) *HostServiceRuntime {
	return &HostServiceRuntime{services: services, policy: policy}
}

type hostParent struct {
	method   string
	deadline time.Time
	terminal bool
}
type hostBindingRecord struct {
	spec     HostBinding
	grant    capability.Grant
	expiry   time.Time
	parent   *hostParent
	terminal bool
}
type hostActiveCall struct {
	method  HostMethod
	binding subprocess.BindingID
	cancel  context.CancelFunc
}

// HostSession is connection-authenticated bookkeeping owned by host code.
// Its random connection identity cannot be chosen by a plugin. Close/Revoke
// fences immediately and cancels descendants without waiting for callbacks.
// Explicit ReverseProfile wiring calls Revoke before business teardown and
// Close on disconnect/crash; a separate cleanup session permits bounded log.
// The zero value has no authority; construct through OpenSession.
type HostSession struct {
	mu               sync.Mutex
	runtime          *HostServiceRuntime
	policy           HostPolicy
	owner            Owner
	connection       string
	grants           map[string]capability.Grant
	ceilings         map[HostMethod]time.Duration
	parents          map[uint64]*hostParent
	bindings         map[subprocess.BindingID]*hostBindingRecord
	active           map[uint64]hostActiveCall
	highWater        uint64
	reverseHighWater uint64
	terminal         map[uint64]hostActiveCall
	terminalOrder    []uint64
	closed           bool
	ownerReady       bool
	admission        reverseAdmission
}

// OpenSession snapshots explicit grants and method ceilings. Empty grants deny
// every call. A ceiling is finite and positive; unknown methods fail closed.
func (r *HostServiceRuntime) OpenSession(owner Owner, grants capability.GrantSet, ceilings map[HostMethod]time.Duration) (*HostSession, error) {
	if r == nil || r.policy == nil {
		return nil, hostRefusal(capability.Unauthenticated, "")
	}
	if err := (capability.RuntimeIdentity{HostInstance: owner.HostInstance, OwnerID: owner.OwnerID, OwnerGeneration: owner.OwnerGeneration}).Validate(); err != nil {
		return nil, err
	}
	id, err := randomBinding()
	if err != nil {
		return nil, err
	}
	s := &HostSession{runtime: r, policy: r.policy, owner: owner, connection: string(id), grants: make(map[string]capability.Grant), ceilings: make(map[HostMethod]time.Duration), parents: make(map[uint64]*hostParent), bindings: make(map[subprocess.BindingID]*hostBindingRecord), active: make(map[uint64]hostActiveCall), terminal: make(map[uint64]hostActiveCall)}
	s.admission.pool = &r.pool
	for method, ceiling := range ceilings {
		if methodDescriptor(method) == "" || ceiling <= 0 {
			return nil, hostRefusal(capability.InvalidRequest, "")
		}
		s.ceilings[method] = ceiling
	}
	if err := s.replaceGrants(grants); err != nil {
		return nil, err
	}
	return s, nil
}
func randomBinding() (subprocess.BindingID, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return subprocess.BindingID(hex.EncodeToString(b[:])), nil
}
func (s *HostSession) replaceGrants(grants capability.GrantSet) error {
	next := make(map[string]capability.Grant, len(grants))
	for _, grant := range grants {
		if err := grant.Validate(); err != nil {
			return err
		}
		if grant.HostInstance != s.owner.HostInstance || grant.OwnerID != s.owner.OwnerID || grant.OwnerGeneration != s.owner.OwnerGeneration {
			return hostRefusal(capability.CapabilityDenied, "")
		}
		if _, exists := next[grant.GrantID]; exists {
			return hostRefusal(capability.InvalidRequest, "")
		}
		grant.Scope = append(json.RawMessage(nil), grant.Scope...)
		next[grant.GrantID] = grant
	}
	s.grants = next
	return nil
}

// ReplaceGrants atomically replaces discovery authority. Changes fence existing
// bindings rather than silently widening them; even same-revision scope edits
// invalidate the old snapshot. Host caller/target policy is also revalidated
// through HostPolicy on every admission and CheckCommit.
func (s *HostSession) ReplaceGrants(grants capability.GrantSet) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.runtime == nil {
		return hostRefusal(capability.TargetUnavailable, "")
	}
	if err := s.replaceGrants(grants); err != nil {
		return err
	}
	for id, binding := range s.bindings {
		grant, ok := s.grants[binding.grant.GrantID]
		if !ok || !sameGrant(grant, binding.grant) {
			s.retireBindingLocked(id, binding)
		}
	}
	return nil
}
func sameGrant(a, b capability.Grant) bool {
	// Scopes are compared as immutable reviewed bytes, never normalized into
	// broader authority. Equal revision alone cannot conceal a changed scope.
	return a.GrantID == b.GrantID && a.Name == b.Name && a.SchemaVersion == b.SchemaVersion && string(a.Scope) == string(b.Scope) && a.PolicyRevision == b.PolicyRevision && a.ExpiresAt == b.ExpiresAt && a.Audience == b.Audience && a.IssuedAt == b.IssuedAt
}

// RegisterParent records a real host-selected invocation BEFORE its first byte
// is published. Only host code calls it; receiving a plugin ParentCall does not
// register anything. Slice4 must wire selection and failed-write/cancel/reply
// retirement. Registering this metadata alone sends no request to the plugin.
func (s *HostSession) RegisterParent(id uint64, method string, deadline time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.runtime == nil || id == 0 || id > capability.MaxSafeInteger || id <= s.highWater || !time.Now().Before(deadline) {
		return hostRefusal(capability.TargetUnavailable, capability.ParentInvalid)
	}
	if len(s.parents) >= 18 {
		return hostRefusal(capability.RateLimited, "")
	}
	s.highWater = id
	s.parents[id] = &hostParent{method: method, deadline: deadline}
	return nil
}

// RetireParent fences its bindings and descendants. Failed publication,
// completion and cancellation all retire authority; IDs are never reusable.
func (s *HostSession) RetireParent(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	parent := s.parents[id]
	if parent == nil {
		return
	}
	parent.terminal = true
	delete(s.parents, id)
	for bid, binding := range s.bindings {
		if binding.parent == parent {
			s.retireBindingLocked(bid, binding)
		}
	}
}
func (s *HostSession) retireBindingLocked(id subprocess.BindingID, binding *hostBindingRecord) {
	binding.terminal = true
	for _, call := range s.active {
		if call.binding == id {
			call.cancel()
		}
	}
}

// IssueBinding issues narrow host authority under a live registered parent.
// Host code owns descriptor-specific narrowing; plugin DTOs cannot call this
// API or supply caller, scope, depth or origin. Opaque scopes are copied.
func (s *HostSession) IssueBinding(spec HostBinding) (subprocess.BindingID, error) {
	if len(spec.Scope) == 0 || len(spec.Scope) > 1<<20 || strictjson.Validate(spec.Scope) != nil || bytes.Equal(bytes.TrimSpace(spec.Scope), []byte("null")) {
		return "", hostRefusal(capability.InvalidRequest, "")
	}
	for _, budget := range []*uint64{spec.Budgets.Bytes, spec.Budgets.Effects, spec.Budgets.Tokens} {
		if budget != nil && *budget > capability.MaxSafeInteger {
			return "", hostRefusal(capability.InvalidRequest, "")
		}
	}
	id, err := randomBinding()
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.runtime == nil {
		return "", hostRefusal(capability.TargetUnavailable, "")
	}
	parent := s.parents[spec.Parent.ID]
	if spec.Parent.RequestOwner != subprocess.HostRPCOwnerHost || parent == nil || parent.terminal {
		return "", hostRefusal(capability.TargetUnavailable, capability.ParentInvalid)
	}
	now := time.Now()
	for key, binding := range s.bindings {
		if !now.Before(binding.expiry) {
			delete(s.bindings, key)
		}
	}
	if len(s.bindings) >= maxHostBindings {
		return "", hostRefusal(capability.RateLimited, "")
	}
	grant, ok := s.grants[spec.GrantID]
	if !ok {
		return "", hostRefusal(capability.CapabilityDenied, "")
	}
	expires, err := time.Parse(time.RFC3339Nano, grant.ExpiresAt)
	if err != nil {
		return "", hostRefusal(capability.CapabilityDenied, "")
	}
	issued, err := time.Parse(time.RFC3339Nano, grant.IssuedAt)
	if err != nil || now.Before(issued) {
		return "", hostRefusal(capability.CapabilityDenied, "")
	}
	expiry := earliest(spec.Deadline, parent.deadline, expires, now.Add(maxBindingLease))
	if !now.Before(expiry) {
		return "", hostRefusal(capability.TargetUnavailable, "")
	}
	if spec.Depth >= 8 {
		return "", hostRefusal(capability.BudgetExceeded, capability.DepthExceeded)
	}
	for _, owner := range spec.Origin {
		if owner == s.owner {
			return "", hostRefusal(capability.ScopeDenied, capability.CallbackCycle)
		}
	}
	spec.Scope = append(json.RawMessage(nil), spec.Scope...)
	spec.Origin = append([]Owner(nil), spec.Origin...)
	spec.Budgets = copyBudgets(spec.Budgets)
	s.bindings[id] = &hostBindingRecord{spec: spec, grant: grant, expiry: expiry, parent: parent}
	return id, nil
}
func earliest(times ...time.Time) time.Time {
	result := times[0]
	for _, candidate := range times[1:] {
		if candidate.Before(result) {
			result = candidate
		}
	}
	return result
}
func copyBudgets(b HostBudgets) HostBudgets {
	clone := func(p *uint64) *uint64 {
		if p == nil {
			return nil
		}
		n := *p
		return &n
	}
	return HostBudgets{Bytes: clone(b.Bytes), Effects: clone(b.Effects), Tokens: clone(b.Tokens)}
}

// OwnerReady records successful host-owned lifecycle activation for policy
// bookkeeping only. It does not negotiate or activate any reverse transport.
// A fenced session cannot be readied again.
func (s *HostSession) OwnerReady() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.runtime == nil {
		return hostRefusal(capability.TargetUnavailable, "")
	}
	s.ownerReady = true
	return nil
}

// Revoke fences this connection without waiting for handlers. Lifecycle
// disable/reload/revoke and crash use this operation; new generations open
// new sessions. Close additionally disposes retained ledger entries.
func (s *HostSession) Revoke() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	for _, call := range s.active {
		call.cancel()
	}
	for _, binding := range s.bindings {
		binding.terminal = true
	}
	for _, parent := range s.parents {
		parent.terminal = true
	}
	s.parents = make(map[uint64]*hostParent)
}
func (s *HostSession) Close() {
	s.Revoke()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bindings = make(map[subprocess.BindingID]*hostBindingRecord)
	s.grants = make(map[string]capability.Grant)
	s.terminal = make(map[uint64]hostActiveCall)
	s.terminalOrder = nil
}
func (s *HostSession) checkAuthority(a HostAuthority) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkAuthorityLocked(a)
}
func (s *HostSession) checkAuthorityLocked(a HostAuthority) error {
	if s.closed || s.runtime == nil || a.Owner != s.owner || a.ConnectionInstance != s.connection {
		return hostRefusal(capability.TargetUnavailable, capability.StaleBinding)
	}
	binding := s.bindings[a.BindingID]
	if binding == nil {
		return hostRefusal(capability.TargetUnavailable, capability.StaleBinding)
	}
	grant, ok := s.grants[a.Grant.GrantID]
	if !ok || !sameGrant(grant, binding.grant) || !sameGrant(a.Grant, binding.grant) {
		return hostRefusal(capability.CapabilityDenied, "")
	}
	if a.Parent != binding.spec.Parent {
		return hostRefusal(capability.TargetUnavailable, capability.ParentInvalid)
	}
	if binding.parent.terminal {
		return hostRefusal(capability.TargetUnavailable, capability.ParentTerminal)
	}
	if binding.terminal || !time.Now().Before(binding.expiry) {
		return hostRefusal(capability.TargetUnavailable, capability.StaleBinding)
	}
	if !time.Now().Before(a.Deadline) {
		return hostRefusal(capability.DeadlineExceeded, "")
	}
	return nil
}
func (s *HostSession) charge(a HostAuthority, bytes, effects, tokens uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthorityLocked(a); err != nil {
		return err
	}
	b := s.bindings[a.BindingID].spec.Budgets
	for _, pair := range []struct {
		remaining *uint64
		spent     uint64
	}{{b.Bytes, bytes}, {b.Effects, effects}, {b.Tokens, tokens}} {
		if pair.remaining != nil && *pair.remaining < pair.spent {
			return hostRefusal(capability.BudgetExceeded, "")
		}
	}
	if b.Bytes != nil {
		*b.Bytes -= bytes
	}
	if b.Effects != nil {
		*b.Effects -= effects
	}
	if b.Tokens != nil {
		*b.Tokens -= tokens
	}
	return nil
}
func hostRefusal(code capability.Code, detail capability.FailureDetail) *capability.Error {
	return &capability.Error{Code: code, EffectState: capability.NotStarted, Detail: detail}
}
func hostContextError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return hostRefusal(capability.DeadlineExceeded, "")
	}
	return hostRefusal(capability.Cancelled, "") //nolint:misspell // SDK wire constant uses this spelling.
}
func methodDescriptor(method HostMethod) string {
	switch method {
	case HostStorageGet:
		return "storage.read"
	case HostStoragePut, HostStorageDelete:
		return "storage.write"
	case HostSecretsGet:
		return "secrets.read"
	case HostEgressRequest:
		return "egress.request"
	case HostEventsPublish:
		return "events.publish"
	case HostLog:
		return "log.write"
	case HostReadonlyQuery:
		return "readonly.query"
	case HostMCPListTools, HostMCPCallTool, HostMCPCancelCall:
		return "mcp.reach"
	case HostBindingsRenew:
		return "binding"
	default:
		return ""
	}
}

// RevokeBinding fences one host-issued lease and its descendants. Possession of
// the ID does not let a plugin invoke this host-only bookkeeping operation.
func (s *HostSession) RevokeBinding(id subprocess.BindingID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if binding := s.bindings[id]; binding != nil {
		s.retireBindingLocked(id, binding)
	}
}

// cancelPluginRequest is the future rpc/cancel receive hook. Only plugin-owned
// requests in this session can be affected; unknown/terminal IDs are no-ops.
func (s *HostSession) cancelPluginRequest(owner subprocess.HostRPCRequestOwner, id uint64) error {
	if owner != subprocess.HostRPCOwnerPlugin {
		return hostRefusal(capability.ScopeDenied, "")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if call, exists := s.active[id]; exists {
		call.cancel()
	}
	return nil
}

// preparedHostBinding owns bounded copied host metadata and entropy. Preparation
// never runs in the writer. Attachment's lock order is Conn -> session -> queue;
// no session path calls back into Conn while holding the session mutex.
type preparedHostBinding struct {
	id     subprocess.BindingID
	record *hostBindingRecord
}

func (s *HostSession) prepareBinding(spec HostBinding) (*preparedHostBinding, error) {
	if len(spec.Scope) == 0 || len(spec.Scope) > 1<<20 || strictjson.Validate(spec.Scope) != nil || bytes.Equal(bytes.TrimSpace(spec.Scope), []byte("null")) {
		return nil, hostRefusal(capability.InvalidRequest, "")
	}
	for _, b := range []*uint64{spec.Budgets.Bytes, spec.Budgets.Effects, spec.Budgets.Tokens} {
		if b != nil && *b > capability.MaxSafeInteger {
			return nil, hostRefusal(capability.InvalidRequest, "")
		}
	}
	if spec.Depth >= 8 {
		return nil, hostRefusal(capability.BudgetExceeded, capability.DepthExceeded)
	}
	spec = cloneHostBinding(spec)
	id, err := randomBinding()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.runtime == nil {
		return nil, hostRefusal(capability.TargetUnavailable, "")
	}
	for _, owner := range spec.Origin {
		if owner == s.owner {
			return nil, hostRefusal(capability.ScopeDenied, capability.CallbackCycle)
		}
	}
	grant, ok := s.grants[spec.GrantID]
	if !ok {
		return nil, hostRefusal(capability.CapabilityDenied, "")
	}
	expires, err := time.Parse(time.RFC3339Nano, grant.ExpiresAt)
	if err != nil {
		return nil, err
	}
	issued, err := time.Parse(time.RFC3339Nano, grant.IssuedAt)
	if err != nil || time.Now().Before(issued) {
		return nil, hostRefusal(capability.CapabilityDenied, "")
	}
	expiry := earliest(spec.Deadline, expires, time.Now().Add(maxBindingLease))
	if !time.Now().Before(expiry) {
		return nil, hostRefusal(capability.TargetUnavailable, "")
	}
	return &preparedHostBinding{id: id, record: &hostBindingRecord{spec: spec, grant: grant, expiry: expiry}}, nil
}
func (s *HostSession) attachParent(id uint64, method string, deadline time.Time, prepared *preparedHostBinding) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.runtime == nil || id == 0 || id > capability.MaxSafeInteger || id <= s.highWater || !time.Now().Before(deadline) {
		return hostRefusal(capability.TargetUnavailable, capability.ParentInvalid)
	}
	if len(s.parents) >= 18 {
		return hostRefusal(capability.RateLimited, "")
	}
	if prepared != nil {
		grant, ok := s.grants[prepared.record.spec.GrantID]
		if !ok || !sameGrant(grant, prepared.record.grant) {
			return hostRefusal(capability.CapabilityDenied, "")
		}
		now := time.Now()
		for key, b := range s.bindings {
			if !now.Before(b.expiry) {
				delete(s.bindings, key)
			}
		}
		if len(s.bindings) >= maxHostBindings {
			return hostRefusal(capability.RateLimited, "")
		}
		if !now.Before(prepared.record.expiry) {
			return hostRefusal(capability.TargetUnavailable, "")
		}
	}
	parent := &hostParent{method: method, deadline: deadline}
	s.highWater = id
	s.parents[id] = parent
	if prepared != nil {
		prepared.record.parent = parent
		prepared.record.spec.Parent = subprocess.ParentCall{RequestOwner: subprocess.HostRPCOwnerHost, ID: id}
		prepared.record.expiry = earliest(prepared.record.expiry, deadline)
		s.bindings[prepared.id] = prepared.record
	}
	return nil
}
