package pluginhost

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

// ReverseProfile explicitly opts a host into the optional reverse profile.
// Init.HostServices supplies the exact offer. Nil keeps profile refusal.
// Runtime must be shared across a host. LifecycleBinding is trusted host
// narrowing for log-only Init/Load/Unload; Parent is filled at writer selection.
// No policy callback runs in the reader or writer.
type ReverseProfile struct {
	Runtime          *HostServiceRuntime
	Required         bool
	LifecycleBinding *HostBinding
}

type hostBindingContextKey struct{}

// WithHostBinding supplies trusted host narrowing for this forward invocation.
// Preparation copies it before admission; the selected parent is attached
// before the first byte. It neither predicts IDs nor accepts plugin authority.
func WithHostBinding(ctx context.Context, binding HostBinding) context.Context {
	binding = cloneHostBinding(binding)
	return context.WithValue(ctx, hostBindingContextKey{}, binding)
}

func cloneHostBinding(b HostBinding) HostBinding {
	b.Scope = append(json.RawMessage(nil), b.Scope...)
	b.Origin = append([]Owner(nil), b.Origin...)
	b.Budgets = copyBudgets(b.Budgets)
	return b
}
func cloneReverseProfile(p *ReverseProfile) *ReverseProfile {
	if p == nil {
		return nil
	}
	out := *p
	if p.LifecycleBinding != nil {
		b := cloneHostBinding(*p.LifecycleBinding)
		out.LifecycleBinding = &b
	}
	return &out
}

type reverseConnection struct {
	profile                                                 ReverseProfile
	params                                                  subprocess.InitParams
	business, cleanup                                       *HostSession
	selected, initialized, loaded, ready, fenced, unloading bool
	high                                                    uint64
	workers                                                 int
	active                                                  map[uint64]context.CancelFunc
}

// WithReverseProfile attaches an explicit offer to a standalone Conn. Process
// callers use Spec.Reverse instead. Invalid configuration fails all calls.
func WithReverseProfile(profile ReverseProfile, params subprocess.InitParams) ConnOption {
	spec := snapshotSpec(Spec{Reverse: &profile, Init: params})
	return func(c *Conn) {
		reverse, err := newReverseConnection(spec)
		if err != nil {
			c.configError = err
			return
		}
		c.reverse = reverse
		if reverse != nil {
			c.writeTimeout = min(c.writeTimeout, time.Duration(spec.Init.HostServices.Limits.WriteTimeoutMS)*time.Millisecond)
		}
	}
}

func profileMismatch() error {
	return &subprocess.InitError{Code: subprocess.InitProfileMismatch, Field: "unsupported_profile", Expected: 0, Received: 1}
}
func validateReverseSpec(s Spec) error {
	if s.Init.HooksProfile != nil {
		return profileMismatch()
	}
	if s.Reverse == nil {
		if s.Init.HostServices != nil {
			return profileMismatch()
		}
		return s.Init.Validate()
	}
	if s.Reverse.Runtime == nil || s.Init.HostServices == nil {
		return profileMismatch()
	}
	if err := s.Init.Validate(); err != nil {
		return err
	}
	for _, method := range s.Init.HostServices.Methods {
		if !s.Reverse.Runtime.services.implements(HostMethod(method)) {
			return profileMismatch()
		}
	}
	// Admission/credit capacities are fixed. Unsupported lower offers fail
	// closed rather than claiming a ceiling which this implementation exceeds.
	l := s.Init.HostServices.Limits
	if l.HostToPluginInflight < 16 || l.PluginToHostInflight < 8 || l.HostGlobalInflight < 64 || l.ControlSlots < 2 || l.MaxFrameBytes < 8<<20 || l.MaxQueuedWriteBytes < 8<<20 || l.MaxDepth < 8 {
		return &subprocess.InitError{Code: subprocess.InitInvalid, Field: "host_services.limits"}
	}
	return nil
}
func newReverseConnection(s Spec) (*reverseConnection, error) {
	if err := validateReverseSpec(s); err != nil {
		return nil, err
	}
	if s.Reverse == nil {
		return nil, nil
	}
	r := &reverseConnection{profile: *s.Reverse, params: s.Init, active: make(map[uint64]context.CancelFunc)}
	owner := Owner{HostInstance: s.Init.Incarnation.HostInstance, OwnerID: s.Init.Incarnation.OwnerID, OwnerGeneration: s.Init.Incarnation.OwnerGeneration}
	ceilings := make(map[HostMethod]time.Duration)
	for name, ms := range s.Init.HostServices.Limits.MethodTimeoutMS {
		ceilings[HostMethod(name)] = time.Duration(ms) * time.Millisecond
	}
	var err error
	r.business, err = s.Reverse.Runtime.OpenSession(owner, s.Init.Grants, ceilings)
	if err != nil {
		return nil, err
	}
	// The cleanup session is distinct and never readied for business. Its only
	// possible operation is explicitly offered/granted log under selected Unload.
	logs := make(map[HostMethod]time.Duration)
	if ceiling, ok := ceilings[HostLog]; ok {
		logs[HostLog] = ceiling
	}
	var grants capability.GrantSet
	for _, g := range s.Init.Grants {
		if g.Name == capability.LogWrite {
			grants = append(grants, g)
		}
	}
	r.cleanup, err = s.Reverse.Runtime.OpenSession(owner, grants, logs)
	if err != nil {
		r.business.Close()
		return nil, err
	}
	return r, nil
}
func (c *Conn) validateInit(p subprocess.InitParams) error {
	c.mu.Lock()
	r := c.reverse
	configError := c.configError
	c.mu.Unlock()
	if configError != nil {
		return configError
	}
	if r == nil {
		return validateInitParams(p)
	}
	// Init policy cannot be replaced after session authentication.
	a, err := json.Marshal(p)
	if err != nil {
		return err
	}
	b, err := json.Marshal(r.params)
	if err != nil {
		return err
	}
	if string(a) != string(b) {
		return profileMismatch()
	}
	return p.Validate()
}
func (c *Conn) verifyInit(p subprocess.InitParams, result subprocess.InitResult) error {
	if err := subprocess.ValidateInitResult(p, result); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reverse == nil {
		return verifyInitResult(p, result)
	}
	if result.HooksProfileVersion != nil {
		return profileMismatch()
	}
	if c.reverse.profile.Required && result.ReverseRPCVersion == nil {
		return profileMismatch()
	}
	return nil
}

// ActivateHostServices marks host activation after successful Load. Lifecycle
// calls this after its Activate callback and current-generation fence. Standalone
// Start calls it after its handshake. It cannot resurrect revoked authority.
func (c *Conn) ActivateHostServices() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reverse == nil {
		return nil
	}
	r := c.reverse
	if r.fenced || !r.initialized || !r.loaded {
		return hostRefusal(capability.TargetUnavailable, "")
	}
	if !r.selected {
		return nil
	}
	if err := r.business.OwnerReady(); err != nil {
		return err
	}
	r.ready = true
	return nil
}

// RevokeHostServices irreversibly fences business authority immediately. It
// preserves only the separate bounded log-only lease for a future Unload.
func (c *Conn) RevokeHostServices() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.revokeBusinessLocked()
}
func (c *Conn) revokeBusinessLocked() {
	if r := c.reverse; r != nil {
		r.fenced = true
		r.ready = false
		c.fenceCorrelationsLocked()
		r.business.Revoke()
	}
}
func (c *Conn) closeReverseLocked() {
	if r := c.reverse; r != nil {
		r.fenced = true
		r.ready = false
		r.business.Close()
		r.cleanup.Close()
		for _, cancel := range r.active {
			cancel()
		}
	}
}
func (c *Conn) retireParentLocked(call *pendingCall) {
	if call.session != nil && call.id != (subprocess.RPCID{}) {
		id, ok := call.id.Integer()
		if ok && id > 0 {
			call.session.RetireParent(uint64(id))
		}
	}
}
func (c *Conn) prepareParent(call *pendingCall, params any) (any, error) {
	c.mu.Lock()
	r := c.reverse
	if r == nil {
		c.mu.Unlock()
		return params, nil
	}
	if r.initialized && !r.selected && call.method != subprocess.MethodInit {
		c.mu.Unlock()
		return params, nil
	}
	if (call.method != subprocess.MethodInit && !r.initialized) || (call.method != subprocess.MethodInit && call.method != subprocess.MethodUnload && r.fenced) || (!call.lifecycle && !r.ready) {
		c.mu.Unlock()
		return nil, hostRefusal(capability.TargetUnavailable, "")
	}
	if call.method == subprocess.MethodInit && r.initialized {
		c.mu.Unlock()
		return nil, profileMismatch()
	}
	session := r.business
	if call.method == subprocess.MethodUnload {
		session = r.cleanup
	}
	binding, has := call.ctx.Value(hostBindingContextKey{}).(HostBinding)
	if call.lifecycle {
		has = r.profile.LifecycleBinding != nil
		if has {
			binding = *r.profile.LifecycleBinding
		}
	}
	call.session = session
	c.mu.Unlock()
	if !has {
		return params, nil
	}
	if !forwardMethod(call.method) {
		return nil, hostRefusal(capability.UnsupportedCapability, "")
	}
	if call.lifecycle {
		session.mu.Lock()
		g, ok := session.grants[binding.GrantID]
		session.mu.Unlock()
		if !ok || g.Name != capability.LogWrite {
			return nil, hostRefusal(capability.CapabilityDenied, "")
		}
	}
	deadline, ok := call.ctx.Deadline()
	if !ok {
		return nil, errors.New("host binding requires a deadline")
	}
	if binding.Parent.ID != 0 || binding.Parent.RequestOwner != "" {
		return nil, errors.New("host binding parent is selected by the writer")
	}
	if binding.Deadline.IsZero() {
		binding.Deadline = deadline
	} else {
		binding.Deadline = earliest(binding.Deadline, deadline)
	}
	prepared, err := session.prepareBinding(binding)
	if err != nil {
		return nil, err
	}
	call.prepared = prepared
	// Encoding uses this prepared opaque reference; it is not live until attach.
	call.ctx = WithForwardBinding(call.ctx, prepared.id)
	return refreshForwardParams(call.ctx, call.method, params)
}

func (s HostServices) implements(method HostMethod) bool {
	switch method {
	case HostStorageGet:
		return s.StorageGet != nil
	case HostStoragePut:
		return s.StoragePut != nil
	case HostStorageDelete:
		return s.StorageDelete != nil
	case HostSecretsGet:
		return s.SecretsGet != nil
	case HostEgressRequest:
		return s.EgressRequest != nil
	case HostEventsPublish:
		return s.EventsPublish != nil
	case HostLog:
		return s.Log != nil
	case HostReadonlyQuery:
		return s.ReadonlyQuery != nil
	case HostMCPListTools:
		return s.MCPListTools != nil
	case HostMCPCallTool:
		return s.MCPCallTool != nil
	case HostMCPCancelCall, HostBindingsRenew:
		return true // session-owned operations with optional vetoes
	default:
		return false
	}
}
