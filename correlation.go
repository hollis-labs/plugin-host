package pluginhost

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

// CorrelationCallError is a local validation refusal from [Conn.CallCorrelation].
// Reason is one of unsupported_method, observer_deadline_required,
// authority_context_conflict or invalid_wire_context. It is not a remote
// JSON-RPC terminal and does not classify a published invocation's effects.
type CorrelationCallError struct{ Reason string }

func (e *CorrelationCallError) Error() string { return "pluginhost: correlation call: " + e.Reason }

// CallCorrelation executes CommandExecute or Health without delegating host
// authority. observer must have an explicit finite deadline. Its cancellation
// cancels this invocation; its deadline is never copied into wire context.
// An absent wire context remains absent. A supplied remote budget is charged
// from preparation through publication, independently of local observation.
// An opaque binding selector is carried unchanged, but creates no host parent,
// grant or lease; reverse helper admission still authenticates genuine authority.
// The runtime must have negotiated reverse RPC and completed host activation.
// Local errors are not remote outcome/rollback evidence. Actual remote typed
// results/errors are preserved; publication receipts remain transport-owned.
func (c *Conn) CallCorrelation(observer context.Context, method string, params any) (json.RawMessage, error) {
	if method != subprocess.MethodCommandExecute && method != subprocess.MethodHealth {
		return nil, &CorrelationCallError{Reason: "unsupported_method"}
	}
	deadline, bounded := observer.Deadline()
	if !bounded {
		return nil, &CorrelationCallError{Reason: "observer_deadline_required"}
	}
	if err := observer.Err(); err != nil {
		return nil, fmt.Errorf("pluginhost: %s: %w", method, err)
	}
	if !time.Now().Before(deadline) {
		return nil, fmt.Errorf("pluginhost: %s: %w", method, context.DeadlineExceeded)
	}
	if _, ok := observer.Value(hostBindingContextKey{}).(HostBinding); ok {
		return nil, &CorrelationCallError{Reason: "authority_context_conflict"}
	}
	if _, ok := observer.Value(forwardBindingKey{}).(subprocess.BindingID); ok {
		return nil, &CorrelationCallError{Reason: "authority_context_conflict"}
	}
	call, err := c.queueCallClass(observer, method, params, true)
	if err != nil {
		return nil, fmt.Errorf("pluginhost: %s: %w", method, err)
	}
	return c.awaitCall(call)
}

func prepareCorrelationParams(params any, started time.Time) (any, time.Time, error) {
	fields, supplied, err := forwardFields(params)
	if err != nil {
		return nil, time.Time{}, &CorrelationCallError{Reason: "invalid_wire_context"}
	}
	if supplied == nil {
		return fields, time.Time{}, nil
	}
	if supplied.TimeoutMS == 0 || (supplied.BindingID != nil && *supplied.BindingID == "") {
		return nil, time.Time{}, &CorrelationCallError{Reason: "invalid_wire_context"}
	}
	// Decode/encode time is consumed by this same absolute monotonic budget.
	return fields, started.Add(time.Duration(supplied.TimeoutMS) * time.Millisecond), nil
}

func (c *Conn) correlationReadyLocked(call *pendingCall) error {
	r := c.reverse
	if r == nil || !r.selected || !r.initialized || !r.loaded || !r.ready || r.fenced || (call.incarnation != nil && call.incarnation != r) {
		return hostRefusal(capability.TargetUnavailable, "")
	}
	call.incarnation = r
	return nil
}

// fenceCorrelationsLocked is transport retirement, not authority retirement.
// No application callback runs here. Conn -> queue is the lock order.
func (c *Conn) fenceCorrelationsLocked() {
	for call := range c.correlations {
		if call.terminal || len(call.reply) != 0 {
			continue
		}
		err := hostRefusal(capability.TargetUnavailable, "")
		c.cancelQueuedCallLocked(call, err)
		call.reply <- callReply{err: err}
		call.stop()
	}
}
