package pluginhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hollis-labs/plugin-sdk/subprocess"
)

const maxRequestID int64 = 9007199254740991
const cancelWriteTimeout = 100 * time.Millisecond

type forwardBindingKey struct{}

// WithForwardBinding carries a host-issued binding reference for this invocation.
// It does not issue, authenticate or widen a binding. The host owns its ledger.
// A call requires a deadline (or the connection default) to carry the reference.
func WithForwardBinding(ctx context.Context, binding subprocess.BindingID) context.Context {
	return context.WithValue(ctx, forwardBindingKey{}, binding)
}

func (c *Conn) allocateID() (subprocess.RPCID, error) {
	for {
		old := c.nextID.Load()
		if old >= maxRequestID {
			return subprocess.RPCID{}, ErrRequestIDExhausted
		}
		if c.nextID.CompareAndSwap(old, old+1) {
			return subprocess.NumberID(old + 1), nil
		}
	}
}

func forwardMethod(method string) bool {
	switch method {
	case subprocess.MethodInit, subprocess.MethodLoad, subprocess.MethodUnload, subprocess.MethodHealth,
		subprocess.MethodCommandExecute, subprocess.MethodEventHandle, subprocess.MethodMCPCallTool,
		subprocess.MethodHTTPHandle, subprocess.MethodMigrate, subprocess.MethodCRUDCreate,
		subprocess.MethodCRUDRead, subprocess.MethodCRUDUpdate, subprocess.MethodCRUDDelete, subprocess.MethodCRUDList:
		return true
	}
	return false
}

// These copies preserve opaque JSON tokens and never modify caller-owned DTOs.
func forwardFields(params any) (map[string]json.RawMessage, *subprocess.ForwardContext, error) {
	fields := map[string]json.RawMessage{}
	if params != nil {
		raw, err := json.Marshal(params)
		if err != nil {
			return nil, nil, err
		}
		if err = json.Unmarshal(raw, &fields); err != nil || fields == nil {
			return nil, nil, errors.New("forward params must be a non-null object")
		}
	}
	var supplied *subprocess.ForwardContext
	if raw, ok := fields["context"]; ok {
		if string(raw) == "null" {
			return nil, nil, errors.New("forward context must not be null")
		}
		var v subprocess.ForwardContext
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, nil, err
		}
		supplied = &v
	}
	return fields, supplied, nil
}

func prepareForwardCall(ctx context.Context, method string, params any) (any, context.Context, context.CancelFunc, error) {
	end := func() {}
	if !forwardMethod(method) {
		return params, ctx, end, nil
	}
	fields, supplied, err := forwardFields(params)
	if err != nil {
		return nil, ctx, end, err
	}
	if supplied != nil {
		limit := time.Now().Add(time.Duration(supplied.TimeoutMS) * time.Millisecond)
		if deadline, ok := ctx.Deadline(); !ok || limit.Before(deadline) {
			ctx, end = context.WithDeadline(ctx, limit)
		}
	}
	// The same local deadline covers encoding, writer wait and remote execution.
	out, err := refreshForwardParams(ctx, method, fields)
	if err != nil {
		end()
	}
	return out, ctx, end, err
}

func refreshForwardParams(ctx context.Context, method string, params any) (any, error) {
	if !forwardMethod(method) {
		return params, nil
	}
	fields, supplied, err := forwardFields(params)
	if err != nil {
		return nil, err
	}
	binding, hasBinding := ctx.Value(forwardBindingKey{}).(subprocess.BindingID)
	if supplied != nil && supplied.BindingID != nil {
		if hasBinding && binding != *supplied.BindingID {
			return nil, errors.New("conflicting forward binding")
		}
		binding, hasBinding = *supplied.BindingID, true
	}
	deadline, bounded := ctx.Deadline()
	if !bounded {
		if hasBinding {
			return nil, errors.New("forward binding requires a call deadline")
		}
		return fields, nil
	}
	remaining := time.Until(deadline).Milliseconds()
	if remaining <= 0 {
		return nil, context.DeadlineExceeded
	}
	if remaining > int64(^uint32(0)) {
		remaining = int64(^uint32(0))
	}
	if supplied != nil && remaining > int64(supplied.TimeoutMS) {
		remaining = int64(supplied.TimeoutMS)
	}
	fc := subprocess.ForwardContext{TimeoutMS: uint32(remaining)}
	if hasBinding {
		fc.BindingID = &binding
	}
	fields["context"], err = json.Marshal(fc)
	return fields, err
}

func (c *Conn) cancelCall(id subprocess.RPCID, cause error) {
	reason := subprocess.CallerCancelled
	if errors.Is(cause, context.DeadlineExceeded) {
		reason = subprocess.DeadlineExpired
	}
	// Cancellation uses a fresh bounded write budget, never the canceled call's.
	// No detached goroutine or retry is created. If control cannot progress, retire
	// the connection so the peer cannot keep executing under a live transport.
	ctx, end := context.WithTimeout(context.Background(), cancelWriteTimeout)
	defer end()
	err := c.send(ctx, subprocess.RPCRequest{JSONRPC: "2.0", Method: "rpc/cancel", Params: subprocess.CancelParams{
		RequestOwner: subprocess.HostRPCOwnerHost, ID: id, Reason: reason,
	}})
	if err != nil {
		c.fail(fmt.Errorf("%w: cancellation control unavailable", ErrGone))
		_ = c.Close()
	}
}
