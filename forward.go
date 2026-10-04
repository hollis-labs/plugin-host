package pluginhost

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
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
	if method == subprocess.MethodInit {
		if err = refuseProfileOffer(fields); err != nil {
			return nil, ctx, end, err
		}
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

func refuseProfileOffer(fields map[string]json.RawMessage) error {
	for _, name := range []string{"host_services", "hooks_profile"} {
		if _, present := fields[name]; present {
			return &subprocess.InitError{Code: subprocess.InitProfileMismatch, Field: "unsupported_profile", Expected: 0, Received: 1}
		}
	}
	return nil
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

// CancelDropped counts cancellation controls that could not be
// published completely. A partial control frame retires the connection.
func (c *Conn) CancelDropped() int64 { return c.droppedCancel.Load() }

func mustDeadline(ctx context.Context) time.Time { deadline, _ := ctx.Deadline(); return deadline }

// encodeFrame reserves ten bytes for a forward timeout (uint32 plus JSON
// whitespace). This lets publication update only the budget, without encoding
// a potentially multi-megabyte opaque payload while holding the writer.
func encodeFrame(request subprocess.RPCRequest) ([]byte, int, error) {
	if request.ID == (subprocess.RPCID{}) || !forwardMethod(request.Method) {
		frame, err := json.Marshal(request)
		return append(frame, '\n'), -1, err
	}
	fields, fc, err := forwardFields(request.Params)
	if err != nil {
		return nil, -1, err
	}
	if fc == nil {
		frame, marshalErr := json.Marshal(request)
		return append(frame, '\n'), -1, marshalErr
	}
	delete(fields, "context")
	payload, err := json.Marshal(fields)
	if err != nil {
		return nil, -1, err
	}
	request.Params = nil
	header, err := json.Marshal(request)
	if err != nil {
		return nil, -1, err
	}
	frame := append(header[:len(header)-1], []byte(`,"params":{"context":{"timeout_ms":`)...)
	offset := len(frame)
	frame = append(frame, []byte("          ")...)
	copy(frame[offset:], strconv.FormatUint(uint64(fc.TimeoutMS), 10))
	if fc.BindingID != nil {
		binding, err := json.Marshal(fc.BindingID)
		if err != nil {
			return nil, -1, err
		}
		frame = append(frame, []byte(`,"binding_id":`)...)
		frame = append(frame, binding...)
	}
	frame = append(frame, '}')
	if len(payload) > 2 {
		frame = append(frame, ',')
		frame = append(frame, payload[1:len(payload)-1]...)
	}
	frame = append(frame, []byte("}}\n")...)
	return frame, offset, nil
}
