package pluginhost

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

// The deadline has passed but timer delivery has not yet set Err. This pins
// the race without relying on the scheduler winning a particular select.
type pendingDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c pendingDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }

func TestDeadlineReplyPreservesUnknownEffectAndLocalExpiry(t *testing.T) {
	for _, code := range []capability.Code{capability.DeadlineExceeded, capability.UnknownOutcome} {
		for _, offset := range []time.Duration{-time.Millisecond, time.Millisecond, time.Hour} {
			ctx := pendingDeadlineContext{context.Background(), time.Now().Add(offset)}
			rpc := &subprocess.RPCError{Code: capability.HostRPCErrorCode, Message: "deadline", Data: subprocess.HostRPCErrorData{Contract: "host-rpc/1", Code: code, RequestID: 1, EffectState: capability.Unknown}}
			_, err := finish(ctx, subprocess.MethodMCPCallTool, subprocess.RPCResponse{Error: rpc})
			wantDeadline := code == capability.DeadlineExceeded || offset <= time.Millisecond
			var got *subprocess.RPCError
			if errors.Is(err, context.DeadlineExceeded) != wantDeadline || !errors.As(err, &got) || got != rpc {
				t.Fatalf("code=%s offset=%s: %v", code, offset, err)
			}
			data := got.Data.(subprocess.HostRPCErrorData)
			if data.EffectState != capability.Unknown {
				t.Fatal("effect state lost")
			}
		}
	}
}

func TestLocalHealthTimeoutRefusesAndHostCancellationPreservesVerdict(t *testing.T) {
	p := newPeer(t)
	gate := NewHealthGate(NewClient(p.conn).Health, -1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result := make(chan HealthVerdict, 1)
	go func() { result <- gate.Probe(ctx) }()
	p.request() // A published health request receives no reply.
	v := <-result
	if v.OK || v.Reachable || v.Inconclusive || !errors.Is(gate.Check(), ErrUnhealthy) {
		t.Fatalf("silent probe: %+v %v", v, gate.Check())
	}
	if err := healthError(context.DeadlineExceeded); !errors.Is(err, ErrUnhealthy) || errors.Is(err, ErrHealthInconclusive) {
		t.Fatal(err)
	}
	if err := healthError(context.Canceled); !errors.Is(err, context.Canceled) || errors.Is(err, ErrUnhealthy) || errors.Is(err, ErrHealthInconclusive) {
		t.Fatal("host cancellation classified as health", err)
	}
	before := v
	canceled, stop := context.WithCancel(context.Background())
	stop()
	v = gate.Probe(canceled)
	if !v.Checked.IsZero() || v.Inconclusive || v.OK {
		t.Fatalf("host cancel became verdict: %+v", v)
	}
	if gate.last != before || !errors.Is(gate.Check(), ErrUnhealthy) {
		t.Fatal("host cancellation erased refusal")
	}
	fresh := NewHealthGate(NewClient(p.conn).Health, -1)
	fresh.Probe(canceled)
	if err := fresh.Check(); err != nil || !fresh.last.Checked.IsZero() {
		t.Fatal("host cancellation poisoned unprobed gate", err)
	}
}

type shortControlWriter struct {
	n   int
	err error
}

func (w shortControlWriter) SetWriteDeadline(time.Time) error { return nil }
func (w shortControlWriter) Write([]byte) (int, error)        { return w.n, w.err }

func TestPartialCancellationControlRetiresConnection(t *testing.T) {
	for _, writeErr := range []error{nil, io.ErrUnexpectedEOF} {
		p := newPeer(t)
		if err := p.conn.w.(io.Closer).Close(); err != nil {
			t.Fatal(err)
		}
		p.conn.w = shortControlWriter{n: 20, err: writeErr}
		p.conn.cancelCall(subprocess.NumberID(1), context.Canceled)
		select {
		case <-p.conn.Done():
		default:
			t.Fatal("partial frame kept stream alive")
		}
		if _, err := p.conn.Call(context.Background(), "next", nil); !errors.Is(err, ErrGone) {
			t.Fatal(err)
		}
		if p.conn.CancelDropped() != 1 {
			t.Fatal("partial drop uncounted")
		}
	}
}

func TestCancellationControlWriteBound(t *testing.T) {
	writer, reader := net.Pipe()
	defer writer.Close()
	defer reader.Close()
	p := newPeer(t)
	if err := p.conn.w.(io.Closer).Close(); err != nil {
		t.Fatal(err)
	}
	p.conn.w = writer
	start := time.Now()
	p.conn.cancelCall(subprocess.NumberID(1), context.Canceled)
	elapsed := time.Since(start)
	// A zero-byte deadline failure leaves the stream usable. No peer reads, so
	// this exercises the actual writer deadline rather than a fake timeout.
	if elapsed < 70*time.Millisecond || elapsed > 300*time.Millisecond {
		t.Fatalf("cancel write took %s, want ~100ms", elapsed)
	}
	if p.conn.CancelDropped() != 1 {
		t.Fatal("timeout uncounted")
	}
	select {
	case <-p.conn.Done():
		t.Fatal("zero-byte timeout retired connection")
	default:
	}
}

func TestForwardBudgetRoundsDownAndClipsUint32(t *testing.T) {
	for _, budget := range []time.Duration{100*time.Millisecond + 900*time.Microsecond, 60 * 24 * time.Hour} {
		ctx := pendingDeadlineContext{context.Background(), time.Now().Add(budget)}
		params, err := refreshForwardParams(ctx, subprocess.MethodHealth, nil)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(params)
		var got struct {
			Context subprocess.ForwardContext `json:"context"`
		}
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if budget < time.Second && (got.Context.TimeoutMS < 98 || got.Context.TimeoutMS > 100) {
			t.Fatalf("not floored: %s", raw)
		}
		if budget > time.Second && got.Context.TimeoutMS != ^uint32(0) {
			t.Fatalf("not clipped: %s", raw)
		}
		p := newPeer(t)
		if err := p.conn.send(ctx, subprocess.RPCRequest{JSONRPC: "2.0", ID: subprocess.NumberID(1), Method: subprocess.MethodHealth, Params: params}); err != nil {
			t.Fatal(err)
		}
		req := p.request()
		raw, _ = json.Marshal(req.Params)
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if budget > time.Second && got.Context.TimeoutMS != ^uint32(0) {
			t.Fatalf("publication not clipped: %s", raw)
		}
		if budget < time.Second && got.Context.TimeoutMS > 100 {
			t.Fatalf("publication not floored: %s", raw)
		}
	}
	ctx := pendingDeadlineContext{context.Background(), time.Now().Add(600 * time.Microsecond)}
	if _, err := refreshForwardParams(ctx, subprocess.MethodHealth, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("sub-ms budget published", err)
	}
}

func TestDTOBudgetNarrowsLocalCallDeadline(t *testing.T) {
	p := newPeer(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	result := make(chan error, 1)
	go func() {
		_, err := p.conn.Call(ctx, subprocess.MethodHealth, map[string]any{"context": subprocess.ForwardContext{TimeoutMS: 30}})
		result <- err
	}()
	p.request()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("DTO budget failed to narrow local deadline")
	}
	if time.Since(start) > 300*time.Millisecond || ctx.Err() != nil {
		t.Fatal("outer context used for narrowed call")
	}
}

func TestReplyKeepsAlreadyDeliveredContextError(t *testing.T) {
	for _, expired := range []bool{false, true} {
		var ctx context.Context
		var cancel context.CancelFunc
		want := context.Canceled
		if expired {
			ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			want = context.DeadlineExceeded
		} else {
			ctx, cancel = context.WithCancel(context.Background())
			cancel()
		}
		defer cancel()
		rpc := &subprocess.RPCError{Code: subprocess.ErrCodeInternal, Message: "handler error"}
		_, err := finish(ctx, "work", subprocess.RPCResponse{Error: rpc})
		var got *subprocess.RPCError
		if !errors.Is(err, want) || !errors.As(err, &got) || got != rpc {
			t.Fatal(err)
		}
	}
}
