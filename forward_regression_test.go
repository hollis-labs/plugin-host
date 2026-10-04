package pluginhost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

type writerWithoutDeadline struct{ io.Writer }

func TestCancelWithoutDeadlineDoesNotRetireConnection(t *testing.T) {
	p := newPeer(t)
	original := p.conn.w
	t.Cleanup(func() { _ = closeIf(original) })
	p.conn.w = writerWithoutDeadline{original}
	ctx, cancel := context.WithCancel(context.Background())
	call := callAsync(ctx, p.conn, "work")
	p.request()
	cancel()
	if got := await(t, call); !errors.Is(got.err, context.Canceled) {
		t.Fatal(got.err)
	}
	waitCancelDropped(t, p.conn)
	next := callAsync(context.Background(), p.conn, "unrelated")
	req := p.request()
	p.reply(req.ID, `true`)
	if got := await(t, next); got.err != nil {
		t.Fatal(got.err)
	}
}

func waitCancelDropped(t *testing.T, c *Conn) {
	t.Helper()
	deadline := time.Now().Add(testWait)
	for c.CancelDropped() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("lost control drop")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestNotifyDoesNotInventForwardDeadline(t *testing.T) {
	p := newPeer(t)
	for _, supplied := range []bool{false, true} {
		params := map[string]any{"event": "tick", "data": map[string]any{}, "pre_hook": false}
		if supplied {
			params["context"] = map[string]any{"timeout_ms": 123}
		}
		if err := p.conn.Notify(subprocess.MethodEventHandle, params); err != nil {
			t.Fatal(err)
		}
		req := p.frame()
		raw, _ := json.Marshal(req.Params)
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(raw, &fields)
		contextRaw, present := fields["context"]
		if present != supplied {
			t.Fatalf("invented context: %s", raw)
		}
		if supplied && string(contextRaw) != `{"timeout_ms":123}` {
			t.Fatal(string(contextRaw))
		}
	}
}

func TestForwardBudgetAccountsForWriterWait(t *testing.T) {
	p := newPeer(t)
	gate := holdNextWrite(t, p.conn)
	barrier := make(chan error, 1)
	go func() { barrier <- p.conn.Notify("barrier", nil) }()
	<-gate.started
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	call := callAsync(ctx, p.conn, subprocess.MethodHealth)
	// The writer barrier holds publication after call admission. Wait long enough
	// to distinguish a publication budget from one captured at call start.
	deadline := time.Now().Add(testWait)
	for {
		p.conn.mu.Lock()
		admitted := p.conn.ordinaryCalls == 1
		p.conn.mu.Unlock()
		if admitted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("call not admitted")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(150 * time.Millisecond)
	close(gate.release)
	if err := <-barrier; err != nil {
		t.Fatal(err)
	}
	if req := p.frame(); req.Method != "barrier" {
		t.Fatal(req.Method)
	}
	req := p.request()
	raw, _ := json.Marshal(req.Params)
	var params struct {
		Context subprocess.ForwardContext `json:"context"`
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		t.Fatal(err)
	}
	if params.Context.TimeoutMS == 0 || params.Context.TimeoutMS > 2850 {
		t.Fatalf("stale publication budget: %d", params.Context.TimeoutMS)
	}
	p.reply(req.ID, `{"ok":true}`)
	if got := await(t, call); got.err != nil {
		t.Fatal(got.err)
	}
}

func TestReplyBeforeCancelWinsAndDoesNotSendControl(t *testing.T) {
	p := newPeer(t)
	ctx, cancel := context.WithCancel(context.Background())
	call := callAsync(ctx, p.conn, "work")
	req := p.request()
	p.conn.deliver([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":true}`))
	cancel()
	cancel()
	got := await(t, call)
	if got.err != nil || string(got.raw) != "true" {
		t.Fatalf("reply lost: %+v", got)
	}
	select {
	case frame := <-p.reqs:
		t.Fatalf("extra frame: %+v", frame)
	default:
	}
	if p.conn.CancelDropped() != 0 {
		t.Fatal("cancel attempted after reply")
	}
}

func TestHealthErrorClassesAndRemoteDeadline(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		app  capability.Code
		want error
	}{
		{"authored", -32603, "", ErrUnhealthy},
		{"invalid_request", -32600, "", ErrProtocolMismatch},
		{"invalid_params", -32602, "", ErrProtocolMismatch},
		{"method_missing", -32601, "", ErrProtocolMismatch},
		{"busy", -32010, capability.RateLimited, ErrHealthInconclusive},
		{"deadline", -32010, capability.DeadlineExceeded, ErrHealthInconclusive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newPeer(t)
			result := make(chan error, 1)
			go func() { _, err := NewClient(p.conn).Health(context.Background()); result <- err }()
			req := p.request()
			rpc := &subprocess.RPCError{Code: tc.code, Message: tc.name}
			if tc.app != "" {
				rpc.Data = subprocess.HostRPCErrorData{Contract: "host-rpc/1", Code: tc.app, RequestID: 1, EffectState: capability.NotStarted}
			}
			frame, err := json.Marshal(subprocess.RPCResponse{JSONRPC: "2.0", ID: req.ID, Error: rpc})
			if err != nil {
				t.Fatal(err)
			}
			p.conn.deliver(frame)
			err = <-result
			var typed *subprocess.RPCError
			if !errors.Is(err, tc.want) || !errors.As(err, &typed) {
				t.Fatal(err)
			}
			if !errors.Is(tc.want, ErrUnhealthy) && errors.Is(err, ErrUnhealthy) {
				t.Fatal("non-handler failure unhealthy")
			}
			if tc.app == capability.DeadlineExceeded && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("lost context deadline")
			}
		})
	}
}

func TestHealthRejectsMalformedResults(t *testing.T) {
	for _, raw := range []string{`null`, `{}`, `{"ok":null}`, `{"ok":"true"}`, `{"ok":true,"message":null}`, `{"ok":true,"unknown":1}`} {
		t.Run(raw, func(t *testing.T) {
			p := newPeer(t)
			result := make(chan error, 1)
			go func() { _, err := NewClient(p.conn).Health(context.Background()); result <- err }()
			req := p.request()
			p.reply(req.ID, raw)
			if err := <-result; !errors.Is(err, ErrProtocolMismatch) {
				t.Fatal(err)
			}
		})
	}
}

func TestHealthGateRetriesInconclusiveProbe(t *testing.T) {
	calls := 0
	gate := NewHealthGate(func(context.Context) (subprocess.HealthResult, error) {
		calls++
		if calls == 1 {
			return subprocess.HealthResult{}, &HealthInconclusiveError{Cause: context.DeadlineExceeded}
		}
		return subprocess.HealthResult{OK: true}, nil
	}, time.Hour)
	if verdict := gate.Probe(context.Background()); !verdict.Inconclusive || verdict.OK {
		t.Fatal(verdict)
	}
	if err := gate.Check(); err != nil {
		t.Fatal(err)
	}
	if verdict := gate.Probe(context.Background()); !verdict.OK || calls != 2 {
		t.Fatalf("inconclusive cached: %+v", verdict)
	}
}

func TestClosedConnectionDoesNotWrite(t *testing.T) {
	p := newPeer(t)
	p.conn.fail(ErrGone)
	if _, err := p.conn.Call(context.Background(), "work", nil); !errors.Is(err, ErrGone) {
		t.Fatal(err)
	}
	if err := p.conn.Notify("work", nil); !errors.Is(err, ErrGone) {
		t.Fatal(err)
	}
	if err := p.conn.send(context.Background(), subprocess.RPCRequest{JSONRPC: "2.0", Method: "work"}); !errors.Is(err, ErrGone) {
		t.Fatal(err)
	}
	select {
	case frame := <-p.reqs:
		t.Fatalf("wrote after close: %+v", frame)
	default:
	}
}

type refusingDeadlineWriter struct{ io.Writer }

func (refusingDeadlineWriter) SetWriteDeadline(time.Time) error {
	return errors.New("deadline unsupported")
}

type failedControlWriter struct{ io.Writer }

func (failedControlWriter) SetWriteDeadline(time.Time) error { return nil }
func (w failedControlWriter) Write(frame []byte) (int, error) {
	var request subprocess.RPCRequest
	if err := json.Unmarshal(frame, &request); err != nil {
		return 0, err
	}
	if request.Method == "rpc/cancel" {
		return 0, errors.New("control write refused")
	}
	return w.Writer.Write(frame)
}

func TestCancelDeadlineAndWriteFailuresLeaveOtherCallsAlive(t *testing.T) {
	for _, rejectDeadline := range []bool{true, false} {
		t.Run(map[bool]string{true: "deadline_refused", false: "write_failed"}[rejectDeadline], func(t *testing.T) {
			p := newPeer(t)
			original := p.conn.w
			t.Cleanup(func() { _ = closeIf(original) })
			if rejectDeadline {
				p.conn.w = refusingDeadlineWriter{p.conn.w}
			} else {
				p.conn.w = failedControlWriter{p.conn.w}
			}
			ctx, cancel := context.WithCancel(context.Background())
			call := callAsync(ctx, p.conn, "work")
			p.request()
			cancel()
			if got := await(t, call); !errors.Is(got.err, context.Canceled) {
				t.Fatal(got.err)
			}
			waitCancelDropped(t, p.conn)
			next := callAsync(context.Background(), p.conn, "next")
			request := p.request()
			p.reply(request.ID, `true`)
			if got := await(t, next); got.err != nil {
				t.Fatal(got.err)
			}
		})
	}
}

func TestClosedConnectionCheckAtPublication(t *testing.T) {
	var writer bytes.Buffer
	c := &Conn{w: &writer, maxFrame: defaultMaxFrame, done: make(chan struct{}), closedBy: ErrGone}
	// Keep Done unready to force the writer-select branch, then check the
	// authoritative fence before publication. Both production branches can win
	// when writer capacity and Done are ready together.
	if err := c.send(context.Background(), subprocess.RPCRequest{JSONRPC: "2.0", Method: "work"}); !errors.Is(err, ErrGone) {
		t.Fatal(err)
	}
	if writer.Len() != 0 {
		t.Fatal("published through closed fence")
	}
}
