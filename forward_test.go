package pluginhost

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-sdk/subprocess"
)

func TestTaggedReplyIDsKeepZeroAndStringsDistinct(t *testing.T) {
	p := newPeer(t)
	for _, id := range []subprocess.RPCID{subprocess.NumberID(0), subprocess.StringID("0"), subprocess.StringID("job-A")} {
		reply := make(chan subprocess.RPCResponse, 1)
		p.conn.mu.Lock()
		p.conn.pending[id] = reply
		p.conn.mu.Unlock()
		p.reply(id, `true`)
		select {
		case r := <-reply:
			if r.ID != id || string(r.Result) != "true" {
				t.Fatal(r)
			}
		case <-time.After(testWait):
			t.Fatal("tagged reply dropped")
		}
		p.conn.mu.Lock()
		delete(p.conn.pending, id)
		p.conn.mu.Unlock()
	}
}

func TestOutboundIDsRefuseExhaustionWithoutWrapping(t *testing.T) {
	p := newPeer(t)
	p.conn.nextID.Store(maxRequestID - 1)
	call := callAsync(context.Background(), p.conn, "last")
	req := p.request()
	if req.ID != subprocess.NumberID(maxRequestID) {
		t.Fatal(req.ID)
	}
	p.reply(req.ID, `true`)
	if got := await(t, call); got.err != nil {
		t.Fatal(got.err)
	}
	if _, err := p.conn.Call(context.Background(), "exhausted", nil); !errors.Is(err, ErrRequestIDExhausted) {
		t.Fatal(err)
	}
	select {
	case r := <-p.reqs:
		t.Fatalf("published exhausted ID: %+v", r)
	default:
	}
}

func TestForwardContextClipsBudgetAndPreservesCallerDTO(t *testing.T) {
	p := newPeer(t)
	ctx, end := context.WithTimeout(context.Background(), time.Second)
	defer end()
	ctx = WithForwardBinding(ctx, subprocess.BindingID("host-issued-binding"))
	original := subprocess.MCPCallRequest{ToolName: "echo", Arguments: map[string]any{"v": 1}, Context: &subprocess.ForwardContext{TimeoutMS: 5000}}
	done := make(chan error, 1)
	go func() { _, err := NewClient(p.conn).MCPCallTool(ctx, original); done <- err }()
	req := p.request()
	raw, _ := json.Marshal(req.Params)
	var got subprocess.MCPCallRequest
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Context == nil || got.Context.TimeoutMS == 0 || got.Context.TimeoutMS > 1000 || got.Context.BindingID == nil || *got.Context.BindingID != "host-issued-binding" {
		t.Fatalf("bad forward context: %+v", got.Context)
	}
	if original.Context.TimeoutMS != 5000 || original.Context.BindingID != nil {
		t.Fatal("mutated caller DTO")
	}
	p.reply(req.ID, `{"content":null}`)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCancellationPublishesClosedHostOwnedControl(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "caller", true: "deadline"}[deadline], func(t *testing.T) {
			p := newPeer(t)
			var ctx context.Context
			var end context.CancelFunc
			if deadline {
				ctx, end = context.WithTimeout(context.Background(), 100*time.Millisecond)
			} else {
				ctx, end = context.WithCancel(context.Background())
			}
			defer end()
			call := callAsync(ctx, p.conn, "work")
			req := p.frame()
			if !deadline {
				end()
			}
			got := await(t, call)
			if !errors.Is(got.err, context.Canceled) && !errors.Is(got.err, context.DeadlineExceeded) {
				t.Fatal(got.err)
			}
			control := p.frame()
			if control.ID != (subprocess.RPCID{}) || control.Method != "rpc/cancel" {
				t.Fatalf("not a notification: %+v", control)
			}
			raw, err := json.Marshal(control.Params)
			if err != nil {
				t.Fatal(err)
			}
			var params subprocess.CancelParams
			if err = json.Unmarshal(raw, &params); err != nil {
				t.Fatal(err)
			}
			want := subprocess.CallerCancelled
			if deadline {
				want = subprocess.DeadlineExpired
			}
			if params.ID != req.ID || params.RequestOwner != subprocess.HostRPCOwnerHost || params.Reason != want {
				t.Fatalf("wrong cancellation: %+v", params)
			}
			if err := p.conn.Notify("barrier", nil); err != nil {
				t.Fatal(err)
			}
			if next := p.frame(); next.Method != "barrier" {
				t.Fatalf("duplicate control before barrier: %+v", next)
			}
		})
	}
}

func TestForwardWithoutDeadlineOmitsContext(t *testing.T) {
	p := newPeer(t, WithDefaultTimeout(-1))
	call := callAsync(context.Background(), p.conn, subprocess.MethodHealth)
	req := p.request()
	raw, _ := json.Marshal(req.Params)
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	if _, exists := fields["context"]; exists {
		t.Fatal("invented forward deadline")
	}
	p.reply(req.ID, `{"ok":true}`)
	if got := await(t, call); got.err != nil {
		t.Fatal(got.err)
	}
}

func TestHealthRPCFailureIsUnhealthyAndKeepsTypedCause(t *testing.T) {
	p := newPeer(t)
	client := NewClient(p.conn)
	done := make(chan error, 1)
	go func() { _, err := client.Health(context.Background()); done <- err }()
	req := p.request()
	p.raw(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"error":{"code":-32603,"message":"health callback failed"}}`)
	err := <-done
	var rpc *subprocess.RPCError
	if !errors.Is(err, ErrUnhealthy) || !errors.As(err, &rpc) || errors.Is(err, ErrProtocolMismatch) {
		t.Fatal(err)
	}
	gate := NewHealthGate(func(context.Context) (subprocess.HealthResult, error) { return subprocess.HealthResult{}, err }, -1)
	v := gate.Probe(context.Background())
	if v.OK || !v.Reachable || !errors.Is(gate.Check(), ErrUnhealthy) {
		t.Fatalf("wrong health classification: %+v", v)
	}
}
