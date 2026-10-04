package pluginhost

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-sdk/subprocess"
)

type heldWrite struct {
	io.Writer
	started, release chan struct{}
	once             sync.Once
}

func (w *heldWrite) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.started); <-w.release })
	return w.Writer.Write(b)
}
func (w *heldWrite) Close() error { return closeIf(w.Writer) }
func (w *heldWrite) SetWriteDeadline(d time.Time) error {
	return w.Writer.(interface{ SetWriteDeadline(time.Time) error }).SetWriteDeadline(d)
}
func holdNextWrite(t *testing.T, c *Conn) *heldWrite {
	t.Helper()
	w := &heldWrite{Writer: c.w, started: make(chan struct{}), release: make(chan struct{})}
	c.w = w
	t.Cleanup(func() {
		select {
		case <-w.release:
		default:
			close(w.release)
		}
	})
	return w
}
func waitQueuedCalls(t *testing.T, c *Conn, n int) {
	t.Helper()
	deadline := time.Now().Add(testWait)
	for {
		c.mu.Lock()
		count := 0
		for _, out := range c.outbound {
			if out.call != nil && !out.call.lifecycle {
				count++
			}
		}
		c.mu.Unlock()
		if count == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("call admission timeout")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWriterIssuesIDsInPublicationOrder(t *testing.T) {
	p := newPeer(t)
	gate := holdNextWrite(t, p.conn)
	barrier := make(chan error, 1)
	go func() { barrier <- p.conn.Notify("barrier", nil) }()
	<-gate.started
	ordinary := callAsync(context.Background(), p.conn, "ordinary")
	waitQueuedCalls(t, p.conn, 1)
	lifecycle := callAsync(context.Background(), p.conn, subprocess.MethodLoad)
	deadline := time.Now().Add(testWait)
	for {
		p.conn.mu.Lock()
		n := 0
		for _, out := range p.conn.outbound {
			if out.call != nil && out.call.lifecycle {
				n++
			}
		}
		p.conn.mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lifecycle admission timeout")
		}
		time.Sleep(time.Millisecond)
	}
	close(gate.release)
	if err := <-barrier; err != nil {
		t.Fatal(err)
	}
	p.frame()
	first := p.request()
	second := p.request()
	if first.Method != subprocess.MethodLoad || first.ID != subprocess.NumberID(1) || second.Method != "ordinary" || second.ID != subprocess.NumberID(2) {
		t.Fatalf("publication: %+v %+v", first, second)
	}
	p.reply(first.ID, `{}`)
	p.reply(second.ID, `true`)
	for _, call := range []<-chan callResult{lifecycle, ordinary} {
		if got := await(t, call); got.err != nil {
			t.Fatal(got.err)
		}
	}
}

func TestCancellationBeforeSelectionDoesNotPublishOrIssueID(t *testing.T) {
	p := newPeer(t)
	gate := holdNextWrite(t, p.conn)
	barrier := make(chan error, 1)
	go func() { barrier <- p.conn.Notify("barrier", nil) }()
	<-gate.started
	ctx, cancel := context.WithCancel(context.Background())
	call := callAsync(ctx, p.conn, "cancel-me")
	waitQueuedCalls(t, p.conn, 1)
	cancel()
	if got := await(t, call); !errors.Is(got.err, context.Canceled) {
		t.Fatal(got.err)
	}
	close(gate.release)
	if err := <-barrier; err != nil {
		t.Fatal(err)
	}
	p.frame()
	next := callAsync(context.Background(), p.conn, "next")
	req := p.request()
	if req.Method != "next" || req.ID != subprocess.NumberID(1) {
		t.Fatalf("canceled publication: %+v", req)
	}
	p.reply(req.ID, `true`)
	if got := await(t, next); got.err != nil {
		t.Fatal(got.err)
	}
}

func TestDemuxIncomingSameIDCannotResolveOutgoingCall(t *testing.T) {
	r, w := io.Pipe()
	frames := make(chan []byte, 8)
	c := NewConn(r, wireWriter(func(b []byte) (int, error) { frames <- append([]byte(nil), b...); return len(b), nil }))
	t.Cleanup(func() { _ = c.Close(); _ = w.Close() })
	call := callAsync(context.Background(), c, "work")
	var req subprocess.RPCRequest
	if err := json.Unmarshal(<-frames, &req); err != nil {
		t.Fatal(err)
	}
	_, err := io.WriteString(w, `{"jsonrpc":"2.0","id":`+itoa(req.ID)+`,"method":"host/log","params":{}}`+"\n")
	if err != nil {
		t.Fatal(err)
	}
	var refusal subprocess.RPCResponse
	select {
	case raw := <-frames:
		if err = json.Unmarshal(raw, &refusal); err != nil {
			t.Fatal(err)
		}
	case <-time.After(testWait):
		t.Fatal("no refusal")
	}
	if refusal.ID != req.ID || refusal.Error == nil || refusal.Error.Code != -32601 {
		t.Fatalf("refusal: %+v", refusal)
	}
	select {
	case got := <-call:
		t.Fatalf("incoming request resolved outgoing call: %+v", got)
	default:
	}
	_, err = io.WriteString(w, `{"jsonrpc":"2.0","id":`+itoa(req.ID)+`,"result":true}`+"\n")
	if err != nil {
		t.Fatal(err)
	}
	if got := await(t, call); got.err != nil || string(got.raw) != "true" {
		t.Fatal(got)
	}
}

func TestMatchingMethodRejectsWrongResult(t *testing.T) {
	p := newPeer(t)
	call := callAsync(context.Background(), p.conn, subprocess.MethodHealth)
	req := p.request()
	p.reply(req.ID, `{"deleted":true}`)
	if got := await(t, call); !errors.Is(got.err, ErrProtocolMismatch) {
		t.Fatal(got.err)
	}
}

func TestPendingRegisteredBeforeFirstByteReply(t *testing.T) {
	r, w := io.Pipe()
	var c *Conn
	c = NewConn(r, wireWriter(func(b []byte) (int, error) {
		var req subprocess.RPCRequest
		if err := json.Unmarshal(b, &req); err != nil {
			return 0, err
		}
		reply := []byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":true}` + "\n")
		// Deliver synchronously inside Write: a late registration cannot win a
		// scheduler race after a pipe read acknowledges the bytes.
		c.deliver(reply)
		return len(b), nil
	}))
	t.Cleanup(func() { _ = c.Close(); _ = w.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	if raw, err := c.Call(ctx, "work", nil); err != nil || string(raw) != "true" {
		t.Fatalf("first byte reply: %s %v", raw, err)
	}
}

func TestReservedCancellationSurvivesSustainedLargeWrites(t *testing.T) {
	for round := range 4 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			p := newPeer(t)
			const calls = 8
			results := make([]<-chan callResult, calls)
			cancels := make([]context.CancelFunc, calls)
			wanted := map[subprocess.RPCID]bool{}
			for i := range calls {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				cancels[i] = cancel
				results[i] = callAsync(ctx, p.conn, "work")
				wanted[p.request().ID] = true
			}
			gate := holdNextWrite(t, p.conn)
			large := map[string]string{"data": strings.Repeat("x", 4<<20)}
			active := make(chan error, 1)
			go func() { active <- p.conn.Notify("large-active", large) }()
			<-gate.started
			queued := make(chan error, 1)
			go func() { queued <- p.conn.Notify("large-queued", large) }()
			deadline := time.Now().Add(testWait)
			for {
				p.conn.mu.Lock()
				n := len(p.conn.outbound)
				p.conn.mu.Unlock()
				if n > 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("ordinary not queued")
				}
				time.Sleep(time.Millisecond)
			}
			for _, cancel := range cancels {
				cancel()
			}
			for _, result := range results {
				if got := await(t, result); !errors.Is(got.err, context.Canceled) {
					t.Fatal(got.err)
				}
			}
			close(gate.release)
			if err := <-active; err != nil {
				t.Fatal(err)
			}
			if frame := p.frame(); frame.Method != "large-active" {
				t.Fatal(frame.Method)
			}
			controlsBeforeOrdinary := 0
			sawOrdinary := false
			for range calls + 1 {
				frame := p.frame()
				if frame.Method == "large-queued" {
					sawOrdinary = true
					if controlsBeforeOrdinary > controlBurst {
						t.Fatalf("ordinary starved behind %d controls", controlsBeforeOrdinary)
					}
					continue
				}
				if frame.Method != "rpc/cancel" || frame.ID != (subprocess.RPCID{}) {
					t.Fatalf("unexpected control: %+v", frame)
				}
				raw, _ := json.Marshal(frame.Params)
				var cancel subprocess.CancelParams
				if err := json.Unmarshal(raw, &cancel); err != nil {
					t.Fatal(err)
				}
				if cancel.RequestOwner != subprocess.HostRPCOwnerHost || !wanted[cancel.ID] {
					t.Fatalf("lost/duplicate cancellation: %+v", cancel)
				}
				delete(wanted, cancel.ID)
				if !sawOrdinary {
					controlsBeforeOrdinary++
				}
			}
			if err := <-queued; err != nil {
				t.Fatal(err)
			}
			if len(wanted) != 0 || !sawOrdinary || p.conn.CancelDropped() != 0 {
				t.Fatalf("controls lost: pending=%d ordinary=%v drops=%d", len(wanted), sawOrdinary, p.conn.CancelDropped())
			}
		})
	}
}

func TestRawInitCannotOfferOrAcknowledgeProfiles(t *testing.T) {
	for _, name := range []string{"host_services", "hooks_profile"} {
		t.Run(name, func(t *testing.T) {
			p := newPeer(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := p.conn.Call(ctx, subprocess.MethodInit, map[string]any{name: map[string]any{}})
			var typed *subprocess.InitError
			if !errors.As(err, &typed) || typed.Code != subprocess.InitProfileMismatch {
				t.Fatal(err)
			}
			if err = p.conn.Notify(subprocess.MethodInit, map[string]any{name: map[string]any{}}); !errors.As(err, &typed) || typed.Code != subprocess.InitProfileMismatch {
				t.Fatal(err)
			}
			select {
			case frame := <-p.reqs:
				t.Fatalf("profile offer published: %+v", frame)
			default:
			}
		})
	}
	for _, name := range []string{"reverse_rpc_version", "hooks_profile_version"} {
		raw := `{"id":"fixture","name":"Fixture","version":"1.0.0","description":"fixture","protocol":2,"capability_contract":1,"` + name + `":1}`
		var typed *subprocess.InitError
		if err := validatePendingResult(subprocess.MethodInit, []byte(raw)); !errors.As(err, &typed) || typed.Code != subprocess.InitProfileMismatch {
			t.Fatal(err)
		}
	}
}

func TestDemuxStrictRepliesDoNotConsumePending(t *testing.T) {
	p := newPeer(t)
	call := callAsync(context.Background(), p.conn, "work")
	req := p.request()
	id := itoa(req.ID)
	for _, raw := range []string{
		`{"id":` + id + `,"result":true}`,
		`{"jsonrpc":"1.0","id":` + id + `,"result":true}`,
		`{"jsonrpc":"2.0","id":` + id + `,"Result":true}`,
		`{"jsonrpc":"2.0","id":` + id + `,"result":true,"id":` + id + `}`,
		`{"jsonrpc":"2.0","id":` + id + `,"result":true,"error":{"code":-32603,"message":"bad"}}`,
		`{"jsonrpc":"2.0","id":` + id + `,"method":"host/log","result":true}`,
	} {
		p.conn.deliver([]byte(raw))
		select {
		case got := <-call:
			t.Fatalf("invalid reply resolved: %s %+v", raw, got)
		default:
		}
	}
	p.reply(req.ID, `"real"`)
	if got := await(t, call); got.err != nil || string(got.raw) != `"real"` {
		t.Fatal(got)
	}
}

func TestDuplicateActiveInboundIDFencesConnection(t *testing.T) {
	p := newPeer(t)
	gate := holdNextWrite(t, p.conn)
	barrier := make(chan error, 1)
	go func() { barrier <- p.conn.Notify("barrier", nil) }()
	<-gate.started
	raw := []byte(`{"jsonrpc":"2.0","id":1,"method":"host/log","params":{}}`)
	p.conn.deliver(raw)
	p.conn.deliver(raw)
	select {
	case <-p.conn.Done():
	case <-time.After(testWait):
		t.Fatal("duplicate active inbound ID did not fence")
	}
	close(gate.release)
	<-barrier
}

func TestCloseableWriterWithoutDeadlinesIsInterrupted(t *testing.T) {
	r, input := io.Pipe()
	output, writer := io.Pipe()
	c := NewConn(r, writer)
	t.Cleanup(func() { _ = c.Close(); _ = input.Close(); _ = output.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := c.Call(ctx, "work", nil)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrGone) {
		t.Fatal(err)
	}
	select {
	case <-c.Done():
	case <-time.After(testWait):
		t.Fatal("writer watchdog did not fence stream")
	}
}

type controlledParams struct{ started, release chan struct{} }

func (p controlledParams) MarshalJSON() ([]byte, error) {
	close(p.started)
	<-p.release
	return []byte(`{"large":"encoded"}`), nil
}

func TestIDPublicationOrderDoesNotDependOnEncodingCompletion(t *testing.T) {
	p := newPeer(t)
	params := controlledParams{started: make(chan struct{}), release: make(chan struct{})}
	slow := make(chan callResult, 1)
	go func() {
		raw, err := p.conn.Call(context.Background(), "slow-encoding", params)
		slow <- callResult{raw, err}
	}()
	<-params.started
	fast := callAsync(context.Background(), p.conn, "fast-encoding")
	first := p.request()
	if first.Method != "fast-encoding" || first.ID != subprocess.NumberID(1) {
		t.Fatal(first)
	}
	close(params.release)
	second := p.request()
	if second.Method != "slow-encoding" || second.ID != subprocess.NumberID(2) {
		t.Fatal(second)
	}
	p.reply(first.ID, `true`)
	p.reply(second.ID, `true`)
	if got := await(t, fast); got.err != nil {
		t.Fatal(got.err)
	}
	if got := await(t, slow); got.err != nil {
		t.Fatal(got.err)
	}
}

func TestOrdinaryAdmissionLeavesLifecycleCapacity(t *testing.T) {
	p := newPeer(t)
	calls := make([]<-chan callResult, ordinaryCallLimit)
	ids := make([]subprocess.RPCID, ordinaryCallLimit)
	for i := range ordinaryCallLimit {
		calls[i] = callAsync(context.Background(), p.conn, "ordinary")
		ids[i] = p.request().ID
	}
	if _, err := p.conn.Call(context.Background(), "overflow", nil); !errors.Is(err, ErrAdmissionFull) {
		t.Fatal(err)
	}
	if _, err := NewClient(p.conn).Health(context.Background()); !errors.Is(err, ErrHealthInconclusive) || !errors.Is(err, ErrAdmissionFull) || errors.Is(err, ErrUnhealthy) {
		t.Fatalf("local capacity became health verdict: %v", err)
	}
	lifecycle := callAsync(context.Background(), p.conn, subprocess.MethodLoad)
	req := p.request()
	if req.Method != subprocess.MethodLoad || req.ID != subprocess.NumberID(ordinaryCallLimit+1) {
		t.Fatal(req)
	}
	p.reply(req.ID, `{}`)
	if got := await(t, lifecycle); got.err != nil {
		t.Fatal(got.err)
	}
	for i, id := range ids {
		p.reply(id, `true`)
		if got := await(t, calls[i]); got.err != nil {
			t.Fatal(got.err)
		}
	}
}

func TestDemuxInvalidResultRetainsTypedInitFailure(t *testing.T) {
	p := newPeer(t)
	call := callAsync(context.Background(), p.conn, subprocess.MethodInit)
	req := p.request()
	p.raw(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":{"id":"fixture","name":"Fixture","version":"1.0.0","description":"","protocol":2,"protocol":2,"capability_contract":1}}`)
	got := await(t, call)
	var typed *subprocess.InitError
	if !errors.Is(got.err, ErrProtocolMismatch) || !errors.As(got.err, &typed) || typed.Code != subprocess.InitInvalid {
		t.Fatal(got.err)
	}
}

func TestInboundIDsKeepABoundedHighWaterMark(t *testing.T) {
	r, w := io.Pipe()
	frames := make(chan []byte, 8)
	c := NewConn(r, wireWriter(func(b []byte) (int, error) { frames <- append([]byte(nil), b...); return len(b), nil }))
	t.Cleanup(func() { _ = c.Close(); _ = w.Close() })
	for _, tc := range []struct {
		id   int64
		code int
	}{{2, -32601}, {1, -32600}, {2, -32600}, {3, -32601}} {
		c.deliver([]byte(`{"jsonrpc":"2.0","id":` + strconv.FormatInt(tc.id, 10) + `,"method":"host/log","params":{}}`))
		var response subprocess.RPCResponse
		select {
		case raw := <-frames:
			if err := json.Unmarshal(raw, &response); err != nil {
				t.Fatal(err)
			}
		case <-time.After(testWait):
			t.Fatal("no correlated refusal")
		}
		if response.ID != subprocess.NumberID(tc.id) || response.Error == nil || response.Error.Code != tc.code {
			t.Fatalf("id%d: %+v", tc.id, response)
		}
		// A physical reply receipt, rather than merely bytes handed to Write,
		// retires the active ID before the next reused-ID probe.
		deadline := time.Now().Add(testWait)
		for {
			c.mu.Lock()
			n := len(c.inboundActive)
			c.mu.Unlock()
			if n == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("inbound ID not retired")
			}
			time.Sleep(time.Millisecond)
		}
	}
}
