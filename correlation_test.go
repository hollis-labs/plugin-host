package pluginhost

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

var correlationCall = (*Conn).CallCorrelation

func TestCorrelationAbsentWireContextAndNoAuthority(t *testing.T) {
	p := reverseReadyPeer(t, reverseTestSpec(t, HostServices{}))
	p.conn.reverse.business.mu.Lock()
	initialBindings := len(p.conn.reverse.business.bindings)
	p.conn.reverse.business.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := correlationCall(p.conn, ctx, subprocess.MethodHealth, map[string]any{}); done <- err }()
	req := p.request()
	var fields map[string]json.RawMessage
	raw, _ := json.Marshal(req.Params)
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	_, hasContext := fields["context"]
	p.conn.mu.Lock()
	session := p.conn.reverse.business
	session.mu.Lock()
	parents, bindings := len(session.parents), len(session.bindings)
	session.mu.Unlock()
	p.conn.mu.Unlock()
	p.reply(req.ID, `{"ok":true}`)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if hasContext || parents != 0 || bindings != initialBindings {
		t.Fatalf("correlation gained wire context/authority: context=%v parents=%d bindings=%d", hasContext, parents, bindings)
	}
}

func TestCorrelationQueuedBudgetClipsAtActualPublication(t *testing.T) {
	p := reverseReadyPeer(t, reverseTestSpec(t, HostServices{}))
	gate := holdNextWrite(t, p.conn)
	barrier := make(chan error, 1)
	go func() { barrier <- p.conn.Notify("barrier", nil) }()
	<-gate.started
	observer, end := context.WithTimeout(context.Background(), 2*time.Second)
	defer end()
	done := make(chan error, 1)
	go func() {
		_, err := p.conn.CallCorrelation(observer, subprocess.MethodHealth, json.RawMessage(`{"context":{"timeout_ms":500,"binding_id":"binding-example"}}`))
		done <- err
	}()
	waitQueuedCalls(t, p.conn, 1)
	time.Sleep(100 * time.Millisecond)
	close(gate.release)
	if err := <-barrier; err != nil {
		t.Fatal(err)
	}
	p.frame()
	req := p.request()
	fc := reverseParams(t, req)
	if fc.TimeoutMS == 0 || fc.TimeoutMS > 400 || fc.BindingID == nil || *fc.BindingID != "binding-example" {
		t.Fatal("queue reset/widened budget/selector", fc)
	}
	p.reply(req.ID, `{"ok":true}`)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

type correlationDelayedReceiptWriter struct {
	io.Writer
	started, release chan struct{}
	once             sync.Once
}

func (w *correlationDelayedReceiptWriter) Write(raw []byte) (int, error) {
	n, err := w.Writer.Write(raw)
	w.once.Do(func() { close(w.started); <-w.release })
	return n, err
}
func (w *correlationDelayedReceiptWriter) Close() error { return closeIf(w.Writer) }
func (w *correlationDelayedReceiptWriter) SetWriteDeadline(d time.Time) error {
	return w.Writer.(interface{ SetWriteDeadline(time.Time) error }).SetWriteDeadline(d)
}

func TestCorrelationCancelCreditRemainsOwnedUntilPublicationReceipt(t *testing.T) {
	p := reverseReadyPeer(t, reverseTestSpec(t, HostServices{}))
	until := time.Now().Add(time.Second)
	for {
		p.conn.queue.mu.Lock()
		idle := p.conn.queue.active == nil
		p.conn.queue.mu.Unlock()
		if idle {
			break
		}
		if time.Now().After(until) {
			t.Fatal("writer not idle")
		}
		time.Sleep(time.Millisecond)
	}
	w := &correlationDelayedReceiptWriter{Writer: p.conn.w, started: make(chan struct{}), release: make(chan struct{})}
	p.conn.w = w
	t.Cleanup(func() {
		select {
		case <-w.release:
		default:
			close(w.release)
		}
	})
	ctx, end := context.WithTimeout(context.Background(), time.Second)
	defer end()
	done := make(chan error, 1)
	go func() { _, err := p.conn.CallCorrelation(ctx, subprocess.MethodHealth, nil); done <- err }()
	req := p.request()
	<-w.started
	end()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	p.conn.queue.mu.Lock()
	reserved := p.conn.queue.reserved
	p.conn.queue.mu.Unlock()
	if reserved != 1 {
		t.Fatal("in-flight publication lost cancellation credit", reserved)
	}
	close(w.release)
	control := p.frame()
	raw, _ := json.Marshal(control.Params)
	var cancel subprocess.CancelParams
	if json.Unmarshal(raw, &cancel) != nil || cancel.ID != req.ID || cancel.Reason != subprocess.CallerCancelled {
		t.Fatal("settled publication lost reserved cancel", control)
	}
}

func TestCorrelationZeroPartialAndCompletedPublicationHaveDistinctExpiry(t *testing.T) {
	for _, state := range []string{"zero", "partial", "complete"} {
		t.Run(state, func(t *testing.T) {
			p := reverseReadyPeer(t, reverseTestSpec(t, HostServices{}))
			// Wait for the handshake's physical writer receipt before replacing only
			// this test-owned stream. The reader and negotiation remain actual Conn.
			until := time.Now().Add(time.Second)
			for {
				p.conn.queue.mu.Lock()
				idle := p.conn.queue.active == nil
				p.conn.queue.mu.Unlock()
				if idle {
					break
				}
				if time.Now().After(until) {
					t.Fatal("handshake writer active")
				}
				time.Sleep(time.Millisecond)
			}
			old := p.conn.w
			left, right := net.Pipe()
			p.conn.w = left
			t.Cleanup(func() { _ = left.Close(); _ = right.Close(); _ = closeIf(old) })
			ctx, end := context.WithTimeout(context.Background(), time.Second)
			defer end()
			call, err := p.conn.queueCallClass(ctx, subprocess.MethodHealth, json.RawMessage(`{"context":{"timeout_ms":40}}`), true)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { _, callErr := p.conn.awaitCall(call); done <- callErr }()
			if state == "partial" {
				buf := make([]byte, 16)
				if _, readErr := right.Read(buf); readErr != nil {
					t.Fatal(readErr)
				}
			}
			if state == "complete" {
				wire := make([]byte, 8192)
				n, readErr := right.Read(wire)
				if readErr != nil {
					t.Fatal(readErr)
				}
				var request subprocess.RPCRequest
				if json.Unmarshal(wire[:n], &request) != nil {
					t.Fatal("incomplete test frame")
				}
				select {
				case earlyErr := <-done:
					t.Fatal("completed remote budget ended observer", earlyErr)
				case <-time.After(80 * time.Millisecond):
				}
				p.reply(request.ID, `{"ok":true}`)
			}
			err = <-done
			if state == "complete" && err != nil {
				t.Fatal(err)
			}
			if state == "zero" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("zero-byte send expiry", err)
			}
			if state == "partial" && !errors.Is(err, ErrGone) {
				t.Fatal("partial send did not fence", err)
			}
			until = time.Now().Add(time.Second)
			for {
				p.conn.mu.Lock()
				settled := call.frame == nil
				pub := call.publication
				closed := p.conn.closedBy
				p.conn.mu.Unlock()
				if settled {
					if state == "zero" && (pub.bytes != 0 || pub.complete || closed != nil) {
						t.Fatalf("zero-byte publication %+v closed=%v", pub, closed)
					}
					if state == "partial" && (pub.bytes != 16 || pub.complete || closed == nil) {
						t.Fatalf("partial publication %+v closed=%v", pub, closed)
					}
					if state == "complete" && (!pub.complete || pub.cancelReason != "" || closed != nil) {
						t.Fatalf("complete publication %+v closed=%v", pub, closed)
					}
					break
				}
				if time.Now().After(until) {
					t.Fatal("physical receipt never settled")
				}
				time.Sleep(time.Millisecond)
			}
			p.conn.queue.mu.Lock()
			reserved := p.conn.queue.reserved
			p.conn.queue.mu.Unlock()
			if reserved != 0 {
				t.Fatal("credit not retired once", reserved)
			}
		})
	}
}

func TestCorrelationRefusesInvalidAuthorityAndInputsBeforePublication(t *testing.T) {
	p := reverseReadyPeer(t, reverseTestSpec(t, HostServices{}))
	bounded, end := context.WithTimeout(context.Background(), time.Second)
	defer end()
	for _, row := range []struct {
		name, method, reason string
		ctx                  context.Context
		params               any
	}{
		{"method", subprocess.MethodLoad, "unsupported_method", bounded, nil},
		{"observer", subprocess.MethodHealth, "observer_deadline_required", context.Background(), nil},
		{"hostbinding", subprocess.MethodHealth, "authority_context_conflict", WithHostBinding(bounded, HostBinding{}), nil},
		{"reference", subprocess.MethodHealth, "authority_context_conflict", WithForwardBinding(bounded, "opaque"), nil},
		{"null", subprocess.MethodHealth, "invalid_wire_context", bounded, json.RawMessage(`{"context":null}`)},
		{"zero", subprocess.MethodHealth, "invalid_wire_context", bounded, json.RawMessage(`{"context":{"timeout_ms":0}}`)},
		{"emptybinding", subprocess.MethodHealth, "invalid_wire_context", bounded, json.RawMessage(`{"context":{"timeout_ms":100,"binding_id":""}}`)},
		{"extracontext", subprocess.MethodHealth, "invalid_wire_context", bounded, json.RawMessage(`{"context":{"timeout_ms":100,"extra":1}}`)},
	} {
		t.Run(row.name, func(t *testing.T) {
			_, err := p.conn.CallCorrelation(row.ctx, row.method, row.params)
			var local *CorrelationCallError
			if !errors.As(err, &local) || local.Reason != row.reason {
				t.Fatalf("error %v", err)
			}
			select {
			case req := <-p.reqs:
				t.Fatalf("invalid invocation published %+v", req)
			default:
			}
		})
	}
}

func TestCorrelationInertSelectorCannotAuthorizeHelper(t *testing.T) {
	var effects atomic.Int32
	p := reverseReadyPeer(t, reverseTestSpec(t, HostServices{StorageGet: func(context.Context, *HostCall, subprocess.StorageGetParams) (subprocess.StorageGetResult, error) {
		effects.Add(1)
		return subprocess.StorageGetResult{Found: false}, nil
	}}))
	ctx, end := context.WithTimeout(context.Background(), time.Second)
	defer end()
	done := make(chan error, 1)
	go func() {
		_, err := p.conn.CallCorrelation(ctx, subprocess.MethodHealth, json.RawMessage(`{"context":{"timeout_ms":500,"binding_id":"binding-example"}}`))
		done <- err
	}()
	req := p.request()
	fc := reverseParams(t, req)
	if fc.BindingID == nil || *fc.BindingID != "binding-example" {
		t.Fatal("selector changed", fc)
	}
	reverseRequest(t, p, 1, HostStorageGet, req)
	refusal := p.frame()
	if refusal.Method != "" || refusal.ID != subprocess.NumberID(1) {
		t.Fatalf("missing typed helper refusal %+v", refusal)
	}
	p.reply(req.ID, `{"ok":true}`)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if effects.Load() != 0 {
		t.Fatal("inert selector reached backend")
	}
	p.conn.reverse.business.mu.Lock()
	defer p.conn.reverse.business.mu.Unlock()
	if p.conn.reverse.business.bindings["binding-example"] != nil || len(p.conn.reverse.business.parents) != 0 {
		t.Fatal("inert selector gained authority")
	}
}

func TestCorrelationObserverCancelUsesActualDirectionalReason(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "deadline"}[deadline], func(t *testing.T) {
			p := reverseReadyPeer(t, reverseTestSpec(t, HostServices{}))
			observer, end := context.WithTimeout(context.Background(), time.Second)
			if deadline {
				end()
				observer, end = context.WithTimeout(context.Background(), 50*time.Millisecond)
			}
			defer end()
			call, err := p.conn.queueCallClass(observer, subprocess.MethodHealth, nil, true)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { _, callErr := p.conn.awaitCall(call); done <- callErr }()
			req := p.request()
			if !deadline {
				end()
			}
			err = <-done
			want := context.Canceled
			reason := subprocess.CallerCancelled
			if deadline {
				want = context.DeadlineExceeded
				reason = subprocess.DeadlineExpired
			}
			if !errors.Is(err, want) {
				t.Fatal(err)
			}
			control := p.frame()
			raw, _ := json.Marshal(control.Params)
			var cancel subprocess.CancelParams
			if json.Unmarshal(raw, &cancel) != nil || control.Method != "rpc/cancel" || cancel.RequestOwner != subprocess.HostRPCOwnerHost || cancel.ID != req.ID || cancel.Reason != reason {
				t.Fatalf("wrong cancel %+v", control)
			}
			until := time.Now().Add(time.Second)
			for {
				p.conn.mu.Lock()
				complete := call.publication.cancelComplete
				p.conn.mu.Unlock()
				if complete {
					break
				}
				if time.Now().After(until) {
					t.Fatal("no cancel physical receipt")
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

func TestCorrelationQueuedBudgetExpiryAndRevocationNeverPublish(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		t.Run(map[bool]string{false: "budget", true: "revoke"}[revoke], func(t *testing.T) {
			p := reverseReadyPeer(t, reverseTestSpec(t, HostServices{}))
			gate := holdNextWrite(t, p.conn)
			barrier := make(chan error, 1)
			go func() { barrier <- p.conn.Notify("barrier", nil) }()
			<-gate.started
			observer, end := context.WithTimeout(context.Background(), time.Second)
			defer end()
			call, err := p.conn.queueCallClass(observer, subprocess.MethodHealth, json.RawMessage(`{"context":{"timeout_ms":40}}`), true)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { _, callErr := p.conn.awaitCall(call); done <- callErr }()
			if revoke {
				p.conn.RevokeHostServices()
			} else {
				time.Sleep(80 * time.Millisecond)
			}
			close(gate.release)
			if barrierErr := <-barrier; barrierErr != nil {
				t.Fatal(barrierErr)
			}
			p.frame()
			err = <-done
			if revoke {
				var refusal *capability.Error
				if !errors.As(err, &refusal) || refusal.Code != capability.TargetUnavailable {
					t.Fatal(err)
				}
			} else if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			select {
			case req := <-p.reqs:
				t.Fatalf("expired/fenced call published %+v", req)
			default:
			}
			p.conn.mu.Lock()
			reserved := p.conn.queue.reserved
			active := len(p.conn.correlations)
			id := call.id
			p.conn.mu.Unlock()
			if reserved != 0 || active != 0 || id != (subprocess.RPCID{}) {
				t.Fatalf("retirement reserved=%d active=%d id=%v", reserved, active, id)
			}
		})
	}
}

func TestCorrelationPublishedRevokeCancelsWithoutAuthorityParent(t *testing.T) {
	p := reverseReadyPeer(t, reverseTestSpec(t, HostServices{}))
	ctx, end := context.WithTimeout(context.Background(), time.Second)
	defer end()
	done := make(chan error, 1)
	go func() { _, err := p.conn.CallCorrelation(ctx, subprocess.MethodHealth, nil); done <- err }()
	req := p.request()
	p.conn.RevokeHostServices()
	var refusal *capability.Error
	if err := <-done; !errors.As(err, &refusal) || refusal.Code != capability.TargetUnavailable {
		t.Fatal(err)
	}
	control := p.frame()
	raw, _ := json.Marshal(control.Params)
	var cancellation subprocess.CancelParams
	if json.Unmarshal(raw, &cancellation) != nil || cancellation.ID != req.ID || cancellation.Reason != subprocess.CallerCancelled {
		t.Fatalf("no published revoke control %+v", control)
	}
	if _, err := p.conn.CallCorrelation(ctx, subprocess.MethodHealth, nil); !errors.As(err, &refusal) {
		t.Fatal("fenced call admitted", err)
	}
}

func TestCorrelationRemoteBudgetDoesNotCancelObserver(t *testing.T) {
	p := reverseReadyPeer(t, reverseTestSpec(t, HostServices{}))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := correlationCall(p.conn, ctx, subprocess.MethodCommandExecute, map[string]any{"name": "uncooperative", "args": "{}", "session_id": "", "context": map[string]any{"timeout_ms": 40, "binding_id": "binding-example"}})
		done <- err
	}()
	req := p.request()
	select {
	case err := <-done:
		t.Fatalf("remote wire timer ended observer before remote terminal: %v", err)
	case <-time.After(80 * time.Millisecond):
	}
	select {
	case cancel := <-p.reqs:
		t.Fatalf("remote timer manufactured host cancel: %+v", cancel)
	default:
	}
	id, _ := json.Marshal(req.ID)
	p.raw(`{"jsonrpc":"2.0","id":` + string(id) + `,"error":{"code":-32010,"message":"remote deadline","data":{"contract":"host-rpc/1","code":"unknown_outcome","request_id":` + string(id) + `,"effect_state":"unknown","retryable":false}}}`)
	err := <-done
	var rpc *subprocess.RPCError
	if !errors.As(err, &rpc) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("remote terminal replaced with observer expiry: %v", err)
	}
	if code, _ := applicationCode(rpc); code != capability.UnknownOutcome {
		t.Fatal(err)
	}
}

func TestCorrelationQueuedCloseRetiresUnpublishedCredit(t *testing.T) {
	p := reverseReadyPeer(t, reverseTestSpec(t, HostServices{}))
	gate := holdNextWrite(t, p.conn)
	barrier := make(chan error, 1)
	go func() { barrier <- p.conn.Notify("barrier", nil) }()
	<-gate.started
	observer, end := context.WithTimeout(context.Background(), time.Second)
	defer end()
	call, err := p.conn.queueCallClass(observer, subprocess.MethodHealth, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, callErr := p.conn.awaitCall(call); done <- callErr }()
	p.conn.fail(io.EOF)
	err = <-done
	close(gate.release)
	<-barrier
	if !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	p.conn.mu.Lock()
	p.conn.queue.mu.Lock()
	reserved := p.conn.queue.reserved
	p.conn.queue.mu.Unlock()
	active := len(p.conn.correlations)
	ordinary := p.conn.ordinaryCalls
	id := call.id
	bytes := call.publication.bytes
	p.conn.mu.Unlock()
	if reserved != 0 || active != 0 || ordinary != 0 || id != (subprocess.RPCID{}) || bytes != 0 {
		t.Fatalf("queued close did not retire effect-free reservation: reserved=%d correlations=%d ordinary=%d id=%v physical_bytes=%d", reserved, active, ordinary, id, bytes)
	}
}

func TestCorrelationCloseRetainsCreditUntilActiveWriteReturns(t *testing.T) {
	p := reverseReadyPeer(t, reverseTestSpec(t, HostServices{}))
	until := time.Now().Add(time.Second)
	for {
		p.conn.queue.mu.Lock()
		idle := p.conn.queue.active == nil
		p.conn.queue.mu.Unlock()
		if idle {
			break
		}
		if time.Now().After(until) {
			t.Fatal("handshake writer active")
		}
		time.Sleep(time.Millisecond)
	}
	w := &correlationDelayedReceiptWriter{Writer: p.conn.w, started: make(chan struct{}), release: make(chan struct{})}
	p.conn.w = w
	t.Cleanup(func() {
		select {
		case <-w.release:
		default:
			close(w.release)
		}
	})
	observer, end := context.WithTimeout(context.Background(), time.Second)
	defer end()
	call, err := p.conn.queueCallClass(observer, subprocess.MethodHealth, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, callErr := p.conn.awaitCall(call); done <- callErr }()
	request := p.request()
	<-w.started
	p.conn.fail(io.EOF)
	end() // Exercise cancellation concurrently with irreversible close.
	if err := <-done; !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	p.conn.queue.mu.Lock()
	held := p.conn.queue.reserved
	active := p.conn.queue.active == call.frame
	p.conn.queue.mu.Unlock()
	if held != 1 || !active {
		t.Fatal("close stole physical writer credit", held, active)
	}
	close(w.release)
	until = time.Now().Add(time.Second)
	for {
		p.conn.mu.Lock()
		p.conn.queue.mu.Lock()
		reserved := p.conn.queue.reserved
		settled := p.conn.queue.active == nil
		p.conn.queue.mu.Unlock()
		complete, bytes, id := call.publication.complete, call.publication.bytes, call.id
		p.conn.mu.Unlock()
		if settled {
			if reserved != 0 || !complete || bytes == 0 || id != request.ID {
				t.Fatal("active close receipt/credit", reserved, complete, bytes, id)
			}
			break
		}
		if time.Now().After(until) {
			t.Fatal("active physical writer not retired")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCorrelationQueuedReaderEOFRetiresUnpublishedCredit(t *testing.T) {
	p := reverseReadyPeer(t, reverseTestSpec(t, HostServices{}))
	gate := holdNextWrite(t, p.conn)
	barrier := make(chan error, 1)
	go func() { barrier <- p.conn.Notify("barrier", nil) }()
	<-gate.started
	observer, end := context.WithTimeout(context.Background(), time.Second)
	defer end()
	call, err := p.conn.queueCallClass(observer, subprocess.MethodHealth, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, callErr := p.conn.awaitCall(call); done <- callErr }()
	if closeErr := p.toConn.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	err = <-done
	close(gate.release)
	<-barrier
	if !errors.Is(err, ErrGone) {
		t.Fatal(err)
	}
	p.conn.mu.Lock()
	p.conn.queue.mu.Lock()
	reserved := p.conn.queue.reserved
	p.conn.queue.mu.Unlock()
	active := len(p.conn.correlations)
	ordinary := p.conn.ordinaryCalls
	id := call.id
	bytes := call.publication.bytes
	p.conn.mu.Unlock()
	if reserved != 0 || active != 0 || ordinary != 0 || id != (subprocess.RPCID{}) || bytes != 0 {
		t.Fatalf("queued close did not retire effect-free reservation: reserved=%d correlations=%d ordinary=%d id=%v physical_bytes=%d", reserved, active, ordinary, id, bytes)
	}
}
