package pluginhost

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-sdk/subprocess"
)

const testWait = 10 * time.Second

// peer is an in-memory plugin: it reads the requests a Conn writes and lets a
// test script the frames that come back.
type peer struct {
	t      *testing.T
	reqs   chan subprocess.RPCRequest
	toConn *io.PipeWriter // peer -> Conn
	conn   *Conn
	mu     sync.Mutex
}

func newPeer(t *testing.T, opts ...ConnOption) *peer {
	t.Helper()
	fromConnR, fromConnW := io.Pipe() // Conn -> peer
	toConnR, toConnW := io.Pipe()     // peer -> Conn
	p := &peer{t: t, reqs: make(chan subprocess.RPCRequest, 64), toConn: toConnW}
	p.conn = NewConn(toConnR, fromConnW, opts...)
	go func() {
		sc := bufio.NewScanner(fromConnR)
		sc.Buffer(nil, 32<<20)
		for sc.Scan() {
			var r subprocess.RPCRequest
			if err := json.Unmarshal(sc.Bytes(), &r); err == nil {
				p.reqs <- r
			}
		}
	}()
	t.Cleanup(func() {
		_ = p.conn.Close()
		_ = toConnW.Close()
		_ = fromConnR.Close()
	})
	return p
}

func (p *peer) request() subprocess.RPCRequest {
	p.t.Helper()
	select {
	case r := <-p.reqs:
		return r
	case <-time.After(testWait):
		p.t.Fatal("peer saw no request")
		return subprocess.RPCRequest{}
	}
}

func (p *peer) raw(line string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, _ = io.WriteString(p.toConn, line+"\n")
}

func (p *peer) reply(id int64, result string) {
	p.raw(`{"jsonrpc":"2.0","id":` + itoa(id) + `,"result":` + result + `}`)
}

func itoa(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}

type callResult struct {
	raw json.RawMessage
	err error
}

func callAsync(ctx context.Context, c *Conn, method string) <-chan callResult {
	ch := make(chan callResult, 1)
	go func() {
		raw, err := c.Call(ctx, method, nil)
		ch <- callResult{raw, err}
	}()
	return ch
}

func await(t *testing.T, ch <-chan callResult) callResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(testWait):
		t.Fatal("call did not return")
		return callResult{}
	}
}

func TestConnIDsStartAtOneAndRepliesMayArriveOutOfOrder(t *testing.T) {
	p := newPeer(t)
	slow := callAsync(context.Background(), p.conn, "slow")
	r1 := p.request()
	fast := callAsync(context.Background(), p.conn, "fast")
	r2 := p.request()
	if r1.ID != 1 || r2.ID != 2 {
		t.Fatalf("ids = %d, %d; want 1, 2", r1.ID, r2.ID)
	}
	p.reply(r2.ID, `"fast-result"`)
	if got := await(t, fast); got.err != nil || string(got.raw) != `"fast-result"` {
		t.Fatalf("fast = %s, %v", got.raw, got.err)
	}
	select {
	case <-slow:
		t.Fatal("slow call returned before its reply")
	default:
	}
	p.reply(r1.ID, `"slow-result"`)
	if got := await(t, slow); got.err != nil || string(got.raw) != `"slow-result"` {
		t.Fatalf("slow = %s, %v", got.raw, got.err)
	}
}

func TestConnPluginErrorIsAnRPCError(t *testing.T) {
	p := newPeer(t)
	ch := callAsync(context.Background(), p.conn, "x")
	r := p.request()
	p.raw(`{"jsonrpc":"2.0","id":` + itoa(r.ID) + `,"error":{"code":-32002,"message":"bad"}}`)
	got := await(t, ch)
	var rpcErr *subprocess.RPCError
	if !errors.As(got.err, &rpcErr) || rpcErr.Code != -32002 {
		t.Fatalf("err = %v, want *RPCError -32002", got.err)
	}
	if errors.Is(got.err, ErrGone) {
		t.Fatal("a plugin error must not look like ErrGone")
	}
}

func TestConnCancelledCallLeavesTheConnectionUsableAndDropsTheLateReply(t *testing.T) {
	p := newPeer(t)
	ctx, cancel := context.WithCancel(context.Background())
	abandoned := callAsync(ctx, p.conn, "abandoned")
	r1 := p.request()
	cancel()
	if got := await(t, abandoned); !errors.Is(got.err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", got.err)
	}
	next := callAsync(context.Background(), p.conn, "next")
	r2 := p.request()
	p.reply(r1.ID, `"late"`) // nobody waits for it
	p.reply(r2.ID, `"mine"`)
	if got := await(t, next); got.err != nil || string(got.raw) != `"mine"` {
		t.Fatalf("next = %s, %v", got.raw, got.err)
	}
}

func TestConnDefaultTimeoutAppliesOnlyWithoutADeadline(t *testing.T) {
	p := newPeer(t, WithDefaultTimeout(150*time.Millisecond))
	start := time.Now()
	got := await(t, callAsync(context.Background(), p.conn, "hang"))
	if !errors.Is(got.err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", got.err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("default timeout did not bound the call")
	}
	<-p.reqs

	// An explicit, longer deadline is not clipped by the default.
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	ch := callAsync(ctx, p.conn, "slowish")
	r := p.request()
	time.Sleep(400 * time.Millisecond)
	select {
	case got := <-ch:
		t.Fatalf("call with its own deadline was cut short: %v", got.err)
	default:
	}
	p.reply(r.ID, `1`)
	if got := await(t, ch); got.err != nil {
		t.Fatal(got.err)
	}
}

func TestConnNegativeDefaultTimeoutMeansNone(t *testing.T) {
	c := NewConn(strings.NewReader(""), io.Discard, WithDefaultTimeout(-1))
	if c.defaultTimeout > 0 {
		t.Fatalf("defaultTimeout = %v", c.defaultTimeout)
	}
}

func TestConnDropsFramesThatAreNotAnswers(t *testing.T) {
	p := newPeer(t)
	ch := callAsync(context.Background(), p.conn, "x")
	r := p.request()
	for _, junk := range []string{
		`not json at all`,
		``,
		`null`,
		"\xff\xfe\xfd",
		`{"jsonrpc":"2.0","id":0,"result":"id zero"}`,
		`{"jsonrpc":"2.0","id":424242,"result":"nobody waits"}`,
		`{"jsonrpc":"2.0","id":"` + itoa(r.ID) + `","result":"string id"}`,
		`{"jsonrpc":"2.0","id":` + itoa(r.ID) + `,"method":"plugin/init","result":"a request, not an answer"}`,
		`[1,2,3]`,
	} {
		p.raw(junk)
	}
	select {
	case got := <-ch:
		t.Fatalf("junk completed the call: %s, %v", got.raw, got.err)
	case <-time.After(100 * time.Millisecond):
	}
	p.reply(r.ID, `"real"`)
	if got := await(t, ch); got.err != nil || string(got.raw) != `"real"` {
		t.Fatalf("got %s, %v", got.raw, got.err)
	}
	select {
	case <-p.conn.Done():
		t.Fatal("junk closed the connection")
	default:
	}
}

func TestConnEOFFailsEveryWaiterWithErrGoneAtOnce(t *testing.T) {
	p := newPeer(t)
	a := callAsync(context.Background(), p.conn, "a")
	b := callAsync(context.Background(), p.conn, "b")
	p.request()
	p.request()
	_ = p.toConn.Close()
	for _, ch := range []<-chan callResult{a, b} {
		if got := await(t, ch); !errors.Is(got.err, ErrGone) {
			t.Fatalf("err = %v, want ErrGone", got.err)
		}
	}
	if _, err := p.conn.Call(context.Background(), "later", nil); !errors.Is(err, ErrGone) {
		t.Fatalf("later call err = %v, want ErrGone", err)
	}
	if err := p.conn.Notify("later", nil); !errors.Is(err, ErrGone) {
		t.Fatalf("notify err = %v, want ErrGone", err)
	}
	select {
	case <-p.conn.Done():
	default:
		t.Fatal("Done not closed")
	}
}

func TestConnDeliversTheFinalFrameWrittenBeforeEOF(t *testing.T) {
	for range 50 {
		p := newPeer(t)
		ch := callAsync(context.Background(), p.conn, "last")
		r := p.request()
		p.reply(r.ID, `"final"`)
		_ = p.toConn.Close() // EOF right behind the answer
		if got := await(t, ch); got.err != nil || string(got.raw) != `"final"` {
			t.Fatalf("got %s, %v; the final frame was lost to EOF", got.raw, got.err)
		}
	}
}

func TestConnRefusesAnOversizedRequestAndWritesNothing(t *testing.T) {
	p := newPeer(t, WithMaxFrame(1024))
	_, err := p.conn.Call(context.Background(), "big", map[string]string{"pad": strings.Repeat("a", 2048)})
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
	ch := callAsync(context.Background(), p.conn, "small")
	r := p.request()
	if r.Method != "small" {
		t.Fatalf("first frame the peer saw was %q; the oversized one was written", r.Method)
	}
	if r.ID != 2 {
		// The refused call consumed id 1; ids stay monotonic and unique.
		t.Fatalf("id = %d, want 2", r.ID)
	}
	p.reply(r.ID, `1`)
	if got := await(t, ch); got.err != nil {
		t.Fatal(got.err)
	}
}

func TestConnNotifyIsIDless(t *testing.T) {
	p := newPeer(t)
	if err := p.conn.Notify("event/handle", map[string]string{"k": "v"}); err != nil {
		t.Fatal(err)
	}
	r := p.request()
	if r.ID != 0 || r.Method != "event/handle" {
		t.Fatalf("request = %+v; a notification carries no id", r)
	}
	// A following call still gets id 1: notifications do not spend ids.
	ch := callAsync(context.Background(), p.conn, "x")
	if r := p.request(); r.ID != 1 {
		t.Fatalf("id = %d, want 1", r.ID)
	}
	_ = ch
}

func TestConnCloseFailsWaitersAndIsIdempotent(t *testing.T) {
	p := newPeer(t)
	ch := callAsync(context.Background(), p.conn, "x")
	p.request()
	if err := p.conn.Close(); err != nil {
		t.Fatal(err)
	}
	if got := await(t, ch); !errors.Is(got.err, ErrGone) {
		t.Fatalf("err = %v, want ErrGone", got.err)
	}
	if err := p.conn.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
}

func TestCallDecodesAndZeroesAnEmptyResult(t *testing.T) {
	p := newPeer(t)
	type out struct{ N int }
	ch := make(chan out, 1)
	go func() {
		v, err := Call[out](context.Background(), p.conn, "m", nil)
		if err != nil {
			t.Error(err)
		}
		ch <- v
	}()
	r := p.request()
	p.reply(r.ID, `{"N":7}`)
	if v := <-ch; v.N != 7 {
		t.Fatalf("N = %d", v.N)
	}
	go func() {
		v, err := Call[out](context.Background(), p.conn, "m", nil)
		if err != nil {
			t.Error(err)
		}
		ch <- v
	}()
	r = p.request()
	p.raw(`{"jsonrpc":"2.0","id":` + itoa(r.ID) + `}`)
	if v := <-ch; v.N != 0 {
		t.Fatalf("N = %d", v.N)
	}
}

func TestConnDiscardsAnOverlongInboundLineAndKeepsTheConnection(t *testing.T) {
	p := newPeer(t, WithMaxInboundFrame(1024))
	ch := callAsync(context.Background(), p.conn, "x")
	r := p.request()
	// Longer than the cap, and longer than the reader's buffer.
	p.raw(`{"jsonrpc":"2.0","id":` + itoa(r.ID) + `,"result":"` + strings.Repeat("a", 300<<10) + `"}`)
	p.reply(r.ID, `"after the flood"`)
	if got := await(t, ch); got.err != nil || string(got.raw) != `"after the flood"` {
		t.Fatalf("got %s, %v; the valid reply behind the flood was lost", got.raw, got.err)
	}
	if n := p.conn.InboundDropped(); n != 1 {
		t.Fatalf("InboundDropped = %d, want 1", n)
	}
	select {
	case <-p.conn.Done():
		t.Fatal("an overlong line closed the connection")
	default:
	}
}

func TestConnInboundMemoryStaysBoundedByTheCapNotTheLine(t *testing.T) {
	const (
		flood      = 256 << 20 // a line with no newline for a very long time
		inboundCap = 1 << 10
	)
	pr, pw := io.Pipe()
	c := NewConn(pr, io.Discard, WithMaxInboundFrame(inboundCap))
	t.Cleanup(func() { _ = c.Close(); _ = pw.Close() })
	ch := callAsync(context.Background(), c, "x")
	time.Sleep(50 * time.Millisecond) // the call is registered as id 1

	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	var peak atomic.Uint64
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		var m runtime.MemStats
		for {
			runtime.ReadMemStats(&m)
			if m.HeapAlloc > peak.Load() {
				peak.Store(m.HeapAlloc)
			}
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}()

	chunk := []byte(strings.Repeat("x", 1<<20))
	for written := 0; written < flood; written += len(chunk) {
		if _, err := pw.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	_, _ = io.WriteString(pw, "\n"+`{"jsonrpc":"2.0","id":1,"result":"ok"}`+"\n")
	got := await(t, ch)
	close(stop)
	<-sampled
	if got.err != nil || string(got.raw) != `"ok"` {
		t.Fatalf("got %s, %v", got.raw, got.err)
	}
	if p := peak.Load(); p > base.HeapAlloc && p-base.HeapAlloc > 32<<20 {
		t.Fatalf("heap grew %d MiB while a %d MiB line streamed in", (p-base.HeapAlloc)>>20, flood>>20)
	}
	if c.InboundDropped() != 1 {
		t.Fatalf("InboundDropped = %d", c.InboundDropped())
	}
}
