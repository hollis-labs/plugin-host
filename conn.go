package pluginhost

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

const (
	defaultCallTimeout = 30 * time.Second
	defaultMaxFrame    = 8 << 20         // plugin-sdk's Serve scanner limit (server.go)
	defaultMaxInbound  = defaultMaxFrame // base-profile limit includes LF
)

// Conn is one plugin's JSON-RPC connection over a pair of streams: requests
// go to w, responses come from r, both newline-delimited (the plugin-sdk
// stdio framing). It owns framing and correlation. It owns neither a process
// nor its lifecycle; see [Process] for that.
//
// Each call receives its positive JS-safe ID at writer selection, in publication
// order. Pending correlation is registered before any bytes are written.
// Notifications omit IDs. A single reader goroutine routes
// each response to the waiter that asked for it, so replies may arrive in any
// order, a slow call delays only itself, and a caller that gives up costs one
// call rather than the connection: it deregisters its own waiter and the late
// reply is dropped.
//
// The reader ends on EOF (or a read error). At that point every waiter still
// registered, and every later call, fails with [ErrGone].
type Conn struct {
	w io.Writer
	r io.Reader
	// br reads r in bounded slices; read() assembles lines under maxInbound
	// (bufio.Scanner would stop reading at its limit, and a stopped reader
	// is a dead connection).
	br *bufio.Reader

	defaultTimeout time.Duration
	maxFrame       int
	maxInbound     int
	droppedInbound atomic.Int64
	droppedCancel  atomic.Int64

	nextID atomic.Int64

	mu      sync.Mutex
	pending map[subprocess.RPCID]*pendingCall
	// closedBy is the error every pending and future call receives once the
	// reader has ended or Close ran. Nil while the connection is live.
	closedBy error

	queue            frameQueue
	wake             chan struct{}
	outbound         map[*queuedFrame]*outboundFrame
	ordinaryCalls    int
	lifecycleCalls   int
	inboundActive    map[subprocess.RPCID]bool
	inboundHighWater int64

	done      chan struct{}
	closeOnce sync.Once
}

// ConnOption configures a [Conn].
type ConnOption func(*Conn)

// WithDefaultTimeout sets the timeout applied to a [Conn.Call] whose context
// has no deadline. The default is 30s; a negative value disables it. A
// context deadline always wins, in either direction.
func WithDefaultTimeout(d time.Duration) ConnOption {
	return func(c *Conn) { c.defaultTimeout = d }
}

// WithMaxFrame sets the cap on one outbound frame (the encoded request plus
// its newline). The default is 8 MiB, matching plugin-sdk's Serve. A larger
// request fails with [ErrFrameTooLarge] and nothing is written. Values below
// 1 keep the default. The fixed 8 MiB writer lane bound applies independently.
func WithMaxFrame(n int) ConnOption {
	return func(c *Conn) {
		if n > 0 {
			c.maxFrame = n
		}
	}
}

// WithMaxInboundFrame sets the cap on one inbound line (a response). A longer
// line is discarded as it streams in, never buffered beyond the cap, and
// counted by [Conn.InboundDropped]; the connection stays up. The default is
// 8 MiB including LF, matching the outbound base-profile cap. Values below 1 keep the default.
func WithMaxInboundFrame(n int) ConnOption {
	return func(c *Conn) {
		if n > 0 {
			c.maxInbound = n
		}
	}
}

// NewConn starts one reader and one writer goroutine and returns the connection.
// Close ends it by closing r and w when they are [io.Closer]s; otherwise it
// ends when r reaches EOF.
func NewConn(r io.Reader, w io.Writer, opts ...ConnOption) *Conn {
	c := &Conn{
		w:              w,
		r:              r,
		br:             bufio.NewReaderSize(r, 64*1024),
		defaultTimeout: defaultCallTimeout,
		maxFrame:       defaultMaxFrame,
		maxInbound:     defaultMaxInbound,
		pending:        map[subprocess.RPCID]*pendingCall{},
		wake:           make(chan struct{}, 1),
		outbound:       map[*queuedFrame]*outboundFrame{},
		inboundActive:  map[subprocess.RPCID]bool{},
		done:           make(chan struct{}),
	}
	for _, o := range opts {
		o(c)
	}
	go c.write()
	go c.read()
	return c
}

// Call sends one request and waits for the response with that id.
//
// ctx bounds this call and nothing else: canceling it deregisters the waiter
// and returns ctx's error. A published request reserves capacity for its
// host-owned rpc/cancel, except terminal unload. Ordinary writer contention
// cannot drop the control. Zero-byte failed controls leave
// the connection up; a partial control frame retires it. [Conn.CancelDropped]
// counts controls that were not published completely. Calls admit immediately
// under separate ordinary (16) and lifecycle (2) limits; a full admission or
// writer lane returns ErrAdmissionFull before publication.
// Other calls retain their own contexts. A plugin-reported error comes back as
// *subprocess.RPCError (use [errors.As]); deadline replies additionally match
// context.DeadlineExceeded via [errors.Is], preserving the RPC effect state.
// The pipe ending is [ErrGone].
func (c *Conn) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if _, ok := ctx.Deadline(); !ok && c.defaultTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.defaultTimeout)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("pluginhost: %s: %w", method, err)
	}

	call, err := c.queueCall(ctx, method, params)
	if err != nil {
		return nil, fmt.Errorf("pluginhost: %s: %w", method, err)
	}
	defer c.releaseCall(call)
	ctx = call.ctx
	complete := func(out callReply) (json.RawMessage, error) {
		if out.err != nil {
			return nil, fmt.Errorf("pluginhost: %s: %w", method, out.err)
		}
		return finish(ctx, method, out.response)
	}
	select {
	case out := <-call.reply:
		return complete(out)
	case <-ctx.Done():
		select {
		case out := <-call.reply:
			return complete(out)
		default:
		}
		c.cancelQueuedCall(call, ctx.Err())
		select {
		case out := <-call.reply:
			return complete(out)
		default:
		}
		return nil, fmt.Errorf("pluginhost: %s: %w", method, ctx.Err())
	case <-c.done:
		select {
		case out := <-call.reply:
			return complete(out)
		default:
		}
		return nil, fmt.Errorf("pluginhost: %s: %w", method, c.gone())
	}
}

func finish(ctx context.Context, method string, response subprocess.RPCResponse) (json.RawMessage, error) {
	if response.Error != nil {
		cause := error(response.Error)
		if ctx.Err() != nil {
			cause = errors.Join(cause, ctx.Err())
		} else if code, ok := applicationCode(response.Error); ok {
			deadline, bounded := ctx.Deadline()
			// The wire budget rounds down to milliseconds. Allow that rounding
			// plus timer delivery skew without classifying an early unknown
			// outcome as a local deadline expiry.
			if code == capability.DeadlineExceeded || (code == capability.UnknownOutcome && bounded && time.Until(deadline) <= 3*time.Millisecond) {
				cause = errors.Join(cause, context.DeadlineExceeded)
			}
		}
		return nil, fmt.Errorf("pluginhost: %s: %w", method, cause)
	}
	return response.Result, nil
}

// Notify sends an id-less request: the plugin runs it and sends no reply
// (an absent ID is the only notification form).
func (c *Conn) Notify(method string, params any) error {
	c.mu.Lock()
	closed := c.closedBy
	c.mu.Unlock()
	if closed != nil {
		return fmt.Errorf("pluginhost: notify %s: %w", method, closed)
	}
	// A notification uses ordinary queue capacity and a bounded whole write.
	ctx := context.Background()
	if c.defaultTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.defaultTimeout)
		defer cancel()
	}
	if err := c.send(ctx, subprocess.RPCRequest{JSONRPC: "2.0", Method: method, Params: params}); err != nil {
		return fmt.Errorf("pluginhost: notify %s: %w", method, err)
	}
	return nil
}

// InboundDropped reports how many inbound lines were discarded for exceeding
// the inbound cap. It is a cheap atomic read, for diagnostics.
func (c *Conn) InboundDropped() int64 { return c.droppedInbound.Load() }

// Done is closed when the connection can carry no more calls: the reader hit
// EOF, or Close ran.
func (c *Conn) Done() <-chan struct{} { return c.done }

// Close fails every outstanding call with [ErrGone] and closes the underlying
// streams when they are [io.Closer]s (which also ends the reader goroutine).
// It is idempotent.
func (c *Conn) Close() error {
	c.fail(fmt.Errorf("%w: connection closed", ErrGone))
	var err error
	c.closeOnce.Do(func() {
		err = errors.Join(closeIf(c.w), closeIf(c.r))
	})
	return err
}

func closeIf(v any) error {
	closer, ok := v.(io.Closer)
	if !ok {
		return nil
	}
	if err := closer.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		return err
	}
	return nil
}

// read is the single reader. It runs until the stream ends.
func (c *Conn) read() {
	var cause error
	defer func() {
		if r := recover(); r != nil {
			cause = fmt.Errorf("reader panic: %v", r)
		}
		if cause != nil {
			c.fail(fmt.Errorf("%w: %w", ErrGone, cause))
			return
		}
		c.fail(ErrGone)
	}()
	var line []byte // the line being assembled; nil while discarding
	discarding := false
	for {
		// ReadSlice hands back at most one buffer's worth. A line longer than
		// the buffer arrives in pieces (ErrBufferFull), so memory never
		// depends on how long the plugin's line is.
		chunk, err := c.br.ReadSlice('\n')
		full := errors.Is(err, bufio.ErrBufferFull)
		if !discarding {
			if len(line)+len(chunk) > c.maxInbound {
				discarding, line = true, nil
			} else {
				line = append(line, chunk...)
			}
		}
		if full {
			continue
		}
		if discarding {
			c.droppedInbound.Add(1)
		} else if len(line) > 0 {
			c.deliver(line)
		}
		line, discarding = nil, false
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, os.ErrClosed) {
				cause = err
			}
			return
		}
	}
}

// deliver is the single direction-aware demultiplexer. Invalid envelopes and
// late replies are dropped; method-bearing frames never inspect pending calls.
func (c *Conn) deliver(line []byte) {
	frame, err := decodeWireEnvelope(line)
	if err != nil && !frame.invalidResult {
		return
	}
	if frame.request {
		c.refuseInbound(frame)
		return
	}
	c.mu.Lock()
	call := c.pending[frame.id]
	c.mu.Unlock()
	if call == nil {
		return
	}
	out := callReply{response: frame.response}
	if frame.invalidResult {
		out.err = fmt.Errorf("%w: invalid result: %w", ErrProtocolMismatch, err)
		if call.method == subprocess.MethodInit {
			out.err = errors.Join(out.err, &subprocess.InitError{Code: subprocess.InitInvalid, Field: "result"})
		}
	} else if frame.response.Error == nil {
		if err := validatePendingResult(call.method, frame.response.Result); err != nil {
			out.err = fmt.Errorf("%w: %s result: %w", ErrProtocolMismatch, call.method, err)
		}
	}
	select {
	case call.reply <- out:
	default:
	}
}

// fail marks the connection unusable with err (first cause wins) and wakes
// every waiter.
func (c *Conn) fail(err error) {
	c.mu.Lock()
	if c.closedBy == nil {
		c.closedBy = err
		close(c.done)
		c.queue.close()
		for _, out := range c.outbound {
			if out.cancel {
				c.droppedCancel.Add(1)
			}
		}
		c.outbound = nil
	}
	c.mu.Unlock()
}

func (c *Conn) gone() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closedBy == nil {
		return ErrGone
	}
	return c.closedBy
}
