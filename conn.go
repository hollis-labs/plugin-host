package pluginhost

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
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
// Every call gets a monotonic id starting at 1 (notifications omit id) and registers a waiter under it. A single reader goroutine routes
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
	pending map[subprocess.RPCID]chan subprocess.RPCResponse
	// closedBy is the error every pending and future call receives once the
	// reader has ended or Close ran. Nil while the connection is live.
	closedBy error

	// writeGate serializes writes: frames are newline-delimited, so two
	// interleaved marshals would corrupt both.
	writeGate chan struct{}

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
// 1 keep the default.
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

// NewConn starts the reader goroutine over r and returns the connection.
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
		pending:        map[subprocess.RPCID]chan subprocess.RPCResponse{},
		writeGate:      make(chan struct{}, 1),
		done:           make(chan struct{}),
	}
	for _, o := range opts {
		o(c)
	}
	go c.read()
	return c
}

// Call sends one request and waits for the response with that id.
//
// ctx bounds this call and nothing else: canceling it deregisters the waiter
// and returns ctx's error. A best-effort host-owned rpc/cancel is attempted for
// a published request, except terminal unload. Zero-byte dropped controls leave
// the connection up; a partial control frame retires it. [Conn.CancelDropped]
// counts controls that were not published completely.
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

	params, ctx, end, err := prepareForwardCall(ctx, method, params)
	if err != nil {
		return nil, fmt.Errorf("pluginhost: %s: %w", method, err)
	}
	defer end()
	id, err := c.allocateID()
	if err != nil {
		return nil, err
	}
	reply := make(chan subprocess.RPCResponse, 1)

	c.mu.Lock()
	if c.closedBy != nil {
		err := c.closedBy
		c.mu.Unlock()
		return nil, fmt.Errorf("pluginhost: %s: %w", method, err)
	}
	c.pending[id] = reply
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()

	if err := c.send(ctx, subprocess.RPCRequest{JSONRPC: "2.0", ID: id, Method: method, Params: params}); err != nil {
		return nil, fmt.Errorf("pluginhost: %s: %w", method, err)
	}

	select {
	case response := <-reply:
		return finish(ctx, method, response)
	case <-ctx.Done():
		// A reply already admitted locally wins over a later cancellation.
		select {
		case response := <-reply:
			return finish(ctx, method, response)
		default:
		}
		if method != subprocess.MethodUnload {
			if response := c.cancelCall(id, ctx.Err()); response != nil {
				return finish(ctx, method, *response)
			}
		}
		return nil, fmt.Errorf("pluginhost: %s: %w", method, ctx.Err())
	case <-c.done:
		// The reader delivers a frame before it can observe EOF, so a reply
		// that raced the pipe closing is already in the channel. Take it: the
		// last frame a plugin wrote before exiting is an answer, not a loss.
		select {
		case response := <-reply:
			return finish(ctx, method, response)
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
	// Like a call, a notification's write is bounded: a plugin that stopped
	// reading stdin must not hold the write lock forever.
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

// send encodes and writes one frame.
//
// A write error is reported as ErrGone: a pipe that cannot be written is one
// whose reader has gone (or that this side closed), and a partial frame
// leaves the stream unusable either way, so the connection is failed too.
func (c *Conn) send(ctx context.Context, request subprocess.RPCRequest) error {
	encoded, budget, err := encodeFrame(request)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}
	if len(encoded) > c.maxFrame {
		return fmt.Errorf("%w: %d bytes, cap %d", ErrFrameTooLarge, len(encoded), c.maxFrame)
	}

	select {
	case c.writeGate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.gone()
	}
	defer func() { <-c.writeGate }()
	if err = ctx.Err(); err != nil {
		return err
	}
	// Only a fixed-width numeric slot changes at publication; opaque payload
	// encoding and allocation have already completed outside the writer.
	if budget >= 0 {
		remaining := time.Until(mustDeadline(ctx)).Milliseconds()
		if remaining <= 0 {
			return context.DeadlineExceeded
		}
		if remaining > int64(^uint32(0)) {
			remaining = int64(^uint32(0))
		}
		for i := range 10 {
			encoded[budget+i] = ' '
		}
		copy(encoded[budget:budget+10], strconv.FormatInt(remaining, 10))
	}
	c.mu.Lock()
	closed := c.closedBy
	c.mu.Unlock()
	if closed != nil {
		return closed
	}

	// A plugin that stops reading its stdin fills the pipe and would block
	// this write, and every writer queued behind it, past any ctx. Streams
	// with write deadlines (os.File pipes) get the call's deadline.
	if d, ok := c.w.(interface{ SetWriteDeadline(time.Time) error }); ok {
		if deadline, has := ctx.Deadline(); has {
			if d.SetWriteDeadline(deadline) == nil {
				defer func() { _ = d.SetWriteDeadline(time.Time{}) }()
			}
		}
	}
	if n, err := c.w.Write(encoded); err != nil || n != len(encoded) {
		if err == nil {
			err = io.ErrShortWrite
		}
		wrapped := fmt.Errorf("%w: write: %w", ErrGone, err)
		c.fail(wrapped)
		return wrapped
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

// deliver routes one frame to whoever waits for its id.
//
// Anything that is not a response to a pending call is dropped rather than
// fatal: junk, invalid UTF-8, JSON null, an absent id, a frame naming a method (the
// plugin never initiates), and an id nobody waits for. The last is not even
// misbehavior: it is the late reply to a call whose caller gave up. Killing
// the connection over any of these would turn one bad line into an outage.
func (c *Conn) deliver(line []byte) {
	var frame struct {
		subprocess.RPCResponse
		Method string `json:"method"`
	}
	if err := json.Unmarshal(line, &frame); err != nil || frame.Method != "" || frame.ID == (subprocess.RPCID{}) {
		return
	}
	c.mu.Lock()
	reply, waiting := c.pending[frame.ID]
	c.mu.Unlock()
	if !waiting {
		return
	}
	select {
	case reply <- frame.RPCResponse:
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
