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

	"github.com/hollis-labs/plugin-sdk/subprocess"
)

const (
	defaultCallTimeout = 30 * time.Second
	defaultMaxFrame    = 8 << 20 // plugin-sdk's Serve scanner limit (server.go)
)

// Conn is one plugin's JSON-RPC connection over a pair of streams: requests
// go to w, responses come from r, both newline-delimited (the plugin-sdk
// stdio framing). It owns framing and correlation. It owns neither a process
// nor its lifecycle; see [Process] for that.
//
// Every call gets a monotonic id starting at 1 (id 0 is a notification in the
// protocol) and registers a waiter under it. A single reader goroutine routes
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
	// br reads r a line at a time with no line limit (bufio.Scanner would cap
	// it, and a capped reader is a reader that stops).
	br *bufio.Reader

	defaultTimeout time.Duration
	maxFrame       int

	nextID atomic.Int64

	mu      sync.Mutex
	pending map[int64]chan subprocess.RPCResponse
	// closedBy is the error every pending and future call receives once the
	// reader has ended or Close ran. Nil while the connection is live.
	closedBy error

	// writeMu serializes writes: frames are newline-delimited, so two
	// interleaved marshals would corrupt both.
	writeMu sync.Mutex

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
		pending:        map[int64]chan subprocess.RPCResponse{},
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
// ctx bounds this call and nothing else: cancelling it deregisters the waiter
// and returns ctx's error, and touches neither the connection, the plugin's
// work, nor any other call. A plugin-reported error comes back as
// *subprocess.RPCError (use [errors.As]); the pipe ending is [ErrGone].
func (c *Conn) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if _, ok := ctx.Deadline(); !ok && c.defaultTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.defaultTimeout)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("pluginhost: %s: %w", method, err)
	}

	id := c.nextID.Add(1)
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
		return finish(method, response)
	case <-ctx.Done():
		return nil, fmt.Errorf("pluginhost: %s: %w", method, ctx.Err())
	case <-c.done:
		// The reader delivers a frame before it can observe EOF, so a reply
		// that raced the pipe closing is already in the channel. Take it: the
		// last frame a plugin wrote before exiting is an answer, not a loss.
		select {
		case response := <-reply:
			return finish(method, response)
		default:
		}
		return nil, fmt.Errorf("pluginhost: %s: %w", method, c.gone())
	}
}

func finish(method string, response subprocess.RPCResponse) (json.RawMessage, error) {
	if response.Error != nil {
		return nil, fmt.Errorf("pluginhost: %s: %w", method, response.Error)
	}
	return response.Result, nil
}

// Notify sends an id-less request: the plugin runs it and sends no reply
// (plugin-sdk's Serve suppresses every response to id 0).
func (c *Conn) Notify(method string, params any) error {
	c.mu.Lock()
	closed := c.closedBy
	c.mu.Unlock()
	if closed != nil {
		return fmt.Errorf("pluginhost: notify %s: %w", method, closed)
	}
	if err := c.send(context.Background(), subprocess.RPCRequest{JSONRPC: "2.0", Method: method, Params: params}); err != nil {
		return fmt.Errorf("pluginhost: notify %s: %w", method, err)
	}
	return nil
}

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
	encoded, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}
	encoded = append(encoded, '\n')
	if len(encoded) > c.maxFrame {
		return fmt.Errorf("%w: %d bytes, cap %d", ErrFrameTooLarge, len(encoded), c.maxFrame)
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

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
	if _, err := c.w.Write(encoded); err != nil {
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
	for {
		line, err := c.br.ReadBytes('\n')
		if len(line) > 0 {
			c.deliver(line)
		}
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
// fatal: junk, invalid UTF-8, JSON null, id 0, a frame naming a method (the
// plugin never initiates), and an id nobody waits for. The last is not even
// misbehaviour: it is the late reply to a call whose caller gave up. Killing
// the connection over any of these would turn one bad line into an outage.
func (c *Conn) deliver(line []byte) {
	var frame struct {
		subprocess.RPCResponse
		Method string `json:"method"`
	}
	if err := json.Unmarshal(line, &frame); err != nil || frame.Method != "" || frame.ID == 0 {
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
