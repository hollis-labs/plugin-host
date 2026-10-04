package pluginhost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/hollis-labs/plugin-sdk/subprocess"
)

// ErrAdmissionFull reports immediate, effect-free refusal at a bounded local
// call or writer admission boundary. It never indicates remote execution.
var ErrAdmissionFull = errors.New("pluginhost: local admission capacity exhausted")

const ordinaryCallLimit = 16
const lifecycleCallLimit = 2
const physicalWriteTimeout = 5 * time.Second

type callReply struct {
	response   subprocess.RPCResponse
	initResult *subprocess.InitResult
	err        error
}
type pendingCall struct {
	session   *HostSession
	prepared  *preparedHostBinding
	method    string
	ctx       context.Context
	end       context.CancelFunc
	reply     chan callReply
	id        subprocess.RPCID // protected by Conn.mu; assigned only at writer selection
	frame     *queuedFrame
	credit    *terminalCredit
	lifecycle bool
}
type outboundFrame struct {
	ctx       context.Context
	call      *pendingCall
	budget    int
	idSlot    int
	written   chan error
	cancel    bool
	inboundID subprocess.RPCID
	receipt   *reverseReceipt
}

func lifecycleMethod(method string) bool {
	return method == subprocess.MethodInit || method == subprocess.MethodLoad || method == subprocess.MethodUnload
}
func (c *Conn) signalWriter() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *Conn) queueCall(ctx context.Context, method string, params any) (*pendingCall, error) {
	call := &pendingCall{ctx: ctx, method: method, reply: make(chan callReply, 1), lifecycle: lifecycleMethod(method)}
	c.mu.Lock()
	if c.closedBy != nil || c.configError != nil {
		err := errors.Join(c.closedBy, c.configError)
		c.mu.Unlock()
		return nil, err
	}
	count, limit := &c.ordinaryCalls, ordinaryCallLimit
	if call.lifecycle {
		count, limit = &c.lifecycleCalls, lifecycleCallLimit
	}
	if *count >= limit {
		c.mu.Unlock()
		return nil, ErrAdmissionFull
	}
	credit, err := c.queue.reserveTerminal()
	if err != nil {
		c.mu.Unlock()
		return nil, ErrAdmissionFull
	}
	*count++
	call.credit = credit
	c.mu.Unlock()
	if method == subprocess.MethodInit {
		if c.reverse == nil {
			fields, _, initErr := forwardFields(params)
			if initErr == nil {
				initErr = refuseProfileOffer(fields)
			}
			if initErr != nil {
				c.releaseCall(call)
				return nil, initErr
			}
		} else {
			raw, initErr := json.Marshal(params)
			if initErr == nil {
				var p subprocess.InitParams
				initErr = json.Unmarshal(raw, &p)
				if initErr == nil {
					initErr = c.validateInit(p)
				}
			}
			if initErr != nil {
				c.releaseCall(call)
				return nil, initErr
			}
		}
	}
	params, ctx, end, err := prepareForwardCall(ctx, method, params)
	call.ctx, call.end = ctx, end
	if err == nil {
		params, err = c.prepareParent(call, params)
		ctx = call.ctx
	}
	if err != nil {
		c.releaseCall(call)
		return nil, err
	}

	// The maximum ID width is charged before admission. Encoding opaque values
	// stays outside both connection and queue locks and the physical writer.
	wire, budget, err := encodeFrame(subprocess.RPCRequest{JSONRPC: "2.0", ID: subprocess.NumberID(maxRequestID), Method: method, Params: params})
	if err == nil && len(wire) > c.maxFrame {
		err = fmt.Errorf("%w: %d bytes, cap %d", ErrFrameTooLarge, len(wire), c.maxFrame)
	}
	if err != nil {
		c.releaseCall(call)
		return nil, err
	}
	slot := bytes.Index(wire, []byte(`"id":`)) + len(`"id":`)
	lane := ordinaryLane
	if call.lifecycle {
		lane = controlLane
	}
	c.mu.Lock()
	if c.closedBy != nil {
		err = c.closedBy
	} else if err = ctx.Err(); err == nil {
		call.frame, err = c.queue.enqueueOwned(lane, wire)
		if err == nil {
			c.outbound[call.frame] = &outboundFrame{ctx: ctx, call: call, budget: budget, idSlot: slot}
		}
	}
	c.mu.Unlock()
	if err != nil {
		c.releaseCall(call)
		if errors.Is(err, errWriterFull) {
			err = ErrAdmissionFull
		}
		return nil, err
	}
	c.signalWriter()
	return call, nil
}
func (c *Conn) releaseCall(call *pendingCall) {
	if call.end != nil {
		call.end()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.retireParentLocked(call)
	delete(c.pending, call.id)
	if call.frame != nil && c.queue.remove(call.frame) {
		delete(c.outbound, call.frame)
	}
	if call.lifecycle {
		c.lifecycleCalls--
	} else {
		c.ordinaryCalls--
	}
	call.credit.release()
}

// cancelQueuedCall consumes reserved control capacity without waiting for an
// ordinary write. A call canceled before selection consumes neither an ID nor
// a cancellation frame. Terminal unload never sends rpc/cancel.
func (c *Conn) cancelQueuedCall(call *pendingCall, cause error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.retireParentLocked(call)
	if len(call.reply) > 0 {
		return
	}
	if c.queue.remove(call.frame) {
		delete(c.outbound, call.frame)
		return
	}
	if call.id == (subprocess.RPCID{}) || call.method == subprocess.MethodUnload || c.closedBy != nil {
		return
	}
	reason := subprocess.CallerCancelled
	if errors.Is(cause, context.DeadlineExceeded) {
		reason = subprocess.DeadlineExpired
	}
	wire, err := json.Marshal(subprocess.RPCRequest{JSONRPC: "2.0", Method: "rpc/cancel", Params: subprocess.CancelParams{RequestOwner: subprocess.HostRPCOwnerHost, ID: call.id, Reason: reason}})
	wire = append(wire, '\n')
	if err != nil || len(wire) > c.maxFrame {
		c.droppedCancel.Add(1)
		return
	}
	frame, err := call.credit.terminal(wire, wire)
	if err != nil {
		c.droppedCancel.Add(1)
		return
	}
	c.outbound[frame] = &outboundFrame{cancel: true, budget: -1, idSlot: -1}
	c.signalWriter()
}

func (c *Conn) send(ctx context.Context, request subprocess.RPCRequest) error {
	if request.Method == subprocess.MethodInit {
		fields, _, err := forwardFields(request.Params)
		if err != nil {
			return err
		}
		if err = refuseProfileOffer(fields); err != nil {
			return err
		}
	}

	wire, budget, err := encodeFrame(request)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}
	if len(wire) > c.maxFrame {
		return fmt.Errorf("%w: %d bytes, cap %d", ErrFrameTooLarge, len(wire), c.maxFrame)
	}
	out := &outboundFrame{ctx: ctx, budget: budget, idSlot: -1, written: make(chan error, 1)}
	var frame *queuedFrame
	c.mu.Lock()
	if c.closedBy != nil {
		err = c.closedBy
	} else {
		frame, err = c.queue.enqueueOwned(ordinaryLane, wire)
		if err == nil {
			c.outbound[frame] = out
		}
	}
	c.mu.Unlock()
	if err != nil {
		if errors.Is(err, errWriterFull) {
			return ErrAdmissionFull
		}
		return err
	}
	c.signalWriter()
	select {
	case err := <-out.written:
		return err
	case <-ctx.Done():
		c.mu.Lock()
		if c.queue.remove(frame) {
			delete(c.outbound, frame)
		}
		c.mu.Unlock()
		return ctx.Err()
	case <-c.done:
		return c.gone()
	}
}

// write is the only physical writer. Selection, ID issuance and correlation
// registration occur together, before the first byte can elicit a reply.
func (c *Conn) write() {
	defer func() {
		if r := recover(); r != nil {
			c.fail(fmt.Errorf("%w: writer panic: %v", ErrGone, r))
		}
	}()
	for {
		c.mu.Lock()
		frame := c.queue.take()
		if frame == nil {
			c.mu.Unlock()
			select {
			case <-c.wake:
				continue
			case <-c.done:
				return
			}
		}
		out := c.outbound[frame]
		delete(c.outbound, frame)
		c.activeReceipt = out.receipt
		var err error
		if out.ctx != nil {
			err = out.ctx.Err()
		}
		if err == nil && out.budget >= 0 {
			remaining := time.Until(mustDeadline(out.ctx)).Milliseconds()
			if remaining <= 0 {
				err = context.DeadlineExceeded
			} else {
				remaining = min(remaining, int64(^uint32(0)))
				for i := range 10 {
					frame.wire[out.budget+i] = ' '
				}
				copy(frame.wire[out.budget:out.budget+10], strconv.FormatInt(remaining, 10))
			}
		}
		if err == nil && out.call != nil {
			out.call.id, err = c.allocateID()
			if err == nil {
				if out.call.method == subprocess.MethodUnload {
					c.revokeBusinessLocked()
					if c.reverse != nil {
						c.reverse.unloading = true
					}
				}
				if out.call.session != nil {
					id, ok := out.call.id.Integer()
					deadline, _ := out.call.ctx.Deadline()
					if !ok || id <= 0 {
						err = ErrRequestIDExhausted
					} else {
						err = out.call.session.attachParent(uint64(id), out.call.method, deadline, out.call.prepared)
					}
				}
				if err == nil {
					c.pending[out.call.id] = out.call
				}
				for i := range 16 {
					frame.wire[out.idSlot+i] = ' '
				}
				copy(frame.wire[out.idSlot:out.idSlot+16], strconv.FormatInt(c.nextID.Load(), 10))
			}
		}
		c.mu.Unlock()
		if err == nil {
			err = c.writeFrame(frame.wire, out)
		}
		c.mu.Lock()
		c.queue.complete(frame)
		if err != nil && out.call != nil {
			c.retireParentLocked(out.call)
		}
		if out.receipt != nil {
			out.receipt.finishLocked(c)
		}
		c.activeReceipt = nil
		if out.call != nil && out.call.frame == frame {
			out.call.frame = nil
		}
		if out.receipt == nil && out.inboundID != (subprocess.RPCID{}) {
			delete(c.inboundActive, out.inboundID)
		}
		c.mu.Unlock()
		if err != nil && out.call != nil {
			select {
			case out.call.reply <- callReply{err: err}:
			default:
			}
		}
		if out.written != nil {
			out.written <- err
		}
	}
}

func (c *Conn) writeFrame(wire []byte, out *outboundFrame) error {
	ctx := out.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	deadline := time.Now().Add(c.writeTimeout)
	if callDeadline, ok := ctx.Deadline(); ok && callDeadline.Before(deadline) {
		deadline = callDeadline
	}
	if out.cancel {
		deadline = time.Now().Add(min(cancelWriteTimeout, c.writeTimeout))
	}
	c.mu.Lock()
	closed := c.closedBy
	c.mu.Unlock()
	if closed != nil {
		if out.cancel {
			c.droppedCancel.Add(1)
		}
		return closed
	}
	d, canDeadline := c.w.(interface{ SetWriteDeadline(time.Time) error })
	if canDeadline {
		if err := d.SetWriteDeadline(deadline); err != nil {
			canDeadline = false
		}
		defer func() { _ = d.SetWriteDeadline(time.Time{}) }()
	}
	// One watchdog per active closeable stream, never per queued frame. Closing
	// interrupts a writer without native deadline support and fences the stream.
	var timer *time.Timer
	if !canDeadline {
		if closer, ok := c.w.(io.Closer); ok {
			timer = time.AfterFunc(max(time.Until(deadline), 0), func() {
				c.fail(fmt.Errorf("%w: write deadline: %w", ErrGone, context.DeadlineExceeded))
				_ = closer.Close()
			})
			defer timer.Stop()
		} else if out.cancel {
			c.droppedCancel.Add(1)
			return nil
		}
	}
	n, err := c.w.Write(wire)
	if err == nil && n == len(wire) {
		return nil
	}
	if out.cancel {
		c.droppedCancel.Add(1)
		if n == 0 {
			return nil
		}
	}
	if err == nil {
		err = io.ErrShortWrite
	}
	wrapped := fmt.Errorf("%w: write: %w", ErrGone, err)
	c.fail(wrapped)
	return wrapped
}

// Without a selected reverse profile, inbound requests receive a
// bounded refusal, while notifications have no effect. This direction never
// consults outgoing pending calls, even when the numeric IDs coincide.
func (c *Conn) refuseInbound(request wireEnvelope) {
	if request.id == (subprocess.RPCID{}) {
		return
	}
	// Refusals must fit their fixed terminal credit without allocating an
	// unbounded escaped identifier while the reader holds connection locks.
	if text, ok := request.id.Text(); ok && len(text) > 128 {
		c.fail(fmt.Errorf("%w: inbound identifier exceeds terminal credit", ErrGone))
		return
	}
	c.mu.Lock()
	if c.closedBy != nil {
		c.mu.Unlock()
		return
	}
	if c.inboundActive[request.id] {
		c.mu.Unlock()
		c.fail(fmt.Errorf("%w: duplicate active inbound id", ErrGone))
		return
	}
	rpcError := &subprocess.RPCError{Code: -32601, Message: "reverse requests are not enabled"}
	if id, numeric := request.id.Integer(); numeric && id > 0 {
		if id <= c.inboundHighWater {
			rpcError = &subprocess.RPCError{Code: -32600, Message: "inbound request IDs must increase"}
		} else {
			c.inboundHighWater = id
		}
	}
	credit, err := c.queue.reserveTerminal()
	if err != nil {
		c.mu.Unlock()
		c.fail(fmt.Errorf("%w: inbound refusal capacity", ErrGone))
		return
	}
	c.inboundActive[request.id] = true
	wire, err := json.Marshal(subprocess.RPCResponse{JSONRPC: "2.0", ID: request.id, Error: rpcError})
	wire = append(wire, '\n')
	if err == nil && len(wire) <= c.maxFrame && len(wire) <= terminalBytes {
		var frame *queuedFrame
		frame, err = credit.terminal(wire, wire)
		if err == nil {
			c.outbound[frame] = &outboundFrame{budget: -1, idSlot: -1, inboundID: request.id}
		}
	} else if err == nil {
		err = ErrFrameTooLarge
	}
	credit.release()
	c.mu.Unlock()
	if err != nil {
		c.fail(fmt.Errorf("%w: inbound refusal: %w", ErrGone, err))
		return
	}
	c.signalWriter()
}
