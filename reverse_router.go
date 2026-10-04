package pluginhost

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

// reverseReceipt owns the transferred terminal credit and inbound ID through
// physical completion/fence. All bookkeeping uses Conn.mu; queue visibility
// follows metadata installation under that mutex (writer takes Conn then queue).
type reverseReceipt struct {
	id     subprocess.RPCID
	credit *terminalCredit
	once   sync.Once
}

func (r *reverseReceipt) finishLocked(c *Conn) {
	r.once.Do(func() { r.credit.release(); delete(c.inboundActive, r.id) })
}

func (c *Conn) validateReply(call *pendingCall, raw json.RawMessage) (*subprocess.InitResult, error) {
	if call.method != subprocess.MethodInit {
		return nil, validatePendingResult(call.method, raw)
	}
	result, err := decodeInitResult(raw)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	r := c.reverse
	c.mu.Unlock()
	if r == nil {
		return nil, validatePendingResult(call.method, raw)
	}
	return &result, c.verifyInit(r.params, result)
}
func (c *Conn) acceptReplyLocked(call *pendingCall, out callReply) {
	r := c.reverse
	if r == nil {
		return
	}
	if call.method == subprocess.MethodInit {
		if call.ctx.Err() != nil || out.err != nil || out.response.Error != nil {
			c.closeReverseLocked()
			return
		}
		result := out.initResult
		if result == nil {
			c.closeReverseLocked()
			return
		}
		r.initialized = true
		r.selected = result.ReverseRPCVersion != nil
		if !r.selected {
			r.business.Close()
			r.cleanup.Close()
		}
	}
	if call.method == subprocess.MethodLoad && out.err == nil && out.response.Error == nil && call.ctx.Err() == nil {
		r.loaded = true
	}
	if call.method == subprocess.MethodUnload {
		r.cleanup.Close()
	}
}

func (c *Conn) routeInbound(frame wireEnvelope) {
	var control subprocess.CancelParams
	controlValid := false
	if frame.method == "rpc/cancel" && len(frame.params) <= 1024 {
		controlValid = json.Unmarshal(frame.params, &control) == nil
	}
	c.mu.Lock()
	r := c.reverse
	if r == nil {
		c.mu.Unlock()
		c.refuseInbound(frame)
		return
	}
	if c.closedBy != nil {
		c.mu.Unlock()
		return
	}
	if frame.method == "rpc/cancel" {
		// Only a notification from the initiating plugin may cancel its own work.
		if frame.id == (subprocess.RPCID{}) && controlValid && control.RequestOwner == subprocess.HostRPCOwnerPlugin {
			id, ok := control.ID.Integer()
			if ok && id > 0 {
				if cancel := r.active[uint64(id)]; cancel != nil {
					cancel()
				}
			}
		}
		c.mu.Unlock()
		return
	}
	if frame.id == (subprocess.RPCID{}) {
		c.mu.Unlock()
		return
	}
	number, ok := frame.id.Integer()
	if !ok || number <= 0 {
		c.mu.Unlock()
		c.refuseInbound(frame)
		return
	}
	id := uint64(number)
	if c.inboundActive[frame.id] {
		c.mu.Unlock()
		c.fail(errors.Join(ErrGone, errors.New("duplicate active reverse id")))
		return
	}
	credit, err := c.queue.reserveTerminal()
	if err != nil {
		c.mu.Unlock()
		c.fail(errors.Join(ErrGone, err))
		return
	}
	receipt := &reverseReceipt{id: frame.id, credit: credit}
	c.inboundActive[frame.id] = true
	refusal := error(nil)
	if id <= r.high {
		refusal = hostRefusal(capability.InvalidRequest, "")
	} else {
		r.high = id
	}
	method := HostMethod(frame.method)
	session := r.business
	if r.unloading {
		session = r.cleanup
	}
	// Init may provisionally log only while its actual live parent exists.
	if !r.selected && (r.initialized || method != HostLog) {
		refusal = hostRefusal(capability.UnsupportedCapability, "")
	}
	if _, offered := session.ceilings[method]; !offered {
		refusal = hostRefusal(capability.UnsupportedCapability, "")
	}
	if r.fenced && !r.unloading {
		refusal = hostRefusal(capability.TargetUnavailable, "")
	}
	if r.workers >= reversePerConnection {
		refusal = hostRefusal(capability.RateLimited, "")
	}
	if refusal != nil {
		c.mu.Unlock()
		wire := hostWireReply(id, nil, refusal)
		c.mu.Lock()
		if c.closedBy != nil {
			receipt.finishLocked(c)
			c.mu.Unlock()
			return
		}
		err = c.publishReverseLocked(receipt, wire, wire)
		c.mu.Unlock()
		if err != nil {
			c.fail(errors.Join(ErrGone, err))
		}
		return
	}
	// Parsing/schema/callback execution occurs in a bounded worker. Cancellation
	// is installed before the reader can receive the next frame, even if that
	// worker has not registered its backend cancellation context yet.
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // G118: bounded worker owns cancel, also fenced by Conn
	r.active[id] = cancel
	r.workers++
	c.mu.Unlock()
	go func() {
		defer cancel()
		defer func() { c.mu.Lock(); delete(r.active, id); r.workers--; c.mu.Unlock() }()
		_, err := session.executeHostWithReply(ctx, id, method, frame.params, frame.receivedAt, true, func(wire, fallback []byte) (*queuedFrame, error) {
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.closedBy != nil {
				receipt.finishLocked(c)
				return nil, c.closedBy
			}
			// A business fence racing terminal construction cannot publish a stale
			// success. Errors remain typed; a possible mutation stays unknown.
			session.mu.Lock()
			fenced := session.closed
			session.mu.Unlock()
			if fenced {
				wire = fallback
			}
			err := c.publishReverseLocked(receipt, wire, fallback)
			return nil, err
		})
		if err != nil {
			c.mu.Lock()
			receipt.finishLocked(c)
			c.mu.Unlock()
			c.fail(errors.Join(ErrGone, err))
		}
	}()
}
func (c *Conn) publishReverseLocked(receipt *reverseReceipt, wire, fallback []byte) error {
	if len(wire) > c.maxFrame {
		wire = fallback
	}
	if len(fallback) > c.maxFrame {
		receipt.finishLocked(c)
		return ErrFrameTooLarge
	}
	frame, err := receipt.credit.terminalOwned(wire, fallback)
	if err != nil {
		receipt.finishLocked(c)
		return err
	}
	c.outbound[frame] = &outboundFrame{budget: -1, idSlot: -1, inboundID: receipt.id, receipt: receipt}
	c.signalWriter()
	return nil
}
