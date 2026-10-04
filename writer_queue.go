package pluginhost

import (
	"errors"
	"sync"
)

const writerLaneFrames = 32
const writerLaneBytes = 8 << 20
const terminalBytes = 1024
const controlBurst = 4

var errWriterFull = errors.New("writer lane capacity exhausted")
var errWriterClosed = errors.New("writer closed")
var errTerminalCredit = errors.New("terminal response exceeds reserved credit")

type writerLane uint8

const (
	ordinaryLane writerLane = iota
	controlLane
)

type queuedFrame struct{ wire []byte }
type frameLane struct {
	frames []*queuedFrame
	bytes  int
}

// frameQueue owns admission and selection, not encoding or physical writes.
// The active whole frame is charged separately from both bounded FIFO lanes.
type frameQueue struct {
	mu       sync.Mutex
	lanes    [2]frameLane
	reserved int
	burst    int
	active   *queuedFrame
	closed   bool
}

type terminalCredit struct {
	queue *frameQueue
	held  bool
}

func (q *frameQueue) fits(lane writerLane, bytes int) bool {
	l := &q.lanes[lane]
	frames, charged := len(l.frames), l.bytes
	if lane == controlLane {
		frames += q.reserved
		charged += q.reserved * terminalBytes
	}
	return frames < writerLaneFrames && bytes <= writerLaneBytes-charged
}

func (q *frameQueue) reserveTerminal() (*terminalCredit, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, errWriterClosed
	}
	if !q.fits(controlLane, terminalBytes) {
		return nil, errWriterFull
	}
	q.reserved++
	return &terminalCredit{queue: q, held: true}, nil
}

func (c *terminalCredit) release() {
	q := c.queue
	q.mu.Lock()
	defer q.mu.Unlock()
	if c.held {
		c.held = false
		q.reserved--
	}
}

func (q *frameQueue) appendFrame(lane writerLane, wire []byte) *queuedFrame {
	frame := &queuedFrame{wire: wire}
	l := &q.lanes[lane]
	l.frames = append(l.frames, frame)
	l.bytes += len(wire)
	return frame
}

func (q *frameQueue) enqueue(lane writerLane, wire []byte) (*queuedFrame, error) {
	return q.enqueueOwned(lane, append([]byte(nil), wire...))
}

// enqueueOwned transfers an already encoded immutable frame without copying
// under connection or queue locks. The caller must not retain mutable ownership.
func (q *frameQueue) enqueueOwned(lane writerLane, wire []byte) (*queuedFrame, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, errWriterClosed
	}
	if !q.fits(lane, len(wire)) {
		return nil, errWriterFull
	}
	return q.appendFrame(lane, wire), nil
}

// terminal publishes success if it fits, otherwise the caller's bounded typed
// fallback. The caller authors its known effect classification before this seam.
func (c *terminalCredit) terminal(success, fallback []byte) (*queuedFrame, error) {
	success = append([]byte(nil), success...)
	fallback = append([]byte(nil), fallback...)
	q := c.queue
	q.mu.Lock()
	defer q.mu.Unlock()
	if !c.held || len(fallback) > terminalBytes {
		return nil, errTerminalCredit
	}
	if q.closed {
		return nil, errWriterClosed
	}
	c.held = false
	q.reserved--
	if !q.fits(controlLane, len(success)) {
		success = fallback
	}
	return q.appendFrame(controlLane, success), nil
}

// take can select only one active frame. Control progresses independently of
// ordinary capacity, but cannot starve an eligible ordinary frame.
func (q *frameQueue) take() *queuedFrame {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.active != nil {
		return nil
	}
	ordinary, control := len(q.lanes[ordinaryLane].frames) > 0, len(q.lanes[controlLane].frames) > 0
	if !ordinary && !control {
		return nil
	}
	lane := ordinaryLane
	if control && (!ordinary || q.burst < controlBurst) {
		lane = controlLane
		q.burst = min(q.burst+1, controlBurst)
	} else {
		q.burst = 0
	}
	l := &q.lanes[lane]
	frame := l.frames[0]
	l.frames[0] = nil
	l.frames = l.frames[1:]
	l.bytes -= len(frame.wire)
	q.active = frame
	return frame
}

func (q *frameQueue) complete(frame *queuedFrame) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.active == frame {
		q.active = nil
	}
}

// remove retracts a frame before selection. An active frame cannot be retracted:
// its whole-write failure must fence the stream instead of replacing bytes.
func (q *frameQueue) remove(frame *queuedFrame) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i := range q.lanes {
		l := &q.lanes[i]
		for j, f := range l.frames {
			if f == frame {
				copy(l.frames[j:], l.frames[j+1:])
				l.frames[len(l.frames)-1] = nil
				l.frames = l.frames[:len(l.frames)-1]
				l.bytes -= len(frame.wire)
				return true
			}
		}
	}
	return false
}

func (q *frameQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	for i := range q.lanes {
		q.lanes[i] = frameLane{}
	}
}
