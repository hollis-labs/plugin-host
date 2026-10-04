package pluginhost

import (
	"bytes"
	"errors"
	"sync"
	"testing"
)

func TestWriterLanesHaveIndependentFrameAndByteBounds(t *testing.T) {
	q := &frameQueue{}
	for i := 0; i < writerLaneFrames; i++ {
		if _, err := q.enqueue(ordinaryLane, []byte("request")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := q.enqueue(ordinaryLane, []byte("extra")); !errors.Is(err, errWriterFull) {
		t.Fatal("frame cap bypassed")
	}
	if _, err := q.enqueue(controlLane, []byte("cancel")); err != nil {
		t.Fatal("ordinary saturation blocked control")
	}
	q = &frameQueue{}
	if _, err := q.enqueue(ordinaryLane, bytes.Repeat([]byte("x"), writerLaneBytes)); err != nil {
		t.Fatal(err)
	}
	if _, err := q.enqueue(ordinaryLane, []byte("x")); !errors.Is(err, errWriterFull) {
		t.Fatal("byte cap bypassed")
	}
	if _, err := q.enqueue(controlLane, []byte("reply")); err != nil {
		t.Fatal("byte saturation blocked reply")
	}
}

func TestTerminalCreditSurvivesFullControlLane(t *testing.T) {
	q := &frameQueue{}
	credit, err := q.reserveTerminal()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = q.enqueue(controlLane, bytes.Repeat([]byte("x"), writerLaneBytes-terminalBytes)); err != nil {
		t.Fatal(err)
	}
	if _, err = q.enqueue(controlLane, []byte("x")); !errors.Is(err, errWriterFull) {
		t.Fatal("reserved bytes borrowed")
	}
	frame, err := credit.terminal(bytes.Repeat([]byte("s"), terminalBytes+1), []byte("bounded committed fallback"))
	if err != nil || string(frame.wire) != "bounded committed fallback" {
		t.Fatalf("terminal fallback: %v %v", frame, err)
	}
	if _, err := credit.terminal(nil, nil); !errors.Is(err, errTerminalCredit) {
		t.Fatal("credit consumed twice")
	}
	credit.release()
}

func TestWriterControlFairnessAndWholeFrameOwnership(t *testing.T) {
	q := &frameQueue{}
	ordinary, _ := q.enqueue(ordinaryLane, []byte("ordinary"))
	for i := 0; i < 6; i++ {
		if _, err := q.enqueue(controlLane, []byte("control")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 4; i++ {
		frame := q.take()
		if frame == nil || string(frame.wire) != "control" {
			t.Fatal("control did not progress")
		}
		if q.take() != nil || q.remove(frame) {
			t.Fatal("active write lost whole-frame ownership")
		}
		q.complete(frame)
	}
	frame := q.take()
	if frame != ordinary {
		t.Fatal("control starved ordinary")
	}
	q.complete(frame)
}

func TestWriterCanceledQueuedFrameRefundsCapacity(t *testing.T) {
	q := &frameQueue{}
	wire := bytes.Repeat([]byte("x"), writerLaneBytes)
	frame, err := q.enqueue(ordinaryLane, wire)
	if err != nil {
		t.Fatal(err)
	}
	wire[0] = 'z'
	if frame.wire[0] != 'x' {
		t.Fatal("queue retained mutable caller bytes")
	}
	if !q.remove(frame) || q.remove(frame) {
		t.Fatal("queued cancellation not exactly once")
	}
	if _, err = q.enqueue(ordinaryLane, wire); err != nil {
		t.Fatal("queued cancellation did not refund bytes")
	}
	credit, err := q.reserveTerminal()
	if err != nil {
		t.Fatal(err)
	}
	q.close()
	if q.take() != nil {
		t.Fatal("closed writer selected work")
	}
	if _, err := q.enqueue(controlLane, nil); !errors.Is(err, errWriterClosed) {
		t.Fatal(err)
	}
	if _, err := credit.terminal(nil, nil); !errors.Is(err, errWriterClosed) {
		t.Fatal(err)
	}
	credit.release()
	credit.release()
}

func TestTerminalReservationsCountAgainstFrameCapacity(t *testing.T) {
	q := &frameQueue{}
	var credits []*terminalCredit
	for i := 0; i < writerLaneFrames; i++ {
		credit, err := q.reserveTerminal()
		if err != nil {
			t.Fatal(err)
		}
		credits = append(credits, credit)
	}
	if _, err := q.reserveTerminal(); !errors.Is(err, errWriterFull) {
		t.Fatal("terminal reservations exceeded frame capacity")
	}
	if _, err := q.enqueue(controlLane, []byte("cancel")); !errors.Is(err, errWriterFull) {
		t.Fatal("control borrowed reserved frame")
	}
	if _, err := credits[1].terminal(nil, bytes.Repeat([]byte("x"), terminalBytes+1)); !errors.Is(err, errTerminalCredit) {
		t.Fatal("unbounded fallback accepted")
	}
	credits[0].release()
	credits[0].release()
	if _, err := q.enqueue(controlLane, []byte("cancel")); err != nil {
		t.Fatal("released credit not refunded")
	}
	if _, err := q.enqueue(controlLane, nil); !errors.Is(err, errWriterFull) {
		t.Fatal("double release widened frame capacity")
	}
	for _, credit := range credits {
		credit.release()
	}
}

func TestWriterPreservesFIFOWithinEachLane(t *testing.T) {
	q := &frameQueue{}
	var ordinary, control []*queuedFrame
	for i := 0; i < 6; i++ {
		frame, err := q.enqueue(ordinaryLane, []byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
		ordinary = append(ordinary, frame)
		frame, err = q.enqueue(controlLane, []byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
		control = append(control, frame)
	}
	for len(ordinary)+len(control) > 0 {
		frame := q.take()
		switch {
		case len(ordinary) > 0 && frame == ordinary[0]:
			ordinary = ordinary[1:]
		case len(control) > 0 && frame == control[0]:
			control = control[1:]
		default:
			t.Fatal("lane reordered or lost frame")
		}
		q.complete(frame)
	}
	if q.take() != nil {
		t.Fatal("unexpected extra frame")
	}
}

func TestWriterConcurrentTerminalReservationAndEnqueueShareCapacity(t *testing.T) {
	q := &frameQueue{}
	var wg sync.WaitGroup
	start := make(chan struct{})
	credits := make(chan *terminalCredit, 64)
	frames := make(chan *queuedFrame, 64)
	for i := 0; i < 64; i++ {
		wg.Go(func() {
			<-start
			credit, err := q.reserveTerminal()
			if err == nil {
				credits <- credit
			} else if !errors.Is(err, errWriterFull) {
				t.Error(err)
			}
		})
		wg.Go(func() {
			<-start
			frame, err := q.enqueue(controlLane, []byte("control"))
			if err == nil {
				frames <- frame
			} else if !errors.Is(err, errWriterFull) {
				t.Error(err)
			}
		})
	}
	close(start)
	wg.Wait()
	close(credits)
	close(frames)
	if len(credits)+len(frames) != writerLaneFrames {
		t.Fatalf("combined admitted capacity = %d", len(credits)+len(frames))
	}
	for credit := range credits {
		credit.release()
	}
	for frame := range frames {
		if !q.remove(frame) {
			t.Fatal("admitted frame missing")
		}
	}
	if _, err := q.enqueue(controlLane, bytes.Repeat([]byte("x"), writerLaneBytes)); err != nil {
		t.Fatal("capacity not refunded", err)
	}
}

func TestWriterActiveFrameIsSeparateAndStillOwnsTheStream(t *testing.T) {
	q := &frameQueue{}
	frame, err := q.enqueue(ordinaryLane, bytes.Repeat([]byte("x"), writerLaneBytes))
	if err != nil {
		t.Fatal(err)
	}
	if q.take() != frame {
		t.Fatal("queued frame not selected")
	}
	if _, err := q.enqueue(ordinaryLane, bytes.Repeat([]byte("y"), writerLaneBytes)); err != nil {
		t.Fatal("active frame charged to queued lane", err)
	}
	if _, err := q.enqueue(ordinaryLane, []byte("z")); !errors.Is(err, errWriterFull) {
		t.Fatal("queued byte cap bypassed")
	}
	q.complete(&queuedFrame{})
	if q.take() != nil {
		t.Fatal("foreign completion released active ownership")
	}
	q.close()
	if q.active != frame {
		t.Fatal("close lost in-progress write ownership")
	}
	q.complete(frame)
	if q.active != nil || q.take() != nil {
		t.Fatal("closed stream retained selectable work")
	}
}
