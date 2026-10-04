package pluginhost

import (
	"bytes"
	"sync"
	"testing"
)

// A physical writer may retain an entire large ordinary frame. Cancellation
// admission must still work while the ordinary queue is full behind that frame.
// This verifies the codec-independent seam; real Conn publication remains gated.
func TestWriterAdmitsEveryCancelDuringSustainedLargeOrdinaryFrames(t *testing.T) {
	q := &frameQueue{}
	large := bytes.Repeat([]byte("x"), writerLaneBytes)
	for round := 0; round < 8; round++ {
		active, err := q.enqueue(ordinaryLane, large)
		if err != nil || q.take() != active {
			t.Fatalf("ordinary selection: %v", err)
		}
		queued, err := q.enqueue(ordinaryLane, large)
		if err != nil {
			t.Fatal(err)
		}
		var writers sync.WaitGroup
		cancels := make(chan *queuedFrame, 16)
		for i := 0; i < 16; i++ {
			writers.Go(func() {
				frame, err := q.enqueue(controlLane, []byte("rpc/cancel"))
				if err != nil {
					t.Errorf("cancel lost with ordinary frame active: %v", err)
					return
				}
				cancels <- frame
			})
		}
		writers.Wait()
		close(cancels)
		pending := make(map[*queuedFrame]bool)
		for frame := range cancels {
			pending[frame] = true
		}
		if len(pending) != 16 || q.take() != nil {
			t.Fatal("cancel admission lost work or interrupted a whole-frame write")
		}
		q.complete(active)
		ordinarySeen, controlsBeforeOrdinary := false, 0
		for frame := q.take(); frame != nil; frame = q.take() {
			if frame == queued {
				ordinarySeen = true
				if controlsBeforeOrdinary > controlBurst {
					t.Fatal("cancels starved ordinary work")
				}
			} else if pending[frame] {
				delete(pending, frame)
				if !ordinarySeen {
					controlsBeforeOrdinary++
				}
			} else {
				t.Fatal("unknown or duplicate frame")
			}
			q.complete(frame)
		}
		if !ordinarySeen || len(pending) != 0 {
			t.Fatal("admitted ordinary/cancel work was not delivered")
		}
	}
}
