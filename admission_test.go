package pluginhost

import (
	"context"
	"sync"
	"testing"
)

func TestReverseAdmissionConnectionAndHostBounds(t *testing.T) {
	pool := &reversePool{}
	var permits []*reversePermit
	var connections []*reverseAdmission
	for i := 0; i < 8; i++ {
		a := &reverseAdmission{pool: pool}
		connections = append(connections, a)
		for j := 0; j < 8; j++ {
			p := a.acquire()
			if p == nil {
				t.Fatal("early refusal")
			}
			permits = append(permits, p)
		}
		if a.acquire() != nil {
			t.Fatal("connection exceeded eight executions")
		}
	}
	extra := &reverseAdmission{pool: pool}
	if extra.acquire() != nil {
		t.Fatal("host exceeded 64 executions")
	}
	permits[0].release()
	permits[0].release()
	p := extra.acquire()
	if p == nil {
		t.Fatal("completed execution did not release host slot")
	}
	if connections[1].acquire() != nil {
		t.Fatal("other connection limit widened")
	}
	p.release()
	for _, p := range permits {
		p.release()
	}
	if pool.active != 0 {
		t.Fatal("permits leaked")
	}
}

func TestCanceledReverseExecutionKeepsPermitUntilReturn(t *testing.T) {
	pool := &reversePool{}
	a := &reverseAdmission{pool: pool}
	ctx, cancel := context.WithCancel(context.Background())
	entered, finish, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	permit := a.acquire()
	var finishOnce sync.Once
	finishExecution := func() { finishOnce.Do(func() { close(finish) }) }
	t.Cleanup(func() { finishExecution(); <-returned })
	go func() { defer close(returned); defer permit.release(); close(entered); <-finish }()
	<-entered
	cancel()
	<-ctx.Done()
	var others []*reversePermit
	for i := 0; i < 7; i++ {
		p := a.acquire()
		if p == nil {
			t.Fatal("early refusal")
		}
		others = append(others, p)
	}
	if p := a.acquire(); p != nil {
		p.release()
		t.Fatal("cancellation released stuck execution permit")
	}
	finishExecution()
	<-returned
	p := a.acquire()
	if p == nil {
		t.Fatal("returned callback did not release permit")
	}
	p.release()
	for _, p := range others {
		p.release()
	}
}

func TestConcurrentReverseAdmissionIsAtomicAcrossConnections(t *testing.T) {
	pool := &reversePool{}
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan *reversePermit, 128)
	for i := 0; i < 16; i++ {
		a := &reverseAdmission{pool: pool}
		for j := 0; j < 8; j++ {
			wg.Go(func() { <-start; results <- a.acquire() })
		}
	}
	close(start)
	wg.Wait()
	close(results)
	admitted := 0
	for p := range results {
		if p != nil {
			admitted++
			p.release()
		}
	}
	if admitted != 64 || pool.active != 0 {
		t.Fatalf("admission=%d remaining=%d", admitted, pool.active)
	}
}

func TestConcurrentReversePermitReleaseIsExactlyOnce(t *testing.T) {
	pool := &reversePool{}
	a := &reverseAdmission{pool: pool}
	permit := a.acquire()
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Go(permit.release)
	}
	wg.Wait()
	if pool.active != 0 || a.active != 0 {
		t.Fatalf("release accounting: host=%d connection=%d", pool.active, a.active)
	}
	var permits []*reversePermit
	for i := 0; i < reversePerConnection; i++ {
		p := a.acquire()
		if p == nil {
			t.Fatal("premature refusal after release")
		}
		permits = append(permits, p)
	}
	if p := a.acquire(); p != nil {
		p.release()
		t.Fatal("concurrent release widened admission")
	}
	for _, p := range permits {
		p.release()
	}
}

func TestConcurrentReverseAdmissionKeepsConnectionCeiling(t *testing.T) {
	pool := &reversePool{}
	a := &reverseAdmission{pool: pool}
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan *reversePermit, 64)
	for i := 0; i < 64; i++ {
		wg.Go(func() { <-start; results <- a.acquire() })
	}
	close(start)
	wg.Wait()
	close(results)
	admitted := 0
	for permit := range results {
		if permit != nil {
			admitted++
			permit.release()
		}
	}
	if admitted != reversePerConnection || pool.active != 0 || a.active != 0 {
		t.Fatalf("admitted=%d host=%d connection=%d", admitted, pool.active, a.active)
	}
}
