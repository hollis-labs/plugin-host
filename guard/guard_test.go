package guard_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-host/guard"
)

func TestGuardedReturnsTheAnswer(t *testing.T) {
	got, err := guard.Guarded(context.Background(), nil, "tool x", time.Second,
		func(context.Context) (int, error) { return 42, nil })
	if err != nil || got != 42 {
		t.Fatalf("got %d, %v", got, err)
	}
	want := errors.New("boom")
	_, err = guard.Guarded(context.Background(), nil, "tool x", time.Second,
		func(context.Context) (int, error) { return 0, want })
	if !errors.Is(err, want) {
		t.Fatalf("err = %v", err)
	}
}

func TestGuardedRecoversAPanicAsAnError(t *testing.T) {
	_, err := guard.Guarded(context.Background(), nil, "tool x", time.Second,
		func(context.Context) (int, error) { panic("kaboom") })
	if err == nil || !strings.Contains(err.Error(), "tool x panicked: kaboom") {
		t.Fatalf("err = %v", err)
	}
}

func TestGuardedSaysWhyItReleasedTheCaller(t *testing.T) {
	block := func(ctx context.Context) (int, error) {
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond)
		return 0, ctx.Err()
	}
	release := make(chan struct{})

	_, budgetErr := guard.Guarded(context.Background(), release, "slow", 50*time.Millisecond, block)
	if !errors.Is(budgetErr, guard.ErrBudget) || errors.Is(budgetErr, guard.ErrShuttingDown) || errors.Is(budgetErr, context.Canceled) {
		t.Fatalf("budget: %v", budgetErr)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	_, cancelErr := guard.Guarded(ctx, release, "slow", time.Minute, block)
	if !errors.Is(cancelErr, context.Canceled) || errors.Is(cancelErr, guard.ErrBudget) {
		t.Fatalf("caller cancel must not read as a budget overrun: %v", cancelErr)
	}

	go func() { time.Sleep(30 * time.Millisecond); close(release) }()
	_, shutErr := guard.Guarded(context.Background(), release, "slow", time.Minute, block)
	if !errors.Is(shutErr, guard.ErrShuttingDown) || errors.Is(shutErr, guard.ErrBudget) {
		t.Fatalf("shutdown: %v", shutErr)
	}

	msgs := map[string]bool{budgetErr.Error(): true, cancelErr.Error(): true, shutErr.Error(): true}
	if len(msgs) != 3 {
		t.Fatalf("the three messages must be distinct: %v", msgs)
	}
}

func TestGuardedDoesNotBlockOnALateAnswer(t *testing.T) {
	late := make(chan struct{})
	start := time.Now()
	_, err := guard.Guarded(context.Background(), nil, "wedged", 50*time.Millisecond,
		func(context.Context) (int, error) { <-late; return 1, nil })
	if !errors.Is(err, guard.ErrBudget) || time.Since(start) > 2*time.Second {
		t.Fatalf("err = %v after %s", err, time.Since(start))
	}
	close(late) // the abandoned goroutine can still finish and exit
}
