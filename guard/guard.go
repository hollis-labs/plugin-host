package guard

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrBudget reports a call that did not return within its budget. The
// caller is released; the call's goroutine is not.
var ErrBudget = errors.New("guard: call exceeded its budget")

// ErrShuttingDown reports a call released early because the host is
// shutting down. It is distinct from [ErrBudget] because the two mean
// opposite things about the callee: one was too slow, the other was doing
// nothing wrong when the process decided to stop.
var ErrShuttingDown = errors.New("guard: host is shutting down")

// Guarded runs call and returns whichever comes first: its answer, a
// recovered panic as an ordinary error, the budget ([ErrBudget]), the
// caller's cancellation (ctx's error), or release closing ([ErrShuttingDown]).
// subject names the callee in messages. call receives a context that ends at
// the budget or when ctx does.
//
// The result channel is buffered so a late answer does not block forever on
// a caller that has already left, which would turn a leaked goroutine into
// one that also pins the whole call.
func Guarded[T any](
	ctx context.Context,
	release <-chan struct{},
	subject string,
	budget time.Duration,
	call func(context.Context) (T, error),
) (T, error) {
	type outcome struct {
		value T
		err   error
	}
	results := make(chan outcome, 1)

	bounded, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				// A panic crossing a goroutine boundary takes the process
				// with it, so it is recovered where it was raised and handed
				// back as an error. The stack is lost; that is the trade.
				var zero T
				results <- outcome{value: zero, err: fmt.Errorf("%s panicked: %v", subject, recovered)}
			}
		}()
		value, err := call(bounded)
		results <- outcome{value: value, err: err}
	}()

	var zero T
	select {
	case result := <-results:
		return result.value, result.err
	case <-release:
		return zero, fmt.Errorf("%w: %s was still running and was not waited on", ErrShuttingDown, subject)
	case <-bounded.Done():
		// One case, not two: bounded derives from ctx, so selecting on both
		// would race, and the outcomes read differently. A caller that
		// cancelled did not "exceed its budget".
		if ctx.Err() != nil {
			return zero, fmt.Errorf("%s: %w", subject, ctx.Err())
		}
		return zero, fmt.Errorf("%w: %s did not return within %s", ErrBudget, subject, budget)
	}
}
