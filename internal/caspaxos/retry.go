package caspaxos

import (
	"context"
	"errors"
)

// LostRound reports whether err means that a round lost to other
// proposers: ErrPreempted, or ErrUnknownOutcome.
func LostRound(err error) bool {
	return errors.Is(err, ErrPreempted) || errors.Is(err, ErrUnknownOutcome)
}

// RetryLost runs op, and runs it again while it loses its round. It runs op
// at most attempts times, and calls pause(ctx, n) before retry n (from 0).
// It returns the last error when every attempt lost, and stops at the first
// other error or when pause fails.
//
// A lost read changes nothing, so op may be any read. A write with an
// unknown outcome may still land. So a write op must read the register
// again on every call and commit only with a compare-and-set on what it
// read, never a blind reapplication of its change.
func RetryLost[T any](ctx context.Context, attempts int, pause func(ctx context.Context, attempt int) error, op func() (T, error)) (T, error) {
	var zero T
	for n := 0; ; n++ {
		v, err := op()
		if err == nil || !LostRound(err) || n+1 >= attempts {
			return v, err
		}
		if perr := pause(ctx, n); perr != nil {
			return zero, errors.Join(perr, err)
		}
	}
}
