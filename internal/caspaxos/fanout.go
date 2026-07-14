package caspaxos

import "context"

// indexed carries one acceptor call's outcome back to the phase loop.
type indexed[T any] struct {
	i   int
	v   T
	err error
}

// fanout invokes call once per index concurrently and returns a channel that
// yields each outcome in completion order. The channel is buffered to n, so
// workers never block on send: the caller may stop receiving as soon as it
// has a quorum (or knows one is impossible) and cancel ctx to hurry the
// stragglers — a cancelled or late call simply lands in the buffer and is
// garbage-collected with it.
//
// This is what turns a phase's latency from the SUM of replica round-trips
// into the max over the fastest quorum, and a dead peer from a full
// per-phase transport timeout into a non-event (W2 in
// docs/plans/quepaxa-learnings-implementation.md).
func fanout[T any](ctx context.Context, n int, call func(ctx context.Context, i int) (T, error)) <-chan indexed[T] {
	out := make(chan indexed[T], n)
	for i := range n {
		go func(i int) {
			v, err := call(ctx, i)
			out <- indexed[T]{i: i, v: v, err: err}
		}(i)
	}
	return out
}
