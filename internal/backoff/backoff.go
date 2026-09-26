// Package backoff provides jittered retry delays for consensus contention.
//
// CASPaxos resolves dueling proposers by ballot bumping, which alone can
// livelock: two symmetric proposers preempt each other indefinitely. A
// randomized delay between retries breaks the symmetry — the W3 stance
// (docs/plans/quepaxa-learnings-implementation.md) borrowed from QuePaxa's
// randomization: a mechanism whose misconfiguration costs latency, never
// liveness or safety. The delay function is injected into the pure consensus
// core (caspaxos.WithBackoff), which stays free of clocks and randomness.
package backoff

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// Func sleeps an appropriate delay before retry number attempt (0-based,
// counting completed failed attempts). It returns early with ctx's error if
// the context ends first. A nil Func means no delay.
type Func func(ctx context.Context, attempt int) error

// FullJitter returns the standard "full jitter" policy: attempt n sleeps
// uniform(0, min(max, base<<n)). It draws from the process-global rand and is
// safe for concurrent use — one policy can serve every proposer in a process.
func FullJitter(base, max time.Duration) Func {
	return jitter(base, max, rand.Int63n)
}

// Seeded returns a FullJitter policy drawing from its own seeded stream
// (mutex-guarded), for deterministic tests and the simulator. Draw order
// across concurrent users is still schedule-dependent; the stream itself is
// reproducible from the seed.
func Seeded(base, max time.Duration, seed int64) Func {
	var (
		mu  sync.Mutex
		rng = rand.New(rand.NewSource(seed))
	)
	return jitter(base, max, func(n int64) int64 {
		mu.Lock()
		defer mu.Unlock()
		return rng.Int63n(n)
	})
}

func jitter(base, max time.Duration, int63n func(int64) int64) Func {
	if base <= 0 {
		base = time.Millisecond
	}
	if max < base {
		max = base
	}
	return func(ctx context.Context, attempt int) error {
		ceil := max
		// base<<attempt with shift-overflow saturation.
		if attempt < 62 {
			if shifted := base << attempt; shifted > 0 && shifted < max {
				ceil = shifted
			}
		}
		d := time.Duration(int63n(int64(ceil) + 1))
		select {
		case <-time.After(d):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
