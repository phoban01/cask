package caspaxos_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/phoban01/cask/internal/backoff"
	"github.com/phoban01/cask/internal/caspaxos"
)

// The backoff hook runs between preempted rounds only — an uncontended round
// pays nothing — and receives the failed attempt number.
func TestBackoffCalledOnPreemption(t *testing.T) {
	ctx := context.Background()
	acc := ownedCluster(3)

	// Raise every register's promise so the victim's first rounds preempt.
	blocker := caspaxos.NewProposer(9, acc)
	if _, err := blocker.Propose(ctx, []byte("k"), caspaxos.Write([]byte("theirs"))); err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int32
	victim := caspaxos.NewProposer(1, acc, caspaxos.WithBackoff(func(ctx context.Context, attempt int) error {
		calls.Add(1)
		return nil
	}))
	if _, err := victim.Propose(ctx, []byte("k"), caspaxos.Write([]byte("mine"))); err != nil {
		t.Fatalf("propose: %v", err)
	}
	if calls.Load() == 0 {
		t.Fatal("backoff never invoked despite a preempted first round")
	}

	// An uncontended proposal on a fresh key never backs off.
	calls.Store(0)
	if _, err := victim.Propose(ctx, []byte("fresh"), caspaxos.Write([]byte("v"))); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatalf("backoff invoked %d times on an uncontended round", calls.Load())
	}
}

// The W3 liveness property: symmetric concurrent writers on one key, no
// driver convention, converge under ballot bumping + randomized backoff
// without exhausting their retry budgets.
func TestDuelingProposersConvergeWithBackoff(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A retried write MUST be a compare-and-set, never a blind reapplication of a change.
	const (
		writers = 6
		ops     = 5
	)
	ctx := context.Background()
	acc := ownedCluster(3)

	var wg sync.WaitGroup
	errs := make([]error, writers)
	for w := range writers {
		pause := backoff.Seeded(200_000, 5_000_000, int64(w)) // 200µs..5ms
		p := caspaxos.NewProposer(uint64(w+1), acc, caspaxos.WithBackoff(pause))
		wg.Add(1)
		go func(w int, p *caspaxos.Proposer) {
			defer wg.Done()
			for i := range ops {
				if err := appendRetrying(ctx, p, pause, fmt.Sprintf("w%d-%d", w, i)); err != nil {
					errs[w] = err
					return
				}
			}
		}(w, p)
	}
	wg.Wait()

	for w, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v (livelock: backoff failed to converge)", w, err)
		}
	}
	// Every op landed exactly once. Propose no longer reapplies a change
	// after a minority accept (issue #72), and the caller retries with a
	// change that detects its own earlier write.
	reader := caspaxos.NewProposer(99, acc)
	got, err := reader.Propose(ctx, []byte("hot"), caspaxos.Identity)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, op := range splitCommas(got) {
		seen[string(op)]++
	}
	for w := range writers {
		for i := range ops {
			if id := fmt.Sprintf("w%d-%d", w, i); seen[id] != 1 {
				t.Fatalf("op %s landed %d times, want 1; committed %q", id, seen[id], got)
			}
		}
	}
}

// clientRetries is the retry budget for one append. A writer that spends it
// makes no progress, and the test reports a livelock.
const clientRetries = 12

// appendRetrying appends marker to the "hot" register exactly once. It is the
// caller side of the ErrUnknownOutcome contract. After an unknown outcome or
// a preempted call, it backs off and proposes again. The next round reads the
// register in its prepare phase. appendOnce then skips marker if an earlier
// attempt landed, so a retry never reapplies the append blindly.
//
// Each Propose call starts its own backoff at attempt 0. The attempt count
// here grows across calls, so the delay between calls grows too. Without
// that, six writers can keep colliding at the shortest delay.
func appendRetrying(ctx context.Context, p *caspaxos.Proposer, pause backoff.Func, marker string) error {
	change := appendOnce(marker)
	var err error
	for attempt := range clientRetries {
		_, err = p.Propose(ctx, []byte("hot"), change)
		if !errors.Is(err, caspaxos.ErrUnknownOutcome) && !errors.Is(err, caspaxos.ErrPreempted) {
			return err
		}
		if perr := pause(ctx, attempt+1); perr != nil {
			return perr
		}
	}
	return err
}

// appendOnce is appendChange that returns the current value unchanged when
// marker is already in the list. A retry after ErrUnknownOutcome then cannot
// append the same marker twice.
func appendOnce(marker string) caspaxos.ChangeFunc {
	return func(cur []byte) ([]byte, error) {
		for _, op := range splitCommas(cur) {
			if string(op) == marker {
				return cur, nil
			}
		}
		return appendChange(marker)(cur)
	}
}

func splitCommas(b []byte) [][]byte {
	if len(b) == 0 {
		return nil
	}
	var out [][]byte
	start := 0
	for i, c := range b {
		if c == ',' {
			out = append(out, b[start:i])
			start = i + 1
		}
	}
	return append(out, b[start:])
}
