package sim

import (
	"context"
	"fmt"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
)

// MVCCWorkload drives Put/CAS/Get/Delete over an mvcc.KV backed by a CASPaxos
// proposer across the sim's acceptors. It is the default gate workload: it
// exercises the exactly-once append path and the per-key version chain that
// invariants S1/S2/S3/S11 check, under whatever faults the profile injects.
type MVCCWorkload struct {
	NumKeys int // distinct keys the workload cycles through (default 4)
}

// Begin builds the proposer + KV over the sim and returns the round runner.
func (w MVCCWorkload) Begin(s *Sim) (Round, error) {
	numKeys := w.NumKeys
	if numKeys == 0 {
		numKeys = 4
	}
	clock := hlc.New(s.Clock.Phys())
	// One CASPaxos proposer over every acceptor — a single-range cluster, which
	// is all PR #0 needs. Range routing arrives with §4.3. *caspaxos.Proposer
	// satisfies mvcc.Proposer directly.
	prop := caspaxos.NewProposer(1, s.Net.Clients())
	kv := mvcc.New(prop, clock, 1)

	keys := make([][]byte, numKeys)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("k%d", i))
	}

	return func(ctx context.Context, round int) error {
		key := keys[s.RNG.Intn(numKeys)]
		s.Observe(key)
		// Advance logical time so HLC timestamps progress (S3 wants strict
		// monotonicity; the clock also moves on its own via Update, but stepping
		// physical time keeps the chain realistic).
		s.Clock.Advance(1_000) // 1µs per round

		switch s.RNG.Intn(4) {
		case 0:
			_, err := kv.Put(ctx, key, []byte(fmt.Sprintf("v%d-%d", round, s.RNG.Intn(1000))))
			return err
		case 1:
			// CAS against the current value (often conflicts; that's fine).
			cur, _, err := kv.Get(ctx, key)
			if err != nil {
				return err
			}
			_, err = kv.CAS(ctx, key, cur, []byte(fmt.Sprintf("cas%d", round)))
			return err
		case 2:
			_, _, err := kv.Get(ctx, key)
			return err
		default:
			_, err := kv.Delete(ctx, key)
			return err
		}
	}, nil
}
