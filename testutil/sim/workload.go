package sim

import (
	"context"
	"fmt"
	"time"

	"github.com/phoban01/cask/internal/agent"
	"github.com/phoban01/cask/internal/backoff"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/lease"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/owner"
	"github.com/phoban01/cask/internal/placement"
	"github.com/phoban01/cask/internal/ranges"
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

// boundProposer late-binds the control-plane proposer, closing the value
// cycle manager → locks → router → manager the way cmd's dynamicProposer does.
type boundProposer struct{ p lease.Proposer }

func (b *boundProposer) Propose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error) {
	return b.p.Propose(ctx, key, change)
}

// OwnershipWorkload drives the MVCCWorkload op mix through the production
// write topology (W1): an agent.Router with the owner.Manager 1-RTT fast path
// over one full-keyspace range, control-plane sessions/locks routed through
// that same router. It exercises grant lifecycle under faults, owned writes
// interleaved with the owner's own full-path fallbacks (including the
// owned_proposer_force_full_round buggify site), fence recovery after
// preemption, and CAS-conflict fallbacks — with S1/S2/S3/S11/S13 auditing the
// results from durable state.
type OwnershipWorkload struct {
	NumKeys int // distinct keys the workload cycles through (default 4)
}

// Begin builds the fast-path topology over the sim and returns the round runner.
func (w OwnershipWorkload) Begin(s *Sim) (Round, error) {
	numKeys := w.NumKeys
	if numKeys == 0 {
		numKeys = 4
	}
	clock := hlc.New(s.Clock.Phys())

	replicas := make([]uint64, s.Net.N())
	dialer := agent.StaticDialer{}
	for i := range replicas {
		replicas[i] = uint64(i)
		dialer[uint64(i)] = s.Net.Client(i)
	}
	rmap := ranges.NewMap([]ranges.Descriptor{{ID: 1, Replicas: replicas, Epoch: 1}})
	hint, ok := placement.Owner(ranges.RangeKey(1), replicas)
	if !ok {
		return nil, fmt.Errorf("sim: no HRW owner hint")
	}

	bp := &boundProposer{}
	sessions := lease.NewSessions(bp, func() int64 { return s.Clock.Now() })
	locks := lease.NewLocks(bp, sessions)
	// TTL far beyond the scenario's virtual-time horizon: lapse-under-skew is
	// W4's territory; here the grant lifecycle is driven by Maintain.
	mgr := owner.New(hint, dialer, sessions, locks, owner.WithSessionTTL(1<<50))
	router := agent.NewRouter(hint, rmap, dialer,
		agent.WithFastPath(mgr),
		agent.WithBackoff(backoff.Seeded(100*time.Microsecond, 2*time.Millisecond, s.RNG.Int63())))
	bp.p = router
	kv := mvcc.New(router, clock, 1)

	keys := make([][]byte, numKeys)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("k%d", i))
	}

	return func(ctx context.Context, round int) error {
		// Grant lifecycle rides the reconcile cadence: acquire/renew every few
		// rounds, tolerating failures under faults (the fast path just stays
		// cold until it succeeds).
		if round%5 == 0 {
			if err := mgr.Maintain(ctx, rmap); err != nil {
				return err
			}
		}
		key := keys[s.RNG.Intn(numKeys)]
		s.Observe(key)
		s.Clock.Advance(1_000)

		switch s.RNG.Intn(4) {
		case 0:
			_, err := kv.Put(ctx, key, fmt.Appendf(nil, "v%d-%d", round, s.RNG.Intn(1000)))
			return err
		case 1:
			cur, _, err := kv.Get(ctx, key)
			if err != nil {
				return err
			}
			_, err = kv.CAS(ctx, key, cur, fmt.Appendf(nil, "cas%d", round))
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
