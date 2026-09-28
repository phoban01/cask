package faults

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/phoban01/cask/internal/backoff"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/testutil/sim"
)

// EpochOldOwnerWrite drives a deposed owner: it establishes a current owner at a
// high epoch on a probe key, then has a stale lower-epoch owner attempt a Write.
// The stale Accept must be fenced out by the epoch-encoded ballot (invariant
// S11). The gate's S1/S11 checks verify no corruption resulted.
type EpochOldOwnerWrite struct{}

func (EpochOldOwnerWrite) Name() string { return sim.FaultEpochOldOwnerWrite }

func (EpochOldOwnerWrite) Inject(s *sim.Sim) {
	ctx := context.Background()
	clients := s.Net.Clients()
	if len(clients) == 0 {
		return
	}
	key := []byte("\x00sim/epoch-probe")
	s.Observe(key)

	const highEpoch = 100

	// A current owner at a high epoch writes a value — this is the committed state.
	cur := caspaxos.NewOwnedProposer(1, clients)
	if err := cur.TakeOwnership(ctx, key, highEpoch); err != nil {
		s.Trace.Add("step %d: epoch_old_owner_write: current owner could not take ownership: %v", s.Step(), err)
		return
	}
	if _, err := cur.Write(ctx, key, caspaxos.Write([]byte("current-owner"))); err != nil {
		s.Trace.Add("step %d: epoch_old_owner_write: current owner write failed: %v", s.Step(), err)
		return
	}

	// A stale owner at a lower epoch must be fenced out: its TakeOwnership or
	// Write must fail rather than overwrite the current owner's value.
	stale := caspaxos.NewOwnedProposer(2, clients)
	if err := stale.TakeOwnership(ctx, key, highEpoch-1); err == nil {
		// It managed phase 1; the subsequent Write must still NACK at the fence.
		if _, werr := stale.Write(ctx, key, caspaxos.Write([]byte("stale-owner"))); werr == nil {
			s.Trace.Add("step %d: epoch_old_owner_write: WARNING stale owner write returned nil error", s.Step())
		} else {
			s.Trace.Add("step %d: epoch_old_owner_write: stale owner write fenced: %v", s.Step(), werr)
		}
	} else {
		s.Trace.Add("step %d: epoch_old_owner_write: stale owner fenced at phase 1: %v", s.Step(), err)
	}
}

// ownedAdapter exposes an OwnedProposer's fast-path Write as an mvcc.Proposer,
// so the fault can drive real MVCC chains (the shape S13 audits) down the
// skip-phase-1 path.
type ownedAdapter struct{ p *caspaxos.OwnedProposer }

func (a ownedAdapter) Propose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error) {
	return a.p.Write(ctx, key, change)
}

// OwnerVsFullProposer drives the W0 ballot-space regression
// (docs/plans/quepaxa-learnings-implementation.md): an owner commits MVCC
// versions on the fast path, a full proposer preempts it on the same key, and
// the deposed owner attempts another stale-cache write. The full proposer's
// epoch jump must fence the owner out (ErrLostOwnership); pre-fix, a +1 ballot
// bump instead let the owner win the NodeID tiebreak and silently overwrite
// the full proposer's committed version — a lost update S13 now catches
// structurally. Both tiebreak orders run, on separate probe keys.
type OwnerVsFullProposer struct{}

func (OwnerVsFullProposer) Name() string { return sim.FaultOwnerVsFullProposer }

func (OwnerVsFullProposer) Inject(s *sim.Sim) {
	// The dangerous order first (owner wins the counter tiebreak pre-fix),
	// then the benign order — safety must not depend on node-id luck.
	duel(s, []byte("\x00sim/owner-duel-hi"), 9, 2)
	duel(s, []byte("\x00sim/owner-duel-lo"), 1, 8)
}

func duel(s *sim.Sim, key []byte, ownerID, fullID uint64) {
	ctx := context.Background()
	clients := s.Net.Clients()
	if len(clients) == 0 {
		return
	}
	s.Observe(key)
	step := s.Step()
	clock := hlc.New(s.Clock.Phys())

	// Distinct node ids per injection keep the trace readable. Each
	// mvcc.New also draws its own incarnation, so OpIDs never repeat even
	// with one node id (restart_same_node_id checks that).
	kvNode := uint64(1_000 + 2*step)

	// Establish the owner. The epoch must exceed whatever the key has already
	// reached (earlier injections, earlier jumps); EpochBehindError reports
	// exactly that, so the acquisition loop converges deterministically.
	owner := caspaxos.NewOwnedProposer(ownerID, clients)
	epoch := uint64(1)
	for attempt := 0; ; attempt++ {
		err := owner.TakeOwnership(ctx, key, epoch)
		if err == nil {
			break
		}
		var behind *caspaxos.EpochBehindError
		if errors.As(err, &behind) && attempt < 4 {
			epoch = behind.Observed + 1
			continue
		}
		s.Trace.Add("step %d: owner_vs_full_proposer: owner could not take ownership: %v", step, err)
		return
	}
	ownerKV := mvcc.New(ownedAdapter{p: owner}, clock, kvNode)
	if _, err := ownerKV.Put(ctx, key, fmt.Appendf(nil, "owner-%d", step)); err != nil {
		s.Trace.Add("step %d: owner_vs_full_proposer: owner fast-path put failed: %v", step, err)
		return
	}

	// A full proposer preempts the owner on the same key.
	fullKV := mvcc.New(caspaxos.NewProposer(fullID, clients), clock, kvNode+1)
	if _, err := fullKV.Put(ctx, key, fmt.Appendf(nil, "full-%d", step)); err != nil {
		s.Trace.Add("step %d: owner_vs_full_proposer: full proposer put failed: %v", step, err)
		return
	}

	// The deposed owner writes again from its stale cache. It MUST be fenced
	// out; a nil error here is the lost-update hazard (and S13 will flag the
	// full proposer's committed version vanishing at the next quiescent step).
	if _, err := ownerKV.Put(ctx, key, fmt.Appendf(nil, "stale-%d", step)); err == nil {
		s.Trace.Add("step %d: owner_vs_full_proposer: WARNING deposed owner write returned nil error (lost update hazard)", step)
	} else if !errors.Is(err, caspaxos.ErrLostOwnership) {
		s.Trace.Add("step %d: owner_vs_full_proposer: deposed owner write failed unexpectedly: %v", step, err)
	}
}

// DuelingProposers is the W3 liveness fault: K symmetric proposers hammer one
// key concurrently with NO driver convention, relying only on ballot bumping
// plus the injected randomized backoff to converge. Every writer must finish
// its ops without exhausting its retry budget — an ErrPreempted here means the
// livelock class that bit the multi-lighthouse bootstrap is back, and the
// WARNING fails the gate via FAULT-ASSERT. This asserts, empirically and on
// every PR, the property QuePaxa gets by construction: contention costs
// latency, never liveness.
type DuelingProposers struct{}

func (DuelingProposers) Name() string { return sim.FaultDuelingProposers }

func (DuelingProposers) Inject(s *sim.Sim) {
	const (
		writers = 4
		ops     = 3
	)
	ctx := context.Background()
	clients := s.Net.Clients()
	if len(clients) == 0 {
		return
	}
	key := []byte("\x00sim/dueling")
	s.Observe(key)
	step := s.Step()
	clock := hlc.New(s.Clock.Phys())

	// Seeds and OpID namespaces are drawn HERE, on the gate goroutine, before
	// any worker exists (the adversary-RNG ownership rule).
	seeds := make([]int64, writers)
	for i := range seeds {
		seeds[i] = s.RNG.Int63()
	}
	kvBase := uint64(10_000 + writers*step)

	var wg sync.WaitGroup
	errs := make([]error, writers)
	for w := range writers {
		prop := caspaxos.NewProposer(uint64(30+w), clients,
			caspaxos.WithBackoff(backoff.Seeded(200*time.Microsecond, 5*time.Millisecond, seeds[w])))
		kv := mvcc.New(prop, clock, kvBase+uint64(w))
		wg.Add(1)
		go func(w int, kv *mvcc.KV) {
			defer wg.Done()
			for op := range ops {
				// Bounded client-level retry: under a profile with raised
				// buggify cruelty (5% spurious NACKs per acceptor), one fixed
				// 12-round proposer budget can exhaust probabilistically —
				// that is noise, not livelock. Liveness demands convergence
				// across a client retry; only exhausting THOSE is a WARNING.
				var err error
				for attempt := 0; attempt < 3; attempt++ {
					if _, err = kv.Put(ctx, key, fmt.Appendf(nil, "w%d-%d-%d", w, step, op)); err == nil || !errors.Is(err, caspaxos.ErrPreempted) {
						break
					}
				}
				if err != nil {
					errs[w] = err
					return
				}
			}
		}(w, kv)
	}
	wg.Wait()

	for w, err := range errs {
		switch {
		case err == nil:
		case errors.Is(err, caspaxos.ErrPreempted):
			s.Trace.Add("step %d: dueling_proposers: WARNING writer %d preempted out (livelock: backoff failed to converge)", step, w)
		default:
			s.Trace.Add("step %d: dueling_proposers: writer %d failed: %v", step, w, err)
		}
	}
}

// RestartSameNodeID drives issue #170. Writer processes that share a node id
// write one probe key in turn: two live processes, then a restart of the
// first. cask-apiserver named its writes by PID, and the PID is 1 in every
// pod. Each process must append its own version. A Put that returns another
// version was dropped as a repeat of an older OpID: mvcc's exactly-once
// dedup turns an OpID collision into a silent lost write, which S2 and S13
// cannot see because the chain stays well-formed. So the fault WARNs.
type RestartSameNodeID struct{}

func (RestartSameNodeID) Name() string { return sim.FaultRestartSameNodeID }

func (RestartSameNodeID) Inject(s *sim.Sim) {
	RestartSameNodeIDWith(s, func(p mvcc.Proposer, c *hlc.Clock, node uint64) *mvcc.KV {
		return mvcc.New(p, c, node)
	})
}

// RestartSameNodeIDWith runs RestartSameNodeID with newKV as the constructor
// of each process's KV. The negative control passes a constructor that pins
// the incarnation, as every process did before the fix.
func RestartSameNodeIDWith(s *sim.Sim, newKV func(mvcc.Proposer, *hlc.Clock, uint64) *mvcc.KV) {
	ctx := context.Background()
	clients := s.Net.Clients()
	if len(clients) == 0 {
		return
	}
	key := []byte("\x00sim/same-node-id")
	s.Observe(key)
	step := s.Step()
	clock := hlc.New(s.Clock.Phys())

	// The node id is the same for every process, as os.Getpid() was.
	const node = 1
	// Proposer ids stay distinct: they come from --id, which is unique.
	procs := []struct {
		name string
		prop uint64
	}{{"east", 40}, {"west", 41}, {"east-restarted", 40}}
	for _, p := range procs {
		kv := newKV(caspaxos.NewProposer(p.prop, clients), clock, node)
		want := fmt.Appendf(nil, "%s-%d", p.name, step)
		v, err := kv.Put(ctx, key, want)
		if err != nil {
			s.Trace.Add("step %d: restart_same_node_id: %s put failed: %v", step, p.name, err)
			continue
		}
		if !bytes.Equal(v.Value, want) {
			s.Trace.Add("step %d: restart_same_node_id: WARNING %s put returned seq %d value %q, not its own write (OpID reused, write lost)",
				step, p.name, v.Seq, v.Value)
		}
	}
}

// KeepaliveBlackhole models session keepalive RPCs being silently dropped. It is
// buggify-driven: the effect comes from the `session_drop_keepalive` site
// firing per profile inside internal/lease, so Inject is a no-op. The gate's
// workload must exercise lease sessions for this fault to bite — documented in
// docs/sim-gate.md.
type KeepaliveBlackhole struct{}

func (KeepaliveBlackhole) Name() string { return sim.FaultKeepaliveBlackhole }

func (KeepaliveBlackhole) Inject(s *sim.Sim) {
	s.Trace.Add("step %d: keepalive_blackhole active (buggify-driven via session_drop_keepalive)", s.Step())
}
