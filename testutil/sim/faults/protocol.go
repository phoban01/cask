package faults

import (
	"context"
	"errors"
	"fmt"

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

	// Distinct OpID namespaces per injection: a reused (node, seq) OpID would
	// be silently deduplicated by mvcc's exactly-once append and the probe
	// writes would be no-ops.
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
