package caspaxos

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/phoban01/cask/internal/buggify"
)

var (
	// ErrNotOwner means a fast-path write was attempted without ownership.
	ErrNotOwner = errors.New("caspaxos: not the owner; take ownership first")
	// ErrLostOwnership means a fast-path round was preempted — a higher epoch
	// owner exists. The caller must re-acquire ownership (which re-reads state).
	ErrLostOwnership = errors.New("caspaxos: ownership lost (preempted)")
	// ErrEpochReserved rejects TakeOwnership at epoch 0: counters below
	// 1<<epochShift are the plain full-proposer ballot space, and an owner
	// minting there would break the ballot-space discipline (see
	// Proposer.nextBallot). Ownership fences start at 1.
	ErrEpochReserved = errors.New("caspaxos: epoch 0 is reserved for full proposers")
)

// EpochBehindError reports that the register has already seen ballots at or
// above the epoch a fast-path operation ran at — a newer owner has taken over,
// or a full proposer jumped into a synthetic epoch after preempting this one
// (see Proposer.nextBallot). The caller must re-acquire its fence at an epoch
// strictly greater than Observed before retrying; blind retries at the same
// fence can never win. errors.Is(err, ErrLostOwnership) holds.
type EpochBehindError struct {
	Observed uint64 // highest epoch seen in a conflicting promise
}

func (e *EpochBehindError) Error() string {
	return fmt.Sprintf("caspaxos: ownership lost; register already at epoch %d", e.Observed)
}

// Unwrap makes errors.Is(err, ErrLostOwnership) hold for callers that do not
// care about the observed epoch.
func (e *EpochBehindError) Unwrap() error { return ErrLostOwnership }

// epochShift splits a ballot Counter into an ownership epoch (high bits) and a
// per-epoch sequence (low bits). Encoding the epoch this way means a newer
// owner's ballots dominate every ballot a previous owner can mint, so a stale
// owner's accepts are always rejected — which is what makes the phase-1 skip
// safe without a time-based lease.
//
// Capacity bound: the epoch occupies the top 24 bits (2^24 ≈ 16.7M ownership
// handoffs per key) and the sequence the low 40 (2^40 writes per epoch). An
// epoch past 2^24 would wrap into undefined ballot ordering; at one handoff
// per second that is ~194 days of continuous churn on a single key, so it is
// a documented limit rather than a guarded one.
const epochShift = 40

// maxEpoch is the highest representable ownership epoch (24 bits above
// epochShift). Beyond it, ballot ordering is undefined — the documented
// capacity limit above — and nextBallot's epoch jump saturates rather than
// overflow the counter.
const maxEpoch = 1<<(64-epochShift) - 1

// OwnedProposer is the 1-RTT fast path for a single key. After TakeOwnership
// runs one phase-1 round (learning the value and installing the epoch's
// promise), Write commits with a single accept round — no prepare — because the
// owner is the sole writer for its epoch and any older owner is fenced out by
// the epoch-encoded ballot. The epoch comes from the ownership lease's fencing
// token, so ownership handoff (a new fence) automatically supersedes the old
// owner.
//
// Safety rests on two facts: (1) at most one writer per epoch (enforced by the
// ownership lock), and (2) epoch ordering of ballots (enforced here). If a newer
// owner has taken over, this owner's accept is NACKed and Write returns
// ErrLostOwnership rather than overwriting; the caller re-acquires, which
// re-reads the latest committed value. No update is ever lost.
type OwnedProposer struct {
	nodeID    uint64
	acceptors []AcceptorClient

	mu     sync.Mutex
	owning bool
	epoch  uint64
	seq    uint64
	value  []byte
}

// NewOwnedProposer returns a fast-path proposer over a single replica group.
func NewOwnedProposer(nodeID uint64, acceptors []AcceptorClient) *OwnedProposer {
	return &OwnedProposer{nodeID: nodeID, acceptors: acceptors}
}

func (p *OwnedProposer) quorum() int { return len(p.acceptors)/2 + 1 }

func (p *OwnedProposer) ballot() Ballot {
	return Ballot{Counter: (p.epoch << epochShift) | p.seq, NodeID: p.nodeID}
}

// TakeOwnership runs one phase-1 round at the given epoch (the ownership lease's
// fence). On success the owner holds the highest promise and has read the
// current value; subsequent Writes skip phase 1. A higher-epoch owner (or a
// full proposer's synthetic epoch) causes an EpochBehindError carrying the
// epoch the register has reached, so the caller can re-acquire its fence above
// it instead of retrying a fence that can never win.
func (p *OwnedProposer) TakeOwnership(ctx context.Context, key []byte, epoch uint64) error {
	if epoch == 0 {
		return ErrEpochReserved
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	p.epoch, p.seq = epoch, 0
	b := p.ballot()

	n := len(p.acceptors)
	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	replies := fanout(pctx, n, func(ctx context.Context, i int) (PrepareReply, error) {
		return p.acceptors[i].Prepare(ctx, key, b)
	})

	var (
		promised int
		pending  = n
		best     Ballot
		value    []byte
		conflict Ballot
	)
	for range n {
		r := <-replies
		pending--
		switch {
		case r.err != nil:
			// unreachable acceptor: a non-vote
		case !r.v.Promised:
			conflict = conflict.Max(r.v.Conflict)
		default:
			promised++
			if best.Less(r.v.Accepted) {
				best = r.v.Accepted
				value = r.v.Value
			}
		}
		if promised >= p.quorum() || promised+pending < p.quorum() {
			break // decided either way; cancel the stragglers
		}
	}
	if promised < p.quorum() {
		p.owning = false
		// A genuine rejection carries a promise at or above our epoch; report
		// it so the caller can fence past it. (A conflict below our epoch can
		// only be a spurious rejection — buggify — or a lost reply: plain
		// ErrLostOwnership, retryable at the same fence.)
		if obs := conflict.Counter >> epochShift; obs >= epoch {
			return &EpochBehindError{Observed: obs}
		}
		return ErrLostOwnership
	}
	p.value = value
	p.owning = true
	return nil
}

// Write commits change with a single accept round (no prepare). It returns the
// new value. ErrNotOwner if ownership was never taken; ErrLostOwnership if a
// newer epoch has superseded this owner (the caller must re-acquire).
func (p *OwnedProposer) Write(ctx context.Context, key []byte, change ChangeFunc) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.owning {
		return nil, ErrNotOwner
	}

	// BUGGIFY: occasionally force the caller back through the slow path
	// (re-TakeOwnership + full phase 1), keeping that path exercised. Dropping
	// ownership here is always safe — it is exactly what a real preemption does.
	if buggify.Maybe("owned_proposer_force_full_round", 0.02) {
		p.owning = false
		return nil, ErrLostOwnership
	}

	next, err := change(p.value)
	if err != nil {
		return nil, err // includes ErrConflict for a failed CAS
	}

	p.seq++
	b := p.ballot()

	n := len(p.acceptors)
	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	replies := fanout(pctx, n, func(ctx context.Context, i int) (AcceptReply, error) {
		return p.acceptors[i].Accept(ctx, key, b, next)
	})

	var (
		accepts  int
		pending  = n
		conflict Ballot
	)
	for range n {
		r := <-replies
		pending--
		switch {
		case r.err != nil:
			// unreachable acceptor: a non-vote
		case !r.v.Accepted:
			conflict = conflict.Max(r.v.Conflict)
		default:
			accepts++
		}
		if accepts >= p.quorum() || accepts+pending < p.quorum() {
			break
		}
	}
	if accepts < p.quorum() {
		p.owning = false // a newer owner exists; force re-acquire
		if obs := conflict.Counter >> epochShift; obs > p.epoch {
			return nil, &EpochBehindError{Observed: obs}
		}
		return nil, ErrLostOwnership
	}
	p.value = next
	return next, nil
}

// Owns reports whether this proposer currently believes it owns the key.
func (p *OwnedProposer) Owns() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.owning
}
