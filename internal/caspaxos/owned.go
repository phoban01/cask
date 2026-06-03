package caspaxos

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrNotOwner means a fast-path write was attempted without ownership.
	ErrNotOwner = errors.New("caspaxos: not the owner; take ownership first")
	// ErrLostOwnership means a fast-path round was preempted — a higher epoch
	// owner exists. The caller must re-acquire ownership (which re-reads state).
	ErrLostOwnership = errors.New("caspaxos: ownership lost (preempted)")
)

// epochShift splits a ballot Counter into an ownership epoch (high bits) and a
// per-epoch sequence (low bits). Encoding the epoch this way means a newer
// owner's ballots dominate every ballot a previous owner can mint, so a stale
// owner's accepts are always rejected — which is what makes the phase-1 skip
// safe without a time-based lease.
const epochShift = 40

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
// current value; subsequent Writes skip phase 1. A higher-epoch owner causes
// ErrLostOwnership.
func (p *OwnedProposer) TakeOwnership(ctx context.Context, key []byte, epoch uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.epoch, p.seq = epoch, 0
	b := p.ballot()

	var (
		promised int
		best     Ballot
		value    []byte
	)
	for _, ac := range p.acceptors {
		reply, err := ac.Prepare(ctx, key, b)
		if err != nil || !reply.Promised {
			continue
		}
		promised++
		if best.Less(reply.Accepted) {
			best = reply.Accepted
			value = reply.Value
		}
	}
	if promised < p.quorum() {
		p.owning = false
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

	next, err := change(p.value)
	if err != nil {
		return nil, err // includes ErrConflict for a failed CAS
	}

	p.seq++
	b := p.ballot()
	accepts := 0
	for _, ac := range p.acceptors {
		reply, aerr := ac.Accept(ctx, key, b, next)
		if aerr != nil || !reply.Accepted {
			continue
		}
		accepts++
	}
	if accepts < p.quorum() {
		p.owning = false // a newer owner exists; force re-acquire
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
