package caspaxos

import (
	"context"
	"errors"
	"sync"

	"github.com/phoban01/cask/internal/buggify"
)

// AcceptorClient is the proposer's view of one acceptor, whether co-located or
// reached over the network. The transport layer provides concrete
// implementations; the pure core depends only on this interface.
type AcceptorClient interface {
	Prepare(ctx context.Context, key []byte, b Ballot) (PrepareReply, error)
	Accept(ctx context.Context, key []byte, b Ballot, val []byte) (AcceptReply, error)
}

// Proposer is the active half of CASPaxos. It drives the two-phase round against
// one or more configuration groups, requiring a majority quorum in EVERY group.
// A single group is ordinary CASPaxos; two groups give joint consensus, the
// safety primitive for reconfiguration — a value is chosen only with a quorum in
// both the old and the new replica set, so no committed value can be lost across
// a membership change.
//
// Acceptors are deduplicated: an acceptor shared by several groups (e.g. a node
// kept across a reconfiguration) is contacted once and its vote counts in each
// group it belongs to. This is required for correctness — contacting the same
// acceptor twice at one ballot would make its second prepare a self-conflict.
//
// It is safe for concurrent use: ballot minting is serialized, and concurrent
// rounds on different keys proceed independently (acceptors lock per key).
type Proposer struct {
	nodeID    uint64
	acceptors []AcceptorClient // unique acceptors across all groups
	groups    [][]int          // each group is a set of indices into acceptors
	maxRounds int

	mu      sync.Mutex
	counter uint64 // highest ballot counter this proposer has used (guarded by mu)
}

// NewProposer returns a single-group Proposer over acceptors.
func NewProposer(nodeID uint64, acceptors []AcceptorClient) *Proposer {
	idx := make([]int, len(acceptors))
	for i := range acceptors {
		idx[i] = i
	}
	return &Proposer{nodeID: nodeID, acceptors: acceptors, groups: [][]int{idx}, maxRounds: 12}
}

// NewJointProposer returns a Proposer requiring a quorum in every group. The
// groups are given as acceptor lists; shared acceptors are deduplicated so a
// node present in multiple groups is contacted once and counts in each.
func NewJointProposer(nodeID uint64, groups [][]AcceptorClient) *Proposer {
	var acceptors []AcceptorClient
	index := map[AcceptorClient]int{}
	gidx := make([][]int, len(groups))
	for g, group := range groups {
		for _, ac := range group {
			i, ok := index[ac]
			if !ok {
				i = len(acceptors)
				index[ac] = i
				acceptors = append(acceptors, ac)
			}
			gidx[g] = append(gidx[g], i)
		}
	}
	return &Proposer{nodeID: nodeID, acceptors: acceptors, groups: gidx, maxRounds: 12}
}

// nextBallot mints a fresh ballot strictly greater than any this proposer has
// produced and at least atLeast (used to jump past a reported conflict).
//
// Ballot-space discipline: a counter at or above 1<<epochShift lies in an
// ownership epoch space (minted by an OwnedProposer, or by a previous jump
// here). A full proposer must never mint *inside* such a space — a +1 bump
// past an owner's conflict would produce the exact counter of the owner's
// next sequence-bumped write, and the NodeID tiebreak could then hand the
// owner's accept (derived from its now-stale cache) the higher ballot,
// silently overwriting the value this proposer committed in between: a lost
// update. Instead the counter is rounded up to the next epoch boundary, which
// strictly dominates every ballot the owner can mint; the owner NACKs,
// reports the observed epoch (EpochBehindError), and re-acquires its fence
// above it. Full proposers therefore only ever mint plain counters
// (< 1<<epochShift) or exact epoch boundaries, on which classic prepare/accept
// ordering — including the tiebreak — is safe.
//
// Cost: contention against an owned key burns one ownership epoch per
// preempted round, bounded by maxRounds per Propose, out of the 2^24 epoch
// budget per key documented at epochShift. At the top of that budget the jump
// would overflow the counter, so it saturates to +1 bumping — the same
// documented-not-guarded regime as epoch exhaustion itself.
func (p *Proposer) nextBallot(atLeast Ballot) Ballot {
	p.mu.Lock()
	defer p.mu.Unlock()
	next := max(p.counter, atLeast.Counter) + 1
	if e := next >> epochShift; e > 0 && e < maxEpoch && next != e<<epochShift {
		next = (e + 1) << epochShift
	}
	p.counter = next
	return Ballot{Counter: next, NodeID: p.nodeID}
}

// quorumInAllGroups reports whether the boolean per-acceptor outcome has a
// majority in every configuration group.
func (p *Proposer) quorumInAllGroups(ok []bool) bool {
	for _, g := range p.groups {
		cnt := 0
		for _, idx := range g {
			if ok[idx] {
				cnt++
			}
		}
		if cnt < len(g)/2+1 {
			return false
		}
	}
	return true
}

// Propose runs CASPaxos for key: gather a prepare quorum (in every group), apply
// change to the highest-accepted value, then gather an accept quorum (in every
// group). A failed CAS still writes the current value back (completing any
// in-flight round) before returning ErrConflict, so the read it observed is
// linearizable.
func (p *Proposer) Propose(ctx context.Context, key []byte, change ChangeFunc) ([]byte, error) {
	var floor Ballot
	for round := 0; round < p.maxRounds; round++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		b := p.nextBallot(floor)

		current, conflict, ok, err := p.prepare(ctx, key, b)
		if err != nil {
			return nil, err
		}
		if !ok {
			floor = floor.Max(conflict)
			continue
		}

		next, cerr := change(current)
		if cerr != nil && !errors.Is(cerr, ErrConflict) {
			return nil, cerr
		}
		writeVal := next
		if cerr != nil {
			writeVal = current
		}

		conflict, ok, err = p.accept(ctx, key, b, writeVal)
		if err != nil {
			return nil, err
		}
		if !ok {
			floor = floor.Max(conflict)
			continue
		}
		if cerr != nil {
			return nil, cerr
		}
		return next, nil
	}
	return nil, ErrPreempted
}

// prepare runs phase 1 and, on success (quorum in every group), returns the
// value carried by the highest accepted ballot seen.
func (p *Proposer) prepare(ctx context.Context, key []byte, b Ballot) (current []byte, conflict Ballot, ok bool, err error) {
	promised := make([]bool, len(p.acceptors))
	var best Ballot
	for i, ac := range p.acceptors {
		// BUGGIFY: drop one acceptor's vote, modelling a lost reply / gray
		// failure. The round must still complete from the remaining quorum.
		if buggify.Maybe("proposer_drop_vote", 0.01) {
			continue
		}
		reply, perr := ac.Prepare(ctx, key, b)
		if perr != nil {
			continue // unreachable acceptor: a non-vote
		}
		if !reply.Promised {
			conflict = conflict.Max(reply.Conflict)
			continue
		}
		promised[i] = true
		if best.Less(reply.Accepted) {
			best = reply.Accepted
			current = reply.Value
		}
	}
	if p.quorumInAllGroups(promised) {
		return current, Ballot{}, true, nil
	}
	return nil, conflict, false, nil
}

// accept runs phase 2, requiring a quorum in every group.
func (p *Proposer) accept(ctx context.Context, key []byte, b Ballot, val []byte) (conflict Ballot, ok bool, err error) {
	accepted := make([]bool, len(p.acceptors))
	for i, ac := range p.acceptors {
		reply, aerr := ac.Accept(ctx, key, b, val)
		if aerr != nil {
			continue
		}
		if !reply.Accepted {
			conflict = conflict.Max(reply.Conflict)
			continue
		}
		accepted[i] = true
	}
	if p.quorumInAllGroups(accepted) {
		return Ballot{}, true, nil
	}
	return conflict, false, nil
}
