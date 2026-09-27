package caspaxos

import (
	"bytes"
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
	backoff   func(ctx context.Context, attempt int) error // nil = retry immediately

	mu      sync.Mutex
	counter uint64 // highest ballot counter this proposer has used (guarded by mu)
}

// Option configures a Proposer.
type Option func(*Proposer)

// WithBackoff installs a delay called between preempted rounds (never before
// the first, so an uncontended round pays nothing). Ballot bumping alone can
// livelock two symmetric proposers; a randomized delay breaks the symmetry
// (see internal/backoff). The function is injected so the core stays free of
// clocks and randomness — the simulator supplies a deterministic one. A
// non-nil error aborts the Propose with that error.
func WithBackoff(f func(ctx context.Context, attempt int) error) Option {
	return func(p *Proposer) { p.backoff = f }
}

// NewProposer returns a single-group Proposer over acceptors.
func NewProposer(nodeID uint64, acceptors []AcceptorClient, opts ...Option) *Proposer {
	idx := make([]int, len(acceptors))
	for i := range acceptors {
		idx[i] = i
	}
	p := &Proposer{nodeID: nodeID, acceptors: acceptors, groups: [][]int{idx}, maxRounds: 12}
	for _, o := range opts {
		o(p)
	}
	return p
}

// NewJointProposer returns a Proposer requiring a quorum in every group. The
// groups are given as acceptor lists; shared acceptors are deduplicated so a
// node present in multiple groups is contacted once and counts in each.
func NewJointProposer(nodeID uint64, groups [][]AcceptorClient, opts ...Option) *Proposer {
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
	p := &Proposer{nodeID: nodeID, acceptors: acceptors, groups: gidx, maxRounds: 12}
	for _, o := range opts {
		o(p)
	}
	return p
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

// quorumStillPossible reports whether every group could still reach a
// majority, counting undecided acceptors as potential yes-votes. When it
// turns false the phase has already failed — waiting for stragglers can only
// add conflicts, never votes.
func (p *Proposer) quorumStillPossible(ok, undecided []bool) bool {
	for _, g := range p.groups {
		cnt := 0
		for _, idx := range g {
			if ok[idx] || undecided[idx] {
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
//
// Retry contract: Propose starts a new round with a fresh ballot when the
// prepare phase fails, or when every acceptor rejected the accept. change
// then runs again on the fresh current value. That is safe, because no
// acceptor holds a value from the failed round. An accept phase that fails
// while some acceptor may hold the value is different. That acceptor
// accepted, or its reply was lost, or it was still in flight. A later round
// can adopt that value and choose it. If the value differs from current,
// Propose returns ErrUnknownOutcome and does not start a new round. The
// caller must re-read and retry with a compare-and-set, or with a change
// that detects its own earlier write (mvcc does this with OpIDs). A round
// that writes back the current value (a read, or a change that returned
// ErrConflict) is still retried, because that value changes nothing.
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
			if err := p.pause(ctx, round); err != nil {
				return nil, err
			}
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

		conflict, ok, mayHold, err := p.accept(ctx, key, b, writeVal)
		if err != nil {
			return nil, err
		}
		//= docs/spec/fleet.md#3-storage-model
		//# A retried write MUST be a compare-and-set, never a blind reapplication of a change.
		if !ok && mayHold && !bytes.Equal(writeVal, current) {
			// Some acceptor may hold the new value, and a later round
			// can choose it. Another round here applies change again.
			// Back off first, so a caller that retries at once does not
			// collide with the same competitor.
			if err := p.pause(ctx, round); err != nil {
				return nil, err
			}
			return nil, ErrUnknownOutcome
		}
		if !ok {
			floor = floor.Max(conflict)
			if err := p.pause(ctx, round); err != nil {
				return nil, err
			}
			continue
		}
		if cerr != nil {
			return nil, cerr
		}
		return next, nil
	}
	return nil, ErrPreempted
}

// pause runs the configured backoff after a preempted round (attempt is the
// 0-based round that just failed).
func (p *Proposer) pause(ctx context.Context, attempt int) error {
	if p.backoff == nil {
		return nil
	}
	return p.backoff(ctx, attempt)
}

// errVoteDropped marks a reply the proposer_drop_vote buggify site discarded.
var errVoteDropped = errors.New("caspaxos: vote dropped (buggify)")

// prepare runs phase 1 concurrently against every acceptor and, on success
// (quorum in every group), returns the value carried by the highest accepted
// ballot within the responding quorum. It returns as soon as the quorum is
// gathered — or as soon as one is provably unreachable — and cancels the
// stragglers; a slow or dead replica costs nothing while a quorum is healthy.
//
// Safety of the early return: the carried value is the max accepted over
// exactly the promised set at return time, which is a valid prepare quorum —
// any previously chosen value has a quorum of acceptors carrying it, and two
// quorums always intersect, so the chosen value is represented.
func (p *Proposer) prepare(ctx context.Context, key []byte, b Ballot) (current []byte, conflict Ballot, ok bool, err error) {
	n := len(p.acceptors)

	// BUGGIFY: drop acceptors' votes, modelling lost replies / gray failure.
	// Decisions are pre-drawn HERE, serially, on the operation's goroutine —
	// never inside the fan-out workers (see the concurrency rule in
	// internal/buggify): the round must still complete from the remaining
	// quorum.
	drop := make([]bool, n)
	for i := range drop {
		drop[i] = buggify.Maybe("proposer_drop_vote", 0.01)
	}

	pctx, cancel := context.WithCancel(ctx)
	defer cancel() // hurry the stragglers once the phase is decided

	replies := fanout(pctx, n, func(ctx context.Context, i int) (PrepareReply, error) {
		if drop[i] {
			return PrepareReply{}, errVoteDropped
		}
		return p.acceptors[i].Prepare(ctx, key, b)
	})

	promised := make([]bool, n)
	undecided := make([]bool, n)
	for i := range undecided {
		undecided[i] = true
	}
	var best Ballot
	for range n {
		r := <-replies
		undecided[r.i] = false
		switch {
		case errors.Is(r.err, ErrRangeChanged):
			// The replica set moved under us; no quorum here can be trusted.
			return nil, Ballot{}, false, ErrRangeChanged
		case r.err != nil:
			// unreachable acceptor (or a dropped vote): a non-vote
		case !r.v.Promised:
			conflict = conflict.Max(r.v.Conflict)
		default:
			promised[r.i] = true
			if best.Less(r.v.Accepted) {
				best = r.v.Accepted
				current = r.v.Value
			}
		}
		if p.quorumInAllGroups(promised) {
			return current, Ballot{}, true, nil
		}
		if !p.quorumStillPossible(promised, undecided) {
			return nil, conflict, false, ctx.Err()
		}
	}
	return nil, conflict, false, ctx.Err()
}

// accept runs phase 2 concurrently, requiring a quorum in every group, with
// the same early-quorum return and straggler cancellation as prepare. A
// cancelled accept may still land on a straggler later — Paxos tolerates
// this, and the duplicate_delivery sim fault exercises the idempotency it
// relies on.
//
// mayHold reports whether any acceptor may now hold val: one that accepted,
// one whose reply failed (the accept may have landed and the reply got
// lost), or a straggler still in flight. Only an explicit rejection proves
// that an acceptor does not hold val. Propose uses mayHold to detect an
// unknown outcome.
func (p *Proposer) accept(ctx context.Context, key []byte, b Ballot, val []byte) (conflict Ballot, ok, mayHold bool, err error) {
	n := len(p.acceptors)
	pctx, cancel := context.WithCancel(ctx)
	defer cancel()

	replies := fanout(pctx, n, func(ctx context.Context, i int) (AcceptReply, error) {
		return p.acceptors[i].Accept(ctx, key, b, val)
	})

	accepted := make([]bool, n)
	undecided := make([]bool, n)
	for i := range undecided {
		undecided[i] = true
	}
	rejected := 0
	for range n {
		r := <-replies
		undecided[r.i] = false
		switch {
		case errors.Is(r.err, ErrRangeChanged):
			return Ballot{}, false, true, ErrRangeChanged
		case r.err != nil:
			// unreachable acceptor: a non-vote, but the accept may have landed
		case !r.v.Accepted:
			conflict = conflict.Max(r.v.Conflict)
			rejected++
		default:
			accepted[r.i] = true
		}
		if p.quorumInAllGroups(accepted) {
			return Ballot{}, true, true, nil
		}
		if !p.quorumStillPossible(accepted, undecided) {
			return conflict, false, rejected < n, ctx.Err()
		}
	}
	return conflict, false, rejected < n, ctx.Err()
}
