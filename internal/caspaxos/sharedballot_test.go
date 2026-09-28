package caspaxos_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/store"
)

// Issue #204. Two proposers that share a node id keep separate ballot
// counters, so they can mint the same ballot. This happens in production:
// roster.New builds a new proposer for every roster operation, and the
// data proposer is rebuilt on every configuration change. The tests below
// run two such proposers against three acceptors under a seeded scheduler
// and check that consensus stays safe.
//
// The reason it stays safe: an acceptor promises a ballot only if it is
// strictly higher than its current promise, so it promises each ballot at
// most once. Two prepare quorums at one ballot would need a shared
// acceptor to promise that ballot twice. So at most one proposer finishes
// phase 1 at a ballot, and only that proposer sends accepts at it.

const sharedNodeID = 7

// pendingCall is one acceptor call that the scheduler holds.
type pendingCall struct {
	prop    int // index of the proposer that made the call
	acc     int // index of the acceptor
	accept  bool
	b       caspaxos.Ballot
	val     []byte
	release chan struct{}
}

// phaseID names one phase of one proposer: a prepare or an accept at one
// ballot. A proposer never runs two phases with the same id.
type phaseID struct {
	prop   int
	accept bool
	b      caspaxos.Ballot
}

// phaseState counts what the scheduler has seen of one phase.
type phaseState struct {
	arrived int  // calls that reached the scheduler
	yes     int  // promises or accepts delivered
	no      bool // a rejection was delivered
}

// ended reports whether the proposer has left the phase. It mirrors the
// early return of Proposer.prepare and Proposer.accept for one group of
// three acceptors with no transport errors: the phase ends at the first
// rejection or at a quorum of two.
func (s phaseState) ended() bool { return s.no || s.yes >= 2 }

// scheduler holds every acceptor call and releases one at a time in a
// seeded order. After each release it waits until the proposers have
// reacted: stragglers of an ended phase are cancelled, and the proposer
// has either returned or sent the whole next phase. The order in which
// acceptors see messages is then a pure function of the seed.
type scheduler struct {
	accs []caspaxos.AcceptorClient

	mu      sync.Mutex
	cond    *sync.Cond
	parked  []*pendingCall
	phases  map[phaseID]*phaseState
	latest  []phaseID // latest phase of each proposer
	done    []bool
	ballots []map[caspaxos.Ballot]bool // ballots each proposer used
	votes   map[caspaxos.Ballot]map[string]bool
}

func newScheduler(accs []caspaxos.AcceptorClient, props int) *scheduler {
	s := &scheduler{
		accs:    accs,
		phases:  map[phaseID]*phaseState{},
		latest:  make([]phaseID, props),
		done:    make([]bool, props),
		ballots: make([]map[caspaxos.Ballot]bool, props),
		votes:   map[caspaxos.Ballot]map[string]bool{},
	}
	for i := range s.ballots {
		s.ballots[i] = map[caspaxos.Ballot]bool{}
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// clients returns the acceptor clients for proposer prop.
func (s *scheduler) clients(prop int) []caspaxos.AcceptorClient {
	out := make([]caspaxos.AcceptorClient, len(s.accs))
	for i := range out {
		out[i] = gatedClient{s: s, prop: prop, acc: i}
	}
	return out
}

// park records c and waits for its release or for ctx to end.
func (s *scheduler) park(ctx context.Context, c *pendingCall) bool {
	s.mu.Lock()
	id := phaseID{prop: c.prop, accept: c.accept, b: c.b}
	ps := s.phases[id]
	if ps == nil {
		ps = &phaseState{}
		s.phases[id] = ps
		s.latest[c.prop] = id
	}
	ps.arrived++
	s.ballots[c.prop][c.b] = true
	s.parked = append(s.parked, c)
	s.cond.Broadcast()
	s.mu.Unlock()

	select {
	case <-c.release:
		return true
	case <-ctx.Done():
		s.mu.Lock()
		defer s.mu.Unlock()
		select {
		case <-c.release:
			// Released at the same moment: the scheduler already
			// removed the call and waits for its result.
			return true
		default:
		}
		s.remove(c)
		s.cond.Broadcast()
		return false
	}
}

// remove drops c from parked. The caller holds mu.
func (s *scheduler) remove(c *pendingCall) {
	for i, p := range s.parked {
		if p == c {
			s.parked = append(s.parked[:i], s.parked[i+1:]...)
			return
		}
	}
}

// deliver records the acceptor's answer to c.
func (s *scheduler) deliver(c *pendingCall, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ps := s.phases[phaseID{prop: c.prop, accept: c.accept, b: c.b}]
	if ok {
		ps.yes++
	} else {
		ps.no = true
	}
	if c.accept && ok {
		if s.votes[c.b] == nil {
			s.votes[c.b] = map[string]bool{}
		}
		s.votes[c.b][string(c.val)] = true
	}
	s.cond.Broadcast()
}

type gatedClient struct {
	s    *scheduler
	prop int
	acc  int
}

func (g gatedClient) Prepare(ctx context.Context, key []byte, b caspaxos.Ballot) (caspaxos.PrepareReply, error) {
	c := &pendingCall{prop: g.prop, acc: g.acc, b: b, release: make(chan struct{})}
	if !g.s.park(ctx, c) {
		return caspaxos.PrepareReply{}, ctx.Err()
	}
	r, err := g.s.accs[g.acc].Prepare(context.Background(), key, b)
	g.s.deliver(c, err == nil && r.Promised)
	return r, err
}

func (g gatedClient) Accept(ctx context.Context, key []byte, b caspaxos.Ballot, val []byte) (caspaxos.AcceptReply, error) {
	c := &pendingCall{prop: g.prop, acc: g.acc, accept: true, b: b, val: val, release: make(chan struct{})}
	if !g.s.park(ctx, c) {
		return caspaxos.AcceptReply{}, ctx.Err()
	}
	r, err := g.s.accs[g.acc].Accept(context.Background(), key, b, val)
	g.s.deliver(c, err == nil && r.Accepted)
	return r, err
}

// settled reports whether every proposer has either returned or has its
// latest phase fully parked and not yet ended, with no stragglers from an
// ended phase still parked. The caller holds mu.
func (s *scheduler) settled() bool {
	for _, c := range s.parked {
		if s.phases[phaseID{prop: c.prop, accept: c.accept, b: c.b}].ended() {
			return false // a straggler that ctx cancellation will remove
		}
	}
	for p := range s.latest {
		if s.done[p] {
			continue
		}
		ps := s.phases[s.latest[p]]
		if ps == nil || ps.ended() || ps.arrived < len(s.accs) {
			return false
		}
	}
	return true
}

// waitSettled waits for settled. It fails the test if the proposers stop
// making progress, which means the harness mirror of the phase rules is
// wrong.
func (s *scheduler) waitSettled(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !s.settled() {
		if time.Now().After(deadline) {
			t.Fatalf("scheduler did not settle: parked=%d done=%v", len(s.parked), s.done)
		}
		s.cond.Wait()
	}
}

// casFrom is a compare-and-set: it writes val only over base.
func casFrom(base, val []byte) caspaxos.ChangeFunc {
	return func(cur []byte) ([]byte, error) {
		if !bytes.Equal(cur, base) {
			return nil, caspaxos.ErrConflict
		}
		return val, nil
	}
}

type sharedResult struct {
	val []byte
	err error
}

// sharedOutcome is what one seeded schedule produced.
type sharedOutcome struct {
	results  []sharedResult
	final    []byte
	shared   bool // the two proposers used at least one common ballot
	twoPerB  bool // acceptors accepted two values at one ballot
	sequence []string
}

// runSharedSchedule runs one schedule. Two proposers with one node id do a
// compare-and-set from base on one key. If base is not nil, a proposer
// with a higher node id writes it first, so both shared proposers see a
// conflict and jump past it with the conflict floor.
func runSharedSchedule(t *testing.T, seed int64, accs []caspaxos.AcceptorClient, base []byte) sharedOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	key := []byte("k")
	if base != nil {
		seedProp := caspaxos.NewProposer(sharedNodeID+2, accs)
		if _, err := seedProp.Propose(ctx, key, caspaxos.Write(base)); err != nil {
			t.Fatalf("seed write: %v", err)
		}
	}

	const props = 2
	s := newScheduler(accs, props)
	out := sharedOutcome{results: make([]sharedResult, props)}
	var wg sync.WaitGroup
	for p := range props {
		prop := caspaxos.NewProposer(sharedNodeID, s.clients(p))
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			v, err := prop.Propose(ctx, key, casFrom(base, fmt.Appendf(nil, "v%d", p+1)))
			s.mu.Lock()
			out.results[p] = sharedResult{val: v, err: err}
			s.done[p] = true
			s.cond.Broadcast()
			s.mu.Unlock()
		}(p)
	}

	rng := rand.New(rand.NewSource(seed))
	s.mu.Lock()
	for {
		s.waitSettled(t)
		if len(s.parked) == 0 {
			break
		}
		sort.Slice(s.parked, func(i, j int) bool {
			a, b := s.parked[i], s.parked[j]
			if a.prop != b.prop {
				return a.prop < b.prop
			}
			if a.accept != b.accept {
				return !a.accept
			}
			return a.acc < b.acc
		})
		c := s.parked[rng.Intn(len(s.parked))]
		s.remove(c)
		op := "prepare"
		if c.accept {
			op = "accept"
		}
		out.sequence = append(out.sequence, fmt.Sprintf("p%d %s %v a%d", c.prop+1, op, c.b, c.acc+1))
		close(c.release)
		// Wait for the acceptor to answer, so acceptors see messages in
		// the order the seed chose.
		id := phaseID{prop: c.prop, accept: c.accept, b: c.b}
		before := *s.phases[id]
		for *s.phases[id] == before {
			s.cond.Wait()
		}
	}
	s.mu.Unlock()
	wg.Wait()

	for b := range s.ballots[0] {
		if s.ballots[1][b] {
			out.shared = true
		}
	}
	for _, vals := range s.votes {
		if len(vals) > 1 {
			out.twoPerB = true
		}
	}
	reader := caspaxos.NewProposer(99, accs)
	final, err := reader.Propose(ctx, key, caspaxos.Identity)
	if err != nil {
		t.Fatalf("seed %d: final read: %v", seed, err)
	}
	out.final = final
	return out
}

// violation returns a description of the first safety violation in o, or
// "" if the schedule was safe.
func (o sharedOutcome) violation() string {
	if o.twoPerB {
		return "acceptors accepted two different values at one ballot"
	}
	var won [][]byte
	for _, r := range o.results {
		if r.err == nil {
			won = append(won, r.val)
		}
	}
	if len(won) > 1 {
		return fmt.Sprintf("two compare-and-sets from one base both committed: %q and %q", won[0], won[1])
	}
	if len(won) == 1 && !bytes.Equal(o.final, won[0]) {
		return fmt.Sprintf("committed %q but a later read returned %q", won[0], o.final)
	}
	return ""
}

// TestSharedNodeIDBallotsStaySafe drives two proposers that share a node
// id through seeded interleavings on one key. They mint the same ballot,
// but the acceptors never accept two values at one ballot, at most one
// compare-and-set commits, and a later read returns the committed value.
func TestSharedNodeIDBallotsStaySafe(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# Two proposers MAY choose the same ballot.
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# An acceptor MUST promise a ballot only if the ballot is higher than every ballot that the acceptor has promised.
	cases := []struct {
		name string
		base []byte
	}{
		// Fresh proposers on an empty key: both mint ballot 1.7.
		{"fresh counters", nil},
		// A higher node id wrote first: both proposers are rejected at
		// 1.7, take the conflict 1.9 as the floor, and both mint 2.7.
		{"after a conflict", []byte("base")},
	}
	const seeds = 200
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shared := 0
			for seed := range int64(seeds) {
				o := runSharedSchedule(t, seed, newCluster(3), tc.base)
				if v := o.violation(); v != "" {
					t.Fatalf("seed %d: %s\nschedule:\n%v", seed, v, o.sequence)
				}
				if o.shared {
					shared++
				}
			}
			// The premise of #204: the proposers really did share ballots.
			if shared == 0 {
				t.Fatalf("no schedule had the two proposers use a common ballot")
			}
			t.Logf("%d of %d schedules used a shared ballot; none was unsafe", shared, seeds)
		})
	}
}

// TestSharedNodeIDBallotsNeedStrictPromise is the negative control for the
// harness. An acceptor that promises a ballot equal to its promise lets
// both shared-ballot proposers finish phase 1. The scheduler then finds
// schedules where the acceptors accept two values at one ballot, and
// schedules where both compare-and-sets from one base commit. This shows
// the harness can see the bug, and that the strict promise prevents it.
func TestSharedNodeIDBallotsNeedStrictPromise(t *testing.T) {
	const seeds = 200
	twoPerBallot, twoCommits := 0, 0
	for seed := range int64(seeds) {
		accs := make([]caspaxos.AcceptorClient, 3)
		for i := range accs {
			accs[i] = &loosePromiseAcceptor{regs: map[string]caspaxos.Register{}}
		}
		o := runSharedSchedule(t, seed, accs, nil)
		if o.twoPerB {
			twoPerBallot++
		}
		if o.results[0].err == nil && o.results[1].err == nil {
			twoCommits++
		}
	}
	if twoPerBallot == 0 || twoCommits == 0 {
		t.Fatalf("with a non-strict promise, %d of %d schedules accepted two values at one ballot and %d committed both writes; want both above 0, or the harness has lost its teeth",
			twoPerBallot, seeds, twoCommits)
	}
	t.Logf("non-strict promise: %d of %d schedules accepted two values at one ballot; %d committed both compare-and-sets",
		twoPerBallot, seeds, twoCommits)
}

// loosePromiseAcceptor is a broken acceptor: it promises a prepare whose
// ballot equals its current promise.
type loosePromiseAcceptor struct {
	mu   sync.Mutex
	regs map[string]caspaxos.Register
}

func (l *loosePromiseAcceptor) Prepare(_ context.Context, key []byte, b caspaxos.Ballot) (caspaxos.PrepareReply, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	reg := l.regs[string(key)]
	if b.AtLeast(reg.Promise) { // the bug: >= instead of >
		reg.Promise = b
		l.regs[string(key)] = reg
		return caspaxos.PrepareReply{Promised: true, Accepted: reg.Accepted, Value: reg.Value}, nil
	}
	return caspaxos.PrepareReply{Conflict: reg.Promise, Accepted: reg.Accepted, Value: reg.Value}, nil
}

func (l *loosePromiseAcceptor) Accept(_ context.Context, key []byte, b caspaxos.Ballot, val []byte) (caspaxos.AcceptReply, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	reg := l.regs[string(key)]
	if b.Less(reg.Promise) {
		return caspaxos.AcceptReply{Conflict: reg.Promise}, nil
	}
	reg.Promise, reg.Accepted, reg.Value = b, b, val
	l.regs[string(key)] = reg
	return caspaxos.AcceptReply{Accepted: true}, nil
}

// TestAcceptorPromisesBallotOnce pins the mechanism at the acceptor: a
// second prepare at the ballot it already promised is rejected, and the
// rejection names that ballot.
func TestAcceptorPromisesBallotOnce(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# An acceptor MUST promise a ballot only if the ballot is higher than every ballot that the acceptor has promised.
	ctx := context.Background()
	a := caspaxos.NewAcceptor(store.NewMem())
	b := caspaxos.Ballot{Counter: 1, NodeID: sharedNodeID}
	first, err := a.Prepare(ctx, []byte("k"), b)
	if err != nil || !first.Promised {
		t.Fatalf("first prepare = %+v, %v; want promised", first, err)
	}
	second, err := a.Prepare(ctx, []byte("k"), b)
	if err != nil {
		t.Fatal(err)
	}
	if second.Promised || second.Conflict != b {
		t.Fatalf("second prepare at %v = %+v; want rejected with conflict %v", b, second, b)
	}
}

// TestSharedNodeIDOwnersCannotBothOwn covers the one path that sends an
// accept without its own phase 1: the owned fast path. Two owned
// proposers with one node id and one fence mint the same phase-1 ballot.
// Only the first takes ownership. The second is told the register has
// reached its epoch, and cannot write.
func TestSharedNodeIDOwnersCannotBothOwn(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# An acceptor MUST promise a ballot only if the ballot is higher than every ballot that the acceptor has promised.
	ctx := context.Background()
	key := []byte("k")
	accs := newCluster(3)
	const epoch = 5
	first := caspaxos.NewOwnedProposer(sharedNodeID, accs)
	second := caspaxos.NewOwnedProposer(sharedNodeID, accs)

	if err := first.TakeOwnership(ctx, key, epoch); err != nil {
		t.Fatalf("first take: %v", err)
	}
	err := second.TakeOwnership(ctx, key, epoch)
	var behind *caspaxos.EpochBehindError
	if !errors.As(err, &behind) || behind.Observed != epoch {
		t.Fatalf("second take at the same ballot = %v; want EpochBehindError at epoch %d", err, epoch)
	}
	if _, err := second.Write(ctx, key, caspaxos.Write([]byte("second"))); !errors.Is(err, caspaxos.ErrNotOwner) {
		t.Fatalf("second write = %v; want ErrNotOwner", err)
	}
	if _, err := first.Write(ctx, key, caspaxos.Write([]byte("first"))); err != nil {
		t.Fatalf("first write: %v", err)
	}
	got, err := caspaxos.NewProposer(99, accs).Propose(ctx, key, caspaxos.Identity)
	if err != nil || string(got) != "first" {
		t.Fatalf("read = %q, %v; want first", got, err)
	}
}
