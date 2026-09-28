package caspaxos_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/store"
)

// blocking is an acceptor that hangs until its context is cancelled — a dead
// peer behind a transport that hasn't timed out yet.
type blocking struct {
	unblocked chan struct{} // closed when a call observed cancellation
}

func newBlocking() *blocking { return &blocking{unblocked: make(chan struct{}, 16)} }

func (b *blocking) wait(ctx context.Context) error {
	<-ctx.Done()
	select {
	case b.unblocked <- struct{}{}:
	default:
	}
	return ctx.Err()
}

func (b *blocking) Prepare(ctx context.Context, key []byte, bal caspaxos.Ballot) (caspaxos.PrepareReply, error) {
	return caspaxos.PrepareReply{}, b.wait(ctx)
}

func (b *blocking) Accept(ctx context.Context, key []byte, bal caspaxos.Ballot, val []byte) (caspaxos.AcceptReply, error) {
	return caspaxos.AcceptReply{}, b.wait(ctx)
}

// slow delegates after a ctx-aware delay — a healthy but laggy replica.
type slow struct {
	inner caspaxos.AcceptorClient
	d     time.Duration
}

func (s slow) delay(ctx context.Context) error {
	select {
	case <-time.After(s.d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s slow) Prepare(ctx context.Context, key []byte, b caspaxos.Ballot) (caspaxos.PrepareReply, error) {
	if err := s.delay(ctx); err != nil {
		return caspaxos.PrepareReply{}, err
	}
	return s.inner.Prepare(ctx, key, b)
}

func (s slow) Accept(ctx context.Context, key []byte, b caspaxos.Ballot, val []byte) (caspaxos.AcceptReply, error) {
	if err := s.delay(ctx); err != nil {
		return caspaxos.AcceptReply{}, err
	}
	return s.inner.Accept(ctx, key, b, val)
}

// A dead peer (blocked, not yet timed out) must cost a phase nothing while a
// quorum is healthy, and the phase's cancellation must release it — the W2
// early-quorum property. Pre-W2, each phase serially waited the full
// transport timeout on the dead peer.
func TestDeadPeerOffCriticalPath(t *testing.T) {
	ctx := context.Background()
	dead := newBlocking()
	acc := []caspaxos.AcceptorClient{
		caspaxos.NewAcceptor(store.NewMem()),
		caspaxos.NewAcceptor(store.NewMem()),
		dead,
	}
	p := caspaxos.NewProposer(1, acc)

	start := time.Now()
	if _, err := p.Propose(ctx, []byte("k"), caspaxos.Write([]byte("v"))); err != nil {
		t.Fatalf("propose: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("propose took %v with a healthy quorum; dead peer is on the critical path", elapsed)
	}

	// Both phases' cancellations must have released the blocked calls.
	for i := range 2 {
		select {
		case <-dead.unblocked:
		case <-time.After(2 * time.Second):
			t.Fatalf("blocked call %d never released (goroutine leak)", i)
		}
	}
}

// A slow replica must not set the pace when the quorum is faster (§3.6
// done-when: p99 with a delayed 1-of-3 replica ≈ no-delay baseline).
func TestSlowReplicaOffCriticalPath(t *testing.T) {
	ctx := context.Background()
	acc := []caspaxos.AcceptorClient{
		caspaxos.NewAcceptor(store.NewMem()),
		caspaxos.NewAcceptor(store.NewMem()),
		slow{inner: caspaxos.NewAcceptor(store.NewMem()), d: 300 * time.Millisecond},
	}
	p := caspaxos.NewProposer(1, acc)

	start := time.Now()
	if _, err := p.Propose(ctx, []byte("k"), caspaxos.Write([]byte("v"))); err != nil {
		t.Fatalf("propose: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("propose took %v; the 300ms replica set the pace (2 phases would be 600ms)", elapsed)
	}

	// The owned fast path gets the same treatment.
	owner := caspaxos.NewOwnedProposer(2, acc)
	if err := owner.TakeOwnership(ctx, []byte("k2"), 1); err != nil {
		t.Fatalf("take ownership: %v", err)
	}
	start = time.Now()
	if _, err := owner.Write(ctx, []byte("k2"), caspaxos.Write([]byte("w"))); err != nil {
		t.Fatalf("owned write: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("owned write took %v; the slow replica set the pace", elapsed)
	}
}

// The early-quorum return must still carry any chosen value: a committed
// value's quorum intersects every prepare quorum, so even with one holder
// slow, the fast quorum includes a holder.
func TestEarlyQuorumCarriesChosenValue(t *testing.T) {
	ctx := context.Background()
	stores := make([]caspaxos.Storage, 5)
	base := make([]caspaxos.AcceptorClient, 5)
	for i := range base {
		stores[i] = store.NewMem()
		base[i] = caspaxos.NewAcceptor(stores[i])
	}

	// Commit v1 on acceptors {0,1,2} only.
	writer := caspaxos.NewProposer(1, base[:3])
	if _, err := writer.Propose(ctx, []byte("k"), caspaxos.Write([]byte("v1"))); err != nil {
		t.Fatalf("seed write: %v", err)
	}

	// Holder 0 turns slow; a reader over all five must still observe v1 from
	// its fast quorum (any 3 of the 4 fast acceptors includes a holder).
	all := []caspaxos.AcceptorClient{
		slow{inner: base[0], d: 300 * time.Millisecond},
		base[1], base[2], base[3], base[4],
	}
	reader := caspaxos.NewProposer(2, all)
	start := time.Now()
	got, err := reader.Propose(ctx, []byte("k"), caspaxos.Identity)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "v1" {
		t.Fatalf("read = %q, want v1 (chosen value dropped by early quorum)", got)
	}
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("read took %v; slow holder set the pace", elapsed)
	}
}

// erroring fails immediately — a peer that is down with a fast transport error.
type erroring struct{}

var errPeerDown = errors.New("peer down")

func (erroring) Prepare(context.Context, []byte, caspaxos.Ballot) (caspaxos.PrepareReply, error) {
	return caspaxos.PrepareReply{}, errPeerDown
}

func (erroring) Accept(context.Context, []byte, caspaxos.Ballot, []byte) (caspaxos.AcceptReply, error) {
	return caspaxos.AcceptReply{}, errPeerDown
}

// When a group can provably no longer reach quorum, the phase fails without
// waiting for the remaining replies — including in a joint configuration
// where only the new group is down.
func TestJointImpossibilityFailsFast(t *testing.T) {
	ctx := context.Background()
	old := []caspaxos.AcceptorClient{
		caspaxos.NewAcceptor(store.NewMem()),
		caspaxos.NewAcceptor(store.NewMem()),
		caspaxos.NewAcceptor(store.NewMem()),
	}
	newGroup := []caspaxos.AcceptorClient{erroring{}, erroring{}, erroring{}}

	p := caspaxos.NewJointProposer(1, [][]caspaxos.AcceptorClient{old, newGroup})
	start := time.Now()
	if _, err := p.Propose(ctx, []byte("k"), caspaxos.Write([]byte("v"))); err == nil {
		t.Fatal("expected failure with the new group down")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("joint impossibility took %v; should fail fast", elapsed)
	}
}

// One acceptor promises, one rejects because it promised a higher ballot,
// and one hangs. The prepare must not wait for the hung acceptor. The
// rejection shows that the round is preempted, so the proposer retries
// above it. Before #143, the prepare waited, because the hung acceptor could
// still complete a quorum. A core change then stalled while a majority
// answered.
func TestRejectionEndsPrepareWithHungPeer(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# A core change MUST finish while a majority of the old core and a majority of the new core answer.
	for _, tc := range []struct {
		name  string
		joint bool
	}{{"single group", false}, {"joint", true}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			key := []byte("k")
			fresh := caspaxos.NewAcceptor(store.NewMem())
			ahead := caspaxos.NewAcceptor(store.NewMem())
			if r, err := ahead.Prepare(ctx, key, caspaxos.Ballot{Counter: 5, NodeID: 9}); err != nil || !r.Promised {
				t.Fatalf("seed a higher promise: %+v, %v", r, err)
			}
			hung := newBlocking()
			var p *caspaxos.Proposer
			if tc.joint {
				// The shape of the core change in #143: old {1,2,3} with
				// 2 hung, new {1,2,3,4,5} with 4 and 5 empty.
				old := []caspaxos.AcceptorClient{fresh, hung, ahead}
				nw := []caspaxos.AcceptorClient{fresh, hung, ahead,
					caspaxos.NewAcceptor(store.NewMem()), caspaxos.NewAcceptor(store.NewMem())}
				p = caspaxos.NewJointProposer(1, [][]caspaxos.AcceptorClient{old, nw})
			} else {
				p = caspaxos.NewProposer(1, []caspaxos.AcceptorClient{fresh, hung, ahead})
			}
			start := time.Now()
			if _, err := p.Propose(ctx, key, caspaxos.Write([]byte("v"))); err != nil {
				t.Fatalf("propose with one hung and one ahead acceptor: %v after %v", err, time.Since(start))
			}
		})
	}
}
