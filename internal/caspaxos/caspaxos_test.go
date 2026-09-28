package caspaxos_test

import (
	"context"
	"errors"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/store"
)

// newCluster returns n acceptors over independent in-memory stores.
func newCluster(n int) []caspaxos.AcceptorClient {
	cs := make([]caspaxos.AcceptorClient, n)
	for i := range cs {
		cs[i] = caspaxos.NewAcceptor(store.NewMem())
	}
	return cs
}

func TestProposeThenRead(t *testing.T) {
	ctx := context.Background()
	acc := newCluster(3)
	p := caspaxos.NewProposer(1, acc)

	if _, err := p.Propose(ctx, []byte("k"), caspaxos.Write([]byte("v1"))); err != nil {
		t.Fatalf("propose: %v", err)
	}
	got, err := p.Propose(ctx, []byte("k"), caspaxos.Identity)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "v1" {
		t.Fatalf("read = %q, want v1", got)
	}
}

// A second proposer must observe a value already accepted by the first.
func TestValueCarriesAcrossProposers(t *testing.T) {
	ctx := context.Background()
	acc := newCluster(5)
	a := caspaxos.NewProposer(1, acc)
	b := caspaxos.NewProposer(2, acc)

	if _, err := a.Propose(ctx, []byte("k"), caspaxos.Write([]byte("from-a"))); err != nil {
		t.Fatalf("a propose: %v", err)
	}
	got, err := b.Propose(ctx, []byte("k"), caspaxos.Identity)
	if err != nil {
		t.Fatalf("b read: %v", err)
	}
	if string(got) != "from-a" {
		t.Fatalf("b read = %q, want from-a", got)
	}
}

// errDown simulates an unreachable acceptor.
var errDown = errors.New("acceptor down")

type faulty struct {
	inner caspaxos.AcceptorClient
	down  *bool
}

func (f faulty) Prepare(ctx context.Context, key []byte, b caspaxos.Ballot) (caspaxos.PrepareReply, error) {
	if *f.down {
		return caspaxos.PrepareReply{}, errDown
	}
	return f.inner.Prepare(ctx, key, b)
}

func (f faulty) Accept(ctx context.Context, key []byte, b caspaxos.Ballot, val []byte) (caspaxos.AcceptReply, error) {
	if *f.down {
		return caspaxos.AcceptReply{}, errDown
	}
	return f.inner.Accept(ctx, key, b, val)
}

func TestNoProgressWithoutQuorum(t *testing.T) {
	ctx := context.Background()
	base := newCluster(3)
	downs := make([]bool, 3)
	acc := make([]caspaxos.AcceptorClient, 3)
	for i := range base {
		acc[i] = faulty{inner: base[i], down: &downs[i]}
	}
	p := caspaxos.NewProposer(1, acc)

	// Take down a majority (2 of 3): no quorum is reachable.
	downs[0], downs[1] = true, true
	if _, err := p.Propose(ctx, []byte("k"), caspaxos.Write([]byte("v"))); err == nil {
		t.Fatal("expected failure with majority down, got nil")
	}

	// Restore quorum and confirm progress resumes.
	downs[1] = false
	if _, err := p.Propose(ctx, []byte("k"), caspaxos.Write([]byte("v"))); err != nil {
		t.Fatalf("expected progress with quorum restored: %v", err)
	}
}

func TestChangeConflictAbortsCleanly(t *testing.T) {
	ctx := context.Background()
	acc := newCluster(3)
	p := caspaxos.NewProposer(1, acc)

	_, err := p.Propose(ctx, []byte("k"), func([]byte) ([]byte, error) {
		return nil, caspaxos.ErrConflict
	})
	if !errors.Is(err, caspaxos.ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
	// The register must be untouched: a subsequent read returns nil.
	got, err := p.Propose(ctx, []byte("k"), caspaxos.Identity)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != nil {
		t.Fatalf("read = %q, want nil after aborted change", got)
	}
}

// acceptDown passes prepares through but fails every accept while *down is
// set. It models an accept phase that reaches only some acceptors.
type acceptDown struct{ inner caspaxos.AcceptorClient }

func (a acceptDown) Prepare(ctx context.Context, key []byte, b caspaxos.Ballot) (caspaxos.PrepareReply, error) {
	return a.inner.Prepare(ctx, key, b)
}

func (acceptDown) Accept(context.Context, []byte, caspaxos.Ballot, []byte) (caspaxos.AcceptReply, error) {
	return caspaxos.AcceptReply{}, errDown
}

// minorityAcceptCluster returns three acceptors. Acceptor 0 accepts. The
// other two fail every accept.
func minorityAcceptCluster() []caspaxos.AcceptorClient {
	base := newCluster(3)
	return []caspaxos.AcceptorClient{base[0], acceptDown{base[1]}, acceptDown{base[2]}}
}

// An accept that reaches only a minority must not start a new round that
// applies the change again. Propose returns ErrUnknownOutcome, and the
// change runs exactly once.
func TestMinorityAcceptReturnsUnknownOutcome(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A retried write MUST be a compare-and-set, never a blind reapplication of a change.
	ctx := context.Background()
	p := caspaxos.NewProposer(1, minorityAcceptCluster())

	calls := 0
	_, err := p.Propose(ctx, []byte("k"), func(cur []byte) ([]byte, error) {
		calls++
		return append(append([]byte{}, cur...), 'x'), nil
	})
	if !errors.Is(err, caspaxos.ErrUnknownOutcome) {
		t.Fatalf("err = %v, want ErrUnknownOutcome", err)
	}
	if calls != 1 {
		t.Fatalf("change ran %d times, want 1", calls)
	}
}

// A round that writes back the current value changes nothing if chosen. So
// a minority accept of a read keeps retrying and never returns
// ErrUnknownOutcome.
func TestMinorityAcceptOfReadRetries(t *testing.T) {
	ctx := context.Background()
	p := caspaxos.NewProposer(1, minorityAcceptCluster())

	_, err := p.Propose(ctx, []byte("k"), caspaxos.Identity)
	if !errors.Is(err, caspaxos.ErrPreempted) {
		t.Fatalf("err = %v, want ErrPreempted after retries", err)
	}
}

// acceptNack passes prepares through and explicitly rejects every accept.
type acceptNack struct{ inner caspaxos.AcceptorClient }

func (a acceptNack) Prepare(ctx context.Context, key []byte, b caspaxos.Ballot) (caspaxos.PrepareReply, error) {
	return a.inner.Prepare(ctx, key, b)
}

func (acceptNack) Accept(context.Context, []byte, caspaxos.Ballot, []byte) (caspaxos.AcceptReply, error) {
	return caspaxos.AcceptReply{Accepted: false}, nil
}

// When every acceptor rejects the accept, no value from the round can be
// chosen. Propose retries and does not return ErrUnknownOutcome. The
// cluster has one acceptor: with more, the accept phase stops at the first
// rejection, and an acceptor that has not rejected may hold the value.
func TestAllRejectRetries(t *testing.T) {
	ctx := context.Background()
	acc := []caspaxos.AcceptorClient{acceptNack{newCluster(1)[0]}}
	p := caspaxos.NewProposer(1, acc)

	calls := 0
	_, err := p.Propose(ctx, []byte("k"), func([]byte) ([]byte, error) {
		calls++
		return []byte("v"), nil
	})
	if !errors.Is(err, caspaxos.ErrPreempted) {
		t.Fatalf("err = %v, want ErrPreempted after retries", err)
	}
	if calls < 2 {
		t.Fatalf("change ran %d times, want a retry", calls)
	}
}
