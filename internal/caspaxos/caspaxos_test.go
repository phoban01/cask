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
