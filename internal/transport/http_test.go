package transport_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/internal/transport"
)

// A real RF=3 group reached over HTTP must agree like the in-process core.
func TestProposeOverHTTP(t *testing.T) {
	ctx := context.Background()

	const rf = 3
	clients := make([]caspaxos.AcceptorClient, rf)
	servers := make([]*httptest.Server, rf)
	for i := 0; i < rf; i++ {
		acc := caspaxos.NewAcceptor(store.NewMem())
		srv := httptest.NewServer(transport.Handler(acc))
		t.Cleanup(srv.Close)
		servers[i] = srv
		clients[i] = transport.NewClient(srv.URL, srv.Client())
	}

	p := caspaxos.NewProposer(1, clients)
	if _, err := p.Propose(ctx, []byte("k"), caspaxos.Write([]byte("hello"))); err != nil {
		t.Fatalf("propose over http: %v", err)
	}
	got, err := p.Propose(ctx, []byte("k"), caspaxos.Identity)
	if err != nil {
		t.Fatalf("read over http: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("read = %q, want hello", got)
	}

	// A second proposer reaching the same servers observes the committed value.
	p2 := caspaxos.NewProposer(2, clients)
	got, err = p2.Propose(ctx, []byte("k"), caspaxos.Identity)
	if err != nil {
		t.Fatalf("p2 read: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("p2 read = %q, want hello", got)
	}
}

// With a majority of acceptor servers stopped, the proposer cannot make
// progress; with a quorum it can.
func TestQuorumOverHTTP(t *testing.T) {
	ctx := context.Background()

	const rf = 3
	clients := make([]caspaxos.AcceptorClient, rf)
	servers := make([]*httptest.Server, rf)
	for i := 0; i < rf; i++ {
		acc := caspaxos.NewAcceptor(store.NewMem())
		srv := httptest.NewServer(transport.Handler(acc))
		servers[i] = srv
		clients[i] = transport.NewClient(srv.URL, srv.Client())
	}
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()

	// Commit once with all up.
	p := caspaxos.NewProposer(1, clients)
	if _, err := p.Propose(ctx, []byte("k"), caspaxos.Write([]byte("v"))); err != nil {
		t.Fatalf("initial propose: %v", err)
	}

	// Stop a majority (2 of 3): no quorum -> failure.
	servers[0].Close()
	servers[1].Close()
	if _, err := p.Propose(ctx, []byte("k"), caspaxos.Write([]byte("v2"))); err == nil {
		t.Fatal("expected failure with majority of servers down")
	}
}
