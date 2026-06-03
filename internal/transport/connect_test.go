package transport_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/internal/transport"
)

// connectServer wraps an acceptor in a ConnectRPC server mounted at its service
// route, returning the running test server.
func connectServer(t *testing.T) *httptest.Server {
	t.Helper()
	acc := caspaxos.NewAcceptor(store.NewMem())
	path, h := transport.ConnectHandler(acc)
	mux := http.NewServeMux()
	mux.Handle(path, h)
	srv := httptest.NewServer(mux)
	return srv
}

// A real RF=3 group reached over ConnectRPC must agree like the in-process core.
func TestProposeOverConnect(t *testing.T) {
	ctx := context.Background()

	const rf = 3
	clients := make([]caspaxos.AcceptorClient, rf)
	for i := 0; i < rf; i++ {
		srv := connectServer(t)
		t.Cleanup(srv.Close)
		clients[i] = transport.NewConnectClient(srv.URL, srv.Client())
	}

	p := caspaxos.NewProposer(1, clients)
	if _, err := p.Propose(ctx, []byte("k"), caspaxos.Write([]byte("hello"))); err != nil {
		t.Fatalf("propose over connect: %v", err)
	}
	got, err := p.Propose(ctx, []byte("k"), caspaxos.Identity)
	if err != nil {
		t.Fatalf("read over connect: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("read = %q, want hello", got)
	}

	// A second proposer reaching the same servers observes the committed value —
	// proving the accepted ballot/value round-trips through the wire types.
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
// progress over ConnectRPC; an unreachable acceptor is counted as a non-vote.
func TestQuorumOverConnect(t *testing.T) {
	ctx := context.Background()

	const rf = 3
	clients := make([]caspaxos.AcceptorClient, rf)
	servers := make([]*httptest.Server, rf)
	for i := 0; i < rf; i++ {
		srv := connectServer(t)
		servers[i] = srv
		clients[i] = transport.NewConnectClient(srv.URL, srv.Client())
	}
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()

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
