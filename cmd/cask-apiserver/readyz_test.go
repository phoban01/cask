package main

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/store"
)

var errAcceptorStopped = errors.New("acceptor stopped")

// stoppableAcceptor is an acceptor that the test can stop and start. A
// stopped acceptor answers every call with an error, as an acceptor that
// the network cannot reach does. Its state stays.
type stoppableAcceptor struct {
	acc     *caspaxos.Acceptor
	stopped atomic.Bool
}

func (s *stoppableAcceptor) Prepare(ctx context.Context, key []byte, b caspaxos.Ballot) (caspaxos.PrepareReply, error) {
	if s.stopped.Load() {
		return caspaxos.PrepareReply{}, errAcceptorStopped
	}
	return s.acc.Prepare(ctx, key, b)
}

func (s *stoppableAcceptor) Accept(ctx context.Context, key []byte, b caspaxos.Ballot, val []byte) (caspaxos.AcceptReply, error) {
	if s.stopped.Load() {
		return caspaxos.AcceptReply{}, errAcceptorStopped
	}
	return s.acc.Accept(ctx, key, b, val)
}

// TestReadyzFollowsStorage stops a majority of the acceptors under a
// ready server. The server then reports not ready. When the acceptors
// start again, the server reports ready again.
func TestReadyzFollowsStorage(t *testing.T) {
	//= docs/spec/fleet.md#2-resources
	//= type=test
	//# The extension server MUST report not ready until its storage is reachable.
	accs := make([]*stoppableAcceptor, 3)
	clients := make([]caspaxos.AcceptorClient, 3)
	for i := range accs {
		accs[i] = &stoppableAcceptor{acc: caspaxos.NewAcceptor(store.NewMem())}
		clients[i] = accs[i]
	}
	ts := startTestServerWith(t, "eu-west-a", false, testServerConfig{acceptors: clients})
	if code := ts.readyz(); code != http.StatusOK {
		t.Fatalf("readyz with a quorum: got %d, want 200", code)
	}

	// Two of three acceptors are a majority.
	accs[0].stopped.Store(true)
	accs[1].stopped.Store(true)
	if code := ts.readyz(); code == http.StatusOK {
		t.Fatalf("readyz without a quorum: got 200, want not ready")
	}

	accs[0].stopped.Store(false)
	accs[1].stopped.Store(false)
	ts.waitReadyz(t, func(code int) bool { return code == http.StatusOK })
}

// TestReadyzWaitsForStorageAtStart starts a server with no quorum. It
// reports not ready until a majority of the acceptors starts.
func TestReadyzWaitsForStorageAtStart(t *testing.T) {
	//= docs/spec/fleet.md#2-resources
	//= type=test
	//# The extension server MUST report not ready until its storage is reachable.
	accs := make([]*stoppableAcceptor, 3)
	clients := make([]caspaxos.AcceptorClient, 3)
	for i := range accs {
		accs[i] = &stoppableAcceptor{acc: caspaxos.NewAcceptor(store.NewMem())}
		accs[i].stopped.Store(true)
		clients[i] = accs[i]
	}
	ts := startTestServerWith(t, "eu-west-a", false, testServerConfig{acceptors: clients, noWait: true})
	if code := ts.readyz(); code == http.StatusOK {
		t.Fatalf("readyz before the storage starts: got 200, want not ready")
	}
	accs[0].stopped.Store(false)
	if code := ts.readyz(); code == http.StatusOK {
		t.Fatalf("readyz with one of three acceptors: got 200, want not ready")
	}
	accs[2].stopped.Store(false)
	ts.waitReadyz(t, func(code int) bool { return code == http.StatusOK })
}
