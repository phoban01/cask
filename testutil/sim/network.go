// Package sim provides a deterministic, fault-injecting in-process network for
// driving a CASPaxos cluster in tests. Acceptors are reached through
// [caspaxos.AcceptorClient] links whose reachability the test (or a nemesis)
// toggles, modelling crashes and partitions. Durable acceptor state lives in
// the injected stores and survives a crash, so flipping a node down and back up
// models a durable restart.
package sim

import (
	"context"
	"errors"
	"sync"

	"github.com/phoban01/cask/internal/caspaxos"
)

// ErrUnreachable is returned by a link to an acceptor the network currently
// treats as crashed or partitioned away. The proposer counts it as a non-vote.
var ErrUnreachable = errors.New("sim: acceptor unreachable")

// Network wires a fixed set of acceptors, each backed by a caller-provided
// store, and lets tests control which are reachable. It is safe for concurrent
// use by many proposer goroutines.
type Network struct {
	mu        sync.RWMutex
	reachable []bool

	accs   []*caspaxos.Acceptor
	stores []caspaxos.Storage
}

// NewNetwork builds a network over one acceptor per store. All nodes start
// reachable.
func NewNetwork(stores []caspaxos.Storage) *Network {
	nw := &Network{
		reachable: make([]bool, len(stores)),
		accs:      make([]*caspaxos.Acceptor, len(stores)),
		stores:    append([]caspaxos.Storage(nil), stores...),
	}
	for i, s := range stores {
		nw.accs[i] = caspaxos.NewAcceptor(s)
		nw.reachable[i] = true
	}
	return nw
}

// N is the number of acceptors.
func (nw *Network) N() int { return len(nw.accs) }

// Clients returns one AcceptorClient link per acceptor, suitable for handing to
// a proposer as its configuration.
func (nw *Network) Clients() []caspaxos.AcceptorClient {
	cs := make([]caspaxos.AcceptorClient, len(nw.accs))
	for i := range nw.accs {
		cs[i] = link{nw: nw, id: i}
	}
	return cs
}

// Client returns the AcceptorClient link for acceptor i — used to build a
// node-id -> acceptor dialer for the router.
func (nw *Network) Client(i int) caspaxos.AcceptorClient { return link{nw: nw, id: i} }

// SetReachable marks acceptor id up or down.
func (nw *Network) SetReachable(id int, up bool) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.reachable[id] = up
}

// Reachable reports whether acceptor id is currently reachable.
func (nw *Network) Reachable(id int) bool {
	nw.mu.RLock()
	defer nw.mu.RUnlock()
	return nw.reachable[id]
}

// Heal marks every acceptor reachable.
func (nw *Network) Heal() {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	for i := range nw.reachable {
		nw.reachable[i] = true
	}
}

// link is an AcceptorClient that consults the network's reachability before
// delegating to the underlying acceptor.
type link struct {
	nw *Network
	id int
}

func (l link) Prepare(ctx context.Context, key []byte, b caspaxos.Ballot) (caspaxos.PrepareReply, error) {
	if !l.nw.Reachable(l.id) {
		return caspaxos.PrepareReply{}, ErrUnreachable
	}
	return l.nw.accs[l.id].Prepare(ctx, key, b)
}

func (l link) Accept(ctx context.Context, key []byte, b caspaxos.Ballot, val []byte) (caspaxos.AcceptReply, error) {
	if !l.nw.Reachable(l.id) {
		return caspaxos.AcceptReply{}, ErrUnreachable
	}
	return l.nw.accs[l.id].Accept(ctx, key, b, val)
}
