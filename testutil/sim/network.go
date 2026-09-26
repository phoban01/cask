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
	"time"

	"github.com/phoban01/cask/internal/caspaxos"
)

// ErrUnreachable is returned by a link to an acceptor the network currently
// treats as crashed or partitioned away. The proposer counts it as a non-vote.
var ErrUnreachable = errors.New("sim: acceptor unreachable")

// Network wires a fixed set of acceptors, each backed by a caller-provided
// store, and lets tests control which are reachable. It is safe for concurrent
// use by many proposer goroutines.
//
// Beyond whole-node reachability (partition/crash), the network models partial
// reachability: an acceptor's Prepare path and Accept path can be downed
// independently, so one RPC direction can fail while the other works — the
// asymmetric-reachability fault that stresses partial-failure handling without
// a full partition.
//
// It also models two gray-failure modes the real transport exhibits but a clean
// up/down link does not: at-least-once delivery (a link can deliver one RPC to
// the acceptor twice, modelling a TCP retransmit or proposer retry with no
// dedup) and link latency (a per-acceptor delay before delivery, modelling a
// slow path distinct from a slow disk). Message *reordering* is not modelled —
// the proposer drives acceptors serially, so there is no in-flight queue to
// reorder; that needs the deferred logical-time scheduler (docs/sim-gate.md).
type Network struct {
	mu          sync.RWMutex
	reachable   []bool
	prepareDown []bool
	acceptDown  []bool
	dup         []bool
	slow        []time.Duration

	accs   []*caspaxos.Acceptor
	stores []caspaxos.Storage
}

// NewNetwork builds a network over one acceptor per store. All nodes start
// reachable.
func NewNetwork(stores []caspaxos.Storage) *Network {
	nw := &Network{
		reachable:   make([]bool, len(stores)),
		prepareDown: make([]bool, len(stores)),
		acceptDown:  make([]bool, len(stores)),
		dup:         make([]bool, len(stores)),
		slow:        make([]time.Duration, len(stores)),
		accs:        make([]*caspaxos.Acceptor, len(stores)),
		stores:      append([]caspaxos.Storage(nil), stores...),
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

// SetPrepareDown downs (or restores) only acceptor id's phase-1 path, leaving
// Accept working — models asymmetric reachability.
func (nw *Network) SetPrepareDown(id int, down bool) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.prepareDown[id] = down
}

// SetAcceptDown downs (or restores) only acceptor id's phase-2 path, leaving
// Prepare working — models asymmetric reachability.
func (nw *Network) SetAcceptDown(id int, down bool) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.acceptDown[id] = down
}

// SetDuplicate arms (or clears) at-least-once delivery for acceptor id: while
// armed, each Prepare/Accept to it is delivered to the underlying acceptor
// twice. Consensus must be idempotent under this — a second delivery of the
// same ballot is a no-op promise/accept — so it stresses the absence of
// dedup on the real wire.
func (nw *Network) SetDuplicate(id int, on bool) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.dup[id] = on
}

// SetSlow sets (or clears, with d==0) a per-call delay before acceptor id's
// link delegates — a slow network path, distinct from a slow disk (slow_fsync).
func (nw *Network) SetSlow(id int, d time.Duration) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.slow[id] = d
}

func (nw *Network) prepareReachable(id int) bool {
	nw.mu.RLock()
	defer nw.mu.RUnlock()
	return nw.reachable[id] && !nw.prepareDown[id]
}

func (nw *Network) acceptReachable(id int) bool {
	nw.mu.RLock()
	defer nw.mu.RUnlock()
	return nw.reachable[id] && !nw.acceptDown[id]
}

func (nw *Network) duplicating(id int) bool {
	nw.mu.RLock()
	defer nw.mu.RUnlock()
	return nw.dup[id]
}

func (nw *Network) latency(id int) time.Duration {
	nw.mu.RLock()
	defer nw.mu.RUnlock()
	return nw.slow[id]
}

// Heal marks every acceptor fully reachable, clearing partial-reachability,
// duplication, and link latency too — so one fault dwell does not leak into the
// next.
func (nw *Network) Heal() {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	for i := range nw.reachable {
		nw.reachable[i] = true
		nw.prepareDown[i] = false
		nw.acceptDown[i] = false
		nw.dup[i] = false
		nw.slow[i] = 0
	}
}

// link is an AcceptorClient that consults the network's reachability before
// delegating to the underlying acceptor.
type link struct {
	nw *Network
	id int
}

func (l link) Prepare(ctx context.Context, key []byte, b caspaxos.Ballot) (caspaxos.PrepareReply, error) {
	if !l.nw.prepareReachable(l.id) {
		return caspaxos.PrepareReply{}, ErrUnreachable
	}
	if err := l.delay(ctx); err != nil {
		return caspaxos.PrepareReply{}, err
	}
	reply, err := l.nw.accs[l.id].Prepare(ctx, key, b)
	if err != nil {
		return caspaxos.PrepareReply{}, err
	}
	if l.nw.duplicating(l.id) {
		// At-least-once delivery: the acceptor processes the message a second
		// time (a retransmit), but the caller correlates to the first reply —
		// the extra copy is invisible on the wire. Re-processing must not
		// corrupt register state.
		_, _ = l.nw.accs[l.id].Prepare(ctx, key, b)
	}
	return reply, nil
}

func (l link) Accept(ctx context.Context, key []byte, b caspaxos.Ballot, val []byte) (caspaxos.AcceptReply, error) {
	if !l.nw.acceptReachable(l.id) {
		return caspaxos.AcceptReply{}, ErrUnreachable
	}
	if err := l.delay(ctx); err != nil {
		return caspaxos.AcceptReply{}, err
	}
	reply, err := l.nw.accs[l.id].Accept(ctx, key, b, val)
	if err != nil {
		return caspaxos.AcceptReply{}, err
	}
	if l.nw.duplicating(l.id) {
		// At-least-once delivery: the same accept lands twice. Re-accepting a
		// ballot already accepted must be an idempotent no-op, not a second
		// commit; the caller sees only the first reply.
		_, _ = l.nw.accs[l.id].Accept(ctx, key, b, val)
	}
	return reply, nil
}

// delay applies the link's latency (if any), honouring context cancellation.
func (l link) delay(ctx context.Context) error {
	d := l.nw.latency(l.id)
	if d == 0 {
		return nil
	}
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
