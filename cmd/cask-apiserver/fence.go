package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/ranges"
	"github.com/phoban01/cask/internal/roster"
	"github.com/phoban01/cask/internal/store"
)

// fencedAcceptor is the embedded acceptor behind the write fence. Every
// data write names the core configuration (the roster ConfigGen) that its
// proposer used. The acceptor rejects a write that names an older one than
// the roster value it has itself accepted.
//
// The fence reads durable acceptor state, not the member's view: the
// roster register in the same store. So it holds from the first request
// after a restart, and a stalled run loop cannot weaken it.
//
// Why this is enough: the roster commits the joint value on a majority of
// the old core, and any later roster value keeps a ConfigGen at least as
// high. So from then on every old quorum has a voter that rejects a stale
// write. A stale write that got in before that voter accepted the joint is
// already on the voter when the carry lists its keys (see listKeys).
//
// Data operations hold mu for reading. listKeys holds it for writing, so a
// data write that passed the fence cannot land after the listing.
type fencedAcceptor struct {
	acc *caspaxos.Acceptor
	st  caspaxos.Storage

	mu sync.RWMutex

	cacheMu     sync.Mutex
	cacheBallot caspaxos.Ballot
	cacheGen    uint64
	cacheOK     bool
}

func newFencedAcceptor(acc *caspaxos.Acceptor, st caspaxos.Storage) *fencedAcceptor {
	return &fencedAcceptor{acc: acc, st: st}
}

// rosterGen returns the ConfigGen of the roster value this acceptor has
// accepted. It reads the store every time. It caches only the decode, by
// accepted ballot, which names one value.
func (f *fencedAcceptor) rosterGen(ctx context.Context) (uint64, error) {
	reg, err := f.st.Load(ctx, roster.Key)
	if err != nil {
		return 0, err
	}
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	if f.cacheOK && f.cacheBallot == reg.Accepted {
		return f.cacheGen, nil
	}
	gen, err := roster.ConfigGenOf(reg.Value)
	if err != nil {
		return 0, err
	}
	f.cacheBallot, f.cacheGen, f.cacheOK = reg.Accepted, gen, true
	return gen, nil
}

// check applies the fence to one data operation. A request that names no
// configuration is rejected too: every data proposer in the extension
// server names one.
func (f *fencedAcceptor) check(ctx context.Context) error {
	//= docs/spec/fleet.md#6-membership
	//# A voter MUST reject a data write that names an older core configuration than the roster value it has accepted.
	gen, err := f.rosterGen(ctx)
	if err != nil {
		return err
	}
	claimed, ok := ranges.ClaimedEpoch(ctx)
	if !ok || claimed < gen {
		return caspaxos.ErrRangeChanged
	}
	return nil
}

func isRosterKey(key []byte) bool { return bytes.HasPrefix(key, roster.Key) }

func (f *fencedAcceptor) Prepare(ctx context.Context, key []byte, b caspaxos.Ballot) (caspaxos.PrepareReply, error) {
	if isRosterKey(key) {
		// The roster register has its own ConfigGen guard.
		return f.acc.Prepare(ctx, key, b)
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.check(ctx); err != nil {
		return caspaxos.PrepareReply{}, err
	}
	return f.acc.Prepare(ctx, key, b)
}

func (f *fencedAcceptor) Accept(ctx context.Context, key []byte, b caspaxos.Ballot, val []byte) (caspaxos.AcceptReply, error) {
	if isRosterKey(key) {
		return f.acc.Accept(ctx, key, b, val)
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err := f.check(ctx); err != nil {
		return caspaxos.AcceptReply{}, err
	}
	return f.acc.Accept(ctx, key, b, val)
}

// keyListing is one voter's answer to a key listing: the ConfigGen of the
// roster value it holds and every data key it holds.
type keyListing struct {
	Gen  uint64   `json:"gen"`
	Keys [][]byte `json:"keys"`
}

// listKeys reads the roster ConfigGen and the data keys as one step with
// respect to data writes. If Gen is at least the joint ConfigGen, every
// stale write this voter will ever accept is already in Keys.
func (f *fencedAcceptor) listKeys(ctx context.Context) (keyListing, error) {
	//= docs/spec/fleet.md#6-membership
	//# A voter MUST NOT list its data keys while a data write that passed its fence is still in progress.
	l, ok := f.st.(store.Lister)
	if !ok {
		return keyListing{}, errors.New("membership: store cannot list keys")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	gen, err := f.rosterGen(ctx)
	if err != nil {
		return keyListing{}, err
	}
	all, err := l.Keys(ctx)
	if err != nil {
		return keyListing{}, fmt.Errorf("membership: list keys: %w", err)
	}
	out := keyListing{Gen: gen}
	for _, k := range all {
		if !isRosterKey(k) {
			out.Keys = append(out.Keys, k)
		}
	}
	return out, nil
}

// errStaleAccept stands in for a fenced accept on the proposer side. The
// proposer counts it as a missing vote.
var errStaleAccept = errors.New("membership: accept fenced by a newer core")

// fencedClient is the proposer side of the fence. A fenced prepare stays
// caspaxos.ErrRangeChanged: it ends the round before change runs, so the
// caller may refresh its core view and run again. A fenced accept may come
// after other acceptors took the value, and running change again then would
// apply it twice. As a missing vote it ends in ErrUnknownOutcome instead,
// which callers already handle by re-reading.
type fencedClient struct {
	caspaxos.AcceptorClient
}

func (c *fencedClient) Accept(ctx context.Context, key []byte, b caspaxos.Ballot, val []byte) (caspaxos.AcceptReply, error) {
	r, err := c.AcceptorClient.Accept(ctx, key, b, val)
	if errors.Is(err, caspaxos.ErrRangeChanged) {
		return caspaxos.AcceptReply{}, errStaleAccept
	}
	return r, err
}
