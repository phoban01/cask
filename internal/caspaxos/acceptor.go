package caspaxos

import (
	"context"
	"hash/fnv"
	"sync"
)

// Storage is the durable home of acceptor [Register] state, keyed by opaque
// bytes. Implementations MUST make a Store durable (fsync) before returning:
// an acceptor reply implies durability, and acknowledging a promise or an
// accept that is later lost to a crash would violate safety.
//
// Load returns the zero Register (and nil error) for a key that has never been
// stored.
type Storage interface {
	Load(ctx context.Context, key []byte) (Register, error)
	Store(ctx context.Context, key []byte, r Register) error
}

// Acceptor is the passive half of CASPaxos. It enforces the promise/accept
// invariants over registers held in Storage and never interprets values.
//
// Each Prepare/Accept is a read-modify-write that MUST be atomic per key: two
// concurrent operations on the same register may not interleave their
// Load/Store, or a promise could be silently lost and two values accepted at a
// quorum. The acceptor serializes per key with striped locks, so Storage
// implementations need not be transactional themselves.
type Acceptor struct {
	store Storage
	locks keyedMutex
}

// NewAcceptor returns an Acceptor backed by store.
func NewAcceptor(store Storage) *Acceptor { return &Acceptor{store: store} }

// Prepare handles phase 1. If b is strictly greater than the register's current
// promise it adopts b as the new promise (durably) and returns the
// already-accepted ballot/value. Otherwise it rejects, returning the conflicting
// promise so the proposer can advance past it.
func (a *Acceptor) Prepare(ctx context.Context, key []byte, b Ballot) (PrepareReply, error) {
	defer a.locks.lock(key)()
	reg, err := a.store.Load(ctx, key)
	if err != nil {
		return PrepareReply{}, err
	}
	// Accept the prepare only if b strictly exceeds the current promise. Because
	// Accept sets Promise == Accepted, the promise already dominates the
	// accepted ballot, so a single comparison is sufficient.
	if reg.Promise.Less(b) {
		reg.Promise = b
		if err := a.store.Store(ctx, key, reg); err != nil {
			return PrepareReply{}, err
		}
		return PrepareReply{Promised: true, Accepted: reg.Accepted, Value: reg.Value}, nil
	}
	return PrepareReply{Promised: false, Conflict: reg.Promise, Accepted: reg.Accepted, Value: reg.Value}, nil
}

// Accept handles phase 2. It durably stores (b, val) when b is at least the
// register's current promise, also adopting b as the promise. Otherwise it
// rejects with the conflicting promise.
func (a *Acceptor) Accept(ctx context.Context, key []byte, b Ballot, val []byte) (AcceptReply, error) {
	defer a.locks.lock(key)()
	reg, err := a.store.Load(ctx, key)
	if err != nil {
		return AcceptReply{}, err
	}
	if b.Less(reg.Promise) {
		return AcceptReply{Accepted: false, Conflict: reg.Promise}, nil
	}
	reg.Promise = b
	reg.Accepted = b
	reg.Value = val
	if err := a.store.Store(ctx, key, reg); err != nil {
		return AcceptReply{}, err
	}
	return AcceptReply{Accepted: true}, nil
}

// keyedMutex serializes operations per key using a fixed set of striped locks.
// Striping bounds memory (no per-key map growth); distinct keys that collide on
// a stripe merely serialize, which is harmless for correctness.
type keyedMutex struct {
	stripes [256]sync.Mutex
}

// lock acquires the stripe for key and returns its unlock function.
func (k *keyedMutex) lock(key []byte) func() {
	h := fnv.New32a()
	h.Write(key)
	m := &k.stripes[h.Sum32()%uint32(len(k.stripes))]
	m.Lock()
	return m.Unlock
}
