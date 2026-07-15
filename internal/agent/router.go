// Package agent is the proposer/router tier. An agent holds no durable state; it
// routes each operation to the replica group of the key's range and runs the
// CASPaxos round. A single mvcc.KV over a Router therefore spans the whole
// keyspace, with each key handled by exactly its range's acceptors.
//
// In the full topology an agent forwards the op to the range's owner, which
// proposes (enabling the 1-RTT fast path once leases exist, M7). For now the
// agent proposes directly over the range's replicas, which is already correct;
// owner selection is available via the range descriptor for when forwarding is
// wired through the node-to-node transport.
package agent

import (
	"context"
	"errors"
	"sync"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/ranges"
)

var (
	// ErrNoRange means no range covers the key (a gap in the keyspace map).
	ErrNoRange = errors.New("agent: no range covers key")
	// ErrNoReplica means a range names a replica the dialer cannot resolve.
	ErrNoReplica = errors.New("agent: replica not resolvable")
)

// Dialer resolves a node id to an acceptor client. The simulator and the
// network transport each provide one.
type Dialer interface {
	Acceptor(node uint64) (caspaxos.AcceptorClient, bool)
}

// FastProposer is an optional 1-RTT write path consulted before the full
// two-phase round (satisfied by *owner.Manager). handled=false means the full
// path must run; a handled result is final. The seam keeps mvcc/lease/roster
// untouched: they see the same Proposer interface either way.
type FastProposer interface {
	FastPropose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) (val []byte, handled bool, err error)
}

// LocalInvalidator is implemented by fast paths that also cache reads
// (*owner.Manager): the router notifies it whenever a write runs the FULL
// path, whose committed result the cache cannot reflect. Without this, the
// owner's own CAS-conflict fallbacks would silently poison its read cache —
// staleness within a single node, no clock skew or second writer required.
type LocalInvalidator interface {
	InvalidateLocal(key []byte)
}

// FullPathGate is implemented by fast paths whose cached reads a full-path
// round could silently invalidate on ANOTHER node (M7): before running the
// full path, the router asks permission. A non-nil error (owner.ErrOwnerLive)
// aborts the proposal — the caller forwards to the owner or retries after its
// lease lapses. Fast-path-handled proposals and internal keys never consult
// the gate.
type FullPathGate interface {
	GateFullPath(ctx context.Context, rangeID uint64, key []byte) error
}

// StaticDialer is a fixed node-id -> acceptor mapping.
type StaticDialer map[uint64]caspaxos.AcceptorClient

// Acceptor resolves node to its client.
func (d StaticDialer) Acceptor(node uint64) (caspaxos.AcceptorClient, bool) {
	a, ok := d[node]
	return a, ok
}

// Router routes each key to the proposer for its range's replica set. It
// satisfies mvcc.Proposer, so wrapping it in an mvcc.KV yields a multi-range
// agent. Proposers are cached per range and rebuilt when a range's epoch changes
// (e.g. after reconfiguration).
type Router struct {
	id      uint64 // this agent's proposer id
	dialer  Dialer
	backoff func(ctx context.Context, attempt int) error
	fast    FastProposer
	refresh func() *ranges.Map // re-resolves placement after ErrRangeChanged

	mu    sync.Mutex
	rmap  *ranges.Map
	cache map[uint64]cachedProposer // by range id
}

type cachedProposer struct {
	epoch uint64
	p     *caspaxos.Proposer
}

// RouterOption configures a Router.
type RouterOption func(*Router)

// WithBackoff installs a contention backoff on every proposer the router
// builds (caspaxos.WithBackoff).
func WithBackoff(f func(ctx context.Context, attempt int) error) RouterOption {
	return func(r *Router) { r.backoff = f }
}

// WithFastPath consults fp before every full round (the W1 ownership fast
// path). Nil disables it.
func WithFastPath(fp FastProposer) RouterOption {
	return func(r *Router) { r.fast = fp }
}

// WithRefresh installs the placement re-resolver used when an acceptor
// rejects a proposal with caspaxos.ErrRangeChanged (§4.1): the router swaps
// in the fresh map, drops its proposer cache, and retries the operation once.
// Without it, ErrRangeChanged propagates to the caller.
func WithRefresh(f func() *ranges.Map) RouterOption {
	return func(r *Router) { r.refresh = f }
}

// NewRouter returns a Router for agent id over the given range map and dialer.
func NewRouter(id uint64, rmap *ranges.Map, dialer Dialer, opts ...RouterOption) *Router {
	r := &Router{id: id, rmap: rmap, dialer: dialer, cache: make(map[uint64]cachedProposer)}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Propose routes key to its range and runs the round there — via the 1-RTT
// fast path when an ownership grant covers the key, falling back to the full
// two-phase round otherwise. A fast-path miss is never an error: the full
// path is always correct (and fences out a stale owner as a side effect).
//
// A caspaxos.ErrRangeChanged rejection means this router's placement is
// stale (the range was reconfigured since the map was built): with a refresh
// source installed, the router re-resolves placement and retries ONCE against
// the current replica set; a second rejection propagates — the caller's
// routing layer is persistently behind and must resynchronize.
func (r *Router) Propose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error) {
	val, err := r.propose(ctx, key, change)
	if errors.Is(err, caspaxos.ErrRangeChanged) && r.refresh != nil {
		if fresh := r.refresh(); fresh != nil {
			r.mu.Lock()
			r.rmap = fresh
			r.cache = make(map[uint64]cachedProposer)
			r.mu.Unlock()
			return r.propose(ctx, key, change)
		}
	}
	return val, err
}

func (r *Router) propose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error) {
	if r.fast != nil {
		if val, handled, err := r.fast.FastPropose(ctx, key, change); handled {
			return val, err
		}
	}
	d, ok := r.lookup(key)
	if !ok {
		return nil, ErrNoRange
	}
	if gate, ok := r.fast.(FullPathGate); ok {
		if err := gate.GateFullPath(ctx, d.ID, key); err != nil {
			return nil, err
		}
	}
	p, err := r.proposerFor(d)
	if err != nil {
		return nil, err
	}
	// Stamp the epoch this route was computed under; the serving node rejects
	// the round with ErrRangeChanged if its descriptor has moved past it.
	val, err := p.Propose(ranges.WithClaimedEpoch(ctx, d.Epoch), key, change)
	// A full round may have committed (even on error paths — a failed CAS
	// writes the current value back); anything the fast path cached for this
	// key is now unreliable.
	if inv, ok := r.fast.(LocalInvalidator); ok {
		inv.InvalidateLocal(key)
	}
	return val, err
}

func (r *Router) lookup(key []byte) (ranges.Descriptor, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rmap.Lookup(key)
}

// Owner returns the storage node that owns key (HRW over its range's replicas).
func (r *Router) Owner(key []byte) (uint64, bool) {
	d, ok := r.lookup(key)
	if !ok {
		return 0, false
	}
	return d.Owner(key)
}

func (r *Router) proposerFor(d ranges.Descriptor) (*caspaxos.Proposer, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.cache[d.ID]; ok && c.epoch == d.Epoch {
		return c.p, nil
	}
	acc := make([]caspaxos.AcceptorClient, 0, len(d.Replicas))
	for _, n := range d.Replicas {
		a, ok := r.dialer.Acceptor(n)
		if !ok {
			return nil, ErrNoReplica
		}
		acc = append(acc, a)
	}
	var opts []caspaxos.Option
	if r.backoff != nil {
		opts = append(opts, caspaxos.WithBackoff(r.backoff))
	}
	p := caspaxos.NewProposer(r.id, acc, opts...)
	r.cache[d.ID] = cachedProposer{epoch: d.Epoch, p: p}
	return p, nil
}
