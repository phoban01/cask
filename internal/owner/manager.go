// Package owner puts the OwnedProposer 1-RTT fast path on the production
// write path (W1 in docs/plans/quepaxa-learnings-implementation.md).
//
// Ownership is per RANGE: one grant — a fencing token from the range's
// ownership lock — covers every key in the range, with per-key OwnedProposers
// created lazily under it. The fence IS the epoch in the ballot's high bits,
// which is what makes the phase-1 skip safe: the lock guarantees at most one
// writer per epoch, and epoch ordering fences any stale owner (see
// internal/caspaxos/owned.go). HRW eligibility decides who *tries* to acquire
// — a routing optimization only; safety never depends on who holds the lock.
//
// Ownership is an optimization exactly the way QuePaxa's leader is: losing
// the owner never blocks a key. Non-owners keep writing through the full
// two-phase path, which fences the owner out (it observes ErrLostOwnership /
// EpochBehindError, bumps its fence, and resumes); every fast-path miss falls
// back to the full path. Misbehavior costs round-trips, never safety.
package owner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/lease"
	"github.com/phoban01/cask/internal/placement"
	"github.com/phoban01/cask/internal/ranges"
)

// Dialer resolves a node id to an acceptor client (structurally identical to
// agent.Dialer; declared here so owner does not import agent, which lets the
// Router depend on this package's Manager through its own interface).
type Dialer interface {
	Acceptor(node uint64) (caspaxos.AcceptorClient, bool)
}

// lockNamePrefix namespaces range-ownership locks in the lock keyspace.
const lockNamePrefix = "rown/"

func lockName(rangeID uint64) string { return fmt.Sprintf("%s%d", lockNamePrefix, rangeID) }

// clientKey reports whether key belongs to the client keyspace. The fast path
// (writes AND cached reads) covers ONLY client keys: internal registers (the
// \x00-prefixed sessions, locks, roster, sim probes) are written full-path by
// every node as a matter of design, so caching them would be stale-by-design
// — and excluding them also breaks the recursion of the manager's own lock
// and session traffic through the proposer stack it serves.
func clientKey(key []byte) bool { return len(key) > 0 && key[0] != 0 }

// ErrOwnerLive is returned by GateFullPath when a full-path write must not
// proceed: another node's ownership session still covers reads (live, or
// lapsed less than MaxOffset ago). Writing around it would invalidate nothing
// on the owner and its zero-RTT reads would go stale. Callers surface this as
// a retryable condition (HTTP 503); it clears when the owner's session lapses
// past the margin — or never blocks at all if forwarding reaches the owner.
var ErrOwnerLive = errors.New("owner: range owner's read lease is live; forward or retry")

// Manager holds this node's range-ownership grants and serves the fast path.
type Manager struct {
	nodeID     uint64
	dialer     Dialer
	sessions   *lease.Sessions
	locks      *lease.Locks
	sessionID  string
	sessionTTL int64             // in the lease clock's units (nanos in production)
	clock      func() int64      // must be the same clock the sessions use
	maxOffset  int64             // assumed bound on inter-node clock skew

	// sessionExpiry is the freshest session expiry this node has CONFIRMED
	// (returned by a committed Grant). The read guard compares against this,
	// never against a locally computed now+ttl.
	sessionExpiry atomic.Int64

	mu     sync.Mutex
	grants map[uint64]*grant // by range id

	// Telemetry (atomic): the input the deferred bandit-placement idea needs.
	fastWrites atomic.Uint64
	fastReads  atomic.Uint64
	fallbacks  atomic.Uint64
	bumps      atomic.Uint64
}

// Option configures a Manager.
type Option func(*Manager)

// WithSessionTTL sets the ownership session TTL in the lease clock's units.
func WithSessionTTL(ttl int64) Option { return func(m *Manager) { m.sessionTTL = ttl } }

// WithClock sets the manager's time source. It MUST be the same clock the
// injected lease.Sessions reads — the read guard compares this clock against
// session expiries.
func WithClock(now func() int64) Option { return func(m *Manager) { m.clock = now } }

// WithMaxOffset sets the assumed bound on clock skew between any two nodes
// (design A, docs/etcd-little-sister.md §4.4; default 500ms). The read guard
// is double-sided around it: the owner stops serving local reads MaxOffset
// BEFORE its session expires, and a takeover of a lapsed holder waits
// MaxOffset AFTER the observed expiry. Under |skew| <= MaxOffset the old
// owner's last served read strictly precedes the new owner's first write;
// beyond the bound, reads may be stale — never lost writes (writes stay
// epoch-fenced regardless of clocks).
func WithMaxOffset(d int64) Option { return func(m *Manager) { m.maxOffset = d } }

// New returns a Manager for nodeID. sessions and locks must operate on the
// control-plane keyspace (in production they share the node's proposer stack;
// the rown key exclusion in FastPropose breaks the recursion).
func New(nodeID uint64, dialer Dialer, sessions *lease.Sessions, locks *lease.Locks, opts ...Option) *Manager {
	m := &Manager{
		nodeID:     nodeID,
		dialer:     dialer,
		sessions:   sessions,
		locks:      locks,
		sessionID:  fmt.Sprintf("owner-%d", nodeID),
		sessionTTL: 10_000_000_000, // 10s in nanos
		clock:      func() int64 { return time.Now().UnixNano() },
		maxOffset:  500_000_000, // 500ms in nanos
		grants:     make(map[uint64]*grant),
	}
	for _, o := range opts {
		o(m)
	}
	return m
}

// grant is one held range-ownership: the fence (= ballot epoch) and the
// per-key fast-path proposers minted under it.
type grant struct {
	rangeID    uint64
	rangeEpoch uint64            // descriptor epoch at acquisition; a change invalidates
	desc       ranges.Descriptor // the interval the grant covers
	acceptors  []caspaxos.AcceptorClient

	mu      sync.Mutex
	fence   uint64
	bumping bool // single-flight guard for fence bumps
	perKey  map[string]*keyOwner
}

// keyOwner wraps one key's OwnedProposer with a take mutex so concurrent
// writers race to a single TakeOwnership instead of self-conflicting.
type keyOwner struct {
	mu sync.Mutex
	op *caspaxos.OwnedProposer
}

// take ensures ownership at fence, idempotently.
func (k *keyOwner) take(ctx context.Context, key []byte, fence uint64) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.op.Owns() {
		return nil
	}
	return k.op.TakeOwnership(ctx, key, fence)
}

func (g *grant) currentFence() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.fence
}

func (g *grant) owner(m *Manager, key []byte) *keyOwner {
	g.mu.Lock()
	defer g.mu.Unlock()
	ko, ok := g.perKey[string(key)]
	if !ok {
		ko = &keyOwner{op: caspaxos.NewOwnedProposer(m.nodeID, g.acceptors)}
		g.perKey[string(key)] = ko
	}
	return ko
}

// Stats is a point-in-time telemetry snapshot.
type Stats struct {
	FastWrites uint64
	FastReads  uint64
	Fallbacks  uint64
	Bumps      uint64
	Grants     int
}

// Stats returns fast-path telemetry.
func (m *Manager) Stats() Stats {
	m.mu.Lock()
	grants := len(m.grants)
	m.mu.Unlock()
	return Stats{
		FastWrites: m.fastWrites.Load(),
		FastReads:  m.fastReads.Load(),
		Fallbacks:  m.fallbacks.Load(),
		Bumps:      m.bumps.Load(),
		Grants:     grants,
	}
}

// FastPropose attempts change on the 1-RTT fast path. handled=false means the
// caller must run the full path — the fast path was unavailable (no grant, a
// stale grant, lost ownership) or must not decide the outcome:
//
//   - A ChangeFunc conflict (caspaxos.ErrConflict, a failed CAS) is judged
//     against the owner's local cache WITHOUT a network round. Until the W4
//     lease guard exists, a deposed-but-unaware owner could judge it against
//     a stale cache, so conflicts are re-run on the full path, which
//     write-backs and decides linearizably. Correctness over cleverness.
//   - Ownership-lock keys are declined outright: the manager's own lock
//     traffic routes through this same stack (recursion guard).
func (m *Manager) FastPropose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, bool, error) {
	if !clientKey(key) {
		return nil, false, nil
	}
	g := m.grantFor(key)
	if g == nil {
		return nil, false, nil
	}

	ko := g.owner(m, key)

	// One recovery retry after a fence bump, then fall back. The retry is
	// load-bearing, not merely an optimization: a fallback full-path write
	// epoch-jumps into the NEXT epoch space — exactly the fence the manager
	// just bumped to — and because it is minted by this same node, the
	// subsequent TakeOwnership would collide with it (equal ballot) and force
	// another bump, forever. Retrying the fast path after the bump commits
	// nothing at the fresh epoch's boundary, so the take lands cleanly.
	for attempt := range 2 {
		if err := ko.take(ctx, key, g.currentFence()); err != nil {
			if attempt == 0 && m.recoverFence(ctx, g, err) {
				continue
			}
			break
		}
		val, err := ko.op.Write(ctx, key, change)
		switch {
		case err == nil:
			m.fastWrites.Add(1)
			return val, true, nil
		case errors.Is(err, caspaxos.ErrConflict):
			// A ChangeFunc conflict is judged against the local cache without
			// a network round; the full path decides it linearizably (W4).
			m.fallbacks.Add(1)
			return nil, false, nil
		case errors.Is(err, caspaxos.ErrLostOwnership):
			if attempt == 0 && m.recoverFence(ctx, g, err) {
				continue
			}
		default:
			// Unknown failure (context, transport): the full path produces
			// the authoritative outcome.
		}
		break
	}
	m.fallbacks.Add(1)
	return nil, false, nil
}

// ReadLocal serves a zero-RTT read from the owner's cache (W4). served=false
// means the caller must run a full linearizable round.
//
// Validity is the double-sided lease guard: the cache is served only while
// this node's CONFIRMED session expiry is more than MaxOffset away on its own
// clock, and a takeover of a lapsed holder waits MaxOffset past the observed
// expiry (acquireGrant). Under |skew| <= MaxOffset the last read the old
// owner can serve strictly precedes the first write a new owner can commit.
//
// Linearizability additionally requires the writes-via-owner discipline: any
// write that bypasses this cache must invalidate it. Three mechanisms enforce
// it cluster-wide (M7): the local Router invalidates after every full-path
// write it carries (InvalidateLocal); other nodes FORWARD client writes to
// the owner (cmd's forwarder); and when forwarding fails, their local full
// path is refused while this owner's read lease is live (GateFullPath /
// ErrOwnerLive). Bare proposers that bypass all three are out of contract —
// see TestBareProposerWriteIsOutOfContract.
func (m *Manager) ReadLocal(ctx context.Context, key []byte) ([]byte, bool, error) {
	if !clientKey(key) {
		return nil, false, nil
	}
	g := m.grantFor(key)
	if g == nil {
		return nil, false, nil
	}
	if m.clock()+m.maxOffset >= m.sessionExpiry.Load() {
		return nil, false, nil // too close to expiry: a successor may be taking over
	}
	ko := g.owner(m, key)
	if err := ko.take(ctx, key, g.currentFence()); err != nil {
		// Reads stay cheap: no recovery dance, just the full path.
		return nil, false, nil
	}
	val, err := ko.op.ReadLocal()
	if err != nil {
		return nil, false, nil
	}
	m.fastReads.Add(1)
	return val, true, nil
}

// InvalidateLocal poisons the cached fast path for key after a write
// committed around it (a full-path round the cache cannot reflect). The next
// operation re-takes ownership, which re-reads through phase 1. The Router
// calls this after every full-path propose it carries for a fast-path-enabled
// keyspace.
func (m *Manager) InvalidateLocal(key []byte) {
	g := m.grantFor(key)
	if g == nil {
		return
	}
	g.mu.Lock()
	ko, ok := g.perKey[string(key)]
	g.mu.Unlock()
	if ok {
		ko.op.Disown()
	}
}

// GateFullPath decides whether a full-path proposal for key may run locally
// (M7). The full path is how a non-owner writes AROUND an owner's cache —
// safe for writes (epoch fencing) but fatal for the owner's zero-RTT reads,
// which detect nothing. The rule mirrors Raft leader leases: you cannot
// commit around a live read lease. nil = proceed; ErrOwnerLive = the owner's
// session still covers reads (live, or lapsed less than MaxOffset ago) —
// forward to it or retry later.
//
// Internal (\x00-prefixed) keys always pass: they are never cached, by the
// clientKey rule. Keys in a range this node holds the grant for always pass:
// the local router invalidates the local cache after every full round.
func (m *Manager) GateFullPath(ctx context.Context, rangeID uint64, key []byte) error {
	if !clientKey(key) {
		return nil
	}
	if g := m.grantFor(key); g != nil {
		return nil // our own range: the router's invalidation covers the cache
	}
	holder, live, _, err := m.locks.Owner(ctx, lockName(rangeID))
	if err != nil || holder == "" || holder == m.sessionID {
		return nil // unknown/none/ours: nothing to protect (or we own it)
	}
	if live {
		return ErrOwnerLive
	}
	if prev, ok, err := m.sessions.Info(ctx, holder); err == nil && ok {
		if m.clock() < prev.Expiry+m.maxOffset {
			return ErrOwnerLive // inside the lapsed holder's read margin
		}
	}
	return nil
}

// grantFor returns the live grant covering key, or nil.
func (m *Manager) grantFor(key []byte) *grant {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, g := range m.grants {
		// One grant per range; ranges are few per node. Descriptor lookup is
		// the router's job — the manager only checks its own grants.
		if g.covers(key) {
			return g
		}
	}
	return nil
}

// covers reports whether key falls in the grant's range interval.
func (g *grant) covers(key []byte) bool { return g.desc.Contains(key) }

// recoverFence bumps the grant's fence past whatever the register observed,
// single-flight per grant, reporting whether the fence moved (the caller may
// retry the fast path only if it did). Recovery always moves the fence
// strictly up, so a retried TakeOwnership can never self-conflict with
// residue from a failed attempt at the old fence. A lock no longer held
// (ErrNotHolder — session lapsed, another node took over) drops the grant.
func (m *Manager) recoverFence(ctx context.Context, g *grant, cause error) bool {
	g.mu.Lock()
	if g.bumping {
		g.mu.Unlock()
		return false // another writer is already recovering; just fall back
	}
	g.bumping = true
	minFence := g.fence + 1
	g.mu.Unlock()

	var behind *caspaxos.EpochBehindError
	if errors.As(cause, &behind) && behind.Observed+1 > minFence {
		minFence = behind.Observed + 1
	}

	tok, err := m.locks.Bump(ctx, lockName(g.rangeID), m.sessionID, minFence)

	g.mu.Lock()
	g.bumping = false
	bumped := err == nil && tok > g.fence
	if bumped {
		g.fence = tok
		m.bumps.Add(1)
		// Fencing evidence means SOMETHING committed around this grant's
		// caches — poison every key's fast path so the next op (read or
		// write) re-takes ownership and re-reads through phase 1. Poison IN
		// PLACE (Disown), never by replacing the keyOwner objects: a caller
		// mid-retry holds a reference, and a replacement object would later
		// self-conflict with whatever the retained one commits post-bump.
		for _, ko := range g.perKey {
			ko.op.Disown()
		}
	}
	g.mu.Unlock()

	if errors.Is(err, lease.ErrNotHolder) {
		m.dropGrant(g.rangeID)
	}
	return bumped
}

func (m *Manager) dropGrant(rangeID uint64) {
	m.mu.Lock()
	delete(m.grants, rangeID)
	m.mu.Unlock()
}

// Maintain reconciles grants against the current range map: renew the
// ownership session, acquire the lock for ranges where this node is the HRW
// owner hint, and release grants for ranges it no longer should (or can) own.
// Call it from the reconcile loop; it is never on the write path.
func (m *Manager) Maintain(ctx context.Context, rmap *ranges.Map) error {
	sess, err := m.sessions.Grant(ctx, m.sessionID, m.sessionID, m.sessionTTL)
	if err != nil {
		return fmt.Errorf("owner: session renew: %w", err)
	}
	// The read guard trusts only expiries a committed Grant confirmed.
	if sess.Expiry > m.sessionExpiry.Load() {
		m.sessionExpiry.Store(sess.Expiry)
	}

	descs := rmap.All()
	live := make(map[uint64]bool, len(descs))
	var firstErr error
	for _, d := range descs {
		live[d.ID] = true
		hint, ok := placement.Owner(ranges.RangeKey(d.ID), d.Replicas)
		eligible := ok && hint == m.nodeID

		m.mu.Lock()
		g, have := m.grants[d.ID]
		m.mu.Unlock()

		if have && (!eligible || g.rangeEpoch != d.Epoch) {
			m.releaseGrant(ctx, d.ID)
			have = false
		}
		if !have && eligible {
			if err := m.acquireGrant(ctx, d); err != nil && firstErr == nil && !errors.Is(err, lease.ErrHeld) {
				firstErr = err
			}
		}
	}
	// Ranges that vanished from the map entirely.
	m.mu.Lock()
	var gone []uint64
	for id := range m.grants {
		if !live[id] {
			gone = append(gone, id)
		}
	}
	m.mu.Unlock()
	for _, id := range gone {
		m.releaseGrant(ctx, id)
	}
	return firstErr
}

func (m *Manager) acquireGrant(ctx context.Context, d ranges.Descriptor) error {
	acc := make([]caspaxos.AcceptorClient, 0, len(d.Replicas))
	for _, n := range d.Replicas {
		a, ok := m.dialer.Acceptor(n)
		if !ok {
			return fmt.Errorf("owner: range %d replica %d not resolvable", d.ID, n)
		}
		acc = append(acc, a)
	}
	// Taker side of the double-sided read guard: a lapsed prior holder may,
	// on ITS clock, still be inside the serve window (it stops MaxOffset
	// early; we may be up to MaxOffset fast). Wait out the mirror margin
	// before taking over — a skipped tick here costs fast-path coverage,
	// never correctness. Writes need no such wait: they are epoch-fenced.
	if holder, live, _, err := m.locks.Owner(ctx, lockName(d.ID)); err == nil && holder != "" && holder != m.sessionID && !live {
		if prev, ok, err := m.sessions.Info(ctx, holder); err == nil && ok {
			if m.clock() < prev.Expiry+m.maxOffset {
				return nil // too soon; retry on a later Maintain tick
			}
		}
	}
	fence, err := m.locks.Acquire(ctx, lockName(d.ID), m.sessionID)
	if err != nil {
		return err // ErrHeld: someone else owns the range — not an error
	}
	g := &grant{
		rangeID:    d.ID,
		rangeEpoch: d.Epoch,
		desc:       d,
		acceptors:  acc,
		fence:      fence,
		perKey:     make(map[string]*keyOwner),
	}
	m.mu.Lock()
	m.grants[d.ID] = g
	m.mu.Unlock()
	return nil
}

func (m *Manager) releaseGrant(ctx context.Context, rangeID uint64) {
	m.dropGrant(rangeID)
	// Best-effort: an unreleased lock is taken over lazily once the session
	// lapses, so a failure here costs a successor some waiting, not safety.
	_ = m.locks.Release(ctx, lockName(rangeID), m.sessionID)
}
