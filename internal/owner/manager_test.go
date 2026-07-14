package owner_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/agent"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/lease"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/owner"
	"github.com/phoban01/cask/internal/placement"
	"github.com/phoban01/cask/internal/ranges"
	"github.com/phoban01/cask/internal/store"
)

// counting wraps an acceptor client and tallies phase RPCs, so tests can
// assert the fast path's round shape (accept-only) on the wire.
type counting struct {
	inner    caspaxos.AcceptorClient
	prepares *atomic.Int64
	accepts  *atomic.Int64
}

func (c counting) Prepare(ctx context.Context, key []byte, b caspaxos.Ballot) (caspaxos.PrepareReply, error) {
	c.prepares.Add(1)
	return c.inner.Prepare(ctx, key, b)
}

func (c counting) Accept(ctx context.Context, key []byte, b caspaxos.Ballot, val []byte) (caspaxos.AcceptReply, error) {
	c.accepts.Add(1)
	return c.inner.Accept(ctx, key, b, val)
}

// harness wires the full production shape over 3 in-memory acceptors: a
// Router with the manager's fast path, control-plane sessions/locks, and an
// mvcc.KV — for the node HRW picks as the range-owner hint.
type harness struct {
	prepares, accepts atomic.Int64
	hint              uint64
	rmap              *ranges.Map
	dialer            agent.StaticDialer
	mgr               *owner.Manager
	kv                *mvcc.KV
	raw               []caspaxos.AcceptorClient // uncounted, for interlopers
}

func newHarness(t *testing.T, epoch uint64) *harness {
	t.Helper()
	h := &harness{}
	replicas := []uint64{0, 1, 2}
	h.raw = make([]caspaxos.AcceptorClient, 3)
	counted := make([]caspaxos.AcceptorClient, 3)
	h.dialer = agent.StaticDialer{}
	for i := range h.raw {
		h.raw[i] = caspaxos.NewAcceptor(store.NewMem())
		counted[i] = counting{inner: h.raw[i], prepares: &h.prepares, accepts: &h.accepts}
		h.dialer[uint64(i)] = counted[i]
	}
	hint, ok := placement.Owner(ranges.RangeKey(1), replicas)
	if !ok {
		t.Fatal("no HRW hint")
	}
	h.hint = hint
	h.rmap = ranges.NewMap([]ranges.Descriptor{{ID: 1, Replicas: replicas, Epoch: epoch}})

	// Control plane over a plain full proposer (recursion-free by design;
	// production routes it through the same router, guarded by the rown key
	// exclusion — TestControlPlaneViaRouter covers that shape).
	ctl := caspaxos.NewProposer(hint, counted)
	now := int64(1_000)
	sessions := lease.NewSessions(ctl, func() int64 { return now })
	locks := lease.NewLocks(ctl, sessions)

	h.mgr = owner.New(hint, h.dialer, sessions, locks)
	router := agent.NewRouter(hint, h.rmap, h.dialer, agent.WithFastPath(h.mgr))
	h.kv = mvcc.New(router, hlc.New(func() int64 { now++; return now }), hint)
	return h
}

// waitCount blocks until c reaches at least want — the straggler-immune way
// to assert wire shape: early-quorum return (W2) means a phase's last RPCs
// can land (or even start) after the call itself has returned, so a fixed
// sleep before a counter reset is a race. Expected totals are exact (3
// acceptors, no vote-dropping outside the sim), so waiting for the total is
// deterministic.
func (h *harness) waitCount(t *testing.T, c *atomic.Int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for c.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("counter stuck at %d, want %d", c.Load(), want)
		}
		time.Sleep(time.Millisecond)
	}
	if got := c.Load(); got != want {
		t.Fatalf("counter overshot: %d, want %d", got, want)
	}
}

// drainStable waits until both counters stop moving (five consecutive quiet
// 10ms windows) — for measurement windows whose preceding totals are not
// statically known (recovery dances involve bump rounds on the lock key).
func (h *harness) drainStable() {
	var last [2]int64
	stable := 0
	for stable < 5 {
		time.Sleep(10 * time.Millisecond)
		cur := [2]int64{h.prepares.Load(), h.accepts.Load()}
		if cur == last {
			stable++
		} else {
			stable, last = 0, cur
		}
	}
}

func (h *harness) maintain(t *testing.T) {
	t.Helper()
	if err := h.mgr.Maintain(context.Background(), h.rmap); err != nil {
		t.Fatalf("maintain: %v", err)
	}
}

// The headline property: after warm-up, an owned write is one accept round —
// zero prepares on the wire.
func TestFastPathWriteIsSingleAcceptRound(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1)
	h.maintain(t)
	if s := h.mgr.Stats(); s.Grants != 1 {
		t.Fatalf("grants = %d, want 1", s.Grants)
	}

	// Zero out Maintain's control-plane rounds (session grant, lock acquire)
	// once they have quiesced, then warm up: the first write pays the one-time
	// TakeOwnership prepare round (3 prepares) plus its accept round (3
	// accepts). Waiting for the exact totals keeps stragglers out of the
	// measured window.
	h.drainStable()
	h.prepares.Store(0)
	h.accepts.Store(0)
	if _, err := h.kv.Put(ctx, []byte("k"), []byte("v0")); err != nil {
		t.Fatalf("warm-up put: %v", err)
	}
	h.waitCount(t, &h.prepares, 3)
	h.waitCount(t, &h.accepts, 3)

	h.prepares.Store(0)
	h.accepts.Store(0)
	if _, err := h.kv.Put(ctx, []byte("k"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	h.waitCount(t, &h.accepts, 3) // exactly one accept round
	if p := h.prepares.Load(); p != 0 {
		t.Fatalf("owned write issued %d prepares, want 0 (1-RTT fast path)", p)
	}
	if s := h.mgr.Stats(); s.FastWrites == 0 {
		t.Fatal("stats recorded no fast writes")
	}
}

// A non-owner full proposer fences the owner; the next owned write falls back
// (and still succeeds via the full path), the manager bumps its fence, and
// the fast path resumes — with every write surviving in the chain.
func TestInterloperFencesThenFastPathRecovers(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1)
	h.maintain(t)
	key := []byte("k")

	if _, err := h.kv.Put(ctx, key, []byte("fast-1")); err != nil {
		t.Fatal(err)
	}

	// Interloper writes through the full path directly (raw clients, distinct
	// mvcc identity), fencing the owner out via the W0 epoch jump.
	interKV := mvcc.New(caspaxos.NewProposer(9, h.raw), hlc.New(func() int64 { return 5_000 }), 9)
	if _, err := interKV.Put(ctx, key, []byte("interloper")); err != nil {
		t.Fatal(err)
	}

	// The deposed owner's next write recovers inline: bump the fence, re-take,
	// and complete on the fast path — the write must succeed either way.
	if _, err := h.kv.Put(ctx, key, []byte("fast-2")); err != nil {
		t.Fatalf("post-fencing put: %v", err)
	}
	if s := h.mgr.Stats(); s.Bumps == 0 {
		t.Fatal("manager never bumped its fence after being fenced out")
	}

	if _, err := h.kv.Put(ctx, key, []byte("fast-3")); err != nil {
		t.Fatal(err)
	}
	// Recovery complete: warm owned writes are prepare-free again.
	h.drainStable()
	h.prepares.Store(0)
	h.accepts.Store(0)
	if _, err := h.kv.Put(ctx, key, []byte("fast-4")); err != nil {
		t.Fatal(err)
	}
	h.waitCount(t, &h.accepts, 3)
	if p := h.prepares.Load(); p != 0 {
		t.Fatalf("fast path did not recover: %d prepares on a warm owned write", p)
	}

	// No write was lost anywhere in the dance.
	chain, err := h.kv.History(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"fast-1", "interloper", "fast-2", "fast-3", "fast-4"}
	if len(chain.Versions) != len(want) {
		t.Fatalf("chain has %d versions, want %d", len(chain.Versions), len(want))
	}
	for i, w := range want {
		if string(chain.Versions[i].Value) != w {
			t.Fatalf("version %d = %q, want %q", i, chain.Versions[i].Value, w)
		}
	}
}

// CAS conflicts must be decided by the full path (the owner's local-cache
// verdict is not trustworthy until the W4 lease guard exists).
func TestCASConflictDecidedByFullPath(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1)
	h.maintain(t)
	key := []byte("k")

	if _, err := h.kv.Put(ctx, key, []byte("actual")); err != nil {
		t.Fatal(err)
	}
	h.drainStable()
	h.prepares.Store(0)
	if _, err := h.kv.CAS(ctx, key, []byte("wrong"), []byte("new")); !errors.Is(err, caspaxos.ErrConflict) {
		t.Fatalf("CAS = %v, want ErrConflict", err)
	}
	if p := h.prepares.Load(); p == 0 {
		t.Fatal("CAS conflict was decided by the owner cache alone (no full round ran)")
	}
	// A correct CAS still works (whichever path carried it).
	if _, err := h.kv.CAS(ctx, key, []byte("actual"), []byte("new")); err != nil {
		t.Fatalf("valid CAS: %v", err)
	}
}

// Nodes that are not the HRW hint acquire nothing and always fall through.
func TestNonHintNodeTakesNoGrant(t *testing.T) {
	h := newHarness(t, 1)
	other := (h.hint + 1) % 3
	ctl := caspaxos.NewProposer(other, []caspaxos.AcceptorClient{h.dialer[0], h.dialer[1], h.dialer[2]})
	sessions := lease.NewSessions(ctl, func() int64 { return 1 })
	mgr := owner.New(other, h.dialer, sessions, lease.NewLocks(ctl, sessions))
	if err := mgr.Maintain(context.Background(), h.rmap); err != nil {
		t.Fatalf("maintain: %v", err)
	}
	if s := mgr.Stats(); s.Grants != 0 {
		t.Fatalf("non-hint node holds %d grants, want 0", s.Grants)
	}
}

// A descriptor epoch change (reconfiguration) invalidates the grant; the next
// Maintain re-acquires under the new epoch with a strictly higher fence.
func TestDescriptorEpochChangeReacquires(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1)
	h.maintain(t)
	if _, err := h.kv.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	h.rmap = ranges.NewMap([]ranges.Descriptor{{ID: 1, Replicas: []uint64{0, 1, 2}, Epoch: 2}})
	h.maintain(t)
	if s := h.mgr.Stats(); s.Grants != 1 {
		t.Fatalf("grants = %d after epoch change, want 1 (re-acquired)", s.Grants)
	}
	// Writes still work end to end on the re-acquired grant.
	if _, err := h.kv.Put(ctx, []byte("k"), []byte("v2")); err != nil {
		t.Fatalf("put after re-acquire: %v", err)
	}
}

// lateProposer is the test's dynamicProposer: control-plane traffic bound to
// the fast-path router after construction, closing the value cycle
// (manager → locks → router → manager) the same way production does.
type lateProposer struct{ p lease.Proposer }

func (l *lateProposer) Propose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error) {
	return l.p.Propose(ctx, key, change)
}

// The production shape: control-plane sessions/locks routed through the SAME
// fast-path router the manager serves — the rown key exclusion must break the
// recursion (a lock op reaching FastPropose would otherwise re-enter the
// manager mid-acquisition).
func TestControlPlaneViaRouter(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1)

	lp := &lateProposer{}
	now := int64(1_000)
	sessions := lease.NewSessions(lp, func() int64 { return now })
	locks := lease.NewLocks(lp, sessions)
	mgr := owner.New(h.hint, h.dialer, sessions, locks)
	full := agent.NewRouter(h.hint, h.rmap, h.dialer, agent.WithFastPath(mgr))
	lp.p = full
	kv := mvcc.New(full, hlc.New(func() int64 { now++; return now }), h.hint)

	if err := mgr.Maintain(ctx, h.rmap); err != nil {
		t.Fatalf("maintain via fast-path router: %v", err)
	}
	if s := mgr.Stats(); s.Grants != 1 {
		t.Fatalf("grants = %d, want 1", s.Grants)
	}
	if _, err := kv.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := mgr.Maintain(ctx, h.rmap); err != nil {
		t.Fatalf("re-maintain (session renew) via fast-path router: %v", err)
	}
}
