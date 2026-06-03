package lease_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/lease"
	"github.com/phoban01/cask/internal/reconfig"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/testutil/sim"
)

// fixture builds a 3-node cluster and a clock the test can advance.
func fixture() (*sim.Network, *int64) {
	stores := make([]caspaxos.Storage, 3)
	for i := range stores {
		stores[i] = store.NewMem()
	}
	return sim.NewNetwork(stores), new(int64)
}

func managers(nw *sim.Network, nodeID uint64, now *int64) (*lease.Sessions, *lease.Locks) {
	prop := caspaxos.NewProposer(nodeID, nw.Clients())
	ss := lease.NewSessions(prop, func() int64 { return atomic.LoadInt64(now) })
	return ss, lease.NewLocks(prop, ss)
}

func TestSingleHolder(t *testing.T) {
	ctx := context.Background()
	nw, now := fixture()
	atomic.StoreInt64(now, 1000)

	ssA, lkA := managers(nw, 1, now)
	ssB, lkB := managers(nw, 2, now)
	if _, err := ssA.Grant(ctx, "sA", "agentA", 100); err != nil {
		t.Fatal(err)
	}
	if _, err := ssB.Grant(ctx, "sB", "agentB", 100); err != nil {
		t.Fatal(err)
	}

	tok, err := lkA.Acquire(ctx, "res", "sA")
	if err != nil {
		t.Fatalf("A acquire: %v", err)
	}
	// B cannot take a lock held by A's live session.
	if _, err := lkB.Acquire(ctx, "res", "sB"); !errors.Is(err, lease.ErrHeld) {
		t.Fatalf("B acquire = %v, want ErrHeld", err)
	}

	// A's session lapses (no keepalive); B may now take over with a higher fence.
	atomic.StoreInt64(now, 1200)
	tok2, err := lkB.Acquire(ctx, "res", "sB")
	if err != nil {
		t.Fatalf("B takeover after expiry: %v", err)
	}
	if tok2 <= tok {
		t.Fatalf("takeover fence %d not greater than %d", tok2, tok)
	}
}

func TestFenceMonotone(t *testing.T) {
	ctx := context.Background()
	nw, now := fixture()
	atomic.StoreInt64(now, 1000)
	ss, lk := managers(nw, 1, now)
	for _, id := range []string{"s1", "s2", "s3"} {
		ss.Grant(ctx, id, id, 100)
	}

	var last uint64
	holders := []string{"s1", "s2", "s3", "s1", "s2"}
	for i, h := range holders {
		tok, err := lk.Acquire(ctx, "res", h)
		if err != nil {
			t.Fatalf("acquire %d by %s: %v", i, h, err)
		}
		if tok <= last {
			t.Fatalf("fence not monotone: %d after %d", tok, last)
		}
		last = tok
		if err := lk.Release(ctx, "res", h); err != nil {
			t.Fatal(err)
		}
	}
}

// One session keeps many locks alive: renewing the single session (not each
// lock) is enough, so keepalive load is O(agents), not O(locks).
func TestBatchedKeepaliveKeepsLocksAlive(t *testing.T) {
	ctx := context.Background()
	nw, now := fixture()
	atomic.StoreInt64(now, 1000)
	ss, lk := managers(nw, 1, now)
	ss.Grant(ctx, "agent", "agent", 100) // expiry 1100

	const locks = 10
	for i := 0; i < locks; i++ {
		if _, err := lk.Acquire(ctx, fmt.Sprintf("res-%d", i), "agent"); err != nil {
			t.Fatalf("acquire res-%d: %v", i, err)
		}
	}

	// Renew the SESSION once, past the original expiry.
	atomic.StoreInt64(now, 1050)
	if _, err := ss.KeepAlive(ctx, "agent", "agent", 100); err != nil { // expiry -> 1150
		t.Fatalf("keepalive: %v", err)
	}

	// At a time past the original expiry but within the renewed one, every lock
	// is still held by a live session — with no per-lock renewal.
	atomic.StoreInt64(now, 1120)
	for i := 0; i < locks; i++ {
		_, live, _, err := lk.Owner(ctx, fmt.Sprintf("res-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if !live {
			t.Fatalf("res-%d not live after single session keepalive", i)
		}
	}
}

// When a session lapses, the reaper frees all of its locks at once.
func TestReaperCascadeFreesDeadSessionLocks(t *testing.T) {
	ctx := context.Background()
	nw, now := fixture()
	atomic.StoreInt64(now, 1000)
	ss, lk := managers(nw, 1, now)
	ss.Grant(ctx, "agent", "agent", 100)

	names := []string{"a", "b", "c"}
	for _, n := range names {
		if _, err := lk.Acquire(ctx, n, "agent"); err != nil {
			t.Fatal(err)
		}
	}

	reaper := lease.NewReaper(lk, "_leader")

	// Session still live: reaping frees nothing.
	if freed, err := reaper.Reap(ctx, names); err != nil || len(freed) != 0 {
		t.Fatalf("reap while live freed %v (err %v), want none", freed, err)
	}

	// Session lapses: the reaper frees all of its locks.
	atomic.StoreInt64(now, 1200)
	freed, err := reaper.Reap(ctx, names)
	if err != nil {
		t.Fatal(err)
	}
	if len(freed) != len(names) {
		t.Fatalf("reaped %v, want all of %v", freed, names)
	}
	for _, n := range names {
		_, live, _, _ := lk.Owner(ctx, n)
		if live {
			t.Fatalf("lock %s still held after reaping", n)
		}
	}
}

// Soft-leadership: exactly one node holds the leader lock at a time.
func TestSoftLeaderElection(t *testing.T) {
	ctx := context.Background()
	nw, now := fixture()
	atomic.StoreInt64(now, 1000)
	ssA, lkA := managers(nw, 1, now)
	ssB, lkB := managers(nw, 2, now)
	ssA.Grant(ctx, "A", "A", 100)
	ssB.Grant(ctx, "B", "B", 100)

	ra := lease.NewReaper(lkA, "_leader")
	rb := lease.NewReaper(lkB, "_leader")

	gotA, err := ra.AcquireLeadership(ctx, "A")
	if err != nil || !gotA {
		t.Fatalf("A leadership = %v,%v", gotA, err)
	}
	gotB, err := rb.AcquireLeadership(ctx, "B")
	if err != nil {
		t.Fatal(err)
	}
	if gotB {
		t.Fatal("B became leader while A holds leadership")
	}
}

// Under clock skew, two agents can briefly disagree about whether a session has
// expired. The protocol does not pretend skew away — instead the FENCING token
// is the single source of truth: at most one agent can hold the latest token, so
// a fenced resource accepts only that holder and rejects the zombie. This is the
// guarantee that matters when wall clocks disagree.
func TestClockSkewFencing(t *testing.T) {
	ctx := context.Background()
	nw, _ := fixture()

	// Agent A's clock reads 1000; agent B's runs 200 ahead.
	var clkA, clkB int64 = 1000, 1200
	propA := caspaxos.NewProposer(1, nw.Clients())
	ssA := lease.NewSessions(propA, func() int64 { return clkA })
	lkA := lease.NewLocks(propA, ssA)
	propB := caspaxos.NewProposer(2, nw.Clients())
	ssB := lease.NewSessions(propB, func() int64 { return clkB })
	lkB := lease.NewLocks(propB, ssB)

	// A grants a session (expiry 1100 by A's clock) and takes the lock.
	ssA.Grant(ctx, "sA", "A", 100)
	tokA, err := lkA.Acquire(ctx, "res", "sA")
	if err != nil {
		t.Fatal(err)
	}

	// By B's faster clock, A's session already looks expired, so B takes over and
	// gets a strictly higher token — even though A still believes it holds.
	ssB.Grant(ctx, "sB", "B", 100)
	tokB, err := lkB.Acquire(ctx, "res", "sB")
	if err != nil {
		t.Fatalf("B acquire under skew: %v", err)
	}
	if tokB <= tokA {
		t.Fatalf("B token %d not greater than A token %d", tokB, tokA)
	}

	// The lock's current fence is B's; A's token is stale and a fenced resource
	// (accept only the highest token seen) rejects A.
	_, _, fence, err := lkA.Owner(ctx, "res")
	if err != nil {
		t.Fatal(err)
	}
	if fence != tokB {
		t.Fatalf("current fence = %d, want B's token %d", fence, tokB)
	}
	if tokA >= fence {
		t.Fatalf("stale holder A token %d is not below current fence %d", tokA, fence)
	}
}

// The fencing token is preserved and stays monotone across a range
// reconfiguration (replica-set move) — a zombie holder from before the move is
// still fenced out.
func TestFenceSurvivesReconfiguration(t *testing.T) {
	ctx := context.Background()
	stores := make([]caspaxos.Storage, 5)
	for i := range stores {
		stores[i] = store.NewMem()
	}
	nw := sim.NewNetwork(stores)
	var now int64 = 1000
	clk := func() int64 { return now }

	old := []caspaxos.AcceptorClient{nw.Client(0), nw.Client(1), nw.Client(2)}
	new := []caspaxos.AcceptorClient{nw.Client(2), nw.Client(3), nw.Client(4)}

	// Acquire and release on the OLD replica set; the fence is now 1, retained.
	ssOld := lease.NewSessions(caspaxos.NewProposer(1, old), clk)
	lkOld := lease.NewLocks(caspaxos.NewProposer(1, old), ssOld)
	ssOld.Grant(ctx, "s1", "s1", 100)
	tok1, err := lkOld.Acquire(ctx, "res", "s1")
	if err != nil {
		t.Fatal(err)
	}
	if err := lkOld.Release(ctx, "res", "s1"); err != nil {
		t.Fatal(err)
	}

	// Reconfigure the range: carry the lock register into the new replica set.
	if err := reconfig.CarryForward(ctx, 9, lease.LockKey("res"), old, new); err != nil {
		t.Fatalf("carry forward lock register: %v", err)
	}

	// On the NEW set a fresh acquire must mint a strictly higher fence — the
	// token survived the move and never went backwards.
	ssNew := lease.NewSessions(caspaxos.NewProposer(2, new), clk)
	lkNew := lease.NewLocks(caspaxos.NewProposer(2, new), ssNew)
	ssNew.Grant(ctx, "s2", "s2", 100)
	tok2, err := lkNew.Acquire(ctx, "res", "s2")
	if err != nil {
		t.Fatalf("acquire on new set: %v", err)
	}
	if tok2 <= tok1 {
		t.Fatalf("fence regressed across reconfiguration: %d then %d", tok1, tok2)
	}
}
