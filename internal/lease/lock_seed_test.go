package lease_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/phoban01/cask/internal/lease"
)

// A seeded lock continues the fences of the other system: the holder
// keeps the seeded fence, and the next acquisition mints above it.
func TestLockSeedHeldThenNextAcquireAbove(t *testing.T) {
	ctx := context.Background()
	nw, now := fixture()
	atomic.StoreInt64(now, 1000)
	ssA, lkA := managers(nw, 1, now)
	ssB, lkB := managers(nw, 2, now)

	if _, err := ssA.Grant(ctx, "sA", "sA", 100); err != nil {
		t.Fatal(err)
	}
	if err := lkA.Seed(ctx, "res", "sA", 7); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// A second seed with the same arguments changes nothing.
	if err := lkA.Seed(ctx, "res", "sA", 7); err != nil {
		t.Fatalf("second seed: %v", err)
	}
	if s, live, f, err := lkA.Owner(ctx, "res"); err != nil || s != "sA" || !live || f != 7 {
		t.Fatalf("owner = %q live=%v fence=%d err=%v, want sA live at 7", s, live, f, err)
	}
	// The holder's acquire returns the seeded fence and mints nothing.
	if f, err := lkA.Acquire(ctx, "res", "sA"); err != nil || f != 7 {
		t.Fatalf("re-entrant acquire = %d, %v; want 7", f, err)
	}

	if _, err := ssB.Grant(ctx, "sB", "sB", 100); err != nil {
		t.Fatal(err)
	}
	if _, err := lkB.Acquire(ctx, "res", "sB"); !errors.Is(err, lease.ErrHeld) {
		t.Fatalf("acquire while held = %v, want ErrHeld", err)
	}
	atomic.StoreInt64(now, 2000) // sA lapses
	f, err := lkB.Acquire(ctx, "res", "sB")
	if err != nil || f != 8 {
		t.Fatalf("takeover = %d, %v; want 8", f, err)
	}
}

// A released seed raises the fence with no holder. Seed never lowers a
// fence and never takes a lock from a holder.
func TestLockSeedReleasedAndConflicts(t *testing.T) {
	ctx := context.Background()
	nw, now := fixture()
	atomic.StoreInt64(now, 1000)
	ss, lk := managers(nw, 1, now)

	if err := lk.Seed(ctx, "free", "", 9); err != nil {
		t.Fatal(err)
	}
	if err := lk.Seed(ctx, "free", "", 4); err != nil {
		t.Fatalf("a lower released seed = %v, want no change", err)
	}
	if _, err := ss.Grant(ctx, "s", "s", 100); err != nil {
		t.Fatal(err)
	}
	if f, err := lk.Acquire(ctx, "free", "s"); err != nil || f != 10 {
		t.Fatalf("acquire after seed = %d, %v; want 10", f, err)
	}

	// The register is now held at 10. A held seed at 10 for another
	// session, a held seed below it, and any seed above it all conflict.
	for _, c := range []struct {
		session string
		fence   uint64
	}{{"other", 10}, {"s", 9}, {"", 11}} {
		if err := lk.Seed(ctx, "free", c.session, c.fence); !errors.Is(err, lease.ErrSeedConflict) {
			t.Errorf("seed %q at %d = %v, want ErrSeedConflict", c.session, c.fence, err)
		}
	}
	if s, _, f, err := lk.Owner(ctx, "free"); err != nil || s != "s" || f != 10 {
		t.Fatalf("owner = %q fence=%d err=%v, want s at 10", s, f, err)
	}
}

// A holder whose session lapsed is free for a seed that raises the fence,
// as it is for Acquire. The seed still never keeps or lowers the fence.
func TestLockSeedTakesOverLapsedHolderOnlyAbove(t *testing.T) {
	ctx := context.Background()
	nw, now := fixture()
	atomic.StoreInt64(now, 1000)
	ss, lk := managers(nw, 1, now)

	if _, err := ss.Grant(ctx, "old", "old", 100); err != nil {
		t.Fatal(err)
	}
	if err := lk.Seed(ctx, "res", "old", 3); err != nil {
		t.Fatal(err)
	}
	atomic.StoreInt64(now, 2000) // "old" lapses; nothing reaps its lock

	// Keeping or lowering the fence conflicts, even with a lapsed holder.
	for _, c := range []struct {
		session string
		fence   uint64
	}{{"new", 3}, {"new", 2}} {
		if err := lk.CheckSeed(ctx, "res", c.session, c.fence); !errors.Is(err, lease.ErrSeedConflict) {
			t.Errorf("check %q at %d = %v, want ErrSeedConflict", c.session, c.fence, err)
		}
		if err := lk.Seed(ctx, "res", c.session, c.fence); !errors.Is(err, lease.ErrSeedConflict) {
			t.Errorf("seed %q at %d = %v, want ErrSeedConflict", c.session, c.fence, err)
		}
	}
	if s, _, f, err := lk.Owner(ctx, "res"); err != nil || s != "old" || f != 3 {
		t.Fatalf("owner = %q fence=%d err=%v, want old at 3", s, f, err)
	}

	// A seed above the lapsed holder takes the lock.
	if _, err := ss.Grant(ctx, "new", "new", 100); err != nil {
		t.Fatal(err)
	}
	if err := lk.CheckSeed(ctx, "res", "new", 7); err != nil {
		t.Fatalf("check above a lapsed holder = %v", err)
	}
	if err := lk.Seed(ctx, "res", "new", 7); err != nil {
		t.Fatalf("seed above a lapsed holder = %v", err)
	}
	if s, live, f, err := lk.Owner(ctx, "res"); err != nil || s != "new" || !live || f != 7 {
		t.Fatalf("owner = %q live=%v fence=%d err=%v, want new live at 7", s, live, f, err)
	}
}

// CheckSeed agrees with Seed and writes nothing.
func TestLockCheckSeedWritesNothing(t *testing.T) {
	ctx := context.Background()
	nw, now := fixture()
	atomic.StoreInt64(now, 1000)
	ss, lk := managers(nw, 1, now)
	if _, err := ss.Grant(ctx, "s", "s", 100); err != nil {
		t.Fatal(err)
	}
	if err := lk.CheckSeed(ctx, "res", "s", 5); err != nil {
		t.Fatal(err)
	}
	if s, _, f, err := lk.Owner(ctx, "res"); err != nil || s != "" || f != 0 {
		t.Fatalf("CheckSeed wrote: owner = %q fence=%d err=%v", s, f, err)
	}
	if err := lk.Seed(ctx, "res", "s", 5); err != nil {
		t.Fatal(err)
	}
	if err := lk.CheckSeed(ctx, "res", "other", 5); !errors.Is(err, lease.ErrSeedConflict) {
		t.Fatalf("check another holder at the held fence = %v, want ErrSeedConflict", err)
	}
}
