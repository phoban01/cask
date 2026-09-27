package lease_test

import (
	"context"
	"sync/atomic"
	"testing"
)

// An acquire by the current holder returns the current fence and does not
// mint a new one. The next acquire after a release mints a higher fence.
func TestAcquireByHolderReturnsSameFence(t *testing.T) {
	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# An acquire by the session that already holds the lock MUST return the current fence and MUST NOT mint a new one.
	ctx := context.Background()
	nw, now := fixture()
	atomic.StoreInt64(now, 1000)
	ss, lk := managers(nw, 1, now)
	if _, err := ss.Grant(ctx, "sA", "agentA", 100); err != nil {
		t.Fatal(err)
	}

	tok, err := lk.Acquire(ctx, "res", "sA")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	again, err := lk.Acquire(ctx, "res", "sA")
	if err != nil {
		t.Fatalf("re-entrant acquire: %v", err)
	}
	if again != tok {
		t.Fatalf("re-entrant acquire fence = %d, want %d", again, tok)
	}
	holder, live, fence, err := lk.Owner(ctx, "res")
	if err != nil {
		t.Fatal(err)
	}
	if holder != "sA" || !live || fence != tok {
		t.Fatalf("owner = %q live=%v fence=%d, want sA live fence=%d", holder, live, fence, tok)
	}

	if err := lk.Release(ctx, "res", "sA"); err != nil {
		t.Fatal(err)
	}
	next, err := lk.Acquire(ctx, "res", "sA")
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	if next != tok+1 {
		t.Fatalf("acquire after release fence = %d, want %d", next, tok+1)
	}
}
