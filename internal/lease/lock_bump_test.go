package lease_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/phoban01/cask/internal/lease"
)

// Bump raises the holder's fence to at least minFence, always strictly, and is
// holder-only; a post-bump takeover still mints a strictly higher token.
func TestLockBump(t *testing.T) {
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
		t.Fatalf("acquire: %v", err)
	}

	// Jump the fence past a synthetic epoch.
	bumped, err := lkA.Bump(ctx, "res", "sA", tok+6)
	if err != nil {
		t.Fatalf("bump: %v", err)
	}
	if bumped != tok+6 {
		t.Fatalf("bumped fence = %d, want %d", bumped, tok+6)
	}

	// A minFence at or below the current fence still bumps strictly.
	again, err := lkA.Bump(ctx, "res", "sA", 1)
	if err != nil {
		t.Fatalf("bump low minFence: %v", err)
	}
	if again != bumped+1 {
		t.Fatalf("second bump = %d, want %d", again, bumped+1)
	}

	// Holder-only: another session cannot bump.
	if _, err := lkB.Bump(ctx, "res", "sB", 100); !errors.Is(err, lease.ErrNotHolder) {
		t.Fatalf("non-holder bump = %v, want ErrNotHolder", err)
	}

	// A takeover after the holder lapses mints strictly above the bumped fence.
	atomic.StoreInt64(now, 1200)
	tok2, err := lkB.Acquire(ctx, "res", "sB")
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if tok2 != again+1 {
		t.Fatalf("takeover fence = %d, want %d", tok2, again+1)
	}

	// Bumping an unheld lock is ErrNotHolder.
	if err := lkB.Release(ctx, "res", "sB"); err != nil {
		t.Fatal(err)
	}
	if _, err := lkB.Bump(ctx, "res", "sB", 1); !errors.Is(err, lease.ErrNotHolder) {
		t.Fatalf("bump after release = %v, want ErrNotHolder", err)
	}
}
