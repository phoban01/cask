package caspaxos_test

import (
	"context"
	"errors"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/store"
)

func ownedCluster(n int) []caspaxos.AcceptorClient {
	cs := make([]caspaxos.AcceptorClient, n)
	for i := range cs {
		cs[i] = caspaxos.NewAcceptor(store.NewMem())
	}
	return cs
}

// After taking ownership, writes commit with a single accept round and are
// really durable: an independent reader sees the latest value.
func TestOwnedFastPathWrites(t *testing.T) {
	ctx := context.Background()
	acc := ownedCluster(5)

	owner := caspaxos.NewOwnedProposer(1, acc)
	if err := owner.TakeOwnership(ctx, []byte("k"), 1); err != nil {
		t.Fatalf("take ownership: %v", err)
	}
	for _, v := range []string{"a", "b", "c"} {
		if _, err := owner.Write(ctx, []byte("k"), caspaxos.Write([]byte(v))); err != nil {
			t.Fatalf("fast-path write %q: %v", v, err)
		}
	}
	// An independent reader observes the latest committed value.
	reader := caspaxos.NewProposer(2, acc)
	got, err := reader.Propose(ctx, []byte("k"), caspaxos.Identity)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "c" {
		t.Fatalf("read = %q, want c", got)
	}
}

// The load-bearing safety property: when a newer-epoch owner takes over, the old
// owner's fast-path writes are rejected (epoch-fenced), so no update is lost.
func TestOwnedHandoffFencesStaleOwner(t *testing.T) {
	ctx := context.Background()
	acc := ownedCluster(5)
	key := []byte("k")

	o1 := caspaxos.NewOwnedProposer(1, acc)
	if err := o1.TakeOwnership(ctx, key, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := o1.Write(ctx, key, caspaxos.Write([]byte("v1"))); err != nil {
		t.Fatal(err)
	}

	// A new owner takes over at a higher epoch (the ownership fence advanced).
	o2 := caspaxos.NewOwnedProposer(2, acc)
	if err := o2.TakeOwnership(ctx, key, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := o2.Write(ctx, key, caspaxos.Write([]byte("v2"))); err != nil {
		t.Fatal(err)
	}

	// The old owner, unaware, tries another fast-path write. It must be fenced
	// out — not silently overwrite v2.
	if _, err := o1.Write(ctx, key, caspaxos.Write([]byte("v1-stale"))); !errors.Is(err, caspaxos.ErrLostOwnership) {
		t.Fatalf("stale owner write = %v, want ErrLostOwnership", err)
	}
	if o1.Owns() {
		t.Fatal("stale owner still believes it owns the key")
	}

	// The committed value is v2; v1-stale never took effect.
	reader := caspaxos.NewProposer(3, acc)
	got, _ := reader.Propose(ctx, key, caspaxos.Identity)
	if string(got) != "v2" {
		t.Fatalf("committed value = %q, want v2 (no lost update)", got)
	}
}

func TestOwnedWriteRequiresOwnership(t *testing.T) {
	ctx := context.Background()
	owner := caspaxos.NewOwnedProposer(1, ownedCluster(3))
	if _, err := owner.Write(ctx, []byte("k"), caspaxos.Write([]byte("x"))); !errors.Is(err, caspaxos.ErrNotOwner) {
		t.Fatalf("write without ownership = %v, want ErrNotOwner", err)
	}
}

// A new owner's phase-1 round carries the previous epoch's committed value
// forward, so the fast path never loses already-committed data.
func TestOwnedTakeOwnershipReadsCommittedValue(t *testing.T) {
	ctx := context.Background()
	acc := ownedCluster(5)
	key := []byte("k")

	o1 := caspaxos.NewOwnedProposer(1, acc)
	o1.TakeOwnership(ctx, key, 1)
	o1.Write(ctx, key, caspaxos.Write([]byte("kept")))

	o2 := caspaxos.NewOwnedProposer(2, acc)
	if err := o2.TakeOwnership(ctx, key, 5); err != nil {
		t.Fatal(err)
	}
	// o2 appends on top of what it read; a CAS proves it observed "kept".
	cas := func(cur []byte) ([]byte, error) {
		if string(cur) != "kept" {
			return nil, caspaxos.ErrConflict
		}
		return []byte("next"), nil
	}
	if _, err := o2.Write(ctx, key, cas); err != nil {
		t.Fatalf("new owner did not observe committed value: %v", err)
	}
}
