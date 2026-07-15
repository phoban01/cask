package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/agent"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/ranges"
	"github.com/phoban01/cask/internal/roster"
	"github.com/phoban01/cask/internal/store"
)

// The full §4.3 driver flow at the cmd layer: driveRanges seeds the genesis
// descriptor from placement, a membership change triggers a background
// reconfiguration, the data survives onto the new replica set, and the
// snapshot/fingerprint plumbing retargets routing.
func TestDriveRangesSeedsAndReconfigures(t *testing.T) {
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	// Data nodes 0-3 (0-2 initial members; 2 later replaced by 3), plus a
	// separate 3-node Core hosting the descriptor register.
	stores := map[uint64]*store.Mem{}
	dialer := agent.StaticDialer{}
	for id := uint64(0); id < 4; id++ {
		stores[id] = store.NewMem()
		dialer[id] = caspaxos.NewAcceptor(stores[id])
	}
	core := make([]caspaxos.AcceptorClient, 3)
	for i := range core {
		core[i] = caspaxos.NewAcceptor(store.NewMem())
	}
	dstore := ranges.NewStore(caspaxos.NewProposer(9, core))
	lister := func(_ context.Context, node uint64) ([][]byte, error) {
		s, ok := stores[node]
		if !ok {
			return nil, errors.New("no such node")
		}
		return s.Keys(context.Background())
	}
	orch := ranges.NewOrchestrator(9, dstore, dialer, lister, nil)

	members := func(ids ...uint64) roster.Value {
		v := roster.Value{Epoch: 1, Core: []uint64{0, 1, 2}}
		for _, id := range ids {
			v.Members = append(v.Members, roster.Member{NodeID: id})
		}
		return v
	}
	snap := newRosterSnap(0)
	var busy atomic.Bool

	// Tick 1: genesis — the descriptor is seeded from placement.
	cur := members(0, 1, 2)
	snap.store(cur)
	descs := driveRanges(ctx, log, dstore, orch, cur, replicationFactor, &busy)
	if len(descs) != 1 || len(descs[0].Replicas) != 3 || descs[0].Epoch != 1 {
		t.Fatalf("seeded descriptors = %+v, want one 3-replica epoch-1 descriptor", descs)
	}
	snap.storeDescs(descs)
	fpBefore := descFingerprint(snap, cur)

	// Commit data through descriptor-driven routing.
	now := int64(1)
	kv := mvcc.New(agent.NewRouter(0, rmapFromSnap(snap, cur), dialer), hlc.New(func() int64 { now++; return now }), 0)
	for i := range 5 {
		if _, err := kv.Put(ctx, fmt.Appendf(nil, "k%d", i), fmt.Appendf(nil, "v%d", i)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	// Tick 2: node 2 leaves, node 3 joins — placement demands a reconfig,
	// which driveRanges runs single-flight in the background.
	cur = members(0, 1, 3)
	cur.Epoch = 2
	snap.store(cur)
	driveRanges(ctx, log, dstore, orch, cur, replicationFactor, &busy)

	deadline := time.Now().Add(10 * time.Second)
	var released ranges.State
	for {
		st, ok, err := dstore.Get(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		if ok && st.Joint == nil && st.Epoch > 1 {
			released = st
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("reconfiguration never completed; state = %+v", st)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !containsID(released.Replicas, 3) || containsID(released.Replicas, 2) {
		t.Fatalf("released replicas = %v, want node 3 in and node 2 out", released.Replicas)
	}

	// Ticks after: the snapshot's descriptors refresh, the fingerprint moves,
	// and a router built from the new map serves ALL the data.
	descs = driveRanges(ctx, log, dstore, orch, cur, replicationFactor, &busy)
	snap.storeDescs(descs)
	if fp := descFingerprint(snap, cur); fp == fpBefore {
		t.Fatal("descriptor fingerprint did not change across the reconfiguration")
	}
	reader := mvcc.New(agent.NewRouter(1, rmapFromSnap(snap, cur), dialer), hlc.New(func() int64 { now++; return now }), 1)
	for i := range 5 {
		got, found, err := reader.Get(ctx, fmt.Appendf(nil, "k%d", i))
		if err != nil || !found || string(got) != fmt.Sprintf("v%d", i) {
			t.Fatalf("key %d after reconfiguration = %q found=%v err=%v (lost)", i, got, found, err)
		}
	}
}

func containsID(ids []uint64, want uint64) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}
