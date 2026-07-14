package main

import (
	"context"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/roster"
	"github.com/phoban01/cask/internal/store"
)

// testDialer builds an overlayDialer backed by in-memory acceptors for the given
// members, so the routing/placement composition can be exercised without a live
// overlay. self uses the local acceptor; the rest are pre-resolved in the cache.
func testDialer(self uint64, members []roster.Member) (*overlayDialer, *caspaxos.Acceptor) {
	local := caspaxos.NewAcceptor(store.NewMem())
	d := newOverlayDialer(self, local, nil)
	for _, m := range members {
		d.addrs[m.NodeID] = m.Addr
		if m.NodeID != self {
			d.cache[m.NodeID] = caspaxos.NewAcceptor(store.NewMem())
		}
	}
	return d, local
}

// The self-forming composition routes a key to its placement-chosen replicas and
// reaches consensus through the dynamic proposer.
func TestPlacementRoutedConsensus(t *testing.T) {
	ctx := context.Background()
	members := []roster.Member{
		{NodeID: 1, Addr: "10.0.0.1:8001", Zone: "a"},
		{NodeID: 2, Addr: "10.0.0.2:8001", Zone: "b"},
		{NodeID: 3, Addr: "10.0.0.3:8001", Zone: "c"},
		{NodeID: 4, Addr: "10.0.0.4:8001", Zone: "a"},
		{NodeID: 5, Addr: "10.0.0.5:8001", Zone: "b"},
	}
	val := roster.Value{Epoch: 1, Members: members}

	// Placement must pick exactly RF replicas, all drawn from the membership.
	rmap := placeRange(val)
	desc, ok := rmap.Lookup([]byte("anything"))
	if !ok {
		t.Fatal("no range covers the keyspace")
	}
	if len(desc.Replicas) != replicationFactor {
		t.Fatalf("got %d replicas, want %d", len(desc.Replicas), replicationFactor)
	}
	present := map[uint64]bool{}
	for _, m := range members {
		present[m.NodeID] = true
	}
	for _, r := range desc.Replicas {
		if !present[r] {
			t.Fatalf("replica %d not in membership", r)
		}
	}

	// Drive consensus through the dynamic proposer over a dialer that resolves
	// every member. self is one of the placed replicas so the local acceptor is
	// genuinely in the quorum.
	self := desc.Replicas[0]
	dialer, _ := testDialer(self, members)
	dyn := &dynamicProposer{}
	dyn.set(routerFor(self, val, dialer, nil))

	if _, err := dyn.Propose(ctx, []byte("k"), caspaxos.Write([]byte("hello"))); err != nil {
		t.Fatalf("propose: %v", err)
	}
	got, err := dyn.Propose(ctx, []byte("k"), caspaxos.Identity)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("read = %q, want hello", got)
	}

	// Re-placing the range under a new epoch keeps reads consistent (same
	// replicas, bumped epoch invalidates the router's proposer cache).
	val.Epoch = 2
	dyn.set(routerFor(self, val, dialer, nil))
	got, err = dyn.Propose(ctx, []byte("k"), caspaxos.Identity)
	if err != nil || string(got) != "hello" {
		t.Fatalf("read after re-place = %q err=%v", got, err)
	}
}

// placement.TargetReplicas spreads across zones — sanity check the helper this
// binary relies on, over the same membership.
func TestPlaceRangeSpreadsZones(t *testing.T) {
	val := roster.Value{Epoch: 1, Members: []roster.Member{
		{NodeID: 1, Zone: "a"}, {NodeID: 2, Zone: "a"},
		{NodeID: 3, Zone: "b"}, {NodeID: 4, Zone: "c"},
	}}
	desc, _ := placeRange(val).Lookup([]byte("k"))
	zones := map[string]bool{}
	byID := map[uint64]string{1: "a", 2: "a", 3: "b", 4: "c"}
	for _, r := range desc.Replicas {
		zones[byID[r]] = true
	}
	if len(zones) < 3 {
		t.Fatalf("replicas %v span only zones %v, want 3", desc.Replicas, zones)
	}
}
