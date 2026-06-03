package placement

import (
	"fmt"
	"testing"
)

func zonesOf(ids []uint64, nodes []Node) map[string]int {
	z := map[uint64]string{}
	for _, n := range nodes {
		z[n.ID] = n.Zone
	}
	out := map[string]int{}
	for _, id := range ids {
		out[z[id]]++
	}
	return out
}

func TestTargetSpreadsAcrossZones(t *testing.T) {
	// 3 zones, 2 nodes each; rf=3 must place one replica per zone.
	nodes := []Node{
		{1, "a"}, {2, "a"},
		{3, "b"}, {4, "b"},
		{5, "c"}, {6, "c"},
	}
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("r%d", i))
		repl := TargetReplicas(key, nodes, 3)
		if len(repl) != 3 {
			t.Fatalf("got %d replicas, want 3", len(repl))
		}
		zc := zonesOf(repl, nodes)
		if len(zc) != 3 {
			t.Fatalf("key %s: replicas span %d zones, want 3 (%v)", key, len(zc), repl)
		}
	}
}

func TestTargetBalancesWhenFewerZones(t *testing.T) {
	// 2 zones, rf=4 -> 2 per zone (no zone holds a quorum of 3).
	nodes := []Node{
		{1, "a"}, {2, "a"}, {3, "a"},
		{4, "b"}, {5, "b"}, {6, "b"},
	}
	repl := TargetReplicas([]byte("r"), nodes, 4)
	zc := zonesOf(repl, nodes)
	for z, c := range zc {
		if c > 2 {
			t.Fatalf("zone %s holds %d replicas, want <= 2", z, c)
		}
	}
}

func TestNeedsReconfigHysteresis(t *testing.T) {
	nodes := []Node{
		{1, "a"}, {2, "b"}, {3, "c"}, {4, "a"}, {5, "b"},
	}
	key := []byte("range-1")
	target := TargetReplicas(key, nodes, 3)

	// A healthy current assignment equal to the target needs no change.
	if _, need := NeedsReconfig(key, target, nodes, 3); need {
		t.Fatal("healthy placement should not reconfigure (hysteresis)")
	}

	// A different-but-healthy assignment with equal zone spread is left alone.
	healthyAlt := []uint64{1, 2, 3} // one per zone a,b,c
	if _, need := NeedsReconfig(key, healthyAlt, nodes, 3); need {
		t.Fatal("a healthy, well-spread placement should not thrash toward HRW target")
	}
}

func TestNeedsReconfigOnDeadReplica(t *testing.T) {
	nodes := []Node{
		{1, "a"}, {2, "b"}, {3, "c"},
	}
	// Current includes node 99, which is not a member.
	if _, need := NeedsReconfig([]byte("r"), []uint64{1, 2, 99}, nodes, 3); !need {
		t.Fatal("should reconfigure when a replica left the roster")
	}
}

func TestNeedsReconfigOnUnderReplication(t *testing.T) {
	nodes := []Node{
		{1, "a"}, {2, "b"}, {3, "c"}, {4, "a"},
	}
	if _, need := NeedsReconfig([]byte("r"), []uint64{1, 2}, nodes, 3); !need {
		t.Fatal("should reconfigure when under-replicated and capacity exists")
	}
}

func TestNeedsReconfigImprovesZoneSpread(t *testing.T) {
	nodes := []Node{
		{1, "a"}, {2, "a"}, {3, "b"}, {4, "c"},
	}
	// Current keeps two replicas in zone a (spans 2 zones); a 3-zone spread
	// exists, so reconfiguration is warranted.
	if _, need := NeedsReconfig([]byte("r"), []uint64{1, 2, 3}, nodes, 3); !need {
		t.Fatal("should reconfigure to improve failure-domain spread")
	}
}
