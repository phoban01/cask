package placement

import (
	"fmt"
	"testing"
)

func TestOwnerMatchesRankHead(t *testing.T) {
	nodes := []uint64{1, 2, 3, 4, 5}
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		owner, ok := Owner(key, nodes)
		if !ok {
			t.Fatal("expected an owner")
		}
		if owner != Rank(key, nodes)[0] {
			t.Fatalf("Owner=%d disagrees with Rank head %d", owner, Rank(key, nodes)[0])
		}
	}
}

func TestDeterministic(t *testing.T) {
	nodes := []uint64{7, 3, 9, 1}
	key := []byte("stable")
	first := Rank(key, nodes)
	for i := 0; i < 10; i++ {
		got := Rank(key, nodes)
		for j := range first {
			if got[j] != first[j] {
				t.Fatalf("Rank not deterministic at %d: %v vs %v", j, got, first)
			}
		}
	}
}

// Removing a node must only remap keys it owned: every other key keeps its
// owner. This minimal-disruption property is why HRW is used for placement.
func TestRemovalOnlyRemapsOwnedKeys(t *testing.T) {
	full := []uint64{1, 2, 3, 4, 5}
	removed := uint64(3)
	reduced := []uint64{1, 2, 4, 5}

	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("k%d", i))
		before, _ := Owner(key, full)
		after, _ := Owner(key, reduced)
		if before != removed && before != after {
			t.Fatalf("key %q owner changed from %d to %d despite owner not removed", key, before, after)
		}
	}
}

func TestTopReturnsKReplicas(t *testing.T) {
	nodes := []uint64{1, 2, 3, 4, 5, 6, 7}
	top := Top([]byte("r42"), nodes, 5)
	if len(top) != 5 {
		t.Fatalf("Top len = %d, want 5", len(top))
	}
	// The first of Top must be the Owner.
	owner, _ := Owner([]byte("r42"), nodes)
	if top[0] != owner {
		t.Fatalf("Top[0]=%d != Owner=%d", top[0], owner)
	}
	// Asking for more than available returns all.
	if got := Top([]byte("r42"), nodes, 99); len(got) != len(nodes) {
		t.Fatalf("Top(k>n) len = %d, want %d", len(got), len(nodes))
	}
}

func TestDistributionRoughlyBalanced(t *testing.T) {
	nodes := []uint64{1, 2, 3, 4, 5}
	counts := map[uint64]int{}
	const n = 50000
	for i := 0; i < n; i++ {
		owner, _ := Owner([]byte(fmt.Sprintf("key-%d", i)), nodes)
		counts[owner]++
	}
	expected := n / len(nodes)
	for node, c := range counts {
		// Allow generous ±20% spread; HRW with fnv is well-distributed.
		if c < expected*8/10 || c > expected*12/10 {
			t.Fatalf("node %d got %d keys, expected ~%d (imbalanced)", node, c, expected)
		}
	}
}
