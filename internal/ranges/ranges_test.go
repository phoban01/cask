package ranges

import (
	"bytes"
	"fmt"
	"testing"
)

func splits(ss ...string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

func TestStaticCoversKeyspace(t *testing.T) {
	nodes := []uint64{1, 2, 3, 4, 5}
	m := Static(splits("g", "n", "t"), nodes, 3)

	if got := len(m.All()); got != 4 {
		t.Fatalf("range count = %d, want 4", got)
	}
	// Every key must map to exactly one range, and it must contain the key.
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("%c%d", 'a'+i%26, i))
		d, ok := m.Lookup(key)
		if !ok {
			t.Fatalf("key %q covered by no range", key)
		}
		if !d.Contains(key) {
			t.Fatalf("Lookup returned range %d not containing %q", d.ID, key)
		}
		if len(d.Replicas) != 3 {
			t.Fatalf("range %d has %d replicas, want 3", d.ID, len(d.Replicas))
		}
	}
}

func TestLookupBoundaries(t *testing.T) {
	m := Static(splits("g", "n"), []uint64{1, 2, 3}, 3)
	cases := []struct {
		key  string
		want uint64 // range id
	}{
		{"", 1},   // -inf .. g
		{"a", 1},  //
		{"f", 1},  //
		{"g", 2},  // boundary is exclusive upper of range 1, inclusive lower of 2
		{"m", 2},  //
		{"n", 3},  // upper boundary -> next range
		{"z", 3},  // .. +inf
		{"zz", 3}, //
	}
	for _, c := range cases {
		d, ok := m.Lookup([]byte(c.key))
		if !ok {
			t.Fatalf("key %q not covered", c.key)
		}
		if d.ID != c.want {
			t.Fatalf("key %q -> range %d, want %d", c.key, d.ID, c.want)
		}
	}
}

func TestRangesDoNotOverlap(t *testing.T) {
	m := Static(splits("d", "k", "r"), []uint64{1, 2, 3, 4, 5}, 3)
	all := m.All()
	for i := 1; i < len(all); i++ {
		// Each range's Start must equal the previous range's End (contiguous).
		if !bytes.Equal(all[i].Start, all[i-1].End) {
			t.Fatalf("gap/overlap between range %d (end %q) and %d (start %q)",
				all[i-1].ID, all[i-1].End, all[i].ID, all[i].Start)
		}
	}
}

func TestSplitProducesAdjacentCoveringRanges(t *testing.T) {
	d := Descriptor{ID: 1, Start: []byte("g"), End: []byte("t"), Replicas: []uint64{1, 2, 3}, Epoch: 4}
	left, right, ok := d.Split([]byte("m"), 10, 11)
	if !ok {
		t.Fatal("split inside range should succeed")
	}
	if !bytes.Equal(left.Start, []byte("g")) || !bytes.Equal(left.End, []byte("m")) {
		t.Fatalf("left = [%q,%q), want [g,m)", left.Start, left.End)
	}
	if !bytes.Equal(right.Start, []byte("m")) || !bytes.Equal(right.End, []byte("t")) {
		t.Fatalf("right = [%q,%q), want [m,t)", right.Start, right.End)
	}
	// Halves are contiguous and together cover the original interval.
	if !bytes.Equal(left.End, right.Start) {
		t.Fatal("halves are not contiguous")
	}
	for _, k := range []string{"g", "h", "l", "m", "s"} {
		if d.Contains([]byte(k)) != (left.Contains([]byte(k)) || right.Contains([]byte(k))) {
			t.Fatalf("coverage mismatch for %q after split", k)
		}
	}
	// Replicas are inherited, independently.
	left.Replicas[0] = 99
	if right.Replicas[0] == 99 {
		t.Fatal("split halves must not alias replica slices")
	}
}

func TestSplitRejectsBoundary(t *testing.T) {
	d := Descriptor{ID: 1, Start: []byte("g"), End: []byte("t")}
	if _, _, ok := d.Split([]byte("g"), 2, 3); ok {
		t.Fatal("split at Start must be rejected")
	}
	if _, _, ok := d.Split([]byte("z"), 2, 3); ok {
		t.Fatal("split outside range must be rejected")
	}
}

func TestMergeAdjacent(t *testing.T) {
	left := Descriptor{ID: 1, Start: []byte("g"), End: []byte("m"), Replicas: []uint64{1, 2, 3}, Epoch: 4}
	right := Descriptor{ID: 2, Start: []byte("m"), End: []byte("t"), Replicas: []uint64{1, 2, 3}, Epoch: 6}
	m, ok := Merge(left, right, 9)
	if !ok {
		t.Fatal("adjacent merge should succeed")
	}
	if !bytes.Equal(m.Start, []byte("g")) || !bytes.Equal(m.End, []byte("t")) {
		t.Fatalf("merged = [%q,%q), want [g,t)", m.Start, m.End)
	}
	if m.Epoch != 7 { // max(4,6)+1
		t.Fatalf("merged epoch = %d, want 7", m.Epoch)
	}
	// Non-adjacent merge is rejected.
	gap := Descriptor{Start: []byte("u"), End: []byte("z")}
	if _, ok := Merge(left, gap, 9); ok {
		t.Fatal("non-adjacent merge must be rejected")
	}
}

func TestOwnerIsAReplica(t *testing.T) {
	nodes := []uint64{10, 20, 30, 40, 50}
	m := Static(splits("m"), nodes, 3)
	for _, d := range m.All() {
		owner, ok := d.Owner([]byte("mango"))
		if !d.Contains([]byte("mango")) {
			continue
		}
		if !ok {
			t.Fatal("expected an owner")
		}
		found := false
		for _, r := range d.Replicas {
			if r == owner {
				found = true
			}
		}
		if !found {
			t.Fatalf("owner %d is not among range replicas %v", owner, d.Replicas)
		}
	}
}
