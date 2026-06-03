// Package ranges models the storage tier's range-sharded, ordered keyspace.
// The keyspace is partitioned into contiguous key intervals; each range is a
// CASPaxos replica group. Because ranges are ordered, a key prefix lives in one
// or a few adjacent ranges — the property that makes prefix scans and watches
// cheap (versus hash sharding, which scatters a prefix across every shard).
//
// For M3 the partitioning is static. Dynamic split/merge and consensus-managed
// descriptors arrive in M5; the Descriptor shape (with Epoch) anticipates that.
package ranges

import (
	"bytes"
	"encoding/binary"
	"sort"

	"github.com/phoban01/cask/internal/placement"
)

// Descriptor describes one range: the half-open key interval [Start, End), the
// node ids replicating it, and a configuration epoch (bumped on reconfiguration).
type Descriptor struct {
	ID       uint64
	Start    []byte // inclusive lower bound; empty == unbounded below (-inf)
	End      []byte // exclusive upper bound; empty == unbounded above (+inf)
	Replicas []uint64
	Epoch    uint64
}

// Contains reports whether key falls in this range.
func (d Descriptor) Contains(key []byte) bool {
	if bytes.Compare(key, d.Start) < 0 {
		return false
	}
	if len(d.End) > 0 && bytes.Compare(key, d.End) >= 0 {
		return false
	}
	return true
}

// Owner returns the per-key owner among the range's replicas (HRW). The owner is
// a routing/latency hint, not required for safety.
func (d Descriptor) Owner(key []byte) (uint64, bool) {
	return placement.Owner(key, d.Replicas)
}

// Map is an immutable, ordered set of non-overlapping ranges covering the
// keyspace. Lookup is a binary search.
type Map struct {
	descs []Descriptor // sorted by Start ascending
}

// NewMap returns a Map over a copy of descs (sorted by Start).
func NewMap(descs []Descriptor) *Map {
	cp := append([]Descriptor(nil), descs...)
	sort.Slice(cp, func(i, j int) bool { return bytes.Compare(cp[i].Start, cp[j].Start) < 0 })
	return &Map{descs: cp}
}

// Lookup returns the range covering key.
func (m *Map) Lookup(key []byte) (Descriptor, bool) {
	// Find the last range whose Start is <= key, then confirm containment.
	i := sort.Search(len(m.descs), func(i int) bool {
		return bytes.Compare(m.descs[i].Start, key) > 0
	})
	if i == 0 {
		return Descriptor{}, false
	}
	if d := m.descs[i-1]; d.Contains(key) {
		return d, true
	}
	return Descriptor{}, false
}

// All returns a copy of the descriptors in key order.
func (m *Map) All() []Descriptor { return append([]Descriptor(nil), m.descs...) }

// RangeKey is the placement key for a range id (its 8-byte big-endian form), so
// replica selection is stable and independent of key contents.
func RangeKey(id uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], id)
	return b[:]
}

// Split divides d at key `at` into two adjacent ranges: [Start, at) and
// [at, End). The split point must lie strictly inside the range. Both halves
// inherit d's replicas and epoch and are given the supplied ids. Splitting
// keeps prefix locality (a prefix still lands in one or a few adjacent ranges).
func (d Descriptor) Split(at []byte, leftID, rightID uint64) (left, right Descriptor, ok bool) {
	if !d.Contains(at) || bytes.Equal(at, d.Start) {
		return Descriptor{}, Descriptor{}, false
	}
	left = Descriptor{ID: leftID, Start: d.Start, End: at, Replicas: cloneIDs(d.Replicas), Epoch: d.Epoch}
	right = Descriptor{ID: rightID, Start: at, End: d.End, Replicas: cloneIDs(d.Replicas), Epoch: d.Epoch}
	return left, right, true
}

// Merge combines two adjacent ranges (left.End == right.Start) into one
// [left.Start, right.End). The replicas of the merged range are taken from left
// (a real coordinator first reconfigures both halves onto a common set). The
// merged range takes the supplied id and the higher of the two epochs, bumped.
func Merge(left, right Descriptor, id uint64) (Descriptor, bool) {
	if len(left.End) == 0 || !bytes.Equal(left.End, right.Start) {
		return Descriptor{}, false
	}
	epoch := left.Epoch
	if right.Epoch > epoch {
		epoch = right.Epoch
	}
	return Descriptor{
		ID:       id,
		Start:    left.Start,
		End:      right.End,
		Replicas: cloneIDs(left.Replicas),
		Epoch:    epoch + 1,
	}, true
}

func cloneIDs(xs []uint64) []uint64 { return append([]uint64(nil), xs...) }

// Static builds a Map of len(splits)+1 ranges from the given exclusive split
// points (which must be sorted, ascending, non-empty). Each range is replicated
// onto the rf highest-HRW nodes for its id.
func Static(splits [][]byte, nodes []uint64, rf int) *Map {
	descs := make([]Descriptor, 0, len(splits)+1)
	for i := 0; i <= len(splits); i++ {
		var start, end []byte
		if i > 0 {
			start = splits[i-1]
		}
		if i < len(splits) {
			end = splits[i]
		}
		id := uint64(i + 1)
		descs = append(descs, Descriptor{
			ID:       id,
			Start:    start,
			End:      end,
			Replicas: placement.Top(RangeKey(id), nodes, rf),
			Epoch:    1,
		})
	}
	return NewMap(descs)
}
