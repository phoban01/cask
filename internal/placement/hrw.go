// Package placement decides which nodes are responsible for a key or range.
// The default placement is computed locally with Highest-Random-Weight (HRW,
// a.k.a. rendezvous) hashing, so any node can derive the same answer from the
// membership set with no shared state. (The authoritative replica set of a
// range is later pinned in a consensus-managed descriptor; HRW seeds it and
// picks the per-key owner among the replicas.)
package placement

import (
	"hash/fnv"
	"sort"
)

// score is the rendezvous weight of (key, node): a hash of the two. The node
// with the highest score owns the key. Using a hash makes the mapping stable
// under membership changes — adding or removing a node only remaps the keys
// whose top node changed.
//
// The key is hashed once with FNV-1a, combined with the node id, then run
// through the SplitMix64 finalizer for good avalanche — FNV alone over short,
// structured keys leaves measurable bias across a small node set.
func score(key []byte, node uint64) uint64 {
	h := fnv.New64a()
	h.Write(key)
	x := h.Sum64() ^ (node * 0x9E3779B97F4A7C15)
	x ^= x >> 30
	x *= 0xBF58476D1CE4E5B9
	x ^= x >> 27
	x *= 0x94D049BB133111EB
	x ^= x >> 31
	return x
}

// Rank returns nodes ordered by descending HRW score for key (highest first).
// Ties — astronomically unlikely — break toward the smaller node id for
// determinism. The input slice is not modified.
func Rank(key []byte, nodes []uint64) []uint64 {
	out := append([]uint64(nil), nodes...)
	sort.Slice(out, func(i, j int) bool {
		si, sj := score(key, out[i]), score(key, out[j])
		if si != sj {
			return si > sj
		}
		return out[i] < out[j]
	})
	return out
}

// Owner returns the single node responsible for key (the top of Rank). It
// returns ok=false when nodes is empty.
func Owner(key []byte, nodes []uint64) (node uint64, ok bool) {
	if len(nodes) == 0 {
		return 0, false
	}
	best, bestScore := nodes[0], score(key, nodes[0])
	for _, n := range nodes[1:] {
		s := score(key, n)
		if s > bestScore || (s == bestScore && n < best) {
			best, bestScore = n, s
		}
	}
	return best, true
}

// Top returns the k highest-ranked nodes for key (fewer if len(nodes) < k) —
// the replica set for a range keyed by its range id.
func Top(key []byte, nodes []uint64, k int) []uint64 {
	ranked := Rank(key, nodes)
	if k < len(ranked) {
		ranked = ranked[:k]
	}
	return ranked
}
