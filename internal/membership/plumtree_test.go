package membership

import (
	"testing"
)

// ptOverlay drives a set of Plumtree nodes deterministically over the overlay
// the membership tests already build.
type ptOverlay struct {
	t     *testing.T
	trees map[ID]*Tree
	dead  map[ID]bool
	queue []TreeMessage
	steps int
}

// treesFrom builds a Plumtree node per live overlay node, peered on its active
// view (the overlay's symmetric active links).
func treesFrom(t *testing.T, o *overlay) *ptOverlay {
	pt := &ptOverlay{t: t, trees: map[ID]*Tree{}, dead: map[ID]bool{}}
	for id, node := range o.nodes {
		if o.dead[id] {
			continue
		}
		pt.trees[id] = NewTree(id, node.ActiveView())
	}
	return pt
}

func (pt *ptOverlay) enqueue(ms []TreeMessage) { pt.queue = append(pt.queue, ms...) }

func (pt *ptOverlay) run() {
	for len(pt.queue) > 0 {
		pt.steps++
		if pt.steps > 2_000_000 {
			pt.t.Fatal("plumtree did not converge")
		}
		m := pt.queue[0]
		pt.queue = pt.queue[1:]
		if pt.dead[m.To] {
			continue
		}
		tree, ok := pt.trees[m.To]
		if !ok {
			continue
		}
		pt.enqueue(tree.Receive(m))
	}
}

// settle delivers messages and repeatedly fires missing-message timeouts until
// the system is fully quiescent (no grafts produced).
func (pt *ptOverlay) settle() {
	pt.run()
	for round := 0; round < 50; round++ {
		var grafts []TreeMessage
		for id, tree := range pt.trees {
			if pt.dead[id] {
				continue
			}
			grafts = append(grafts, tree.FireMissing()...)
		}
		if len(grafts) == 0 {
			return
		}
		pt.enqueue(grafts)
		pt.run()
	}
}

func (pt *ptOverlay) kill(id ID) {
	pt.dead[id] = true
	delete(pt.trees, id)
	for _, tree := range pt.trees {
		tree.RemovePeer(id)
	}
}

func (pt *ptOverlay) live() []ID {
	var ids []ID
	for id := range pt.trees {
		ids = append(ids, id)
	}
	return ids
}

func (pt *ptOverlay) assertAllDelivered(id MsgID) {
	for nid, tree := range pt.trees {
		if !tree.Has(id) {
			pt.t.Fatalf("node %s did not deliver %s", nid, id)
		}
		// Delivered exactly once.
		count := 0
		for _, d := range tree.Delivered() {
			if d == id {
				count++
			}
		}
		if count != 1 {
			pt.t.Fatalf("node %s delivered %s %d times, want 1", nid, id, count)
		}
	}
}

func eagerEdgeCount(pt *ptOverlay) int {
	n := 0
	for _, tree := range pt.trees {
		n += len(tree.EagerPeers())
	}
	return n // counts each undirected edge twice
}

func TestReliableBroadcast(t *testing.T) {
	o := build(t, 30, DefaultConfig())
	pt := treesFrom(t, o)

	origin := pt.trees["node-0"]
	pt.enqueue(origin.Broadcast("m1", []byte("hello")))
	pt.settle()

	pt.assertAllDelivered("m1")
}

func TestBroadcastPrunesToSpanningTree(t *testing.T) {
	o := build(t, 30, DefaultConfig())
	pt := treesFrom(t, o)

	before := eagerEdgeCount(pt)
	pt.enqueue(pt.trees["node-0"].Broadcast("m1", []byte("a")))
	pt.settle()
	after := eagerEdgeCount(pt)

	// Pruning must shed redundant eager edges...
	if after >= before {
		t.Fatalf("expected pruning to reduce eager edges: before=%d after=%d", before, after)
	}
	// ...down toward a spanning tree: an undirected tree over k nodes has k-1
	// edges, counted twice here. Allow a little slack for in-flight asymmetry.
	live := len(pt.live())
	if after > 2*(live-1)+live {
		t.Fatalf("eager edges %d far exceed a spanning tree (~%d)", after, 2*(live-1))
	}

	// The pruned tree must still reach everyone on a second broadcast.
	pt.enqueue(pt.trees["node-0"].Broadcast("m2", []byte("b")))
	pt.settle()
	pt.assertAllDelivered("m2")
}

func TestGraftRepairsTreeAfterFailure(t *testing.T) {
	o := build(t, 30, DefaultConfig())
	pt := treesFrom(t, o)

	// First broadcast prunes the flood down to a spanning tree.
	pt.enqueue(pt.trees["node-0"].Broadcast("m1", []byte("a")))
	pt.settle()

	// Kill several interior nodes (not the origin), breaking tree branches.
	for _, id := range []ID{"node-5", "node-11", "node-17", "node-23"} {
		pt.kill(id)
	}

	// A new broadcast: eager branches through dead nodes are gone, so reliability
	// now depends on lazy IHAVE advertisements being grafted back.
	pt.enqueue(pt.trees["node-0"].Broadcast("m2", []byte("b")))
	pt.settle()

	pt.assertAllDelivered("m2")
}
