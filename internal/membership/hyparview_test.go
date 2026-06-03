package membership

import (
	"fmt"
	"testing"
)

// overlay is a deterministic, synchronous driver for a set of HyParView nodes.
// Messages are delivered FIFO to quiescence; messages to dead nodes are dropped.
type overlay struct {
	t     *testing.T
	cfg   Config
	nodes map[ID]*HyParView
	dead  map[ID]bool
	queue []Message
	steps int
}

func newOverlay(t *testing.T, cfg Config) *overlay {
	return &overlay{t: t, cfg: cfg, nodes: map[ID]*HyParView{}, dead: map[ID]bool{}}
}

func (o *overlay) add(id ID, seed int64) *HyParView {
	n := New(id, o.cfg, seed)
	o.nodes[id] = n
	return n
}

func (o *overlay) enqueue(msgs []Message) { o.queue = append(o.queue, msgs...) }

// run delivers queued messages until the overlay is quiescent.
func (o *overlay) run() {
	for len(o.queue) > 0 {
		o.steps++
		if o.steps > 2_000_000 {
			o.t.Fatal("overlay did not converge (possible message loop)")
		}
		m := o.queue[0]
		o.queue = o.queue[1:]
		if o.dead[m.To] {
			continue
		}
		n, ok := o.nodes[m.To]
		if !ok {
			continue
		}
		o.enqueue(n.Receive(m))
	}
}

func (o *overlay) live() []ID {
	var ids []ID
	for id := range o.nodes {
		if !o.dead[id] {
			ids = append(ids, id)
		}
	}
	return ids
}

// kill marks a node dead and notifies every live node so it drops the dead peer
// from its views (modelling the failure detector's eventual notification).
func (o *overlay) kill(id ID) {
	o.dead[id] = true
	for other, n := range o.nodes {
		if other == id || o.dead[other] {
			continue
		}
		o.enqueue(n.Down(id))
	}
}

// connectedComponentSize returns the size of the connected component (over the
// undirected active-link graph) reachable from start, among live nodes only.
func (o *overlay) connectedComponentSize(start ID) int {
	seen := map[ID]bool{start: true}
	stack := []ID{start}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		node := o.nodes[cur]
		// neighbours = anyone cur links to, or who links to cur (symmetric).
		neigh := map[ID]bool{}
		for _, a := range node.ActiveView() {
			neigh[a] = true
		}
		for other, n := range o.nodes {
			if o.dead[other] {
				continue
			}
			if contains(n.ActiveView(), cur) {
				neigh[other] = true
			}
		}
		for nb := range neigh {
			if !o.dead[nb] && !seen[nb] {
				seen[nb] = true
				stack = append(stack, nb)
			}
		}
	}
	return len(seen)
}

func (o *overlay) assertViewsBounded() {
	for id, n := range o.nodes {
		if o.dead[id] {
			continue
		}
		if got := len(n.ActiveView()); got > o.cfg.ActiveSize {
			o.t.Fatalf("node %s active view %d exceeds cap %d", id, got, o.cfg.ActiveSize)
		}
		if got := len(n.PassiveView()); got > o.cfg.PassiveSize {
			o.t.Fatalf("node %s passive view %d exceeds cap %d", id, got, o.cfg.PassiveSize)
		}
	}
}

// build joins n nodes through a single contact and returns the overlay.
func build(t *testing.T, n int, cfg Config) *overlay {
	o := newOverlay(t, cfg)
	contact := ID("node-0")
	o.add(contact, 0)
	for i := 1; i < n; i++ {
		id := ID(fmt.Sprintf("node-%d", i))
		node := o.add(id, int64(i))
		o.enqueue(node.Join(contact))
		o.run()
	}
	return o
}

func (o *overlay) shuffleRounds(rounds int) {
	for r := 0; r < rounds; r++ {
		for id, n := range o.nodes {
			if o.dead[id] {
				continue
			}
			o.enqueue(n.Shuffle())
		}
		o.run()
	}
}

func (o *overlay) healRounds(rounds int) {
	for r := 0; r < rounds; r++ {
		for id, n := range o.nodes {
			if o.dead[id] {
				continue
			}
			o.enqueue(n.Shuffle())
			o.enqueue(n.Heal())
		}
		o.run()
	}
}

func TestOverlayConnectsAndBounded(t *testing.T) {
	const n = 30
	o := build(t, n, DefaultConfig())

	o.assertViewsBounded()

	// Every node must have at least one active neighbour and the whole overlay
	// must be a single connected component.
	for id, node := range o.nodes {
		if len(node.ActiveView()) == 0 {
			t.Fatalf("node %s is isolated after join", id)
		}
	}
	if got := o.connectedComponentSize("node-0"); got != n {
		t.Fatalf("overlay not fully connected: component %d of %d", got, n)
	}
}

func TestOverlayConnectsVariousSizes(t *testing.T) {
	for _, n := range []int{5, 10, 20, 50, 80, 120} {
		o := build(t, n, DefaultConfig())
		o.assertViewsBounded()
		if got := o.connectedComponentSize("node-0"); got != n {
			t.Fatalf("n=%d: overlay not fully connected: component %d", n, got)
		}
	}
}

func TestShufflePopulatesPassiveViews(t *testing.T) {
	const n = 30
	o := build(t, n, DefaultConfig())
	o.shuffleRounds(8)

	o.assertViewsBounded()
	withPassive := 0
	for id, node := range o.nodes {
		if o.dead[id] {
			continue
		}
		if len(node.PassiveView()) > 0 {
			withPassive++
		}
	}
	// Shuffling should give the large majority of nodes a non-empty passive view
	// (their reserve for healing).
	if withPassive < n*8/10 {
		t.Fatalf("only %d/%d nodes have a passive view after shuffling", withPassive, n)
	}
}

func TestHealsAfterMassFailure(t *testing.T) {
	const (
		n    = 30
		kill = 9 // 30% of the cluster
	)
	o := build(t, n, DefaultConfig())
	o.shuffleRounds(8) // populate passive views (the reserve healing draws on)

	for i := 1; i <= kill; i++ { // keep node-0 as a stable reference
		o.kill(ID(fmt.Sprintf("node-%d", i)))
	}
	o.run()
	o.healRounds(10)

	o.assertViewsBounded()
	survivors := len(o.live())
	for _, id := range o.live() {
		if len(o.nodes[id].ActiveView()) == 0 {
			t.Fatalf("survivor %s is isolated after healing", id)
		}
	}
	if got := o.connectedComponentSize("node-0"); got != survivors {
		t.Fatalf("overlay not reconnected after failures: component %d of %d survivors", got, survivors)
	}
}
