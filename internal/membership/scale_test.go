package membership

import (
	"fmt"
	"testing"
)

// efficientComponent counts the connected component reachable from start using a
// precomputed undirected adjacency map — O(N·activeView) rather than the O(N²)
// reverse-edge scan used by the small tests, so it is usable at scale.
func efficientComponent(o *overlay, start ID) int {
	adj := map[ID]map[ID]bool{}
	add := func(a, b ID) {
		if adj[a] == nil {
			adj[a] = map[ID]bool{}
		}
		adj[a][b] = true
	}
	for id, n := range o.nodes {
		if o.dead[id] {
			continue
		}
		for _, nb := range n.ActiveView() {
			if !o.dead[nb] {
				add(id, nb)
				add(nb, id) // active links are symmetric
			}
		}
	}
	seen := map[ID]bool{start: true}
	stack := []ID{start}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for nb := range adj[cur] {
			if !seen[nb] {
				seen[nb] = true
				stack = append(stack, nb)
			}
		}
	}
	return len(seen)
}

// TestOverlayScalesToThousands is the M8 scale-validation gate. It demonstrates
// the property that lets cask reach 10k+ nodes: per-node view sizes stay bounded
// by the (constant) caps no matter how large the cluster grows — O(log N) state,
// not O(N) — while the overlay remains a single connected component.
func TestOverlayScalesToThousands(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test skipped in -short mode")
	}
	cfg := DefaultConfig()
	for _, n := range []int{1000, 3000} {
		o := build(t, n, cfg)
		// Joins alone leave a few stragglers at scale; the overlay reaches full
		// connectivity through its steady-state maintenance (shuffle spreads
		// passive views, heal refills active views from them).
		o.shuffleRounds(10)
		o.healRounds(10)

		maxActive, sumActive, maxPassive := 0, 0, 0
		for id, node := range o.nodes {
			if o.dead[id] {
				continue
			}
			a, p := len(node.ActiveView()), len(node.PassiveView())
			if a > maxActive {
				maxActive = a
			}
			if p > maxPassive {
				maxPassive = p
			}
			sumActive += a
		}

		// State per node is bounded by the caps, independent of N.
		if maxActive > cfg.ActiveSize {
			t.Fatalf("n=%d: max active view %d exceeds cap %d", n, maxActive, cfg.ActiveSize)
		}
		if maxPassive > cfg.PassiveSize {
			t.Fatalf("n=%d: max passive view %d exceeds cap %d", n, maxPassive, cfg.PassiveSize)
		}
		// And the average active view stays small (a handful), not growing with N.
		avg := float64(sumActive) / float64(n)
		if avg > float64(cfg.ActiveSize) {
			t.Fatalf("n=%d: average active view %.2f exceeds cap %d", n, avg, cfg.ActiveSize)
		}

		// The overlay is fully connected.
		if got := efficientComponent(o, "node-0"); got != n {
			t.Fatalf("n=%d: overlay not fully connected: component %d", n, got)
		}
		t.Logf("n=%d: connected; active view avg=%.2f max=%d; passive max=%d (caps %d/%d)",
			n, avg, maxActive, maxPassive, cfg.ActiveSize, cfg.PassiveSize)
	}
	_ = fmt.Sprint
}
