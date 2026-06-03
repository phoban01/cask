package roster_test

import (
	"context"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/failure"
	"github.com/phoban01/cask/internal/placement"
	"github.com/phoban01/cask/internal/roster"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/testutil/sim"
)

func newRoster(rf int) *roster.Roster {
	stores := make([]caspaxos.Storage, rf)
	for i := range stores {
		stores[i] = store.NewMem()
	}
	nw := sim.NewNetwork(stores)
	return roster.New(caspaxos.NewProposer(1, nw.Clients()))
}

func member(id uint64) roster.Member {
	return roster.Member{NodeID: id, Addr: "10.0.0." + string(rune('0'+id)), Zone: "z"}
}

func TestGenesisThenAddRemove(t *testing.T) {
	ctx := context.Background()
	r := newRoster(3)

	v, err := r.Genesis(ctx, []roster.Member{member(1), member(2), member(3)})
	if err != nil {
		t.Fatal(err)
	}
	if v.Epoch != 1 || len(v.Members) != 3 {
		t.Fatalf("genesis = epoch %d, %d members; want 1,3", v.Epoch, len(v.Members))
	}

	// Genesis again must not clobber an existing roster.
	v2, _ := r.Genesis(ctx, []roster.Member{member(9)})
	if v2.Epoch != 1 || len(v2.Members) != 3 {
		t.Fatalf("second genesis clobbered roster: %+v", v2)
	}

	// Add bumps the epoch; re-adding is a no-op.
	v, _ = r.Add(ctx, member(4))
	if v.Epoch != 2 || len(v.Members) != 4 {
		t.Fatalf("after add: epoch %d, %d members; want 2,4", v.Epoch, len(v.Members))
	}
	v, _ = r.Add(ctx, member(4))
	if v.Epoch != 2 {
		t.Fatalf("re-add changed epoch to %d, want 2", v.Epoch)
	}

	// Remove bumps the epoch; removing an absent node is a no-op.
	v, _ = r.Remove(ctx, 2)
	if v.Epoch != 3 || len(v.Members) != 3 {
		t.Fatalf("after remove: epoch %d, %d members; want 3,3", v.Epoch, len(v.Members))
	}
	v, _ = r.Remove(ctx, 99)
	if v.Epoch != 3 {
		t.Fatalf("removing absent changed epoch to %d, want 3", v.Epoch)
	}
}

// The roster's membership is what HRW placement is computed over.
func TestRosterDrivesPlacement(t *testing.T) {
	ctx := context.Background()
	r := newRoster(3)
	if _, err := r.Genesis(ctx, []roster.Member{member(1), member(2), member(3), member(4), member(5)}); err != nil {
		t.Fatal(err)
	}
	ids, err := r.NodeIDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	owner, ok := placement.Owner([]byte("some-key"), ids)
	if !ok {
		t.Fatal("expected an owner from roster membership")
	}
	// Remove the owner from the roster; placement must move to a different node.
	if _, err := r.Remove(ctx, owner); err != nil {
		t.Fatal(err)
	}
	ids, _ = r.NodeIDs(ctx)
	newOwner, _ := placement.Owner([]byte("some-key"), ids)
	if newOwner == owner {
		t.Fatalf("owner %d still selected after removal from roster", owner)
	}
}

// The roster changes membership only on the cut detector's threshold-crossed
// decision, not on transient (sub-threshold) gossip suspicion.
func TestRosterStableUntilCutDecides(t *testing.T) {
	ctx := context.Background()
	r := newRoster(3)
	if _, err := r.Genesis(ctx, []roster.Member{member(1), member(2), member(3)}); err != nil {
		t.Fatal(err)
	}
	ctrl := roster.NewController(r)
	discovered := []roster.Member{member(1), member(2), member(3)}

	// Two of three observers suspect node 3 — below the quorum threshold.
	cut := failure.NewCutDetector(3)
	cut.Report("o1", "3", true)
	cut.Report("o2", "3", true)

	v, err := ctrl.Reconcile(ctx, discovered, downIDs(cut))
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Members) != 3 {
		t.Fatalf("roster shrank on sub-threshold suspicion: %d members", len(v.Members))
	}

	// The third observer agrees: now the cut is decided and the roster removes it.
	cut.Report("o3", "3", true)
	v, err = ctrl.Reconcile(ctx, discovered, downIDs(cut))
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Members) != 2 {
		t.Fatalf("roster did not remove condemned node: %d members", len(v.Members))
	}
	for _, m := range v.Members {
		if m.NodeID == 3 {
			t.Fatal("node 3 still present after cut decided")
		}
	}
}

// downIDs maps the cut detector's string subjects back to node ids for the
// controller (subjects here are the decimal node ids).
func downIDs(c *failure.CutDetector) []uint64 {
	var out []uint64
	for _, s := range c.Down() {
		if s == "3" {
			out = append(out, 3)
		}
	}
	return out
}
