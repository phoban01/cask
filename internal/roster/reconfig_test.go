package roster

import (
	"context"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/testutil/sim"
)

// reconfigHarness wires a Roster over a sim network where node id == acceptor
// index, with a real proposer factory that resolves id-groups to acceptor links
// (single group -> ordinary CASPaxos, multiple -> joint consensus). This is the
// in-test analogue of cmd/cask's dialer-backed factory.
func reconfigHarness(t *testing.T, n int, self uint64) (*Roster, *sim.Network) {
	t.Helper()
	stores := make([]caspaxos.Storage, n)
	for i := range stores {
		stores[i] = store.NewMem()
	}
	nw := sim.NewNetwork(stores)
	mk := func(groups [][]uint64) Proposer {
		acl := make([][]caspaxos.AcceptorClient, len(groups))
		for g, ids := range groups {
			for _, id := range ids {
				acl[g] = append(acl[g], nw.Client(int(id)))
			}
		}
		if len(acl) == 1 {
			return caspaxos.NewProposer(self, acl[0])
		}
		return caspaxos.NewJointProposer(self, acl)
	}
	return New(self, mk), nw
}

func mem(id uint64) Member { return Member{NodeID: id, Addr: "10.42.0.x", Zone: "z"} }

func ids(v Value) []uint64 { return v.Core }

// A single --bootstrap node founds a one-member register whose Core is itself.
func TestFounderBootstrapsSingleton(t *testing.T) {
	ctx := context.Background()
	r, _ := reconfigHarness(t, 1, 0)

	v, err := r.Founder(ctx, mem(0))
	if err != nil {
		t.Fatal(err)
	}
	if v.Epoch != 1 || v.ConfigGen != 1 {
		t.Fatalf("founder: epoch=%d cfg_gen=%d; want 1,1", v.Epoch, v.ConfigGen)
	}
	if len(v.Members) != 1 || len(v.Core) != 1 || v.Core[0] != 0 {
		t.Fatalf("founder core/members = %v / %d members; want core [0], 1 member", v.Core, len(v.Members))
	}
	if v.Joint != nil {
		t.Fatalf("founder left a joint marker: %+v", v.Joint)
	}
}

// Growing the core 1 -> 3 -> 5 preserves the full membership across every
// transition, and after each release the value is readable against the new core
// alone.
func TestCoreGrows1to3to5(t *testing.T) {
	ctx := context.Background()
	r, _ := reconfigHarness(t, 5, 0)
	if _, err := r.Founder(ctx, mem(0)); err != nil {
		t.Fatal(err)
	}
	// Members join (value-only until promoted into the core).
	for id := uint64(1); id <= 4; id++ {
		if _, err := r.Add(ctx, mem(id)); err != nil {
			t.Fatalf("add %d: %v", id, err)
		}
	}

	// Grow to {0,1,2}.
	v, err := r.Reconfigure(ctx, []uint64{0, 1, 2})
	if err != nil {
		t.Fatal(err)
	}
	if got := ids(v); len(got) != 3 {
		t.Fatalf("after grow-to-3 core=%v; want 3 members", got)
	}
	if len(v.Members) != 5 {
		t.Fatalf("membership lost growing core: %d members", len(v.Members))
	}

	// Grow to the 3 highest ids {2,3,4}.
	v, err = r.Reconfigure(ctx, []uint64{2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	if got := ids(v); len(got) != 3 || got[0] != 2 || got[2] != 4 {
		t.Fatalf("after grow-to-{2,3,4} core=%v", got)
	}
	if v.Joint != nil {
		t.Fatalf("joint not cleared after release: %+v", v.Joint)
	}

	// Read against the NEW core only (a fresh handle that knows just {2,3,4}).
	reader, _ := reconfigHarnessShared(t, r)
	reader.AdoptCore([]uint64{2, 3, 4})
	rv, err := reader.Get(ctx)
	if err != nil {
		t.Fatalf("read on new core: %v", err)
	}
	if len(rv.Members) != 5 {
		t.Fatalf("new core lost membership: %d members", len(rv.Members))
	}
}

// A condemned core member is replaced: removal strips it from members and core,
// and the reconfiguration finalises onto a healthy node — never the dead one.
func TestCondemnedCoreMemberReplaced(t *testing.T) {
	ctx := context.Background()
	r, nw := reconfigHarness(t, 5, 0)
	if _, err := r.Founder(ctx, mem(0)); err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 4; id++ {
		if _, err := r.Add(ctx, mem(id)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Reconfigure(ctx, []uint64{2, 3, 4}); err != nil {
		t.Fatal(err)
	}

	// Node 2 (a core member) fails. The controller removes it and reconfigures.
	// Reconfiguration is driven by the highest-id core member (node 4), so run the
	// controller from a handle whose self is that driver.
	nw.SetReachable(2, false)
	driver := &Roster{self: 4, mk: r.mk, key: r.key}
	driver.AdoptCore([]uint64{2, 3, 4})
	ctrl := NewController(driver).EnableCoreReconfig(3)
	discovered := []Member{mem(0), mem(1), mem(3), mem(4)} // 2 is gone
	v, err := ctrl.Reconcile(ctx, discovered, []uint64{2})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range v.Members {
		if m.NodeID == 2 {
			t.Fatal("condemned node 2 still in membership")
		}
	}
	for _, id := range v.Core {
		if id == 2 {
			t.Fatalf("condemned node 2 still in core %v", v.Core)
		}
	}
	if len(v.Core) != 3 {
		t.Fatalf("core not refilled after replacement: %v", v.Core)
	}
}

// A membership Add committed DURING a joint phase survives the transition,
// because the joint-aware write issues it through the joint quorum.
func TestAddDuringJointSurvives(t *testing.T) {
	ctx := context.Background()
	r, _ := reconfigHarness(t, 6, 0)
	if _, err := r.Founder(ctx, mem(0)); err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 4; id++ {
		if _, err := r.Add(ctx, mem(id)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Reconfigure(ctx, []uint64{0, 1, 2}); err != nil {
		t.Fatal(err)
	}

	// Manually enter the joint phase {0,1,2} -> {2,3,4} and pause before release.
	v, err := r.publishJoint(ctx, []uint64{0, 1, 2}, []uint64{2, 3, 4}, mustGen(t, r, ctx))
	if err != nil {
		t.Fatalf("publish joint: %v", err)
	}
	if v.Joint == nil {
		t.Fatal("joint not published")
	}

	// Add a new member while the joint is in flight (uses the joint proposer).
	if _, err := r.Add(ctx, mem(5)); err != nil {
		t.Fatalf("add during joint: %v", err)
	}

	// Finalise the reconfiguration.
	out, err := r.finishJoint(ctx, v)
	if err != nil {
		t.Fatalf("finish joint: %v", err)
	}
	if out.Joint != nil {
		t.Fatalf("joint not cleared: %+v", out.Joint)
	}
	found := false
	for _, m := range out.Members {
		if m.NodeID == 5 {
			found = true
		}
	}
	if !found {
		t.Fatalf("member added during joint was lost: members=%v", out.Members)
	}
}

// A node whose believed core was fully reconfigured away (and is now
// unreachable) cannot read against the stale set, but recovers by adopting the
// current core from a discovery hint.
func TestStaleCoreRecoversViaAdopt(t *testing.T) {
	ctx := context.Background()
	r, nw := reconfigHarness(t, 5, 0)
	if _, err := r.Founder(ctx, mem(0)); err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 4; id++ {
		if _, err := r.Add(ctx, mem(id)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Reconfigure(ctx, []uint64{0, 1, 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconfigure(ctx, []uint64{2, 3, 4}); err != nil {
		t.Fatal(err)
	}

	// A stale reader believes the core is {0,1} and those nodes are now down.
	stale, _ := reconfigHarnessShared(t, r)
	nw.SetReachable(0, false)
	nw.SetReachable(1, false)
	stale.AdoptCore([]uint64{0, 1})
	if _, err := stale.Get(ctx); err == nil {
		t.Fatal("expected stale read against a dead core to fail")
	}

	// Recover: adopt the live core from a hint and read successfully.
	stale.AdoptCore([]uint64{2, 3, 4})
	v, err := stale.Get(ctx)
	if err != nil {
		t.Fatalf("recovered read: %v", err)
	}
	if len(v.Members) != 5 {
		t.Fatalf("recovered read lost membership: %d", len(v.Members))
	}
}

// reconfigHarnessShared returns a second Roster handle sharing the same sim
// network as r, modelling a different node reading/writing the same register.
func reconfigHarnessShared(t *testing.T, r *Roster) (*Roster, *sim.Network) {
	t.Helper()
	// r.mk already closes over the shared network; reuse it under a new id.
	return &Roster{self: 99, mk: r.mk, key: r.key}, nil
}

func mustGen(t *testing.T, r *Roster, ctx context.Context) uint64 {
	t.Helper()
	v, err := r.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return v.ConfigGen
}
