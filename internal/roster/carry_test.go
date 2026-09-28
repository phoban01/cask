package roster

import (
	"context"
	"errors"
	"slices"
	"testing"
)

// The carry hook runs once per core change, in the joint phase, before the
// release. A failed carry stops the release, so the old core stays in
// charge.
func TestReconfigureRunsCarryBeforeRelease(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# A core change MUST carry every data register forward to the new core before it releases the old core.
	ctx := context.Background()
	r, _ := reconfigHarness(t, 3, 0)
	if _, err := r.Founder(ctx, mem(0)); err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 2; id++ {
		if _, err := r.Add(ctx, mem(id)); err != nil {
			t.Fatalf("add %d: %v", id, err)
		}
	}

	failCarry := errors.New("carry failed")
	var calls []Value
	r.SetCarry(func(ctx context.Context, v Value) error {
		calls = append(calls, v)
		// The register still holds the joint marker: no release yet.
		cur, err := r.Get(ctx)
		if err != nil {
			return err
		}
		if cur.Joint == nil {
			t.Errorf("carry ran after the release: core %v", cur.Core)
		}
		if len(calls) == 1 {
			return failCarry
		}
		return nil
	})

	if _, err := r.Reconfigure(ctx, []uint64{0, 1, 2}); !errors.Is(err, failCarry) {
		t.Fatalf("reconfigure with a failed carry: err = %v, want %v", err, failCarry)
	}
	cur, err := r.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cur.Joint == nil || !idsEqual(cur.Core, []uint64{0}) {
		t.Fatalf("after a failed carry: core %v joint %+v, want core [0] still joint", cur.Core, cur.Joint)
	}

	// A retry resumes the joint change, carries, and releases.
	v, err := r.Reconfigure(ctx, []uint64{0, 1, 2})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !idsEqual(v.Core, []uint64{0, 1, 2}) || v.Joint != nil {
		t.Fatalf("after resume: core %v joint %+v, want [0 1 2] released", v.Core, v.Joint)
	}
	if len(calls) != 2 {
		t.Fatalf("carry ran %d times, want 2", len(calls))
	}
	for _, c := range calls {
		if c.Joint == nil || !idsEqual(c.Joint.Old, []uint64{0}) || !idsEqual(c.Joint.New, []uint64{0, 1, 2}) {
			t.Fatalf("carry got joint %+v, want {[0] [0 1 2]}", c.Joint)
		}
	}
}

// A Remove during a joint phase bumps ConfigGen when it changes the joint,
// so every cached quorum is rebuilt. It keeps a node of Joint.Old in
// Members, so members can still resolve its address for the old quorum.
func TestRemoveDuringJointKeepsOldQuorumResolvable(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# During a core change, a data write MUST reach a quorum of both the old and the new core.
	ctx := context.Background()
	r, _ := reconfigHarness(t, 5, 0)
	if _, err := r.Founder(ctx, mem(0)); err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 4; id++ {
		if _, err := r.Add(ctx, mem(id)); err != nil {
			t.Fatalf("add %d: %v", id, err)
		}
	}
	if _, err := r.Reconfigure(ctx, []uint64{0, 1, 2}); err != nil {
		t.Fatal(err)
	}
	// Stop the next change in its joint phase.
	stop := errors.New("stop in joint")
	r.SetCarry(func(context.Context, Value) error { return stop })
	if _, err := r.Reconfigure(ctx, []uint64{0, 1, 2, 3, 4}); !errors.Is(err, stop) {
		t.Fatalf("reconfigure: %v", err)
	}
	before, err := r.Get(ctx)
	if err != nil || before.Joint == nil {
		t.Fatalf("want a joint roster, got %+v, %v", before, err)
	}

	// Node 4 is only in Joint.New: it leaves Members and Joint.New.
	v, err := r.Remove(ctx, 4)
	if err != nil {
		t.Fatal(err)
	}
	if v.ConfigGen <= before.ConfigGen {
		t.Fatalf("ConfigGen %d after a Joint.New change, want > %d", v.ConfigGen, before.ConfigGen)
	}
	if slices.Contains(memberIDs(v.Members), 4) || slices.Contains(v.Joint.New, 4) {
		t.Fatalf("node 4 still present: members %v joint %+v", memberIDs(v.Members), v.Joint)
	}

	// Node 1 is in Joint.Old: it stays in Members until the release.
	v, err = r.Remove(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(memberIDs(v.Members), 1) {
		t.Fatalf("node 1 of Joint.Old left Members during the joint phase: %v", memberIDs(v.Members))
	}
	if slices.Contains(v.Joint.New, 1) {
		t.Fatalf("node 1 is still in Joint.New: %v", v.Joint.New)
	}

	// After the release, Remove drops it.
	r.SetCarry(nil)
	if v, err = r.Reconfigure(ctx, v.Joint.New); err != nil || v.Joint != nil {
		t.Fatalf("release: %+v, %v", v, err)
	}
	if v, err = r.Remove(ctx, 1); err != nil || slices.Contains(memberIDs(v.Members), 1) {
		t.Fatalf("remove after release: members %v, %v", memberIDs(v.Members), err)
	}
}
