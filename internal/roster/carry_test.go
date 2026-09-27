package roster

import (
	"context"
	"errors"
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
