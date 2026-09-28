package roster

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/reconfig"
	"github.com/phoban01/cask/testutil/sim"
)

// removeHarness founds a roster on node 1 over a sim network of acceptors
// 1 to 5 (index 0 is unused) and grows the core to {1..5}. It returns a
// carry hook that moves keys the way the extension server does.
func removeHarness(t *testing.T, keys [][]byte) (*Roster, *sim.Network, CarryFunc) {
	t.Helper()
	ctx := context.Background()
	r, nw := reconfigHarness(t, 6, 1)
	if _, err := r.Founder(ctx, mem(1)); err != nil {
		t.Fatal(err)
	}
	for id := uint64(2); id <= 5; id++ {
		if _, err := r.Add(ctx, mem(id)); err != nil {
			t.Fatalf("add %d: %v", id, err)
		}
	}
	if _, err := r.Reconfigure(ctx, []uint64{1, 2, 3, 4, 5}); err != nil {
		t.Fatal(err)
	}
	carry := func(ctx context.Context, v Value) error {
		if v.Joint == nil {
			return nil
		}
		return reconfig.CarryForwardKeys(ctx, 1, keys, simClients(nw, v.Joint.Old), simClients(nw, v.Joint.New))
	}
	return r, nw, carry
}

func simClients(nw *sim.Network, ids []uint64) []caspaxos.AcceptorClient {
	out := make([]caspaxos.AcceptorClient, len(ids))
	for i, id := range ids {
		out[i] = nw.Client(int(id))
	}
	return out
}

// The second review of PR #117 found this. Remove dropped voters from the
// core without a joint change, so the carry hook never ran. Five keys
// commit on {3, 4, 5} while voters 1 and 2 hang. Removing 5 and then 4
// leaves core {1, 2, 3}. With voter 3 stopped, a read from node 1 must
// still see every key.
func TestReviewRemoveShrinkNoCarry(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# The voter set MUST change only by joint-consensus reconfiguration of the roster.
	ctx := context.Background()
	var keys [][]byte
	for i := range 5 {
		keys = append(keys, fmt.Appendf(nil, "data/%d", i))
	}
	r, nw, carry := removeHarness(t, keys)
	r.SetCarry(carry)

	nw.SetReachable(1, false)
	nw.SetReachable(2, false)
	data := caspaxos.NewProposer(1, simClients(nw, []uint64{1, 2, 3, 4, 5}))
	for _, k := range keys {
		want := append([]byte("v-"), k...)
		if _, err := data.Propose(ctx, k, func([]byte) ([]byte, error) { return want, nil }); err != nil {
			t.Fatalf("write %s: %v", k, err)
		}
	}
	nw.SetReachable(1, true)
	nw.SetReachable(2, true)

	for _, id := range []uint64{5, 4} {
		if _, err := r.Remove(ctx, id); err != nil {
			t.Fatalf("remove %d: %v", id, err)
		}
	}
	v, err := r.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !idsEqual(v.Core, []uint64{1, 2, 3}) || v.Joint != nil {
		t.Fatalf("core %v joint %+v, want [1 2 3] released", v.Core, v.Joint)
	}

	nw.SetReachable(3, false)
	read := caspaxos.NewProposer(1, simClients(nw, v.Core))
	for _, k := range keys {
		got, err := read.Propose(ctx, k, caspaxos.Identity)
		if err != nil {
			t.Fatalf("read %s: %v", k, err)
		}
		if want := append([]byte("v-"), k...); string(got) != string(want) {
			t.Errorf("read %s from node 1 = %q, want %q", k, got, want)
		}
	}
}

// With a carry hook set, removing a voter runs a joint change that carries.
// Removing a participant stays a plain member change.
func TestRemoveVoterRunsCarryParticipantDoesNot(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# The voter set MUST change only by joint-consensus reconfiguration of the roster.
	ctx := context.Background()
	r, _ := reconfigHarness(t, 4, 1)
	if _, err := r.Founder(ctx, mem(1)); err != nil {
		t.Fatal(err)
	}
	for id := uint64(2); id <= 3; id++ {
		if _, err := r.Add(ctx, mem(id)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Reconfigure(ctx, []uint64{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	var joints []Joint
	r.SetCarry(func(_ context.Context, v Value) error {
		joints = append(joints, *v.Joint)
		return nil
	})

	v, err := r.Remove(ctx, 3)
	if err != nil {
		t.Fatalf("remove voter 3: %v", err)
	}
	if !idsEqual(v.Core, []uint64{1, 2}) || v.Joint != nil || slices.Contains(memberIDs(v.Members), 3) {
		t.Fatalf("after remove 3: core %v joint %+v members %v", v.Core, v.Joint, memberIDs(v.Members))
	}
	if len(joints) != 1 || !idsEqual(joints[0].Old, []uint64{1, 2, 3}) || !idsEqual(joints[0].New, []uint64{1, 2}) {
		t.Fatalf("carry saw joints %+v, want one {[1 2 3] [1 2]}", joints)
	}

	// Node 4 joins as a participant and leaves by a plain member change.
	if _, err := r.Add(ctx, mem(4)); err != nil {
		t.Fatal(err)
	}
	before, err := r.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	v, err = r.Remove(ctx, 4)
	if err != nil {
		t.Fatalf("remove participant 4: %v", err)
	}
	if v.ConfigGen != before.ConfigGen || len(joints) != 1 || slices.Contains(memberIDs(v.Members), 4) {
		t.Fatalf("participant remove: cfg_gen %d -> %d, carries %d, members %v",
			before.ConfigGen, v.ConfigGen, len(joints), memberIDs(v.Members))
	}
}

// With a carry hook set, Remove refuses to take away the last voter.
func TestRemoveRefusesLastVoter(t *testing.T) {
	ctx := context.Background()
	r, _ := reconfigHarness(t, 2, 1)
	if _, err := r.Founder(ctx, mem(1)); err != nil {
		t.Fatal(err)
	}
	r.SetCarry(func(context.Context, Value) error { return nil })
	if _, err := r.Remove(ctx, 1); !errors.Is(err, ErrLastVoter) {
		t.Fatalf("remove last voter: err = %v, want %v", err, ErrLastVoter)
	}
	v, err := r.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !idsEqual(v.Core, []uint64{1}) || len(v.Members) != 1 {
		t.Fatalf("after refused remove: core %v members %v", v.Core, memberIDs(v.Members))
	}
}
