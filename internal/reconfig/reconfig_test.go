package reconfig_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/reconfig"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/testutil/sim"
)

func clock() *hlc.Clock {
	var tick int64
	return hlc.New(func() int64 { return atomic.AddInt64(&tick, 1) })
}

// group returns the acceptor links for the given node indices.
func group(nw *sim.Network, idx ...int) []caspaxos.AcceptorClient {
	out := make([]caspaxos.AcceptorClient, len(idx))
	for i, n := range idx {
		out[i] = nw.Client(n)
	}
	return out
}

// A range moved old -> joint -> new keeps every committed value, and the value
// history written under the old set is fully readable on the new set.
func TestReconfigurationPreservesValues(t *testing.T) {
	ctx := context.Background()
	stores := make([]caspaxos.Storage, 5)
	for i := range stores {
		stores[i] = store.NewMem()
	}
	nw := sim.NewNetwork(stores)
	clk := clock()
	key := []byte("k")

	old := group(nw, 0, 1, 2)
	new := group(nw, 2, 3, 4) // overlaps on node 2

	// Commit v1 on the OLD replica set only.
	oldKV := mvcc.New(caspaxos.NewProposer(1, old), clk, 1)
	if _, err := oldKV.Put(ctx, key, []byte("v1")); err != nil {
		t.Fatalf("put v1 on old: %v", err)
	}

	// Enter the joint phase and commit v2 with a quorum in BOTH sets. This both
	// commits v2 and carries v1 forward into the new replicas.
	jointKV := mvcc.New(reconfig.JointProposer(2, old, new), clk, 2)
	if _, err := jointKV.Put(ctx, key, []byte("v2")); err != nil {
		t.Fatalf("put v2 joint: %v", err)
	}
	// Belt-and-braces catch-up before release (idempotent).
	if err := reconfig.CarryForward(ctx, 2, key, old, new); err != nil {
		t.Fatalf("carry forward: %v", err)
	}

	// Release: serve from the NEW replica set only.
	newKV := mvcc.New(caspaxos.NewProposer(3, new), clk, 3)
	got, found, err := newKV.Get(ctx, key)
	if err != nil || !found {
		t.Fatalf("get on new: %q found=%v err=%v", got, found, err)
	}
	if string(got) != "v2" {
		t.Fatalf("new set has %q, want v2 (latest preserved)", got)
	}
	// The full history (v1 then v2) survived the move.
	if v, ok, _ := newKV.GetAt(ctx, key, 1); !ok || string(v.Value) != "v1" {
		t.Fatalf("history lost across reconfiguration: seq1=%q,%v want v1", v.Value, ok)
	}
}

// Reconfiguration tolerates losing an old replica during the joint phase: the
// joint quorum is still reachable, and after release the value is intact on the
// new set (which excludes the failed node).
func TestReconfigurationUnderChurn(t *testing.T) {
	ctx := context.Background()
	stores := make([]caspaxos.Storage, 5)
	for i := range stores {
		stores[i] = store.NewMem()
	}
	nw := sim.NewNetwork(stores)
	clk := clock()
	key := []byte("k")

	old := group(nw, 0, 1, 2)
	new := group(nw, 2, 3, 4)

	oldKV := mvcc.New(caspaxos.NewProposer(1, old), clk, 1)
	if _, err := oldKV.Put(ctx, key, []byte("committed")); err != nil {
		t.Fatal(err)
	}

	// A node in the OLD set fails just as we reconfigure.
	nw.SetReachable(0, false)

	// Joint write still has a quorum: old {1,2} of {0,1,2}, new {2,3,4}.
	jointKV := mvcc.New(reconfig.JointProposer(2, old, new), clk, 2)
	if _, err := jointKV.Put(ctx, key, []byte("during")); err != nil {
		t.Fatalf("joint write under churn: %v", err)
	}

	// Release to the new set (which never included the failed node).
	newKV := mvcc.New(caspaxos.NewProposer(3, new), clk, 3)
	got, found, err := newKV.Get(ctx, key)
	if err != nil || !found || string(got) != "during" {
		t.Fatalf("value lost across reconfiguration under churn: %q,%v err=%v", got, found, err)
	}
}

// Many keys are all carried into the new set by a range reconfiguration.
func TestCarryForwardManyKeys(t *testing.T) {
	ctx := context.Background()
	stores := make([]caspaxos.Storage, 5)
	for i := range stores {
		stores[i] = store.NewMem()
	}
	nw := sim.NewNetwork(stores)
	clk := clock()
	old := group(nw, 0, 1, 2)
	new := group(nw, 2, 3, 4)

	oldKV := mvcc.New(caspaxos.NewProposer(1, old), clk, 1)
	var keys [][]byte
	want := map[string]string{}
	for i := 0; i < 50; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		v := fmt.Sprintf("val-%d", i)
		if _, err := oldKV.Put(ctx, k, []byte(v)); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
		want[string(k)] = v
	}

	if err := reconfig.CarryForwardKeys(ctx, 1, keys, old, new); err != nil {
		t.Fatalf("carry forward keys: %v", err)
	}

	newKV := mvcc.New(caspaxos.NewProposer(1, new), clk, 1)
	for k, v := range want {
		got, found, err := newKV.Get(ctx, []byte(k))
		if err != nil || !found || string(got) != v {
			t.Fatalf("key %q lost: %q,%v err=%v want %q", k, got, found, err, v)
		}
	}
}
