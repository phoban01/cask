package ranges_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/phoban01/cask/internal/agent"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/ranges"
	"github.com/phoban01/cask/internal/store"
)

// fixture: six data nodes (0-2 the old replica set, 3-5 the new), a separate
// three-node Core hosting the descriptor register, and a KeyLister reading
// each node's store directly.
type reconfigFixture struct {
	stores map[uint64]*store.Mem
	dialer agent.StaticDialer
	rstore *ranges.Store
	orch   *ranges.Orchestrator
	now    int64
}

func newReconfigFixture(t *testing.T, settle func(ctx context.Context) error) *reconfigFixture {
	t.Helper()
	f := &reconfigFixture{stores: map[uint64]*store.Mem{}, dialer: agent.StaticDialer{}, now: 1}
	for id := uint64(0); id < 6; id++ {
		f.stores[id] = store.NewMem()
		f.dialer[id] = caspaxos.NewAcceptor(f.stores[id])
	}
	core := make([]caspaxos.AcceptorClient, 3)
	for i := range core {
		core[i] = caspaxos.NewAcceptor(store.NewMem())
	}
	f.rstore = ranges.NewStore(caspaxos.NewProposer(99, core))

	lister := func(_ context.Context, node uint64) ([][]byte, error) {
		s, ok := f.stores[node]
		if !ok {
			return nil, errors.New("no such node")
		}
		return s.Keys(context.Background())
	}
	f.orch = ranges.NewOrchestrator(99, f.rstore, f.dialer, lister, settle)
	return f
}

// routerNow builds a router whose placement reflects the descriptor register
// RIGHT NOW (what a refreshed client sees), with §4.1 refresh wired to the
// register too.
func (f *reconfigFixture) routerNow(t *testing.T, id uint64) *mvcc.KV {
	t.Helper()
	fetch := func() *ranges.Map {
		s, ok, err := f.rstore.Get(context.Background(), 1)
		if err != nil || !ok {
			return nil
		}
		return ranges.NewMap([]ranges.Descriptor{s.Descriptor})
	}
	r := agent.NewRouter(id, fetch(), f.dialer, agent.WithRefresh(fetch))
	return mvcc.New(r, hlc.New(func() int64 { f.now++; return f.now }), id)
}

// The release-blocking property: every committed write — pre-joint and
// during-joint — survives a replica-set move onto a DISJOINT set, and is
// served by the new replicas alone after release.
func TestReconfigCarriesAllData(t *testing.T) {
	ctx := context.Background()
	var jointWriteErr error
	var f *reconfigFixture
	// settle models the routing-lease wait; here a refreshed writer commits
	// DURING the joint phase — the migration must not lose it.
	settle := func(ctx context.Context) error {
		kv := f.routerNow(t, 7)
		_, jointWriteErr = kv.Put(ctx, []byte("k-joint"), []byte("written-mid-migration"))
		return nil
	}
	f = newReconfigFixture(t, settle)

	if _, err := f.orch.Seed(ctx, ranges.Descriptor{ID: 1, Replicas: []uint64{0, 1, 2}, Epoch: 1}); err != nil {
		t.Fatal(err)
	}

	// Commit data on the old set.
	writer := f.routerNow(t, 6)
	for i := range 10 {
		key := fmt.Appendf(nil, "k%d", i)
		if _, err := writer.Put(ctx, key, fmt.Appendf(nil, "v%d", i)); err != nil {
			t.Fatalf("seed put %d: %v", i, err)
		}
	}

	released, err := f.orch.ReconfigReplicas(ctx, 1, []uint64{3, 4, 5})
	if err != nil {
		t.Fatalf("reconfig: %v", err)
	}
	if jointWriteErr != nil {
		t.Fatalf("joint-era write failed: %v", jointWriteErr)
	}
	if released.Joint != nil || !equal(released.Replicas, []uint64{3, 4, 5}) {
		t.Fatalf("released state = %+v, want replicas [3 4 5], no joint", released)
	}

	// A refreshed reader — routing ONLY to the new set — sees everything.
	reader := f.routerNow(t, 8)
	for i := range 10 {
		key := fmt.Appendf(nil, "k%d", i)
		got, found, err := reader.Get(ctx, key)
		if err != nil || !found || string(got) != fmt.Sprintf("v%d", i) {
			t.Fatalf("key %d after release = %q found=%v err=%v (lost in migration)", i, got, found, err)
		}
	}
	got, found, err := reader.Get(ctx, []byte("k-joint"))
	if err != nil || !found || string(got) != "written-mid-migration" {
		t.Fatalf("joint-era key after release = %q found=%v err=%v (lost in migration)", got, found, err)
	}
}

// An in-flight joint from a crashed driver is completed toward ITS target
// before any new move is considered — resumability.
func TestReconfigResumesInFlightJoint(t *testing.T) {
	ctx := context.Background()
	f := newReconfigFixture(t, nil)
	if _, err := f.orch.Seed(ctx, ranges.Descriptor{ID: 1, Replicas: []uint64{0, 1, 2}, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	writer := f.routerNow(t, 6)
	if _, err := writer.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	// A "crashed driver" published the joint but never carried or released.
	if _, err := f.rstore.Publish(ctx, 1, func(s ranges.State, _ bool) (ranges.State, error) {
		s.Joint = &ranges.ReplicaJoint{Old: []uint64{0, 1, 2}, New: []uint64{3, 4, 5}}
		s.Epoch++
		return s, nil
	}); err != nil {
		t.Fatal(err)
	}

	// The successor asks for a DIFFERENT target; the in-flight joint wins.
	released, err := f.orch.ReconfigReplicas(ctx, 1, []uint64{0, 1, 5})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !equal(released.Replicas, []uint64{3, 4, 5}) {
		t.Fatalf("resumed onto %v, want the in-flight target [3 4 5]", released.Replicas)
	}
	got, found, err := f.routerNow(t, 8).Get(ctx, []byte("k"))
	if err != nil || !found || string(got) != "v" {
		t.Fatalf("key after resume = %q found=%v err=%v", got, found, err)
	}
}

// Key enumeration needs only a MAJORITY of old replicas — one dead lister is
// tolerated; a dead majority fails loudly rather than migrating partially.
func TestReconfigKeyEnumerationMajority(t *testing.T) {
	ctx := context.Background()

	run := func(dead int) error {
		f := newReconfigFixture(t, nil)
		deadSet := map[uint64]bool{}
		for id := uint64(0); id < uint64(dead); id++ {
			deadSet[id] = true
		}
		base := f.orch
		_ = base
		lister := func(_ context.Context, node uint64) ([][]byte, error) {
			if deadSet[node] {
				return nil, errors.New("down")
			}
			return f.stores[node].Keys(context.Background())
		}
		f.orch = ranges.NewOrchestrator(99, f.rstore, f.dialer, lister, nil)

		if _, err := f.orch.Seed(ctx, ranges.Descriptor{ID: 1, Replicas: []uint64{0, 1, 2}, Epoch: 1}); err != nil {
			return err
		}
		if _, err := f.routerNow(t, 6).Put(ctx, []byte("k"), []byte("v")); err != nil {
			return err
		}
		_, err := f.orch.ReconfigReplicas(ctx, 1, []uint64{3, 4, 5})
		return err
	}

	if err := run(1); err != nil {
		t.Fatalf("reconfig with 1 dead lister: %v (majority should suffice)", err)
	}
	if err := run(2); err == nil {
		t.Fatal("reconfig with a dead majority succeeded; must fail loudly")
	}
}

// No-op moves and mid-lifecycle ranges are left alone.
func TestReconfigNoOpAndGuards(t *testing.T) {
	ctx := context.Background()
	f := newReconfigFixture(t, nil)
	if _, err := f.orch.Seed(ctx, ranges.Descriptor{ID: 1, Replicas: []uint64{0, 1, 2}, Epoch: 1}); err != nil {
		t.Fatal(err)
	}

	s, err := f.orch.ReconfigReplicas(ctx, 1, []uint64{2, 1, 0}) // same set, any order
	if err != nil || s.Epoch != 1 {
		t.Fatalf("no-op reconfig = %+v err=%v, want untouched epoch 1", s, err)
	}

	if _, err := f.rstore.Publish(ctx, 1, func(st ranges.State, _ bool) (ranges.State, error) {
		st.Splitting = true
		return st, nil
	}); err != nil {
		t.Fatal(err)
	}
	s, err = f.orch.ReconfigReplicas(ctx, 1, []uint64{3, 4, 5})
	if err != nil || !equal(s.Replicas, []uint64{0, 1, 2}) {
		t.Fatalf("reconfig of a splitting range = %+v err=%v, want untouched", s, err)
	}
}

func equal(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[uint64]int{}
	for _, x := range a {
		seen[x]++
	}
	for _, x := range b {
		seen[x]--
	}
	for _, c := range seen {
		if c != 0 {
			return false
		}
	}
	return true
}
