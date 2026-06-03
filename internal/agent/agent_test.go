package agent_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/phoban01/cask/internal/agent"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/ranges"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/testutil/sim"
)

func splits(ss ...string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

// fixture builds a sim cluster, a static range map over it, and a dialer.
func fixture(nNodes, rf int, splitPts [][]byte) (*ranges.Map, agent.Dialer, *hlc.Clock) {
	stores := make([]caspaxos.Storage, nNodes)
	for i := range stores {
		stores[i] = store.NewMem()
	}
	nw := sim.NewNetwork(stores)
	nodes := make([]uint64, nNodes)
	dialer := agent.StaticDialer{}
	for i := 0; i < nNodes; i++ {
		nodes[i] = uint64(i)
		dialer[uint64(i)] = nw.Client(i)
	}
	var tick int64
	clock := hlc.New(func() int64 { return atomic.AddInt64(&tick, 1) })
	return ranges.Static(splitPts, nodes, rf), dialer, clock
}

// Keys spread across ranges round-trip correctly, and two independent agents
// route consistently to the same data.
func TestMultiRangeRouting(t *testing.T) {
	ctx := context.Background()
	rmap, dialer, clock := fixture(5, 3, splits("g", "n", "t"))

	a := mvcc.New(agent.NewRouter(1, rmap, dialer), clock, 1)
	b := mvcc.New(agent.NewRouter(2, rmap, dialer), clock, 2)

	want := map[string]string{}
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("%c-key-%d", 'a'+i%26, i) // spans all four ranges
		val := fmt.Sprintf("val-%d", i)
		if _, err := a.Put(ctx, []byte(key), []byte(val)); err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
		want[key] = val
	}
	// Agent b independently routes and must observe everything agent a wrote.
	for key, val := range want {
		got, found, err := b.Get(ctx, []byte(key))
		if err != nil {
			t.Fatalf("get %q: %v", key, err)
		}
		if !found || string(got) != val {
			t.Fatalf("get %q = %q,%v want %q", key, got, found, val)
		}
	}
}

// Router.Owner returns a node that actually replicates the key's range.
func TestRouterOwnerIsReplica(t *testing.T) {
	rmap, dialer, _ := fixture(5, 3, splits("g", "n", "t"))
	r := agent.NewRouter(1, rmap, dialer)
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("k%d", i))
		owner, ok := r.Owner(key)
		if !ok {
			t.Fatalf("no owner for %q", key)
		}
		d, _ := rmap.Lookup(key)
		found := false
		for _, n := range d.Replicas {
			if n == owner {
				found = true
			}
		}
		if !found {
			t.Fatalf("owner %d not in range %d replicas %v", owner, d.ID, d.Replicas)
		}
	}
}

// A snapshot at an HLC timestamp reflects a consistent point in time across keys
// that live in different ranges.
func TestMultiRangeSnapshot(t *testing.T) {
	ctx := context.Background()
	rmap, dialer, clock := fixture(5, 3, splits("g", "n", "t"))
	kv := mvcc.New(agent.NewRouter(1, rmap, dialer), clock, 1)

	apple := []byte("apple") // range 1
	zebra := []byte("zebra") // range 4
	keys := [][]byte{apple, zebra}

	a1, _ := kv.Put(ctx, apple, []byte("a1"))
	z1, _ := kv.Put(ctx, zebra, []byte("z1"))
	a2, _ := kv.Put(ctx, apple, []byte("a2"))
	z2, _ := kv.Put(ctx, zebra, []byte("z2"))

	// As of z1's timestamp: apple=a1, zebra=z1.
	snap, err := kv.SnapshotRead(ctx, keys, z1.HLC)
	if err != nil {
		t.Fatal(err)
	}
	assertSnap(t, snap, "z1", map[string]string{"apple": "a1", "zebra": "z1"})

	// As of a2's timestamp: apple=a2, zebra still z1.
	snap, _ = kv.SnapshotRead(ctx, keys, a2.HLC)
	assertSnap(t, snap, "a2", map[string]string{"apple": "a2", "zebra": "z1"})

	// As of z2's timestamp: apple=a2, zebra=z2 (the latest).
	snap, _ = kv.SnapshotRead(ctx, keys, z2.HLC)
	assertSnap(t, snap, "z2", map[string]string{"apple": "a2", "zebra": "z2"})

	_ = a1 // referenced for clarity of the timeline
}

func assertSnap(t *testing.T, snap map[string]mvcc.Version, at string, want map[string]string) {
	t.Helper()
	for k, v := range want {
		got, ok := snap[k]
		if !ok || string(got.Value) != v {
			t.Fatalf("snapshot@%s[%s] = %q,%v want %q", at, k, got.Value, ok, v)
		}
	}
}

// --- concurrent multi-key linearizability through the router ----------------

const indeterminate = int64(1) << 62

type regIn struct {
	Op, Arg string
}
type regOut struct{ Val string }

var registerModel = porcupine.Model{
	Init: func() interface{} { return "" },
	Step: func(st, in, out interface{}) (bool, interface{}) {
		s := st.(string)
		switch in.(regIn).Op {
		case "put":
			return true, in.(regIn).Arg
		case "get":
			return out.(regOut).Val == s, s
		}
		return false, s
	},
	Equal: func(a, b interface{}) bool { return a.(string) == b.(string) },
}

// Two agents hammer several keys spanning ranges under a single-node outage;
// each key's recorded history must be linearizable. This exercises the router,
// concurrent shared per-range proposers, and per-key consensus together.
func TestMultiKeyLinearizable(t *testing.T) {
	ctx := context.Background()

	stores := make([]caspaxos.Storage, 5)
	for i := range stores {
		stores[i] = store.NewMem()
	}
	nw := sim.NewNetwork(stores)
	nodes := make([]uint64, 5)
	dialer := agent.StaticDialer{}
	for i := 0; i < 5; i++ {
		nodes[i] = uint64(i)
		dialer[uint64(i)] = nw.Client(i)
	}
	rmap := ranges.Static(splits("g", "n", "t"), nodes, 3)
	var tick int64
	clock := hlc.New(func() int64 { return atomic.AddInt64(&tick, 1) })

	keys := []string{"alpha", "kilo", "uniform", "zulu"} // one per range
	var evClock int64
	now := func() int64 { return atomic.AddInt64(&evClock, 1) }

	// Nemesis: roll a single-node outage.
	stop := make(chan struct{})
	var nwg sync.WaitGroup
	nwg.Add(1)
	go func() {
		defer nwg.Done()
		rng := rand.New(rand.NewSource(99))
		for {
			select {
			case <-stop:
				nw.Heal()
				return
			default:
			}
			nw.SetReachable(rng.Intn(5), false)
			nw.Heal()
		}
	}()

	const agents = 2
	histByKey := make([]map[string][]porcupine.Operation, agents)
	var wg sync.WaitGroup
	for a := 0; a < agents; a++ {
		wg.Add(1)
		go func(aid int) {
			defer wg.Done()
			kv := mvcc.New(agent.NewRouter(uint64(aid+1), rmap, dialer), clock, uint64(aid+1))
			rng := rand.New(rand.NewSource(int64(aid) + 1))
			hist := map[string][]porcupine.Operation{}
			for i := 0; i < 40; i++ {
				key := keys[rng.Intn(len(keys))]
				if rng.Intn(3) == 0 {
					call := now()
					v, found, err := kv.Get(ctx, []byte(key))
					ret := now()
					if err != nil {
						continue
					}
					val := ""
					if found {
						val = string(v)
					}
					hist[key] = append(hist[key], porcupine.Operation{
						ClientId: aid, Input: regIn{Op: "get"},
						Call: call, Output: regOut{Val: val}, Return: ret,
					})
					continue
				}
				val := fmt.Sprintf("a%d-%d", aid, i)
				call := now()
				_, err := kv.Put(ctx, []byte(key), []byte(val))
				ret := now()
				if err != nil {
					ret = indeterminate
				}
				hist[key] = append(hist[key], porcupine.Operation{
					ClientId: aid, Input: regIn{Op: "put", Arg: val},
					Call: call, Output: regOut{}, Return: ret,
				})
			}
			histByKey[aid] = hist
		}(a)
	}
	wg.Wait()
	close(stop)
	nwg.Wait()

	// Check each key's merged history independently.
	for _, key := range keys {
		var ops []porcupine.Operation
		for a := 0; a < agents; a++ {
			ops = append(ops, histByKey[a][key]...)
		}
		if len(ops) == 0 {
			continue
		}
		if porcupine.CheckOperationsTimeout(registerModel, ops, 20*time.Second) == porcupine.Illegal {
			t.Fatalf("key %q history is NOT linearizable (%d ops)", key, len(ops))
		}
	}
}
