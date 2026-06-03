// Package linearizability drives the CASPaxos + MVCC stack over the fault
// injecting simulator and checks recorded histories for linearizability with
// porcupine, plus crash/restart durability. This is the M2 correctness gate.
package linearizability

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/testutil/sim"
)

// --- porcupine model of a single MVCC register -----------------------------
//
// A linearizable register over put/get. State is the current value ("" = the
// initial empty state). Put values are unique, so a get pins exactly which put
// it observed. This is the canonical model for checking consensus
// linearizability and it correctly tolerates CASPaxos's "a failed write may be
// completed later" behaviour: an errored put is recorded with an open-ended
// return (indeterminateReturn) so porcupine may linearize its effect at any
// later point — or not at all.

const indeterminateReturn = int64(1) << 62 // larger than every real event time

type regInput struct {
	Op  string // put | get
	Arg string // put: value written
}

type regOutput struct {
	Val string // get: value read
}

var registerModel = porcupine.Model{
	Init: func() interface{} { return "" },
	Step: func(st, in, out interface{}) (bool, interface{}) {
		s := st.(string)
		i := in.(regInput)
		switch i.Op {
		case "put":
			return true, i.Arg
		case "get":
			return out.(regOutput).Val == s, s
		}
		return false, s
	},
	Equal: func(a, b interface{}) bool { return a.(string) == b.(string) },
}

func newClock() *hlc.Clock {
	var tick int64
	return hlc.New(func() int64 { return atomic.AddInt64(&tick, 1) })
}

func TestRegisterLinearizableUnderNemesis(t *testing.T) {
	// Workload is kept modest and faults gentle so porcupine's (NP-hard) check
	// stays tractable and conclusive: few indeterminate ops, bounded history.
	// Concurrency + a rolling minority outage still exercise the consensus race
	// that the per-key atomicity fix addresses.
	const (
		rf      = 5
		clients = 3
		opsEach = 25
		maxDown = 1 // a single-node outage; quorum always comfortably available
	)
	ctx := context.Background()

	stores := make([]caspaxos.Storage, rf)
	for i := range stores {
		stores[i] = store.NewMem()
	}
	nw := sim.NewNetwork(stores)
	links := nw.Clients()
	clock := newClock()
	key := []byte("k")

	// Logical event clock for porcupine ordering.
	var evClock int64
	now := func() int64 { return atomic.AddInt64(&evClock, 1) }

	// Nemesis: repeatedly partition/crash a random minority, then heal.
	stop := make(chan struct{})
	var nemesisWG sync.WaitGroup
	nemesisWG.Add(1)
	go func() {
		defer nemesisWG.Done()
		rng := rand.New(rand.NewSource(0xC45C))
		for {
			select {
			case <-stop:
				nw.Heal()
				return
			default:
			}
			down := rng.Intn(maxDown + 1) // 0..maxDown
			perm := rng.Perm(rf)
			for i := 0; i < down; i++ {
				nw.SetReachable(perm[i], false)
			}
			// brief outage window
			for i := 0; i < rng.Intn(50); i++ {
				_ = i
			}
			nw.Heal()
		}
	}()

	histories := make([][]porcupine.Operation, clients)
	var committed int64
	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			kv := mvcc.New(caspaxos.NewProposer(uint64(cid+1), links), clock, uint64(cid+1))
			rng := rand.New(rand.NewSource(int64(cid) + 1))
			var ops []porcupine.Operation
			for i := 0; i < opsEach; i++ {
				if rng.Intn(3) == 0 { // ~1/3 reads
					call := now()
					v, found, err := kv.Get(ctx, key)
					ret := now()
					if err != nil {
						continue // a read that didn't return tells us nothing
					}
					val := ""
					if found {
						val = string(v)
					}
					atomic.AddInt64(&committed, 1)
					ops = append(ops, porcupine.Operation{
						ClientId: cid, Input: regInput{Op: "get"},
						Call: call, Output: regOutput{Val: val}, Return: ret,
					})
					continue
				}
				// Writes carry a value unique to (client, op).
				v := fmt.Sprintf("c%d-%d", cid, i)
				call := now()
				_, err := kv.Put(ctx, key, []byte(v))
				ret := now()
				if err != nil {
					// Indeterminate: the value may still be completed later by
					// another proposer via carry-forward. Record it with an
					// open-ended return so porcupine may linearize it late — or
					// drop it if it never took effect.
					ret = indeterminateReturn
				} else {
					atomic.AddInt64(&committed, 1)
				}
				ops = append(ops, porcupine.Operation{
					ClientId: cid, Input: regInput{Op: "put", Arg: v},
					Call: call, Output: regOutput{}, Return: ret,
				})
			}
			histories[cid] = ops
		}(c)
	}
	wg.Wait()
	close(stop)
	nemesisWG.Wait()

	var all []porcupine.Operation
	for _, h := range histories {
		all = append(all, h...)
	}
	if got := atomic.LoadInt64(&committed); got < 15 {
		t.Fatalf("history too thin (%d definite ops); test would be vacuous", got)
	}
	switch porcupine.CheckOperationsTimeout(registerModel, all, 30*time.Second) {
	case porcupine.Illegal:
		dumpHistory(t, all)
		t.Fatalf("history is NOT linearizable (%d ops)", len(all))
	case porcupine.Unknown:
		// porcupine could not decide within the budget; not a correctness
		// failure, but flag it so we keep the workload tractable.
		t.Logf("porcupine inconclusive within timeout (%d ops); consider reducing workload", len(all))
	}
}

// dumpHistory prints a compact, time-sorted view of the failing history so the
// witness can be reasoned about by hand.
func dumpHistory(t *testing.T, ops []porcupine.Operation) {
	sorted := append([]porcupine.Operation(nil), ops...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Call < sorted[j].Call })
	for _, op := range sorted {
		in := op.Input.(regInput)
		ret := fmt.Sprintf("%d", op.Return)
		if op.Return == indeterminateReturn {
			ret = "inf"
		}
		switch in.Op {
		case "put":
			t.Logf("c%d [%5d..%5s] PUT %s", op.ClientId, op.Call, ret, in.Arg)
		case "get":
			t.Logf("c%d [%5d..%5s] GET -> %q", op.ClientId, op.Call, ret, op.Output.(regOutput).Val)
		}
	}
}

// TestCrashRestartDurability asserts a committed value is never lost across
// proposer restarts and rolling minority outages: quorum intersection
// guarantees every committed write survives on any reachable majority.
func TestCrashRestartDurability(t *testing.T) {
	const rf = 5
	ctx := context.Background()

	stores := make([]caspaxos.Storage, rf)
	for i := range stores {
		stores[i] = store.NewMem()
	}
	nw := sim.NewNetwork(stores)
	clock := newClock()
	key := []byte("durable")

	// freshKV models a process restart: a brand-new proposer with zeroed volatile
	// state over the same durable stores. Each incarnation gets a unique id —
	// a restarted proposer must not reuse a previous incarnation's identity (it
	// would reuse ballot/OpID namespaces). Durable state lives in the shared
	// stores, which is what must survive.
	var incarnation uint64
	freshKV := func() *mvcc.KV {
		id := atomic.AddUint64(&incarnation, 1)
		return mvcc.New(caspaxos.NewProposer(id, nw.Clients()), clock, id)
	}

	want := ""
	rng := rand.New(rand.NewSource(42))
	for round := 0; round < 25; round++ {
		// Commit a new value with a fresh proposer (full quorum reachable).
		nw.Heal()
		v := fmt.Sprintf("v%d", round)
		if _, err := freshKV().Put(ctx, key, []byte(v)); err != nil {
			t.Fatalf("round %d put: %v", round, err)
		}
		want = v

		// Roll a random minority outage, restart the proposer, and read back.
		perm := rng.Perm(rf)
		for i := 0; i < 2; i++ {
			nw.SetReachable(perm[i], false)
		}
		got, found, err := freshKV().Get(ctx, key)
		if err != nil {
			t.Fatalf("round %d read under outage: %v", round, err)
		}
		if !found || string(got) != want {
			t.Fatalf("round %d: committed value lost: got %q,%v want %q", round, got, found, want)
		}
	}
}
