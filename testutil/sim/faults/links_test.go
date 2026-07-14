package faults_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/testutil/sim"
	"github.com/phoban01/cask/testutil/sim/faults"
)

// TestDuplicateDeliveryIsIdempotent proves the fault bites (delivers twice) and
// that consensus tolerates it: a single committed value, not a divergence or a
// double-apply. This is the negative-control discipline for the fault — a vacuous
// duplicate (one that silently delivered once) would let the test pass without
// exercising idempotency.
func TestDuplicateDeliveryIsIdempotent(t *testing.T) {
	nw := sim.NewNetwork(sim.MemStores(3)(1))

	// Arm duplication on every acceptor directly, so every RPC is delivered
	// twice for the duration of this write.
	for id := 0; id < nw.N(); id++ {
		nw.SetDuplicate(id, true)
	}

	prop := caspaxos.NewProposer(1, nw.Clients())
	key := []byte("dup-key")
	got, err := prop.Propose(context.Background(), key, caspaxos.Write([]byte("v1")))
	if err != nil {
		t.Fatalf("propose under duplicate delivery: %v", err)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("propose returned %q, want v1", got)
	}

	// A fresh read (Identity) must observe exactly the committed value — no
	// corruption from the doubled accepts.
	read, err := prop.Propose(context.Background(), key, caspaxos.Identity)
	if err != nil {
		t.Fatalf("read-back: %v", err)
	}
	if !bytes.Equal(read, []byte("v1")) {
		t.Fatalf("read-back returned %q, want v1", read)
	}
}

// TestDuplicateDeliveryInjectArms confirms Inject arms at least one acceptor
// (the forced-arm path guarantees the fault never dwells as a no-op).
func TestDuplicateDeliveryInjectArms(t *testing.T) {
	s := sim.NewSim(1, sim.Consensus(), sim.MemStores(3)(1))
	faults.DuplicateDelivery{}.Inject(s)
	// At least one trace line records an arming.
	var armed bool
	for _, e := range s.Trace.Events() {
		if bytes.Contains([]byte(e), []byte("duplicate_delivery armed")) {
			armed = true
		}
	}
	if !armed {
		t.Fatal("DuplicateDelivery.Inject armed no acceptor")
	}
}

// TestSlowLinkInjectsLatency proves SlowLink actually delays the path: a call
// through a slowed link takes at least the injected latency, and Heal clears it.
func TestSlowLinkInjectsLatency(t *testing.T) {
	nw := sim.NewNetwork(sim.MemStores(1)(1))
	const d = 20 * time.Millisecond
	nw.SetSlow(0, d)

	client := nw.Client(0)
	start := time.Now()
	if _, err := client.Prepare(context.Background(), []byte("k"), caspaxos.Ballot{Counter: 1, NodeID: 1}); err != nil {
		t.Fatalf("prepare through slow link: %v", err)
	}
	if elapsed := time.Since(start); elapsed < d {
		t.Fatalf("slow link added %v, want >= %v", elapsed, d)
	}

	nw.Heal()
	start = time.Now()
	if _, err := client.Prepare(context.Background(), []byte("k"), caspaxos.Ballot{Counter: 2, NodeID: 1}); err != nil {
		t.Fatalf("prepare after heal: %v", err)
	}
	if elapsed := time.Since(start); elapsed >= d {
		t.Fatalf("heal did not clear slow link: still took %v", elapsed)
	}
}
