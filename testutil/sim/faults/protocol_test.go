package faults_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/testutil/sim"
	"github.com/phoban01/cask/testutil/sim/faults"
)

func warnings(s *sim.Sim) []string {
	var out []string
	for _, e := range s.Trace.Events() {
		if strings.Contains(e, "WARNING") {
			out = append(out, e)
		}
	}
	return out
}

// Issue #170: processes that share a node id keep every write.
func TestRestartSameNodeIDKeepsEveryWrite(t *testing.T) {
	stores := []caspaxos.Storage{store.NewMem(), store.NewMem(), store.NewMem()}
	s := sim.NewSim(1, sim.Consensus(), stores)
	faults.RestartSameNodeID{}.Inject(s)
	faults.RestartSameNodeID{}.Inject(s)
	if w := warnings(s); len(w) > 0 {
		t.Fatalf("fault reported a lost write: %v", w)
	}
	kv := mvcc.New(caspaxos.NewProposer(99, s.Net.Clients()), hlc.New(s.Clock.Phys()), 99)
	chain, err := kv.History(context.Background(), []byte("\x00sim/same-node-id"))
	if err != nil {
		t.Fatal(err)
	}
	if len(chain.Versions) != 6 {
		t.Fatalf("probe key has %d versions, want 6 (three processes, two injections)", len(chain.Versions))
	}
}

// Negative control: processes that share an incarnation, as every process
// did before the fix, lose writes, and the fault WARNs.
func TestRestartSameNodeIDCatchesSharedOpIDs(t *testing.T) {
	stores := []caspaxos.Storage{store.NewMem(), store.NewMem(), store.NewMem()}
	s := sim.NewSim(1, sim.Consensus(), stores)
	faults.RestartSameNodeIDWith(s, func(p mvcc.Proposer, c *hlc.Clock, node uint64) *mvcc.KV {
		return mvcc.New(p, c, node, mvcc.WithIncarnation(1))
	})
	if len(warnings(s)) == 0 {
		t.Fatalf("shared OpIDs went unnoticed; trace: %v", s.Trace.Events())
	}
}

// The W0 duel end-to-end on a live cluster: the deposed owner is fenced (no
// WARNING traces) and both probe keys' committed chains hold exactly the owner
// write then the full-proposer write — the stale write never lands.
func TestOwnerVsFullProposerFencesStaleOwner(t *testing.T) {
	stores := []caspaxos.Storage{store.NewMem(), store.NewMem(), store.NewMem()}
	s := sim.NewSim(1, sim.Consensus(), stores)

	faults.OwnerVsFullProposer{}.Inject(s)

	for _, e := range s.Trace.Events() {
		if strings.Contains(e, "WARNING") {
			t.Fatalf("fault reported a hazard: %s", e)
		}
	}

	ctx := context.Background()
	kv := mvcc.New(caspaxos.NewProposer(99, s.Net.Clients()), hlc.New(s.Clock.Phys()), 99)
	for _, key := range [][]byte{[]byte("\x00sim/owner-duel-hi"), []byte("\x00sim/owner-duel-lo")} {
		chain, err := kv.History(ctx, key)
		if err != nil {
			t.Fatalf("history %q: %v", key, err)
		}
		if len(chain.Versions) != 2 {
			t.Fatalf("key %q has %d versions, want 2 (owner, full)", key, len(chain.Versions))
		}
		if !bytes.HasPrefix(chain.Versions[0].Value, []byte("owner-")) {
			t.Fatalf("key %q version 1 = %q, want owner write", key, chain.Versions[0].Value)
		}
		if !bytes.HasPrefix(chain.Versions[1].Value, []byte("full-")) {
			t.Fatalf("key %q version 2 = %q, want full-proposer write (must survive the duel)", key, chain.Versions[1].Value)
		}
	}
}
