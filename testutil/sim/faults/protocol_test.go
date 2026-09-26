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
