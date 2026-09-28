package faults

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/phoban01/cask/internal/backoff"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/testutil/sim"
)

// SharedNodeIDProposers drives issue #204. Writers in one process share a
// node id and write one probe key at the same time. Each write builds a
// fresh proposer, as roster.New does for every roster operation, so every
// proposer starts its ballot counter at zero. They are rejected by the
// key's promise, take it as the conflict floor, and mint the same next
// ballot. The acceptors' strict promise must keep this safe: every write
// that returned success must be in the key's chain. A missing one is a
// lost update, and the fault WARNs.
type SharedNodeIDProposers struct{}

func (SharedNodeIDProposers) Name() string { return sim.FaultSharedNodeIDProposers }

func (SharedNodeIDProposers) Inject(s *sim.Sim) {
	const (
		writers = 3
		ops     = 3
		node    = 60 // the one node id every proposer uses
	)
	ctx := context.Background()
	clients := s.Net.Clients()
	if len(clients) == 0 {
		return
	}
	key := []byte("\x00sim/shared-node-id")
	s.Observe(key)
	step := s.Step()
	clock := hlc.New(s.Clock.Phys())

	// Seeds are drawn here, on the gate goroutine, before any worker
	// exists (the adversary-RNG ownership rule).
	seeds := make([]int64, writers)
	for i := range seeds {
		seeds[i] = s.RNG.Int63()
	}
	// OpIDs stay distinct per writer; only the ballot node id is shared.
	kvBase := uint64(20_000 + writers*step)

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		committed [][]byte
	)
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			bo := caspaxos.WithBackoff(backoff.Seeded(200*time.Microsecond, 5*time.Millisecond, seeds[w]))
			for op := range ops {
				val := fmt.Appendf(nil, "shared-w%d-%d-%d", w, step, op)
				var err error
				for attempt := 0; attempt < 3; attempt++ {
					// A fresh proposer per attempt: its counter starts at
					// zero, like a proposer from roster.New.
					kv := mvcc.New(caspaxos.NewProposer(node, clients, bo), clock, kvBase+uint64(w))
					var v mvcc.Version
					if v, err = kv.Put(ctx, key, val); err == nil {
						if !bytes.Equal(v.Value, val) {
							s.Trace.Add("step %d: shared_node_id_proposers: WARNING writer %d put returned %q, not its own write", step, w, v.Value)
						}
						mu.Lock()
						committed = append(committed, val)
						mu.Unlock()
						break
					}
					if !errors.Is(err, caspaxos.ErrPreempted) {
						break
					}
				}
				if err != nil {
					s.Trace.Add("step %d: shared_node_id_proposers: writer %d put failed: %v", step, w, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	// Read the chain through a proposer with its own node id.
	reader := mvcc.New(caspaxos.NewProposer(node+1, clients), clock, kvBase+writers)
	chain, err := reader.History(ctx, key)
	if err != nil {
		s.Trace.Add("step %d: shared_node_id_proposers: history failed: %v", step, err)
		return
	}
	for _, val := range committed {
		found := false
		for _, v := range chain.Versions {
			if bytes.Equal(v.Value, val) {
				found = true
				break
			}
		}
		if !found && chain.CompactedBelow == 0 {
			s.Trace.Add("step %d: shared_node_id_proposers: WARNING committed write %q is missing from the chain (lost update)", step, val)
		}
	}
}
