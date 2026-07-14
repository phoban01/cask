package faults

import (
	"time"

	"github.com/phoban01/cask/testutil/sim"
)

// linkLatency is the per-call delay SlowLink injects. Bounded and fixed so the
// seed budget stays within its wall-clock target; the point is to widen the
// window during which an RPC is in flight, not to model realistic RTT.
const linkLatency = 200 * time.Microsecond

// DuplicateDelivery arms at-least-once delivery on a seeded subset of acceptors:
// while armed, each Prepare/Accept to them is delivered to the acceptor twice.
// The real transport has no message dedup (a TCP retransmit or a proposer retry
// re-sends), so consensus must be idempotent under duplicate delivery — a second
// promise/accept of the same ballot is a no-op. The gate's S1/S2 checks verify
// no value diverges and no MVCC op lands twice as a result.
type DuplicateDelivery struct{}

func (DuplicateDelivery) Name() string { return sim.FaultDuplicateDelivery }

func (DuplicateDelivery) Inject(s *sim.Sim) {
	n := s.Net.N()
	if n == 0 {
		return
	}
	armed := 0
	for id := 0; id < n; id++ {
		if s.RNG.Intn(2) == 0 {
			s.Net.SetDuplicate(id, true)
			armed++
			s.Trace.Add("step %d: duplicate_delivery armed on acceptor %d", s.Step(), id)
		}
	}
	if armed == 0 {
		// Ensure the fault bites at least once rather than dwelling as a no-op.
		id := s.RNG.Intn(n)
		s.Net.SetDuplicate(id, true)
		s.Trace.Add("step %d: duplicate_delivery armed on acceptor %d (forced)", s.Step(), id)
	}
}

// SlowLink injects link latency on a seeded subset of acceptors, modelling a
// slow network path (distinct from slow_fsync's slow disk). It widens the
// in-flight window so the gate can observe state mid-RTT, and stresses proposer
// timeout/backpressure handling. Healed (cleared) between dwells by Net.Heal.
type SlowLink struct{}

func (SlowLink) Name() string { return sim.FaultSlowLink }

func (SlowLink) Inject(s *sim.Sim) {
	n := s.Net.N()
	if n == 0 {
		return
	}
	// Slow a minority so a quorum can still make progress without the laggard.
	k := 1 + s.RNG.Intn((n+1)/2)
	for _, id := range s.RNG.Perm(n)[:k] {
		s.Net.SetSlow(id, linkLatency)
		s.Trace.Add("step %d: slow_link %s on acceptor %d", s.Step(), linkLatency, id)
	}
}
