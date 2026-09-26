package faults

import "github.com/phoban01/cask/testutil/sim"

// Partition downs a seeded minority-or-larger subset of acceptors, modelling a
// network split. The gate heals between applications, so a partition lasts for
// one fault dwell. Used to find quorum loss and dueling proposers.
type Partition struct{}

func (Partition) Name() string { return sim.FaultPartition }

func (Partition) Inject(s *sim.Sim) {
	n := s.Net.N()
	if n == 0 {
		return
	}
	// Down a random non-empty subset of up to half the nodes (a minority side),
	// which is the interesting case: progress must continue on the majority side.
	k := 1 + s.RNG.Intn((n+1)/2)
	for _, id := range s.RNG.Perm(n)[:k] {
		s.Net.SetReachable(id, false)
		s.Trace.Add("step %d: partition down acceptor %d", s.Step(), id)
	}
}

// CrashRestart kills one acceptor and immediately restarts it. Durable store
// state survives (the sim store models durable RAM), so this exercises
// crash-recovery of Promise/Accept. Modeled as a down→up flip within one Inject.
type CrashRestart struct{}

func (CrashRestart) Name() string { return sim.FaultCrashRestart }

func (CrashRestart) Inject(s *sim.Sim) {
	n := s.Net.N()
	if n == 0 {
		return
	}
	id := s.RNG.Intn(n)
	s.Net.SetReachable(id, false)
	s.Net.SetReachable(id, true)
	s.Trace.Add("step %d: crash+restart acceptor %d (durable state retained)", s.Step(), id)
}

// MassFailure downs ~30% of acceptors at once, the HyParView/quorum scale-gate
// stressor.
type MassFailure struct{}

func (MassFailure) Name() string { return sim.FaultMassFailure }

func (MassFailure) Inject(s *sim.Sim) {
	n := s.Net.N()
	if n == 0 {
		return
	}
	k := n * 30 / 100
	if k == 0 {
		k = 1
	}
	for _, id := range s.RNG.Perm(n)[:k] {
		s.Net.SetReachable(id, false)
		s.Trace.Add("step %d: mass-failure down acceptor %d", s.Step(), id)
	}
}

// AsymmetricReachability downs only one RPC direction (Prepare or Accept) of a
// single acceptor, leaving the other working — partial failure without a full
// partition. Stresses code that assumes reachability is symmetric.
type AsymmetricReachability struct{}

func (AsymmetricReachability) Name() string { return sim.FaultAsymmetricReach }

func (AsymmetricReachability) Inject(s *sim.Sim) {
	n := s.Net.N()
	if n == 0 {
		return
	}
	id := s.RNG.Intn(n)
	if s.RNG.Intn(2) == 0 {
		s.Net.SetPrepareDown(id, true)
		s.Trace.Add("step %d: asymmetric — acceptor %d prepare path down, accept up", s.Step(), id)
	} else {
		s.Net.SetAcceptDown(id, true)
		s.Trace.Add("step %d: asymmetric — acceptor %d accept path down, prepare up", s.Step(), id)
	}
}
