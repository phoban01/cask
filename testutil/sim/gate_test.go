package sim_test

import (
	"context"
	"testing"

	"github.com/phoban01/cask/internal/buggify"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/testutil/sim"
	"github.com/phoban01/cask/testutil/sim/faults"
)

// The gate must run clean on the believed-correct code across many seeds and
// every runnable profile.
func TestGateCleanAcrossProfiles(t *testing.T) {
	for name, profile := range sim.Profiles() {
		profile := profile
		t.Run(name, func(t *testing.T) {
			v, err := sim.Run(context.Background(), sim.GateConfig{
				SeedStart: 1,
				SeedCount: 60,
				Profile:   profile,
				Faults:    faults.ForProfile(profile),
				Workload:  sim.MVCCWorkload{NumKeys: 4},
				NewStores: storeFactory(profile),
				Heal:      func(s *sim.Sim) { s.Net.Heal(); faults.HealStores(s) },
				Rounds:    40,
			})
			if err != nil {
				t.Fatalf("gate run error: %v", err)
			}
			if v != nil {
				t.Fatalf("unexpected invariant violation: %s\ntrace:\n%v", v, v.Trace)
			}
		})
	}
}

// storeFactory wraps the stores in SlowStores when slow_fsync is in the profile,
// so the fault has something to arm.
func storeFactory(p *sim.Profile) func(int64) []caspaxos.Storage {
	base := sim.MemStores(3)
	if !p.HasFault(sim.FaultSlowFsync) {
		return base
	}
	return func(seed int64) []caspaxos.Storage {
		raw := base(seed)
		out := make([]caspaxos.Storage, len(raw))
		for i, s := range raw {
			out[i] = faults.NewSlowStore(s, 200_000) // 200µs
		}
		return out
	}
}

// Negative control: a hand-crafted snapshot that breaks each invariant must be
// flagged by that invariant's predicate. This proves the checks are not vacuous.
func TestInvariantsCatchViolations(t *testing.T) {
	byID := map[string]sim.Invariant{}
	for _, inv := range sim.All() {
		byID[inv.ID] = inv
	}

	t.Run("S1 distinct values at one ballot", func(t *testing.T) {
		b := caspaxos.Ballot{Counter: 5, NodeID: 1}
		cur := sim.Snapshot{Registers: map[string][]caspaxos.Register{
			"k": {
				{Accepted: b, Value: []byte("one")},
				{Accepted: b, Value: []byte("two")},
			},
		}}
		if err := byID["S1"].Check(sim.Snapshot{}, cur); err == nil {
			t.Fatal("S1 should flag two distinct values accepted at the same ballot")
		}
	})

	t.Run("S2 duplicate op / non-increasing seq", func(t *testing.T) {
		// Two versions sharing the same Op (exactly-once break) at the chosen value.
		val := []byte(`{"versions":[{"seq":1,"hlc":{"Physical":1,"Logical":0},"op":{"node":1,"seq":1}},{"seq":2,"hlc":{"Physical":2,"Logical":0},"op":{"node":1,"seq":1}}]}`)
		cur := sim.Snapshot{Registers: map[string][]caspaxos.Register{
			"k": {{Accepted: caspaxos.Ballot{Counter: 1, NodeID: 1}, Value: val}},
		}}
		if err := byID["S2"].Check(sim.Snapshot{}, cur); err == nil {
			t.Fatal("S2 should flag a duplicate OpID")
		}
	})

	t.Run("S3 non-increasing HLC", func(t *testing.T) {
		val := []byte(`{"versions":[{"seq":1,"hlc":{"Physical":5,"Logical":0},"op":{"node":1,"seq":1}},{"seq":2,"hlc":{"Physical":2,"Logical":0},"op":{"node":1,"seq":2}}]}`)
		cur := sim.Snapshot{Registers: map[string][]caspaxos.Register{
			"k": {{Accepted: caspaxos.Ballot{Counter: 1, NodeID: 1}, Value: val}},
		}}
		if err := byID["S3"].Check(sim.Snapshot{}, cur); err == nil {
			t.Fatal("S3 should flag a backwards HLC")
		}
	})

	t.Run("S11 chosen ballot regression", func(t *testing.T) {
		prev := sim.Snapshot{Registers: map[string][]caspaxos.Register{
			"k": {{Accepted: caspaxos.Ballot{Counter: 10, NodeID: 1}, Value: []byte("hi")}},
		}}
		cur := sim.Snapshot{Registers: map[string][]caspaxos.Register{
			"k": {{Accepted: caspaxos.Ballot{Counter: 4, NodeID: 1}, Value: []byte("lo")}},
		}}
		if err := byID["S11"].Check(prev, cur); err == nil {
			t.Fatal("S11 should flag a chosen ballot that regressed")
		}
	})

	// Chain values for the S13 cases. op(2,1) plays the interloper's committed
	// write; the "lost" chain replaces it with a different op at the same seq —
	// exactly what a stale-cache fast-path overwrite produces (W0).
	chainWithInterloper := []byte(`{"versions":[{"seq":1,"hlc":{"Physical":1,"Logical":0},"op":{"node":1,"seq":1}},{"seq":2,"hlc":{"Physical":2,"Logical":0},"op":{"node":2,"seq":1}}]}`)
	chainLostUpdate := []byte(`{"versions":[{"seq":1,"hlc":{"Physical":1,"Logical":0},"op":{"node":1,"seq":1}},{"seq":2,"hlc":{"Physical":3,"Logical":0},"op":{"node":1,"seq":2}}]}`)

	quorumAt := func(b caspaxos.Ballot, val []byte) []caspaxos.Register {
		return []caspaxos.Register{
			{Accepted: b, Value: val},
			{Accepted: b, Value: val},
			{Accepted: b, Value: val},
		}
	}

	t.Run("S13 committed version disappears", func(t *testing.T) {
		prev := sim.Snapshot{Registers: map[string][]caspaxos.Register{
			"k": quorumAt(caspaxos.Ballot{Counter: 10, NodeID: 1}, chainWithInterloper),
		}}
		cur := sim.Snapshot{Registers: map[string][]caspaxos.Register{
			"k": quorumAt(caspaxos.Ballot{Counter: 20, NodeID: 2}, chainLostUpdate),
		}}
		if err := byID["S13"].Check(prev, cur); err == nil {
			t.Fatal("S13 should flag a committed version replaced by a different op (lost update)")
		}
	})

	t.Run("S13 tolerates compaction", func(t *testing.T) {
		compacted := []byte(`{"versions":[{"seq":2,"hlc":{"Physical":2,"Logical":0},"op":{"node":2,"seq":1}}],"compacted_below":2}`)
		prev := sim.Snapshot{Registers: map[string][]caspaxos.Register{
			"k": quorumAt(caspaxos.Ballot{Counter: 10, NodeID: 1}, chainWithInterloper),
		}}
		cur := sim.Snapshot{Registers: map[string][]caspaxos.Register{
			"k": quorumAt(caspaxos.Ballot{Counter: 20, NodeID: 2}, compacted),
		}}
		if err := byID["S13"].Check(prev, cur); err != nil {
			t.Fatalf("S13 flagged legitimate compaction: %v", err)
		}
	})

	t.Run("S13 ignores abandoned partial accepts", func(t *testing.T) {
		// One straggler acceptor carries a higher-ballot chain from an
		// abandoned round whose extra op never reached quorum. A later round
		// that never saw it is legitimate — only quorum-committed versions are
		// owed durability.
		abandoned := []byte(`{"versions":[{"seq":1,"hlc":{"Physical":1,"Logical":0},"op":{"node":1,"seq":1}},{"seq":2,"hlc":{"Physical":2,"Logical":0},"op":{"node":9,"seq":1}}]}`)
		base := []byte(`{"versions":[{"seq":1,"hlc":{"Physical":1,"Logical":0},"op":{"node":1,"seq":1}}]}`)
		prev := sim.Snapshot{Registers: map[string][]caspaxos.Register{
			"k": {
				{Accepted: caspaxos.Ballot{Counter: 10, NodeID: 1}, Value: base},
				{Accepted: caspaxos.Ballot{Counter: 10, NodeID: 1}, Value: base},
				{Accepted: caspaxos.Ballot{Counter: 15, NodeID: 9}, Value: abandoned},
			},
		}}
		cur := sim.Snapshot{Registers: map[string][]caspaxos.Register{
			"k": quorumAt(caspaxos.Ballot{Counter: 20, NodeID: 2}, chainWithInterloper),
		}}
		if err := byID["S13"].Check(prev, cur); err != nil {
			t.Fatalf("S13 flagged an abandoned partial accept: %v", err)
		}
	})
}

// A profile override forces a site to fire (or disables it). Confirms the
// buggify wiring routes through profile probability and the seeded RNG.
func TestBuggifyProfileControls(t *testing.T) {
	t.Cleanup(buggify.Reset)

	profile := &sim.Profile{
		Name:          "test",
		SiteProb:      map[string]float64{"always": 1.0},
		DisabledSites: map[string]bool{"never": true},
	}
	s := sim.NewSim(1, profile, sim.MemStores(3)(1))
	s.InstallBuggify()

	if !buggify.Maybe("always", 0.0) {
		t.Fatal("a site overridden to p=1.0 must fire")
	}
	if buggify.Maybe("never", 1.0) {
		t.Fatal("a disabled site must never fire")
	}
}

// The registry is seeded from the declared sites at install time, so the full
// cruelty surface is known up front (not only after a site fires).
func TestRegistrySeededFromDeclared(t *testing.T) {
	t.Cleanup(buggify.Reset)
	s := sim.NewSim(1, sim.Smoke(), sim.MemStores(3)(1))
	s.InstallBuggify()

	want := "owned_proposer_force_full_round"
	found := false
	for _, site := range s.Registry.All() {
		if site.Name == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("registry missing declared site %q; declared sites must seed the registry", want)
	}
}

// The gate machinery must surface a violation end-to-end: an always-failing
// invariant produces a Violation with the seed/profile/trace, and Run returns
// it as a non-nil error (which the CI script turns into a non-zero exit).
func TestGateReportsInjectedViolation(t *testing.T) {
	tripwire := sim.Invariant{
		ID:          "TRIPWIRE",
		Implemented: true,
		Check: func(_, cur sim.Snapshot) error {
			if len(cur.Registers) == 0 {
				return nil // wait until the workload has written something
			}
			return errAlwaysFail
		},
	}
	v, err := sim.Run(context.Background(), sim.GateConfig{
		SeedStart:  1,
		SeedCount:  3,
		Profile:    sim.Smoke(),
		Workload:   sim.MVCCWorkload{NumKeys: 2},
		NewStores:  sim.MemStores(3),
		Invariants: []sim.Invariant{tripwire},
		Rounds:     10,
	})
	if v == nil || err == nil {
		t.Fatal("gate must report the injected violation as a non-nil Violation and error")
	}
	if v.Invariant != "TRIPWIRE" {
		t.Fatalf("violation invariant = %q, want TRIPWIRE", v.Invariant)
	}
	if v.Profile != "smoke" {
		t.Fatalf("violation profile = %q, want smoke", v.Profile)
	}
	// v.Trace may be empty when the violation trips before any fault/buggify
	// event is logged; the seed + profile + round are what pin the replay.
}

var errAlwaysFail = stringErr("tripwire: always fails once state exists")

type stringErr string

func (e stringErr) Error() string { return string(e) }

// The regression set must pass on current code.
func TestRegressions(t *testing.T) {
	if err := sim.RunRegressions(context.Background(), faults.ForProfile, func(msg string) { t.Log(msg) }); err != nil {
		t.Fatalf("regression: %v", err)
	}
}
