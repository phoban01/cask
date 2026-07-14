package sim

import (
	"context"
	"fmt"

	"github.com/phoban01/cask/internal/caspaxos"
)

// RegressionScenario is a replayable scenario captured from a prior invariant
// violation. CI runs every non-skipped scenario unconditionally before
// exploring fresh seeds, so a fixed bug can never silently regress. The seed +
// profile pin the adversary; the gate re-runs it and asserts no violation.
type RegressionScenario struct {
	Name      string
	Seed      int64
	Profile   *Profile
	Workload  Workload
	NewStores func(seed int64) []caspaxos.Storage
	Faults    []Fault
	Rounds    int

	// Skip, when non-empty, records why a scenario is not yet runnable (its
	// repro is still being reconstructed). It is reported, not silently dropped.
	Skip string
}

// Scenarios returns the regression set. The three entries are the historical M2
// bugs (README §M2). The exactly-once-append bug reproduces directly under
// proposer_drop_vote + retries and is asserted via S2; the other two were
// porcupine-caught and their deterministic repro is still being reconstructed,
// so they are present-but-skipped (the harness runs whatever exists).
func Scenarios() []RegressionScenario {
	return []RegressionScenario{
		{
			// M2: a retried MVCC append could land twice in the version chain.
			// proposer_drop_vote forces retries; S2 flags any duplicate OpID or
			// non-increasing Seq. Fixed by the OpID exactly-once dedup in
			// internal/mvcc; this guards it.
			Name:      "m2_exactly_once_append",
			Seed:      0xA2,
			Profile:   Consensus(),
			Workload:  MVCCWorkload{NumKeys: 3},
			NewStores: MemStores(3),
			Rounds:    80,
		},
		{
			Name:    "m2_non_atomic_per_key_rmw",
			Seed:    0xB2,
			Profile: Consensus(),
			Skip:    "backfill: porcupine-caught; deterministic seed repro under reconstruction",
		},
		{
			Name:    "m2_non_linearizable_cas_abort",
			Seed:    0xC2,
			Profile: Consensus(),
			Skip:    "backfill: porcupine-caught; deterministic seed repro under reconstruction",
		},
	}
}

// RunRegressions runs every non-skipped regression scenario and returns the
// first violation (as an error) or nil if all pass. Skipped scenarios are
// reported through report, if non-nil.
//
// resolve maps a scenario's profile to the scheduled faults to inject (the
// caller passes faults.ForProfile; the faults package imports sim, so sim
// cannot reference the catalog directly). A scenario with explicit Faults
// keeps them; otherwise it runs under its profile's full fault set, the same
// adversary class as the gate. A nil resolve leaves nil Faults as-is
// (buggify sites still fire via the profile).
func RunRegressions(ctx context.Context, resolve func(*Profile) []Fault, report func(string)) error {
	for _, sc := range Scenarios() {
		if sc.Skip != "" {
			if report != nil {
				report(fmt.Sprintf("SKIP %s: %s", sc.Name, sc.Skip))
			}
			continue
		}
		rounds := sc.Rounds
		if rounds == 0 {
			rounds = 50
		}
		fs := sc.Faults
		if fs == nil && resolve != nil {
			fs = resolve(sc.Profile)
		}
		v, err := Run(ctx, GateConfig{
			SeedStart: sc.Seed,
			SeedCount: 1,
			Profile:   sc.Profile,
			Faults:    fs,
			Workload:  sc.Workload,
			NewStores: sc.NewStores,
			Rounds:    rounds,
		})
		if err != nil && v == nil {
			return fmt.Errorf("regression %q: %w", sc.Name, err)
		}
		if v != nil {
			return fmt.Errorf("regression %q reproduced a violation: %w", sc.Name, v)
		}
		if report != nil {
			report(fmt.Sprintf("PASS %s (seed=%#x, profile=%s)", sc.Name, sc.Seed, sc.Profile.Name))
		}
	}
	return nil
}
