package sim_test

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/testutil/sim"
	"github.com/phoban01/cask/testutil/sim/faults"
)

// TestGate is the CI/release entry point invoked by scripts/sim-gate.sh. It is
// parameterised by environment so the script can shard a large seed budget
// across processes (the global buggify hook prevents in-process seed
// parallelism):
//
//	SIM_PROFILE     profile name (default "smoke")
//	SIM_SEED_START  first seed (default 1)
//	SIM_SEEDS       seed count for this shard (default 200)
//	SIM_RECORD_DIR  directory for JSON violation records (optional)
//
// A violation fails the test with the seed + invariant + trace, which the
// script surfaces as a non-zero exit.
func TestGate(t *testing.T) {
	name := envOr("SIM_PROFILE", "smoke")
	profile, ok := sim.Profiles()[name]
	if !ok {
		t.Fatalf("unknown profile %q (have: smoke, consensus, lease, cluster)", name)
	}
	seedStart := envInt(t, "SIM_SEED_START", 1)
	seeds := envInt(t, "SIM_SEEDS", 200)

	v, err := sim.Run(context.Background(), sim.GateConfig{
		SeedStart: seedStart,
		SeedCount: seeds,
		Profile:   profile,
		Faults:    faults.ForProfile(profile),
		Workload:  sim.MVCCWorkload{NumKeys: 4},
		NewStores: ciStores(profile),
		Heal:      func(s *sim.Sim) { s.Net.Heal(); faults.HealStores(s) },
		Rounds:    40,
		RecordDir: os.Getenv("SIM_RECORD_DIR"),
	})
	if err != nil && v == nil {
		t.Fatalf("gate run error: %v", err)
	}
	if v != nil {
		t.Fatalf("INVARIANT VIOLATION %s\ntrace:\n%v", v, v.Trace)
	}
	t.Logf("gate clean: profile=%s seeds=[%d,%d)", name, seedStart, seedStart+seeds)
}

// TestGateRegressions runs the regression set unconditionally — the CI script
// runs it before exploring fresh seeds.
func TestGateRegressions(t *testing.T) {
	if err := sim.RunRegressions(context.Background(), faults.ForProfile, func(msg string) { t.Log(msg) }); err != nil {
		t.Fatalf("regression: %v", err)
	}
}

func ciStores(p *sim.Profile) func(int64) []caspaxos.Storage {
	base := sim.MemStores(3)
	if !p.HasFault(sim.FaultSlowFsync) {
		return base
	}
	return func(seed int64) []caspaxos.Storage {
		raw := base(seed)
		out := make([]caspaxos.Storage, len(raw))
		for i, s := range raw {
			out[i] = faults.NewSlowStore(s, 200_000)
		}
		return out
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(t *testing.T, key string, def int64) int64 {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatalf("env %s=%q: %v", key, v, err)
	}
	return n
}
