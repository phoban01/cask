package sim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/lease"
)

// Round runs one unit of protocol work for a scenario. The gate calls it once
// per round, after any scheduled fault is injected. Returning an error aborts
// the scenario without recording an invariant violation (protocol errors like
// ErrPreempted under a fault are expected — a Round should tolerate those and
// return nil).
type Round func(ctx context.Context, round int) error

// Workload builds per-scenario protocol state (proposers, KV, leases) over a
// freshly-seeded Sim and returns the round runner.
type Workload interface {
	Begin(s *Sim) (Round, error)
}

// GateConfig parameterises a gate run: a seed range, a profile, the faults to
// inject, the invariants to check, a store factory (fresh stores per seed), and
// the workload.
type GateConfig struct {
	SeedStart  int64
	SeedCount  int64
	Profile    *Profile
	Faults     []Fault
	Invariants []Invariant
	Workload   Workload

	// NewStores returns fresh durable stores for one scenario. The number of
	// stores fixes the acceptor count.
	NewStores func(seed int64) []caspaxos.Storage

	// Heal undoes a fault between dwells. Defaults to s.Net.Heal; supply a
	// composed heal (e.g. also faults.HealStores) when stores carry fault state.
	Heal func(s *Sim)

	// Rounds is the number of protocol rounds per scenario (default 50).
	Rounds int
	// FaultEvery injects a scheduled fault every N rounds (default 5; 0 disables).
	FaultEvery int

	// RecordDir, if set, receives one JSON regression record per violation.
	RecordDir string
}

// Violation is a recorded invariant break: enough to reproduce the adversary
// (seed + profile) and inspect what happened (the event trace).
type Violation struct {
	Seed      int64    `json:"seed"`
	Profile   string   `json:"profile"`
	Round     int      `json:"round"`
	Step      int      `json:"step"`
	Invariant string   `json:"invariant"`
	Detail    string   `json:"detail"`
	Trace     []string `json:"trace"`
}

func (v Violation) Error() string {
	return fmt.Sprintf("seed=%d profile=%s round=%d invariant=%s: %s", v.Seed, v.Profile, v.Round, v.Invariant, v.Detail)
}

// Run executes the gate over every seed, fail-fast on the first invariant
// violation. It returns the violation (if any) and a non-nil error mirroring it,
// so callers can branch on either. A clean run returns (nil, nil).
func Run(ctx context.Context, cfg GateConfig) (*Violation, error) {
	rounds := cfg.Rounds
	if rounds == 0 {
		rounds = 50
	}
	faultEvery := cfg.FaultEvery
	if faultEvery == 0 {
		faultEvery = 5
	}
	heal := cfg.Heal
	if heal == nil {
		heal = func(s *Sim) { s.Net.Heal() }
	}
	invs := cfg.Invariants
	if invs == nil {
		invs = Implemented()
	}

	for seed := cfg.SeedStart; seed < cfg.SeedStart+cfg.SeedCount; seed++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		v, err := runScenario(ctx, seed, rounds, faultEvery, heal, invs, cfg)
		if err != nil {
			return nil, err
		}
		if v != nil {
			if cfg.RecordDir != "" {
				if werr := writeRecord(cfg.RecordDir, *v); werr != nil {
					return v, fmt.Errorf("violation %w; additionally failed to write record: %v", v, werr)
				}
			}
			return v, v
		}
	}
	return nil, nil
}

func runScenario(ctx context.Context, seed int64, rounds, faultEvery int, heal func(*Sim), invs []Invariant, cfg GateConfig) (*Violation, error) {
	stores := cfg.NewStores(seed)
	s := NewSim(seed, cfg.Profile, stores)
	s.InstallBuggify()
	defer s.Reset()

	run, err := cfg.Workload.Begin(s)
	if err != nil {
		return nil, fmt.Errorf("seed %d: workload begin: %w", seed, err)
	}

	prev := Snapshot{Registers: map[string][]caspaxos.Register{}}
	traceSeen := 0
	for round := 0; round < rounds; round++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Schedule a seeded fault for this round; it stays active across the
		// round's work and is healed after invariants are checked.
		faulted := false
		if faultEvery > 0 && round > 0 && round%faultEvery == 0 && len(cfg.Faults) > 0 {
			f := cfg.Faults[s.RNG.Intn(len(cfg.Faults))]
			f.Inject(s)
			faulted = true
		}

		// Run protocol work; tolerate expected protocol errors under faults.
		if werr := run(ctx, round); werr != nil {
			switch {
			case !expectedUnderFault(werr):
				s.Trace.Add("round %d: workload error: %v", round, werr)
			case !faulted && retryBudgetExhausted(werr):
				// L3 — healed-dwell progress: with no fault active this round
				// (the previous dwell healed), exhausting a retry budget means
				// the contention machinery (ballot bumping + randomized
				// backoff) failed to converge — the livelock class that bit
				// the multi-lighthouse bootstrap. Under an active fault the
				// same error is an expected outcome; here it is a liveness
				// violation.
				return &Violation{
					Seed:      seed,
					Profile:   profileName(cfg.Profile),
					Round:     round,
					Step:      s.Step(),
					Invariant: "L3",
					Detail:    fmt.Sprintf("retry budget exhausted during a healed dwell: %v", werr),
					Trace:     s.Trace.Events(),
				}, nil
			}
		}

		step := s.Advance()
		cur, serr := s.snapshot(ctx)
		if serr != nil {
			return nil, serr
		}
		if v := evaluate(invs, prev, cur, seed, round, step, cfg.Profile, s.Trace); v != nil {
			return v, nil
		}
		// A fault's own assertion failing is a violation, not a log line.
		// Snapshot-pair invariants only see quiescent endpoints, so a safety
		// break a fault provokes AND observes within one round (e.g. a
		// committed write overwritten mid-round) is visible only to the fault
		// itself — it reports via a WARNING trace event, and the gate fails on
		// it here.
		events := s.Trace.Events()
		for _, e := range events[traceSeen:] {
			if strings.Contains(e, "WARNING") {
				return &Violation{
					Seed:      seed,
					Profile:   profileName(cfg.Profile),
					Round:     round,
					Step:      step,
					Invariant: "FAULT-ASSERT",
					Detail:    e,
					Trace:     events,
				}, nil
			}
		}
		traceSeen = len(events)
		prev = cur

		if faulted {
			heal(s)
		}
	}
	return nil, nil
}

// evaluate runs every implemented invariant against the snapshot pair and
// returns the first violation.
func evaluate(invs []Invariant, prev, cur Snapshot, seed int64, round, step int, profile *Profile, trace *EventTrace) *Violation {
	for _, inv := range invs {
		if !inv.Implemented || inv.Check == nil {
			continue
		}
		if err := inv.Check(prev, cur); err != nil {
			return &Violation{
				Seed:      seed,
				Profile:   profileName(profile),
				Round:     round,
				Step:      step,
				Invariant: inv.ID,
				Detail:    err.Error(),
				Trace:     trace.Events(),
			}
		}
	}
	return nil
}

func profileName(p *Profile) string {
	if p == nil {
		return ""
	}
	return p.Name
}

// retryBudgetExhausted reports whether err is a retry budget running dry —
// tolerable under an active fault, a liveness violation (L3) without one.
func retryBudgetExhausted(err error) bool {
	return errors.Is(err, caspaxos.ErrPreempted) ||
		errors.Is(err, caspaxos.ErrUnknownOutcome) ||
		errors.Is(err, lease.ErrContended)
}

// expectedUnderFault reports whether err is a normal protocol outcome under an
// injected fault (no quorum, preemption, lost ownership, an unreachable peer) —
// these are not workload bugs and must not be flagged.
func expectedUnderFault(err error) bool {
	switch {
	case errors.Is(err, caspaxos.ErrNoQuorum),
		errors.Is(err, caspaxos.ErrPreempted),
		errors.Is(err, caspaxos.ErrUnknownOutcome),
		errors.Is(err, caspaxos.ErrLostOwnership),
		errors.Is(err, caspaxos.ErrConflict),
		errors.Is(err, ErrUnreachable),
		errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, context.Canceled):
		return true
	default:
		return false
	}
}

func writeRecord(dir string, v Violation) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	name := fmt.Sprintf("violation-%s-seed%d-%s.json", v.Profile, v.Seed, v.Invariant)
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, name), raw, 0o644)
}
