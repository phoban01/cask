package sim

import (
	"math/rand"
	"sync"

	"github.com/phoban01/cask/internal/buggify"
)

// InstallBuggify points the global buggify hook at this scenario: every
// buggify.Maybe call is decided against the profile-adjusted probability. It
// first seeds the scenario's site registry from the process-global declared
// sites (populated by each package's init()), so the full cruelty surface is
// known up front, then records hits as sites fire.
//
// The hook draws from its OWN seeded RNG stream, guarded by a mutex — never
// from s.RNG. Two reasons (both from the W2 concurrent fan-out): acceptor-side
// sites now fire from fan-out worker goroutines, which would race the
// unguarded adversary RNG; and isolating the streams means the number of
// buggify draws can never perturb the fault/workload schedule, which makes
// seed replay MORE stable than when both shared one stream. Draw order across
// concurrent workers remains schedule-dependent — the residual, documented
// limit of the concurrent gate (see internal/buggify's concurrency rule;
// proposer-side sites are pre-drawn serially and keep full determinism).
//
// Because the buggify hook is a process-wide global, scenarios are NOT safe to
// run concurrently in the same process — the gate runs them serially and the CI
// script shards seeds across processes for parallelism. Call Reset (or rely on
// the gate's per-scenario teardown) before installing another scenario's hook.
// (The hook slot itself is atomic, so straggler fan-out goroutines from a torn
// down scenario cannot race the swap — see internal/buggify.)
func (s *Sim) InstallBuggify() {
	for _, site := range buggify.Declared() {
		s.Registry.Add(site.Name, site.Description, site.DefaultProb)
	}
	var (
		mu  sync.Mutex
		rng = rand.New(rand.NewSource(s.Seed ^ buggifySeedSalt))
	)
	buggify.SetHook(func(name string, prob float64) bool {
		if s.Profile != nil && s.Profile.SiteDisabled(name) {
			return false
		}
		site := s.Registry.Touch(name, prob)
		p := prob
		if s.Profile != nil {
			p = s.Profile.AdjustedProb(name, prob)
		}
		mu.Lock()
		fired := rng.Float64() < p
		mu.Unlock()
		if fired {
			s.Trace.Add("step %d: buggify site %q fired (p=%.3f)", s.Step(), site.Name, p)
		}
		return fired
	})
}

// buggifySeedSalt derives the buggify stream's seed from the scenario seed so
// the two streams are decorrelated but both reproducible from one number.
const buggifySeedSalt = 0x6275676769667921

// Reset clears the global buggify hook. The gate calls it after each scenario.
func (s *Sim) Reset() { buggify.Reset() }
