package sim

import "github.com/phoban01/cask/internal/buggify"

// InstallBuggify points the global buggify hook at this scenario: every
// buggify.Maybe call is decided by s.RNG against the profile-adjusted
// probability. It first seeds the scenario's site registry from the
// process-global declared sites (populated by each package's init()), so the
// full cruelty surface is known up front, then records hits as sites fire.
//
// Because buggify.Hook is a package-level global, scenarios are NOT safe to run
// concurrently in the same process — the gate runs them serially and the CI
// script shards seeds across processes for parallelism. Call Reset (or rely on
// the gate's per-scenario teardown) before installing another scenario's hook.
func (s *Sim) InstallBuggify() {
	for _, site := range buggify.Declared() {
		s.Registry.Add(site.Name, site.Description, site.DefaultProb)
	}
	buggify.Hook = func(name string, prob float64) bool {
		if s.Profile != nil && s.Profile.SiteDisabled(name) {
			return false
		}
		site := s.Registry.Touch(name, prob)
		p := prob
		if s.Profile != nil {
			p = s.Profile.AdjustedProb(name, prob)
		}
		fired := s.RNG.Float64() < p
		if fired {
			s.Trace.Add("step %d: buggify site %q fired (p=%.3f)", s.Step(), site.Name, p)
		}
		return fired
	}
}

// Reset clears the global buggify hook. The gate calls it after each scenario.
func (s *Sim) Reset() { buggify.Reset() }
