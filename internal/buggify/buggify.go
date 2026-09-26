// Package buggify provides FDB-style fault-injection annotation sites.
//
// Call sites in protocol code use [Maybe] to declare "here is a point where
// reality could be cruel" — a slow fsync, a dropped vote, a forced full Paxos
// round, a widened race window. In production builds no hook is installed, so
// Maybe is a single predicted nil-comparison that returns false and the cruel
// path is never taken. In simulation builds the simulator installs a [Hook]
// that decides, with a seeded per-site probability, whether each site fires.
//
// The discipline this package exists to support (cask roadmap §6.6.1): every
// protocol change adds at least one Maybe site at a spot the author thought
// "this should be fine" — and every bug found in the field adds a site at the
// exact location that would have surfaced it. The site names are the finest
// grain of the fault catalog (§6.7).
//
// This package depends on NOTHING else in the tree. The dependency arrow is
// one-directional: testutil/sim assigns Hook and reads [Declared]; this package
// must never import testutil/sim (or any protocol package), so that protocol
// code importing buggify stays clean and cheap.
//
// # Concurrency rule
//
// Code that fans work out to goroutines (the W2 proposer phases) MUST pre-draw
// its Maybe decisions serially on the goroutine that owns the operation,
// before spawning workers — never call Maybe from inside them. Sites that
// inherently fire on concurrent goroutines (acceptor-side sites, reached via
// concurrent RPC handling) are tolerated: the simulator's hook serializes
// draws on its own dedicated RNG stream, so they are race-free — but their
// draw ORDER is schedule-dependent, which weakens replay determinism for
// those sites. Pre-drawing keeps every proposer-side site on the
// deterministic serial stream.
package buggify

import (
	"sort"
	"sync/atomic"
)

// HookFunc decides whether a [Maybe] site fires. The simulator installs one
// via [SetHook]; production never does. It returns true to make the call site
// take the "cruel" path. The name identifies the site for the simulator's
// report and the fault catalog; prob is the site's default firing probability,
// which the simulator may override per fault profile.
type HookFunc func(name string, prob float64) bool

// hook holds the installed HookFunc atomically: cancelled fan-out worker
// goroutines (W2) can still be inside an acceptor call — and hence inside
// Maybe — while the simulator tears one scenario down and installs the next,
// so the swap must not race the readers. A straggler that observes the next
// scenario's hook merely draws from that scenario's independent buggify
// stream (harmless); after Reset it draws nothing.
var hook atomic.Pointer[HookFunc]

// SetHook installs f as the process-wide buggify decider (nil uninstalls).
func SetHook(f HookFunc) {
	if f == nil {
		hook.Store(nil)
		return
	}
	hook.Store(&f)
}

// SiteInfo describes a buggify site declared via [Register].
type SiteInfo struct {
	Name        string
	Description string
	DefaultProb float64
}

// declared holds every site declared at package-init time, independent of any
// hook. The simulator reads it via Declared to seed its registry — Register
// runs at init, before any Hook is installed, so a hook-based registry would
// miss every site.
var declared = map[string]SiteInfo{}

// Maybe reports whether the named site should take its cruel path. It returns
// false whenever no simulator hook is installed (the production case), so a
// site costs one predicted branch outside of simulation.
//
// Examples:
//
//	if buggify.Maybe("owned_proposer_force_full_round", 0.02) {
//	    p.owning = false
//	    return nil, ErrLostOwnership // force the caller back through phase 1
//	}
//
//	if buggify.Maybe("proposer_drop_vote", 0.01) {
//	    continue // treat this acceptor's reply as a non-vote
//	}
func Maybe(name string, prob float64) bool {
	h := hook.Load()
	if h == nil {
		return false
	}
	return (*h)(name, prob)
}

// Register declares a buggify site so the simulator knows the full site set at
// startup (the first declaration of a name wins). It is safe and cheap in
// production. Convention: call it from a package init().
//
// Site names are effectively public API: the regression set references them by
// string, so renaming a site breaks regressions. Treat a name like an exported
// symbol.
func Register(name, description string, defaultProb float64) {
	if _, ok := declared[name]; ok {
		return
	}
	declared[name] = SiteInfo{Name: name, Description: description, DefaultProb: defaultProb}
}

// Declared returns every declared site, sorted by name. The simulator seeds its
// registry from this.
func Declared() []SiteInfo {
	out := make([]SiteInfo, 0, len(declared))
	for _, s := range declared {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Reset clears the installed hook (used by the simulator between scenarios).
// Declared sites are compile-time constants and persist across scenarios.
func Reset() { hook.Store(nil) }
