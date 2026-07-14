package sim

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/phoban01/cask/internal/caspaxos"
)

// Sim is the per-scenario context shared by every piece of the gate: the seeded
// RNG that is the single source of nondeterminism, the controllable clock, the
// buggify site registry, the active fault profile, the network, the durable
// stores (for invariant snapshots), and an append-only event trace for failure
// reports.
//
// Determinism note (cask roadmap §6.6, PR #0): a scenario is deterministic in
// its *adversary* — every fault, buggify decision, and clock advance is derived
// from RNG, which is seeded from the scenario seed — but NOT in goroutine
// interleaving, which still runs on the real Go scheduler. A replayed seed
// re-runs the same adversary; reproducing a specific failure is best-effort via
// the persisted seed plus the event trace. Full single-goroutine logical-time
// determinism is deferred until a bug proves the concurrent gate insufficient.
//
// RNG is NOT goroutine-safe and is deliberately unguarded: every consumer (the
// gate loop's fault scheduling, fault Inject methods, the workload round) runs
// serially on the gate goroutine. "Interleaving is uncontrolled" refers to
// protocol-internal goroutines, which must never touch the adversary's state —
// do not pass RNG (or anything derived from it) into code that runs
// concurrently with the round.
//
// The buggify hook does NOT draw from RNG: since the W2 concurrent fan-out,
// acceptor-side sites fire from worker goroutines, so the hook has its own
// seeded, mutex-guarded stream (see InstallBuggify). Isolating the streams
// also means buggify draw counts can never perturb the fault/workload
// schedule. Proposer-side sites are pre-drawn serially per the concurrency
// rule in internal/buggify, keeping them fully deterministic; acceptor-side
// draw order is schedule-dependent — the documented residual limit of the
// concurrent gate.
type Sim struct {
	Seed     int64
	RNG      *rand.Rand
	Clock    *SimClock
	Registry *SiteRegistry
	Profile  *Profile
	Net      *Network
	Stores   []caspaxos.Storage
	Trace    *EventTrace

	mu   sync.Mutex
	step int
	keys map[string]struct{} // keys the workload has touched, for snapshots
}

// NewSim builds a scenario context over stores, seeded from seed and running
// the given profile. The caller wires the workload (proposers, leases, watches)
// over Net and Clock.
func NewSim(seed int64, profile *Profile, stores []caspaxos.Storage) *Sim {
	return &Sim{
		Seed:     seed,
		RNG:      rand.New(rand.NewSource(seed)),
		Clock:    NewSimClock(),
		Registry: NewSiteRegistry(),
		Profile:  profile,
		Net:      NewNetwork(stores),
		Stores:   stores,
		Trace:    NewEventTrace(),
		keys:     make(map[string]struct{}),
	}
}

// Step returns the current scenario step counter.
func (s *Sim) Step() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.step
}

// Advance increments the step counter and returns the new value. The gate calls
// it at each quiescent point before evaluating invariants.
func (s *Sim) Advance() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.step++
	return s.step
}

// Observe records that the workload has touched key, so the gate knows to
// snapshot its register state. Safe for concurrent use.
func (s *Sim) Observe(key []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys[string(key)] = struct{}{}
}

// ObservedKeys returns the sorted set of keys touched so far.
func (s *Sim) ObservedKeys() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.keys))
	for k := range s.keys {
		names = append(names, k)
	}
	sort.Strings(names)
	out := make([][]byte, len(names))
	for i, k := range names {
		out[i] = []byte(k)
	}
	return out
}

// SimClock is a controllable physical-time source. It hands hlc.New a
// PhysicalFunc closure (via Phys) and lets the workload/faults advance logical
// nanoseconds deterministically. The zero value is not usable; use NewSimClock.
type SimClock struct {
	nanos int64 // atomic
}

// NewSimClock returns a clock starting at a fixed, nonzero epoch so timestamps
// are comparable and never collide with the zero Timestamp.
func NewSimClock() *SimClock {
	c := &SimClock{}
	atomic.StoreInt64(&c.nanos, 1_000_000_000) // start at t=1s
	return c
}

// Phys returns the PhysicalFunc to hand to hlc.New.
func (c *SimClock) Phys() func() int64 {
	return func() int64 { return atomic.LoadInt64(&c.nanos) }
}

// Advance moves the clock forward by delta nanoseconds and returns the new
// reading.
func (c *SimClock) Advance(delta int64) int64 { return atomic.AddInt64(&c.nanos, delta) }

// Now returns the current physical reading.
func (c *SimClock) Now() int64 { return atomic.LoadInt64(&c.nanos) }

// Site is a registered buggify fault-injection point.
type Site struct {
	Name        string
	Description string
	DefaultProb float64
	Hits        uint64 // times the site was consulted this scenario
}

// SiteRegistry holds the buggify sites declared via buggify.Register, so the
// gate can report on the full cruelty surface and override per-site
// probabilities by profile. Safe for concurrent use.
type SiteRegistry struct {
	mu    sync.Mutex
	sites map[string]*Site
}

// NewSiteRegistry returns an empty registry.
func NewSiteRegistry() *SiteRegistry { return &SiteRegistry{sites: make(map[string]*Site)} }

// Add records a declared site. Re-registering a name keeps the first
// description/probability (names are stable; first declaration wins).
func (r *SiteRegistry) Add(name, description string, defaultProb float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.sites[name]; ok {
		return
	}
	r.sites[name] = &Site{Name: name, Description: description, DefaultProb: defaultProb}
}

// Touch records that a site was consulted and returns its registered entry,
// creating a bare entry if the site fired without a prior Register call.
func (r *SiteRegistry) Touch(name string, prob float64) *Site {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.sites[name]
	if !ok {
		s = &Site{Name: name, DefaultProb: prob}
		r.sites[name] = s
	}
	s.Hits++
	return s
}

// All returns the registered sites sorted by name.
func (r *SiteRegistry) All() []Site {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Site, 0, len(r.sites))
	for _, s := range r.sites {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// EventTrace is an append-only log of scenario events, persisted with a
// violation so a failure can be inspected. Safe for concurrent use.
type EventTrace struct {
	mu     sync.Mutex
	events []string
}

// NewEventTrace returns an empty trace.
func NewEventTrace() *EventTrace { return &EventTrace{} }

// Add appends a formatted event.
func (t *EventTrace) Add(format string, args ...any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, fmt.Sprintf(format, args...))
}

// Events returns a copy of the recorded events.
func (t *EventTrace) Events() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.events...)
}
