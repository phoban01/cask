// Package faults is the cask deterministic-simulator fault catalog (roadmap
// §6.7). Each fault is a named adversary implementing [sim.Fault]; the gate
// injects them at seeded steps. Adding a fault is one new value here plus one
// line in a profile (testutil/sim/profile.go).
//
// PR #0 implements the faults whose protocol surface exists today. Faults named
// in §6.7 but deferred to a later feature PR (mid_cutover_partition,
// concurrent_split_same_range, clock_skew_*, mid_grv_partition, lighthouse_loss,
// core_stale_rejoin) are intentionally absent and documented in docs/sim-gate.md.
//
// Gray-failure link faults (duplicate_delivery, slow_link) model two transport
// behaviours a clean up/down link omits. Message reordering is NOT yet modelled:
// the gate's network delivers Prepare/Accept synchronously and serially, so
// there is no in-flight queue to reorder — that needs the deferred logical-time
// scheduler. Membership-layer (HyParView/Plumtree) duplication and reordering is
// a separate gap: those protocols are exercised by their own package simulators,
// not driven through this gate. Both are tracked in docs/sim-gate.md.
package faults

import "github.com/phoban01/cask/testutil/sim"

// All returns the PR #0 fault catalog keyed by name. The gate selects the
// active subset from the running profile.
func All() map[string]sim.Fault {
	list := []sim.Fault{
		Partition{},
		CrashRestart{},
		MassFailure{},
		AsymmetricReachability{},
		SlowFsync{},
		KeepaliveBlackhole{},
		EpochOldOwnerWrite{},
		OwnerVsFullProposer{},
		DuelingProposers{},
		DuplicateDelivery{},
		SlowLink{},
	}
	out := make(map[string]sim.Fault, len(list))
	for _, f := range list {
		out[f.Name()] = f
	}
	return out
}

// ForProfile returns the faults active in profile p, in catalog order.
func ForProfile(p *sim.Profile) []sim.Fault {
	all := All()
	var out []sim.Fault
	for _, name := range p.Faults {
		if f, ok := all[name]; ok {
			out = append(out, f)
		}
	}
	return out
}
