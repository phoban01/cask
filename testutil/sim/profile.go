package sim

// Profile is a named subset of faults plus per-site buggify probability
// overrides. The gate runs each profile to its seed budget. Faults are
// referenced by string name so this package never imports testutil/sim/faults
// (that arrow is one-directional: faults imports sim).
//
// The PR #0 profiles below mirror the cask roadmap §6.7 table, restricted to
// the faults whose protocol surface exists today. The deferred profiles
// (`ranges`, `snapshot`) land with their features (§4.3 / §4.6); the deferred
// faults (clock_skew_*, lighthouse_loss, …) are noted in faults/ and docs.
type Profile struct {
	Name string

	// Faults is the set of fault names active in this profile.
	Faults []string

	// SiteProb overrides a buggify site's default probability. A name absent
	// here uses the site's own default.
	SiteProb map[string]float64

	// DisabledSites lists buggify sites turned off for this profile.
	DisabledSites map[string]bool
}

// AdjustedProb returns the firing probability for a buggify site under this
// profile: the override if present, else the site's own default.
func (p *Profile) AdjustedProb(name string, def float64) float64 {
	if p == nil {
		return def
	}
	if v, ok := p.SiteProb[name]; ok {
		return v
	}
	return def
}

// SiteDisabled reports whether a buggify site is turned off for this profile.
func (p *Profile) SiteDisabled(name string) bool {
	return p != nil && p.DisabledSites[name]
}

// HasFault reports whether a fault name is active in this profile.
func (p *Profile) HasFault(name string) bool {
	for _, f := range p.Faults {
		if f == name {
			return true
		}
	}
	return false
}

// The fault names in the catalog that PR #0 implements (see faults/). Kept as
// constants so profiles and faults agree on spelling — a renamed fault that
// drifts from a profile string would silently never run.
const (
	FaultPartition           = "partition"
	FaultCrashRestart        = "crash_restart"
	FaultMassFailure         = "mass_failure"
	FaultSlowFsync           = "slow_fsync"
	FaultAsymmetricReach     = "asymmetric_reachability"
	FaultKeepaliveBlackhole  = "keepalive_blackhole"
	FaultEpochOldOwnerWrite  = "epoch_old_owner_write"
	FaultDuplicateDelivery   = "duplicate_delivery"
	FaultSlowLink            = "slow_link"
	FaultOwnerVsFullProposer = "owner_vs_full_proposer"
	FaultDuelingProposers    = "dueling_proposers"
)

// Smoke is the fastest profile — runs on every PR.
func Smoke() *Profile {
	return &Profile{
		Name:   "smoke",
		Faults: []string{FaultPartition, FaultCrashRestart},
	}
}

// Consensus exercises the consensus-facing faults — runs on every PR. Includes
// the gray-failure link faults (duplicate delivery, slow link) that stress
// consensus idempotency and timeout handling beyond a clean up/down partition.
func Consensus() *Profile {
	return &Profile{
		Name: "consensus",
		Faults: []string{
			FaultPartition, FaultCrashRestart, FaultSlowFsync,
			FaultAsymmetricReach, FaultEpochOldOwnerWrite,
			FaultDuplicateDelivery, FaultSlowLink,
			FaultOwnerVsFullProposer, FaultDuelingProposers,
		},
	}
}

// Lease exercises session/keepalive faults. The clock_skew_* faults named in
// the roadmap §6.7 `lease` profile are deferred until hlc.MaxOffset lands
// (§4.4); documented in docs/sim-gate.md.
func Lease() *Profile {
	return &Profile{
		Name:   "lease",
		Faults: []string{FaultPartition, FaultCrashRestart, FaultKeepaliveBlackhole},
	}
}

// Cluster exercises mass failure on top of partition. The lighthouse_loss and
// core_stale_rejoin faults named in the roadmap are deferred until their
// surface is sim-driven.
func Cluster() *Profile {
	return &Profile{
		Name:   "cluster",
		Faults: []string{FaultPartition, FaultMassFailure},
	}
}

// Profiles returns the runnable PR #0 profiles by name.
func Profiles() map[string]*Profile {
	ps := []*Profile{Smoke(), Consensus(), Lease(), Cluster()}
	out := make(map[string]*Profile, len(ps))
	for _, p := range ps {
		out[p.Name] = p
	}
	return out
}
