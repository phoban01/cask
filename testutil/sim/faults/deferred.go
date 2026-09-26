package faults

import "github.com/phoban01/cask/internal/buggify"

// Deferred buggify sites whose protocol surface is not yet built. Declaring
// them keeps the fault catalog complete — they show up in the "buggify catalog"
// view as known-but-inactive — and reserves their stable names so the firing
// sites can be added in the feature PR without a rename breaking regressions.
func init() {
	buggify.Register("router_force_refetch",
		"agent.Router returns ErrRangeChanged spuriously, forcing a descriptor refetch (lands with §4.1/§4.3)", 0.02)
	buggify.Register("split_pause_pre_cutover",
		"ranges.Orchestrator.Split pauses between CreateR and the roster cutover (lands with §4.3)", 0.05)
}
