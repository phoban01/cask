package caspaxos

import "github.com/phoban01/cask/internal/buggify"

// Buggify sites in the CASPaxos core. Names are stable (the regression set
// references them); treat a rename like an API break. The sites fire only under
// the simulator; in production buggify.Maybe is a single nil-check.
func init() {
	buggify.Register("owned_proposer_force_full_round",
		"OwnedProposer.Write drops to the full phase-1 path instead of the 1-RTT fast path", 0.02)
	buggify.Register("proposer_drop_vote",
		"Proposer treats one acceptor's prepare reply as a non-vote", 0.01)
	buggify.Register("acceptor_spurious_preempted",
		"Acceptor.Prepare rejects spuriously, as if a higher ballot had arrived", 0.01)
}
