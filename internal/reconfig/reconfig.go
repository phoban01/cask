// Package reconfig performs safe per-range replica-set changes. A range is moved
// from an old replica set to a new one through a JOINT phase in which every
// write needs a quorum in both sets; before the old set is released, each key's
// committed value is carried into the new set (catch-up-before-release). Because
// a joint quorum overlaps every old and every new quorum, and the latest value
// is present in the new set before release, no committed value is ever lost —
// the property model-checked in tla/Reconfig.tla.
//
// The orchestration order a coordinator must follow:
//  1. Publish the range as JOINT {old, new} (so all writers use a joint quorum).
//  2. CarryForward every key (this package) so the new replicas hold the data.
//  3. Publish the range as new-only (release the old replicas).
//
// Steps 1 and 3 are consensus-ordered config changes on the range descriptor;
// this package provides step 2 and the joint proposer that step 1 enables.
package reconfig

import (
	"context"

	"github.com/phoban01/cask/internal/caspaxos"
)

// CarryForward reads key under a joint quorum of old+new and writes the value
// back, which (via CASPaxos carry-forward) installs the committed value into the
// new replica set. It is idempotent and safe to retry.
func CarryForward(ctx context.Context, nodeID uint64, key []byte, old, new []caspaxos.AcceptorClient) error {
	joint := caspaxos.NewJointProposer(nodeID, [][]caspaxos.AcceptorClient{old, new})
	_, err := joint.Propose(ctx, key, caspaxos.Identity)
	return err
}

// CarryForwardKeys carries every key forward into the new replica set. A range
// reconfiguration calls this for the keys it holds before releasing the old set.
func CarryForwardKeys(ctx context.Context, nodeID uint64, keys [][]byte, old, new []caspaxos.AcceptorClient) error {
	for _, key := range keys {
		if err := CarryForward(ctx, nodeID, key, old, new); err != nil {
			return err
		}
	}
	return nil
}

// JointProposer builds a proposer requiring a quorum in both replica sets, for
// use by writers while a range is in its joint phase.
func JointProposer(nodeID uint64, old, new []caspaxos.AcceptorClient) *caspaxos.Proposer {
	return caspaxos.NewJointProposer(nodeID, [][]caspaxos.AcceptorClient{old, new})
}
