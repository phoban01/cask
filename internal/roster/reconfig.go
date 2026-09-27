package roster

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/phoban01/cask/internal/buggify"
	"github.com/phoban01/cask/internal/caspaxos"
)

// errBuggifyAbort models a crash between the joint-publish and the carry-forward
// of a reflexive reconfiguration. The published Joint is left in flight; a later
// Reconfigure (by this node or a peer) resumes it idempotently — which is
// exactly the recovery path this site is meant to exercise.
var errBuggifyAbort = errors.New("roster: reconfig aborted mid-joint (buggify)")

func init() {
	buggify.Register("roster_abort_joint",
		"Reconfigure aborts after publishing the Joint marker, before carry-forward", 0.05)
}

// RegisterRF is the target size of the roster register's acceptor set (the
// Core): the bounded number of nodes that physically store the membership
// register. It is odd so a single failure never costs the register its quorum.
const RegisterRF = 3

// nextCore is the deterministic target Core given the current core and the live
// membership. It MINIMISES CHURN: it keeps every current core member that is
// still in the roster and, only if the core is under-filled, adds the
// highest-id non-core members up to rf. Minimal churn matters for liveness — a
// reconfiguration must reach a quorum of BOTH the old and new core, so keeping
// the (already-connected) current members and only ADDING nodes avoids a
// disjoint handoff that would require the driver to reach an entirely new set at
// once. The result is deterministic from (current, members), so every node
// computes the same target. (A zone-spread tiebreak is a future refinement.)
func nextCore(current []uint64, members []Member, rf int) []uint64 {
	valid := map[uint64]bool{}
	for _, m := range members {
		valid[m.NodeID] = true
	}
	keep := map[uint64]bool{}
	for _, id := range current {
		if valid[id] {
			keep[id] = true
		}
	}
	// Already at or above target: keep the highest-id rf of the survivors.
	if len(keep) >= rf {
		ids := mapKeys(keep)
		if len(ids) > rf {
			ids = ids[len(ids)-rf:]
		}
		return normalizeIDs(ids)
	}
	// Under-filled: add the highest-id members not already kept, until rf.
	all := memberIDs(members) // sorted ascending
	for i := len(all) - 1; i >= 0 && len(keep) < rf; i-- {
		keep[all[i]] = true
	}
	return normalizeIDs(mapKeys(keep))
}

func mapKeys(m map[uint64]bool) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Reconfigure moves the register's own acceptor set toward target by running
// joint consensus ON THE ROSTER KEY ITSELF — the same lossless old->joint->new
// dance that internal/reconfig performs for data ranges, here applied
// reflexively. The register value records the joint state, so the operation is
// idempotent and any node can resume it after a crash by re-reading.
//
// Steps (each a CASPaxos round on Key):
//  1. Publish JOINT {Old: Core, New: target}, proposed against the OLD Core
//     (still the sole authority at that instant); bump ConfigGen.
//  2. Carry-forward: a joint Identity round over {Old, New} installs the
//     committed value (incl. the Joint marker) into the new acceptors — exactly
//     reconfig.CarryForward, expressed through the proposer factory.
//  3. Release: set Core=New, clear Joint, proposed against the JOINT quorum (so
//     it safely supersedes any writer still operating under joint rules).
func (r *Roster) Reconfigure(ctx context.Context, target []uint64) (Value, error) {
	target = normalizeIDs(target)
	const attempts = 16
	var lastErr error
	for i := 0; i < attempts; i++ {
		cur, err := r.Get(ctx)
		if err != nil {
			return Value{}, err
		}
		if cur.Joint != nil {
			// A reconfiguration is already in flight (started by us or a peer).
			// Drive it to completion; the caller re-evaluates the target next tick.
			v, err := r.finishJoint(ctx, cur)
			if err != nil {
				if retryable(err) {
					lastErr = err
					continue
				}
				return Value{}, err
			}
			return v, nil
		}
		if idsEqual(cur.Core, target) {
			return cur, nil // already at target
		}
		v, err := r.publishJoint(ctx, cur.Core, target, cur.ConfigGen)
		if err != nil {
			if retryable(err) {
				lastErr = err
				continue
			}
			return Value{}, err
		}
		// BUGGIFY: crash here, after the Joint is published but before
		// carry-forward. The in-flight Joint must be resumable, not lost.
		if buggify.Maybe("roster_abort_joint", 0.05) {
			return Value{}, errBuggifyAbort
		}
		v, err = r.finishJoint(ctx, v)
		if err != nil {
			if retryable(err) {
				lastErr = err
				continue
			}
			return Value{}, err
		}
		return v, nil
	}
	if lastErr == nil {
		lastErr = caspaxos.ErrPreempted
	}
	return Value{}, lastErr
}

// publishJoint commits the JOINT marker against the old Core (Step 1). If a
// peer already published a joint, it adopts that one unchanged.
func (r *Roster) publishJoint(ctx context.Context, old, target []uint64, expectGen uint64) (Value, error) {
	v, err := commitWith(ctx, r.mk([][]uint64{old}), r.key, func(raw Value, present bool) (Value, error) {
		if !present {
			return Value{}, fmt.Errorf("roster: not initialised")
		}
		if raw.Joint != nil {
			return raw, nil // a reconfiguration is already in flight; adopt it
		}
		if raw.ConfigGen != expectGen {
			return Value{}, errConfigShifted
		}
		raw.Joint = &Joint{Old: normalizeIDs(old), New: target}
		raw.ConfigGen++
		return raw, nil
	})
	if err == nil {
		r.learn(v)
	}
	return v, err
}

// finishJoint performs the carry-forward (Step 2) and release (Step 3) for an
// in-flight joint value. Both steps are idempotent: a concurrent driver that
// already released leaves Joint nil, in which case release is a no-op.
func (r *Roster) finishJoint(ctx context.Context, v Value) (Value, error) {
	if v.Joint == nil {
		return v, nil
	}
	old, newCore := v.Joint.Old, v.Joint.New

	// Step 2 — carry-forward: a joint Identity round writes the current value
	// into the new acceptors (equivalent to reconfig.CarryForward(Key, old, new)).
	if _, err := r.read(ctx, [][]uint64{old, newCore}); err != nil {
		return Value{}, err
	}

	// Step 3 — release: choose new-only Core under a joint quorum.
	out, err := commitWith(ctx, r.mk([][]uint64{old, newCore}), r.key, func(raw Value, present bool) (Value, error) {
		if !present {
			return Value{}, fmt.Errorf("roster: not initialised")
		}
		if raw.Joint == nil {
			return raw, nil // already released by a concurrent driver
		}
		raw.Core = normalizeIDs(raw.Joint.New)
		raw.Joint = nil
		raw.ConfigGen++
		return raw, nil
	})
	if err == nil {
		r.learn(out)
	}
	return out, err
}

// retryable reports whether a roster write may run again from a fresh read.
// caspaxos.ErrUnknownOutcome is retryable because every retry re-reads the
// roster first and then commits under a ConfigGen guard. Every roster change
// is also idempotent: Add, Remove, and the range-id rewrite give the same
// value when applied twice, and the joint steps adopt a step that landed.
func retryable(err error) bool {
	//= docs/spec/fleet.md#3-storage-model
	//# A retried write MUST be a compare-and-set, never a blind reapplication of a change.
	return errors.Is(err, errConfigShifted) ||
		errors.Is(err, caspaxos.ErrPreempted) ||
		errors.Is(err, caspaxos.ErrUnknownOutcome)
}
