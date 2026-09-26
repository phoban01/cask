package ranges

import (
	"bytes"
	"context"
	"fmt"
	"slices"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/reconfig"
)

// Orchestrator drives descriptor-register lifecycles (§4.3). This file
// implements replica-set reconfiguration — the release-blocking piece: moving
// a range's data to a new replica set without losing a committed write.
//
// The protocol is the same three-step joint dance the roster uses, applied to
// a whole range through its descriptor register:
//
//  1. PUBLISH JOINT: descriptor gains Joint{Old, New}, epoch bump. From here
//     every router that sees the descriptor proposes through joint quorums
//     (a quorum in BOTH sets), and §4.1's epoch check makes servers reject
//     writers still routing under the pre-joint epoch.
//  2. WAIT OUT ROUTING, then CARRY FORWARD: routers refresh their placement
//     at least every refresh interval, so after waiting it out, no plain
//     old-quorum write can commit (every old-majority contains an
//     epoch-aware server). Then every key on the old set — enumerated as the
//     union over a MAJORITY of old replicas, which necessarily contains every
//     quorum-committed key (two majorities always intersect) — is read and
//     re-committed under a joint quorum (reconfig.CarryForward). Concurrent
//     joint writes are safe: the carry's joint read sees them.
//  3. RELEASE: descriptor becomes {Replicas: New, Joint: nil}, epoch bump,
//     committed while still under the joint regime.
//
// Every step is idempotent and the Joint marker lives in the register, so a
// crashed driver's successor resumes wherever it left off.
type Orchestrator struct {
	self    uint64
	store   *Store
	dialer  Dialer
	listKey KeyLister
	settle  func(ctx context.Context) error // waits out the routing refresh interval
	cutover Cutover                         // roster RangeIDs commit (split/merge)
}

// Dialer resolves node ids to acceptor clients (structurally agent.Dialer).
type Dialer interface {
	Acceptor(node uint64) (caspaxos.AcceptorClient, bool)
}

// KeyLister enumerates the keys node's acceptor store currently holds.
type KeyLister func(ctx context.Context, node uint64) ([][]byte, error)

// NewOrchestrator builds an orchestrator. settle must block for at least the
// interval within which every live router and replica refreshes its placement
// (the routing lease; in cmd this is the reconcile interval plus slack, in
// tests it can be a no-op because the test controls every router).
func NewOrchestrator(self uint64, store *Store, dialer Dialer, listKey KeyLister, settle func(ctx context.Context) error) *Orchestrator {
	return &Orchestrator{self: self, store: store, dialer: dialer, listKey: listKey, settle: settle}
}

// ReconfigReplicas moves range id onto target. It returns the released state,
// or the unchanged state when no move is needed. Resumable: an in-flight
// Joint (from this or a crashed driver) is completed first — toward ITS
// target — before any new move is considered.
func (o *Orchestrator) ReconfigReplicas(ctx context.Context, id uint64, target []uint64) (State, error) {
	cur, present, err := o.store.Get(ctx, id)
	if err != nil {
		return State{}, err
	}
	if !present {
		return State{}, fmt.Errorf("ranges: reconfig: descriptor %d does not exist", id)
	}

	// Resume an in-flight joint first (its target wins this round).
	if cur.Joint != nil {
		return o.completeJoint(ctx, id, cur)
	}
	if cur.Tombstoned || cur.Split != nil || cur.Merge != nil {
		return cur, nil // range is mid-lifecycle elsewhere; not our move
	}
	if equalIDs(cur.Replicas, target) {
		return cur, nil
	}

	// Step 1 — publish the joint descriptor (CAS-guarded on the state we read).
	published, err := o.store.Publish(ctx, id, func(s State, present bool) (State, error) {
		if !present || s.Epoch != cur.Epoch || s.busy() || s.Tombstoned {
			return State{}, caspaxos.ErrConflict // moved under us: retry next tick
		}
		s.Joint = &ReplicaJoint{Old: slices.Clone(s.Replicas), New: slices.Clone(target)}
		s.Epoch++
		return s, nil
	})
	if err != nil {
		return State{}, err
	}
	return o.completeJoint(ctx, id, published)
}

// completeJoint runs steps 2–3 for an already-published joint descriptor.
func (o *Orchestrator) completeJoint(ctx context.Context, id uint64, cur State) (State, error) {
	if o.settle != nil {
		// The routing lease: after this, every live router proposes jointly
		// and every replica rejects pre-joint epochs (§4.1).
		if err := o.settle(ctx); err != nil {
			return State{}, err
		}
	}

	old, err := o.clientsFor(cur.Joint.Old)
	if err != nil {
		return State{}, err
	}
	new_, err := o.clientsFor(cur.Joint.New)
	if err != nil {
		return State{}, err
	}

	// Step 2 — enumerate (majority union) and carry every key forward.
	keys, err := o.majorityKeys(ctx, cur)
	if err != nil {
		return State{}, err
	}
	for _, key := range keys {
		if err := reconfig.CarryForward(ctx, o.self, key, old, new_); err != nil {
			return State{}, fmt.Errorf("ranges: carry forward %q: %w", key, err)
		}
	}

	// Step 3 — release, still CAS-guarded on the joint epoch we completed.
	released, err := o.store.Publish(ctx, id, func(s State, present bool) (State, error) {
		if !present || s.Epoch != cur.Epoch || s.Joint == nil {
			return State{}, caspaxos.ErrConflict
		}
		s.Replicas = slices.Clone(s.Joint.New)
		s.Joint = nil
		s.Epoch++
		return s, nil
	})
	if err != nil {
		return State{}, err
	}
	return released, nil
}

// majorityKeys unions the key lists of at least a majority of the old
// replicas, filtered to the range's interval. Every quorum-committed key sits
// on a majority of the old set, and any two majorities intersect, so the
// union over ANY majority contains every committed key. Core-hosted control
// registers (the roster and descriptor registers themselves) are excluded —
// they are not range data, and their quorums are the Core's, not this
// range's.
func (o *Orchestrator) majorityKeys(ctx context.Context, cur State) ([][]byte, error) {
	need := len(cur.Joint.Old)/2 + 1
	got := 0
	union := map[string]struct{}{}
	var lastErr error
	for _, node := range cur.Joint.Old {
		keys, err := o.listKey(ctx, node)
		if err != nil {
			lastErr = err
			continue
		}
		got++
		for _, k := range keys {
			if !cur.Contains(k) || controlKey(k) {
				continue
			}
			union[string(k)] = struct{}{}
		}
	}
	if got < need {
		return nil, fmt.Errorf("ranges: key enumeration reached %d of %d old replicas, need %d: %w", got, len(cur.Joint.Old), need, lastErr)
	}
	out := make([][]byte, 0, len(union))
	for k := range union {
		out = append(out, []byte(k))
	}
	return out, nil
}

// controlKey reports whether key is a Core-hosted control register rather
// than range data. (Lease/lock/session registers ARE range data — they live
// on range replicas and must be carried.)
func controlKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte("\x00roster")) || bytes.HasPrefix(key, []byte("\x00rd/"))
}

func (o *Orchestrator) clientsFor(ids []uint64) ([]caspaxos.AcceptorClient, error) {
	out := make([]caspaxos.AcceptorClient, 0, len(ids))
	for _, n := range ids {
		a, ok := o.dialer.Acceptor(n)
		if !ok {
			return nil, fmt.Errorf("ranges: replica %d not resolvable", n)
		}
		out = append(out, a)
	}
	return out, nil
}

func equalIDs(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	as, bs := slices.Clone(a), slices.Clone(b)
	slices.Sort(as)
	slices.Sort(bs)
	return slices.Equal(as, bs)
}

// Seed publishes the initial descriptor for a range if none exists yet —
// idempotent, so every node may call it and exactly one genesis wins.
func (o *Orchestrator) Seed(ctx context.Context, d Descriptor) (State, error) {
	return o.store.Publish(ctx, d.ID, func(s State, present bool) (State, error) {
		if present {
			return s, nil // already seeded: keep the committed value
		}
		return State{Descriptor: d}, nil
	})
}

// Cutover applies a split/merge's roster commit: remove the old range ids
// from the live list and add the new ones (cmd wires this to
// roster.UpdateRangeIDs). It must be idempotent — resume paths re-apply it.
type Cutover func(ctx context.Context, remove []uint64, add []uint64) error

// SetCutover injects the roster cutover; Split and Merge require it.
func (o *Orchestrator) SetCutover(f Cutover) { o.cutover = f }

// Split divides range id at key `at` into left/right (§4.3 Variant 1):
//
//  1. record the SplitIntent on the old descriptor (CAS — serializes
//     concurrent splits and makes every later step resumable)
//  2. create both new descriptors on the Core (no routing change yet: the
//     roster still names only the old range)
//  3. CUTOVER: the roster's RangeIDs swap old -> left,right (one commit) —
//     clients route to the halves from their next snapshot poll
//  4. tombstone the old descriptor, pointing at its replacements
//
// Data moves nowhere: both halves inherit the old replica set, and later
// ReconfigReplicas calls rebalance them independently. The new descriptors
// start at old.Epoch+1 — NOT 1 — so §4.1's per-key epoch comparison stays
// monotonic across the lineage (a stale pre-split claim rejects against
// either half). Calling Split again with the recorded intent's parameters
// resumes a crashed run; different parameters conflict.
func (o *Orchestrator) Split(ctx context.Context, id uint64, at []byte, leftID, rightID uint64) (left, right State, err error) {
	if o.cutover == nil {
		return State{}, State{}, fmt.Errorf("ranges: split: no cutover injected")
	}
	cur, present, err := o.store.Get(ctx, id)
	if err != nil {
		return State{}, State{}, err
	}
	if !present {
		return State{}, State{}, fmt.Errorf("ranges: split: descriptor %d does not exist", id)
	}
	if cur.Tombstoned {
		return State{}, State{}, fmt.Errorf("ranges: split: range %d is tombstoned (replaced by %v)", id, cur.ReplacedBy)
	}
	if cur.Joint != nil || cur.Merge != nil {
		return State{}, State{}, fmt.Errorf("ranges: split: range %d has another lifecycle operation in flight", id)
	}
	switch {
	case cur.Split == nil:
		// Step 1 — record the intent.
		cur, err = o.store.Publish(ctx, id, func(s State, present bool) (State, error) {
			if !present || s.Epoch != cur.Epoch || s.busy() || s.Tombstoned {
				return State{}, caspaxos.ErrConflict
			}
			s.Split = &SplitIntent{At: at, Left: leftID, Right: rightID}
			return s, nil
		})
		if err != nil {
			return State{}, State{}, err
		}
	case bytes.Equal(cur.Split.At, at) && cur.Split.Left == leftID && cur.Split.Right == rightID:
		// Resuming the recorded intent.
	default:
		return State{}, State{}, fmt.Errorf("ranges: split: range %d already splitting with different parameters %+v", id, cur.Split)
	}

	// Step 2 — create both descriptors (idempotent: Seed keeps a committed value).
	l, r, ok := cur.Descriptor.Split(at, leftID, rightID)
	if !ok {
		return State{}, State{}, fmt.Errorf("ranges: split point %q outside range %d", at, id)
	}
	l.Epoch, r.Epoch = cur.Epoch+1, cur.Epoch+1
	l.Joint, r.Joint = nil, nil
	leftState, err := o.Seed(ctx, l)
	if err != nil {
		return State{}, State{}, err
	}
	rightState, err := o.Seed(ctx, r)
	if err != nil {
		return State{}, State{}, err
	}

	// Step 3 — the cutover commit (idempotent set arithmetic on the roster).
	if err := o.cutover(ctx, []uint64{id}, []uint64{leftID, rightID}); err != nil {
		return State{}, State{}, fmt.Errorf("ranges: split cutover: %w", err)
	}

	// Step 4 — tombstone the old descriptor for direct readers.
	if _, err := o.store.Publish(ctx, id, func(s State, present bool) (State, error) {
		if s.Tombstoned {
			return s, nil
		}
		s.Tombstoned = true
		s.ReplacedBy = []uint64{leftID, rightID}
		s.Split = nil
		s.Epoch++
		return s, nil
	}); err != nil {
		return State{}, State{}, err
	}
	return leftState, rightState, nil
}

// Merge absorbs right into left as a new range (the split inverse). Both
// halves must be adjacent and already share one replica set — reconfigure
// them onto a common set first; merging moves no data.
func (o *Orchestrator) Merge(ctx context.Context, leftID, rightID, newID uint64) (State, error) {
	if o.cutover == nil {
		return State{}, fmt.Errorf("ranges: merge: no cutover injected")
	}
	left, lok, err := o.store.Get(ctx, leftID)
	if err != nil {
		return State{}, err
	}
	right, rok, err := o.store.Get(ctx, rightID)
	if err != nil {
		return State{}, err
	}
	if !lok || !rok {
		return State{}, fmt.Errorf("ranges: merge: descriptor missing (left=%v right=%v)", lok, rok)
	}
	if left.Tombstoned || right.Tombstoned || left.Joint != nil || right.Joint != nil || left.Split != nil || right.Split != nil || right.Merge != nil {
		return State{}, fmt.Errorf("ranges: merge: a lifecycle operation is in flight")
	}
	if !equalIDs(left.Replicas, right.Replicas) {
		return State{}, fmt.Errorf("ranges: merge: replica sets differ (%v vs %v); reconfigure onto a common set first", left.Replicas, right.Replicas)
	}

	switch {
	case left.Merge == nil:
		left, err = o.store.Publish(ctx, leftID, func(s State, present bool) (State, error) {
			if !present || s.Epoch != left.Epoch || s.busy() || s.Tombstoned {
				return State{}, caspaxos.ErrConflict
			}
			s.Merge = &MergeIntent{With: rightID, Into: newID}
			return s, nil
		})
		if err != nil {
			return State{}, err
		}
	case left.Merge.With == rightID && left.Merge.Into == newID:
		// Resuming the recorded intent.
	default:
		return State{}, fmt.Errorf("ranges: merge: range %d already merging with different parameters %+v", leftID, left.Merge)
	}

	merged, ok := Merge(left.Descriptor, right.Descriptor, newID)
	if !ok {
		return State{}, fmt.Errorf("ranges: merge: ranges %d and %d are not adjacent", leftID, rightID)
	}
	mergedState, err := o.Seed(ctx, merged)
	if err != nil {
		return State{}, err
	}

	if err := o.cutover(ctx, []uint64{leftID, rightID}, []uint64{newID}); err != nil {
		return State{}, fmt.Errorf("ranges: merge cutover: %w", err)
	}

	for _, oldID := range []uint64{leftID, rightID} {
		if _, err := o.store.Publish(ctx, oldID, func(s State, present bool) (State, error) {
			if s.Tombstoned {
				return s, nil
			}
			s.Tombstoned = true
			s.ReplacedBy = []uint64{newID}
			s.Merge = nil
			s.Epoch++
			return s, nil
		}); err != nil {
			return State{}, err
		}
	}
	return mergedState, nil
}
