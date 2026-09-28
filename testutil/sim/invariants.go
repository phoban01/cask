package sim

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/phoban01/cask/internal/caspaxos"
)

// Snapshot is the durable cluster state at one quiescent point: every observed
// key mapped to its per-acceptor [caspaxos.Register]. Safety invariants are
// predicates over this — register state is exactly what survives a crash, so a
// violation here is a real, durable safety break, not a transient.
type Snapshot struct {
	// Registers[key] holds one Register per acceptor, index-aligned with the
	// sim's stores. A never-touched acceptor contributes the zero Register.
	Registers map[string][]caspaxos.Register
}

// snapshot reads every observed key from every store. It is taken at a
// quiescent point (the workload paused), so the per-acceptor reads form a
// coherent picture.
func (s *Sim) snapshot(ctx context.Context) (Snapshot, error) {
	snap := Snapshot{Registers: make(map[string][]caspaxos.Register)}
	for _, key := range s.ObservedKeys() {
		regs := make([]caspaxos.Register, len(s.Stores))
		for i, st := range s.Stores {
			r, err := st.Load(ctx, key)
			if err != nil {
				return Snapshot{}, fmt.Errorf("snapshot load key %q acceptor %d: %w", key, i, err)
			}
			regs[i] = r
		}
		snap.Registers[string(key)] = regs
	}
	return snap, nil
}

// chosen returns the register carrying the highest Accepted ballot across the
// acceptors — the value a reader would carry forward, i.e. the latest committed
// state. ok is false when no acceptor has accepted anything.
func chosen(regs []caspaxos.Register) (caspaxos.Register, bool) {
	var (
		best caspaxos.Register
		ok   bool
	)
	for _, r := range regs {
		if r.Accepted.IsZero() {
			continue
		}
		if !ok || best.Accepted.Less(r.Accepted) {
			best, ok = r, true
		}
	}
	return best, ok
}

// Invariant is one enumerable safety (S*) or liveness (L*) predicate. Check
// returns a non-nil error to report a violation; Implemented is false for items
// whose protocol surface does not yet exist (their Check is a no-op until the
// feature lands — see the TODO on each).
type Invariant struct {
	ID          string
	Desc        string
	Implemented bool
	Check       func(prev, cur Snapshot) error
}

// All returns the full §6.5 invariant table. Implemented==true items are
// evaluated every quiescent step; the rest are registered for honest coverage
// accounting and activate when their feature PR lands.
func All() []Invariant {
	return []Invariant{
		{ID: "S1", Desc: "per-register agreement (per-ballot value agreement)", Implemented: true, Check: checkS1},
		{ID: "S2", Desc: "per-key MVCC Seq monotonic + exactly-once op", Implemented: true, Check: checkS2},
		{ID: "S3", Desc: "per-key HLC monotonic", Implemented: true, Check: checkS3},
		{ID: "S11", Desc: "owner-epoch dominance (chosen ballot never regresses)", Implemented: true, Check: checkS11},
		{ID: "S13", Desc: "committed MVCC history is append-only (no lost update)", Implemented: true, Check: checkS13},

		// Registered, not yet implemented — each needs subsystem observation
		// state the PR #0 gate does not yet collect, or a feature not yet built.
		{ID: "S4", Desc: "no committed value lost across reconfig — TODO: observe roster.Reconfigure carry-forward", Implemented: false},
		{ID: "S5", Desc: "catch-up before release — TODO: observe joint phase transitions", Implemented: false},
		{ID: "S6", Desc: "single lock holder — TODO: observe lease.Sessions holder set", Implemented: false},
		{ID: "S7", Desc: "fence monotonicity — TODO: observe issued fencing tokens", Implemented: false},
		{ID: "S8", Desc: "no two replica sets per key — TODO(PR#2 §4.3): ranges orchestrator", Implemented: false},
		{ID: "S9", Desc: "descriptor-epoch carry-forward — TODO(PR#1 §4.1 + PR#2 §4.3)", Implemented: false},
		{ID: "S10", Desc: "cross-range HLC skew bound — TODO(§4.4): hlc.MaxOffset", Implemented: false},
		{ID: "S12", Desc: "watcher non-starvation — TODO: observe watch cursors vs compact watermark", Implemented: false},
		{ID: "L1", Desc: "eventual rmap consistency — TODO(PR#2): fairness model", Implemented: false},
		{ID: "L2", Desc: "eventual lock takeover — TODO: fairness model", Implemented: false},
		// L3 is enforced inline by the gate loop (gate.go), not as a snapshot
		// predicate: it needs the per-round fault schedule, which snapshots
		// don't carry. Registered here for honest coverage accounting.
		{ID: "L3", Desc: "healed-dwell progress: no retry-budget exhaustion without an active fault (checked inline by the gate)", Implemented: false},
	}
}

// Implemented returns only the invariants the gate actively evaluates.
func Implemented() []Invariant {
	var out []Invariant
	for _, inv := range All() {
		if inv.Implemented {
			out = append(out, inv)
		}
	}
	return out
}

// checkS1: at any single ballot, an acceptor set may accept only one value. Two
// acceptors reporting the same Accepted ballot but different values is a
// per-register agreement break — the strongest CASPaxos safety property.
func checkS1(_, cur Snapshot) error {
	for key, regs := range cur.Registers {
		byBallot := make(map[caspaxos.Ballot][32]byte)
		for _, r := range regs {
			if r.Accepted.IsZero() {
				continue
			}
			h := sha256.Sum256(r.Value)
			if prev, ok := byBallot[r.Accepted]; ok && prev != h {
				return fmt.Errorf("S1: key %q has two distinct values accepted at ballot %s", key, r.Accepted)
			}
			byBallot[r.Accepted] = h
		}
	}
	return nil
}

// checkS2: in the chosen value's MVCC chain, Seq is strictly increasing and no
// OpID appears twice (the exactly-once guarantee). Registers whose value is not
// an MVCC history (e.g. the roster register) are skipped.
func checkS2(_, cur Snapshot) error {
	for key, regs := range cur.Registers {
		r, ok := chosen(regs)
		if !ok {
			continue
		}
		h, isMVCC := decodeMVCC(r.Value)
		if !isMVCC {
			continue
		}
		var lastSeq uint64
		seenOps := make(map[mvccOp]struct{})
		for i, v := range h.Versions {
			if i > 0 && v.Seq <= lastSeq {
				return fmt.Errorf("S2: key %q version %d has Seq %d not > previous %d", key, i, v.Seq, lastSeq)
			}
			lastSeq = v.Seq
			if _, dup := seenOps[v.Op]; dup {
				return fmt.Errorf("S2: key %q op %+v appears twice (exactly-once violated)", key, v.Op)
			}
			seenOps[v.Op] = struct{}{}
		}
	}
	return nil
}

// checkS3: in the chosen value's MVCC chain, HLC timestamps are strictly
// increasing in commit order.
func checkS3(_, cur Snapshot) error {
	for key, regs := range cur.Registers {
		r, ok := chosen(regs)
		if !ok {
			continue
		}
		h, isMVCC := decodeMVCC(r.Value)
		if !isMVCC {
			continue
		}
		for i := 1; i < len(h.Versions); i++ {
			a, b := h.Versions[i-1].HLC, h.Versions[i].HLC
			if !(a.Physical < b.Physical || (a.Physical == b.Physical && a.Logical < b.Logical)) {
				return fmt.Errorf("S3: key %q HLC not strictly increasing at version %d (%d.%d then %d.%d)",
					key, i, a.Physical, a.Logical, b.Physical, b.Logical)
			}
		}
	}
	return nil
}

// checkS11: the chosen ballot for a key never regresses between snapshots. The
// epoch is encoded in the ballot's high bits, so a stale owner committing at a
// lower epoch would show up as a backwards chosen ballot.
func checkS11(prev, cur Snapshot) error {
	for key, regs := range cur.Registers {
		c, ok := chosen(regs)
		if !ok {
			continue
		}
		pregs, ok := prev.Registers[key]
		if !ok {
			continue
		}
		p, ok := chosen(pregs)
		if !ok {
			continue
		}
		if c.Accepted.Less(p.Accepted) {
			return fmt.Errorf("S11: key %q chosen ballot regressed from %s to %s (stale owner committed?)",
				key, p.Accepted, c.Accepted)
		}
	}
	return nil
}

// committed returns the register whose value is quorum-committed: a majority
// of acceptors hold the same Accepted ballot. Unlike chosen (max accepted
// anywhere), this excludes partial accepts from abandoned rounds, which a
// later round may legitimately not carry forward — comparing those across
// snapshots would be a false positive. A quorum-accepted value, by contrast,
// is chosen forever; ok is false when no ballot currently has a quorum.
func committed(regs []caspaxos.Register) (caspaxos.Register, bool) {
	count := make(map[caspaxos.Ballot]int)
	for _, r := range regs {
		if !r.Accepted.IsZero() {
			count[r.Accepted]++
		}
	}
	quorum := len(regs)/2 + 1
	var (
		best caspaxos.Register
		ok   bool
	)
	for _, r := range regs {
		if r.Accepted.IsZero() || count[r.Accepted] < quorum {
			continue
		}
		if !ok || best.Accepted.Less(r.Accepted) {
			best, ok = r, true
		}
	}
	return best, ok
}

// checkS13: a quorum-committed MVCC chain only ever grows (modulo compaction).
// Every version in the previous committed chain, at or above the current
// compaction watermark, must appear unchanged (same Seq, same Op) in the
// current committed chain. A committed version disappearing or mutating is a
// lost update — the failure class of a fast-path writer committing from a
// stale cache over an interleaved full-proposer round (W0 in
// docs/plans/quepaxa-learnings-implementation.md). Structural: it catches the
// class regardless of which fault produced it.
func checkS13(prev, cur Snapshot) error {
	for key, regs := range cur.Registers {
		curC, ok := committed(regs)
		if !ok {
			continue
		}
		pregs, ok := prev.Registers[key]
		if !ok {
			continue
		}
		prevC, ok := committed(pregs)
		if !ok {
			continue
		}
		ph, pok := decodeMVCC(prevC.Value)
		ch, cok := decodeMVCC(curC.Value)
		if !pok || !cok {
			continue
		}
		bySeq := make(map[uint64]mvccVersion, len(ch.Versions))
		for _, v := range ch.Versions {
			bySeq[v.Seq] = v
		}
		for _, v := range ph.Versions {
			if v.Seq < ch.CompactedBelow {
				continue // legitimately garbage-collected
			}
			cv, present := bySeq[v.Seq]
			if !present || cv.Op != v.Op {
				return fmt.Errorf("S13: key %q committed version seq=%d op=%+v disappeared or changed (lost update)",
					key, v.Seq, v.Op)
			}
		}
	}
	return nil
}

// --- minimal MVCC history decoding for S2/S3 -------------------------------
//
// PR #0 decodes the M1 inline-JSON history shape (internal/mvcc encodes the
// version chain as JSON in the register value). When mvcc moves history out of
// the register value (a later milestone), update this decoder in lockstep.

type mvccOp struct {
	Node uint64 `json:"node"`
	Inc  uint64 `json:"inc,omitempty"`
	Seq  uint64 `json:"seq"`
}

type mvccTS struct {
	Physical int64  `json:"Physical"`
	Logical  uint32 `json:"Logical"`
}

type mvccVersion struct {
	Seq uint64 `json:"seq"`
	HLC mvccTS `json:"hlc"`
	Op  mvccOp `json:"op"`
}

type mvccHistory struct {
	Versions       []mvccVersion `json:"versions"`
	CompactedBelow uint64        `json:"compacted_below,omitempty"`
}

// decodeMVCC reports whether raw is an MVCC history and returns it. A value that
// is not valid history JSON (nil, or another register type) returns ok=false.
func decodeMVCC(raw []byte) (mvccHistory, bool) {
	if len(raw) == 0 {
		return mvccHistory{}, false
	}
	var h mvccHistory
	if err := json.Unmarshal(raw, &h); err != nil {
		return mvccHistory{}, false
	}
	if h.Versions == nil {
		return mvccHistory{}, false
	}
	return h, true
}
