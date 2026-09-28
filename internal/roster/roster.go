// Package roster holds cask's authoritative cluster membership in a CASPaxos
// register. This is the stable source of truth that HRW placement and per-range
// quorums are computed over — deliberately distinct from the gossip/liveness
// view, which may flap. A node is added or removed here only by consensus, so a
// transient gossip false positive can never destabilise placement; only a
// multiply-witnessed, committed change does.
//
// The register is REFLEXIVE: its value records its own acceptor set (the Core).
// The cluster founds from a single node and the Core grows or shrinks as
// membership changes, via joint-consensus reconfiguration on the register's own
// key (see reconfig.go) — there is no static, baked-in genesis member set. The
// full Members list (every node) drives data placement; the Core is a bounded
// subset that physically stores this register.
package roster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"

	"github.com/phoban01/cask/internal/caspaxos"
)

// Member is one cluster node: its consensus/placement id, its address (for the
// dialer and membership plane), and its failure domain (for zone-aware placement).
type Member struct {
	NodeID uint64 `json:"node"`
	Addr   string `json:"addr"`
	Zone   string `json:"zone"`
}

// Joint records an in-flight reconfiguration of the register's own acceptor set.
// It is non-nil only between the start and finalisation of a Core change; while
// it is set, every writer must use a joint quorum (a majority in BOTH Old and
// New), which is what makes the transition lossless.
type Joint struct {
	Old []uint64 `json:"old"`
	New []uint64 `json:"new"`
}

// Value is the register's contents.
type Value struct {
	// Epoch is bumped on every Members change; it is the version data placement
	// keys off.
	Epoch uint64 `json:"epoch"`
	// Members is the full membership set — the input to zone-aware HRW placement.
	Members []Member `json:"members"`
	// Core is the register's own CASPaxos acceptor set (sorted node ids): the
	// bounded subset of Members that physically stores this register.
	Core []uint64 `json:"core"`
	// Joint is non-nil only while the Core is being reconfigured.
	Joint *Joint `json:"joint,omitempty"`
	// ConfigGen is bumped ONLY when Core/Joint changes, so a reader can cheaply
	// detect that the register's acceptor set has moved.
	ConfigGen uint64 `json:"cfg_gen"`
	// RangeIDs is the authoritative list of live range ids (§4.3): the roster
	// is the index, each range's descriptor register (\x00rd/<id>, hosted on
	// the Core) is the placement authority. Empty means the pre-§4.3 implicit
	// single range (id 1) — readers treat the two identically, so existing
	// clusters upgrade in place. Split/merge commits update this list; that
	// update IS the routing cutover.
	RangeIDs []uint64 `json:"range_ids,omitempty"`
}

// Proposer is the consensus operation the roster needs (satisfied by
// *caspaxos.Proposer and by the agent Router).
type Proposer interface {
	Propose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error)
}

// ProposerFactory builds a consensus proposer over the given acceptor groups,
// each a set of node ids. A single group is ordinary CASPaxos; two groups give
// joint consensus for reconfiguration. The roster calls this to target its
// current Core (or, mid-reconfiguration, the joint Old+New sets); the caller's
// closure resolves node ids to acceptor links (over the overlay, or the sim
// network in tests).
type ProposerFactory func(groups [][]uint64) Proposer

// Key is the reserved register key the roster lives at.
var Key = []byte("\x00roster")

// errConfigShifted aborts a write whose proposer was built for a configuration
// that the register has since moved past; the caller refreshes and retries.
var errConfigShifted = errors.New("roster: configuration shifted under write")

// Roster is a handle to the membership register.
type Roster struct {
	self uint64
	mk   ProposerFactory
	key  []byte

	// believed is the acceptor set this handle currently proposes against,
	// learned from the last successful read and advanced as the Core moves.
	mu       sync.Mutex
	believed []uint64
	gen      uint64

	// carry moves registers other than the roster key onto the new core.
	// It is nil when the core hosts only the roster key.
	carry CarryFunc
}

// New returns a Roster for node self whose proposers are built by mk. The
// handle has no believed acceptor set yet: a founder establishes it via
// Founder/Genesis; a joiner seeds it via AdoptCore (from a peer's snapshot)
// before its first read.
func New(self uint64, mk ProposerFactory) *Roster {
	return &Roster{self: self, mk: mk, key: Key}
}

// NewWithProposer returns a Roster backed by a single fixed proposer, ignoring
// acceptor-group selection. It is for the static, single-group case (tests and
// any deployment with a fixed roster acceptor set).
func NewWithProposer(self uint64, p Proposer) *Roster {
	return New(self, func([][]uint64) Proposer { return p })
}

// AdoptCore seeds the believed acceptor set from an out-of-band hint (e.g. a
// peer's roster snapshot) so a joining node can read the register before it
// knows the authoritative Core. The first successful read corrects it.
func (r *Roster) AdoptCore(ids []uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.believed = normalizeIDs(ids)
}

// believedCore returns a copy of the current believed acceptor set.
func (r *Roster) believedCore() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.believed...)
}

// learn advances the believed acceptor set from a freshly read value. During a
// joint phase the believed set is the union of Old and New, so reads and writes
// keep a quorum that overlaps wherever the latest value was chosen.
func (r *Roster) learn(v Value) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if v.ConfigGen < r.gen {
		return
	}
	if v.Joint != nil {
		r.believed = normalizeIDs(append(append([]uint64(nil), v.Joint.Old...), v.Joint.New...))
	} else {
		r.believed = normalizeIDs(v.Core)
	}
	r.gen = v.ConfigGen
}

// Genesis installs the initial membership if the register is empty, with the
// member set as the initial Core. It is a no-op (returning the existing value)
// if a roster already exists, so concurrent seeders converge instead of
// clobbering one another. It proposes against the members being founded — for a
// single-node Founder that is just {self}, a trivially available quorum.
func (r *Roster) Genesis(ctx context.Context, members []Member) (Value, error) {
	ids := memberIDs(members)
	prop := r.mk([][]uint64{ids})
	v, err := commitWith(ctx, prop, r.key, func(cur Value, present bool) (Value, error) {
		if present {
			return cur, nil
		}
		return Value{Epoch: 1, Members: normalize(members), Core: normalizeIDs(ids), ConfigGen: 1}, nil
	})
	if err == nil {
		r.learn(v)
	}
	return v, err
}

// Founder bootstraps the cluster as a single-node register: it founds the
// roster with Core {self}. Exactly one node (the one started with --bootstrap)
// calls this; every other node joins via Add against the discovered Core. This
// asymmetry is the split-brain defence — a node that is not the founder never
// creates a register, it only joins one.
func (r *Roster) Founder(ctx context.Context, self Member) (Value, error) {
	return r.Genesis(ctx, []Member{self})
}

// Add inserts m (idempotent: re-adding an existing node id is a no-op). It is
// joint-aware: while a reconfiguration is in flight it proposes against the
// joint quorum so the membership change survives the transition.
func (r *Roster) Add(ctx context.Context, m Member) (Value, error) {
	return r.write(ctx, func(cur Value) (Value, error) {
		for _, e := range cur.Members {
			if e.NodeID == m.NodeID {
				cur.Members = normalize(cur.Members) // canonicalise, no change
				return cur, nil
			}
		}
		cur.Members = normalize(append(cur.Members, m))
		cur.Epoch++
		return cur, nil
	})
}

// Remove deletes the node with the given id (idempotent if absent). It also
// strips the id from the Core and from any in-flight Joint.New, so a
// reconfiguration never finalises onto a removed node. A change to the Core
// or to the Joint bumps ConfigGen, so every holder of a cached quorum sees
// that it moved.
//
// When a carry hook is set, other registers live on the core too, so Remove
// never drops a voter from a quiescent core directly. It first runs a core
// change to the core without that voter, which carries every register, and
// then removes the member. It refuses to remove the last voter. A Remove
// that narrows an in-flight Joint.New stays safe: finishJoint sees the
// narrowed joint and carries again before it releases.
//
// While a Joint is in flight, a node in Joint.Old stays in Members. The old
// quorum still counts it, and members resolve its address from Members.
// Remove drops it on a later call, after the release.
func (r *Roster) Remove(ctx context.Context, nodeID uint64) (Value, error) {
	const attempts = 16
	for i := 0; i < attempts; i++ {
		carries := r.carryFunc() != nil
		v, err := r.write(ctx, func(cur Value) (Value, error) {
			return removeFrom(cur, nodeID, carries)
		})
		if !errors.Is(err, errNeedsCoreChange) {
			return v, err
		}
		//= docs/spec/fleet.md#6-membership
		//# The voter set MUST change only by joint-consensus reconfiguration of the roster.
		if _, err := r.reconfigureTo(ctx, func(core []uint64) []uint64 {
			if next := dropID(core, nodeID); len(next) > 0 {
				return next
			}
			return core // the next write refuses to remove the last voter
		}); err != nil {
			return Value{}, fmt.Errorf("roster: remove voter %d: %w", nodeID, err)
		}
	}
	return Value{}, fmt.Errorf("roster: remove voter %d: core did not settle after %d core changes", nodeID, attempts)
}

// errNeedsCoreChange stops a Remove write that would drop a voter from a
// quiescent core while other registers live on it. Remove then runs a core
// change and tries again.
var errNeedsCoreChange = errors.New("roster: removing a voter needs a core change")

// ErrLastVoter is returned by Remove when the node is the only voter left.
var ErrLastVoter = errors.New("roster: cannot remove the last voter")

// removeFrom is the Remove change on one roster value. carries is true when
// a carry hook is set: then a voter of a quiescent core leaves only through
// a core change.
func removeFrom(cur Value, nodeID uint64, carries bool) (Value, error) {
	if carries {
		if cur.Joint == nil && slices.Contains(cur.Core, nodeID) {
			if len(cur.Core) == 1 {
				return Value{}, ErrLastVoter
			}
			return Value{}, errNeedsCoreChange
		}
		if cur.Joint != nil && idsEqual(cur.Joint.New, []uint64{nodeID}) {
			return Value{}, ErrLastVoter
		}
	}
	configChanged := false
	if next := dropID(cur.Core, nodeID); len(next) != len(cur.Core) {
		cur.Core = next
		configChanged = true
	}
	inOld := false
	if cur.Joint != nil {
		if next := dropID(cur.Joint.New, nodeID); len(next) != len(cur.Joint.New) {
			cur.Joint.New = next
			configChanged = true
		}
		inOld = slices.Contains(cur.Joint.Old, nodeID)
	}
	if configChanged {
		cur.ConfigGen++
	}
	if inOld {
		return cur, nil
	}
	out := cur.Members[:0:0]
	removed := false
	for _, e := range cur.Members {
		if e.NodeID == nodeID {
			removed = true
			continue
		}
		out = append(out, e)
	}
	if !removed {
		return cur, nil
	}
	cur.Members = out
	cur.Epoch++
	return cur, nil
}

// ConfigGenOf returns the ConfigGen of an encoded roster value. An empty
// value has ConfigGen 0. The write fence in the extension server reads it
// from the roster value its acceptor holds.
func ConfigGenOf(raw []byte) (uint64, error) {
	if len(raw) == 0 {
		return 0, nil
	}
	var v struct {
		ConfigGen uint64 `json:"cfg_gen"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return 0, fmt.Errorf("roster: decode: %w", err)
	}
	return v.ConfigGen, nil
}

// UpdateRangeIDs atomically rewrites the live range-id list (§4.3). This is
// the split/merge CUTOVER commit: the moment it lands, snapshot polls route
// clients to the new ranges. mutate receives the current list — the implicit
// single range materialized as [1] so mutations compose — and returns the
// desired list; an actual change bumps Epoch (a new range is observable like
// any membership change).
func (r *Roster) UpdateRangeIDs(ctx context.Context, mutate func(ids []uint64) []uint64) (Value, error) {
	return r.write(ctx, func(cur Value) (Value, error) {
		in := cur.RangeIDs
		if len(in) == 0 {
			in = []uint64{1}
		}
		out := normalizeIDs(mutate(append([]uint64(nil), in...)))
		if idsEqual(out, normalizeIDs(in)) {
			return cur, nil
		}
		cur.RangeIDs = out
		cur.Epoch++
		return cur, nil
	})
}

// Get returns the current membership (linearizable read). It reads against the
// believed acceptor set and, if a reconfiguration is in flight, re-reads against
// the joint union so the value returned is the latest chosen one. Either way it
// advances the believed set so subsequent operations target the live Core.
func (r *Roster) Get(ctx context.Context) (Value, error) {
	core := r.believedCore()
	if len(core) == 0 {
		return Value{}, fmt.Errorf("roster: no believed acceptor set (adopt a core or found first)")
	}
	v, err := r.read(ctx, [][]uint64{core})
	if err != nil {
		return Value{}, err
	}
	r.learn(v)
	if v.Joint != nil {
		v, err = r.read(ctx, [][]uint64{v.Joint.Old, v.Joint.New})
		if err != nil {
			return Value{}, err
		}
		r.learn(v)
	}
	return v, nil
}

// read runs a linearizable read against the given acceptor groups.
func (r *Roster) read(ctx context.Context, groups [][]uint64) (Value, error) {
	raw, err := r.mk(groups).Propose(ctx, r.key, caspaxos.Identity)
	if err != nil {
		return Value{}, err
	}
	return decode(raw)
}

// write applies mutate under the register's current configuration: it reads to
// learn the live Core/Joint, builds the matching proposer (single-group when
// quiescent, joint while reconfiguring), and commits with a guard that aborts if
// the configuration shifted under it (then refreshes and retries). The guard is
// load-bearing: CASPaxos surfaces the current value during prepare, so a write
// that began single-group sees a freshly-published Joint and bails rather than
// committing under a now-insufficient quorum.
func (r *Roster) write(ctx context.Context, mutate func(cur Value) (Value, error)) (Value, error) {
	const attempts = 16
	var lastErr error
	for i := 0; i < attempts; i++ {
		cur, err := r.Get(ctx)
		if err != nil {
			return Value{}, err
		}
		var groups [][]uint64
		if cur.Joint != nil {
			groups = [][]uint64{cur.Joint.Old, cur.Joint.New}
		} else {
			groups = [][]uint64{cur.Core}
		}
		expectGen := cur.ConfigGen
		v, err := commitWith(ctx, r.mk(groups), r.key, func(raw Value, present bool) (Value, error) {
			if !present {
				return Value{}, fmt.Errorf("roster: not initialised")
			}
			if raw.ConfigGen != expectGen || jointDiffers(raw.Joint, cur.Joint) {
				return Value{}, errConfigShifted
			}
			return mutate(raw)
		})
		if err == nil {
			r.learn(v)
			return v, nil
		}
		if retryable(err) {
			lastErr = err
			continue
		}
		return Value{}, err
	}
	if lastErr == nil {
		lastErr = caspaxos.ErrPreempted
	}
	return Value{}, lastErr
}

// Get-derived helpers --------------------------------------------------------

// NodeIDs returns the member node ids in sorted order — the input to HRW
// placement.
func (r *Roster) NodeIDs(ctx context.Context) ([]uint64, error) {
	v, err := r.Get(ctx)
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, len(v.Members))
	for i, m := range v.Members {
		ids[i] = m.NodeID
	}
	return ids, nil
}

// commitWith applies mutate as a CASPaxos change on the register through prop.
func commitWith(ctx context.Context, prop Proposer, key []byte, mutate func(cur Value, present bool) (Value, error)) (Value, error) {
	raw, err := prop.Propose(ctx, key, func(current []byte) ([]byte, error) {
		cur, err := decode(current)
		if err != nil {
			return nil, err
		}
		next, err := mutate(cur, len(current) > 0)
		if err != nil {
			return nil, err
		}
		return encode(next)
	})
	if err != nil {
		return Value{}, err
	}
	return decode(raw)
}

func memberIDs(members []Member) []uint64 {
	ids := make([]uint64, len(members))
	for i, m := range members {
		ids[i] = m.NodeID
	}
	return normalizeIDs(ids)
}

func normalize(ms []Member) []Member {
	// Dedup by node id (last wins) and sort, so the encoded value is canonical.
	seen := map[uint64]Member{}
	for _, m := range ms {
		seen[m.NodeID] = m
	}
	out := make([]Member, 0, len(seen))
	for _, m := range seen {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

// normalizeIDs dedups and sorts a node-id set so the encoded value is canonical.
func normalizeIDs(ids []uint64) []uint64 {
	seen := map[uint64]bool{}
	for _, id := range ids {
		seen[id] = true
	}
	out := make([]uint64, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func dropID(ids []uint64, id uint64) []uint64 {
	out := ids[:0:0]
	for _, x := range ids {
		if x != id {
			out = append(out, x)
		}
	}
	return out
}

func jointDiffers(a, b *Joint) bool {
	if (a == nil) != (b == nil) {
		return true
	}
	if a == nil {
		return false
	}
	return !idsEqual(a.Old, b.Old) || !idsEqual(a.New, b.New)
}

func idsEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func decode(raw []byte) (Value, error) {
	var v Value
	if len(raw) == 0 {
		return v, nil
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return Value{}, fmt.Errorf("roster: decode: %w", err)
	}
	return v, nil
}

func encode(v Value) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		return nil, fmt.Errorf("roster: encode: %w", err)
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}
