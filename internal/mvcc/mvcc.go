// Package mvcc layers multi-version concurrency control over a CASPaxos
// register. The register's value holds a version chain: an append-only list of
// versions, each tagged with a per-key sequence number and an HLC timestamp.
// Per-key history gives time-travel and compare-and-set; HLC timestamps let a
// caller read a consistent snapshot across keys without any global log.
//
// For the M1 milestone the chain is stored inline in the register value as
// JSON. Later milestones move history rows into the LSM and keep only the head
// in the register; the public API here is designed to survive that change.
package mvcc

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/phoban01/cask/internal/buggify"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
)

func init() {
	buggify.Register("mvcc_skip_owner_cache",
		"mvcc.KV.read bypasses the owner cache and takes the full Paxos round, keeping the fallback exercised", 0.05)
}

// OpID uniquely identifies one logical mutation. It is generated once per
// Put/Delete/CAS call and reused across every CASPaxos retry of that call, so
// the operation is applied exactly once even though the proposer may re-run its
// change function many times (and another proposer may complete it via
// carry-forward). Without this, a retried mutation could append its value to
// the chain more than once, which is observably non-linearizable.
//
// The dedup also means that an OpID must never repeat. A repeated OpID makes
// a new mutation a silent no-op: commit then returns the old version with that
// OpID as if the new mutation had written it. Inc makes OpIDs unique across
// KVs that share a Node, and across restarts of one process (issue #170).
type OpID struct {
	Node uint64 `json:"node"`
	// Inc is the random incarnation of the KV that minted the OpID. It is
	// 0 in versions written before it existed, and never 0 after.
	Inc uint64 `json:"inc,omitempty"`
	Seq uint64 `json:"seq"`
}

// Version is one entry in a key's history.
type Version struct {
	Seq       uint64        `json:"seq"`       // per-key, strictly increasing from 1
	HLC       hlc.Timestamp `json:"hlc"`       // commit timestamp, strictly increasing per key
	Value     []byte        `json:"value"`     // nil for a tombstone
	Tombstone bool          `json:"tombstone"` // true if this version deletes the key
	Op        OpID          `json:"op"`        // the operation that produced this version
}

// Live reports whether the version represents a present value (not a delete).
func (v Version) Live() bool { return !v.Tombstone }

type history struct {
	Versions []Version `json:"versions"`
	// CompactedBelow is the lowest sequence number still retained: versions with
	// Seq < CompactedBelow have been garbage-collected. A watcher that tries to
	// resume from before this point gets a "compacted" signal and must re-list.
	CompactedBelow uint64 `json:"compacted_below,omitempty"`
}

func decode(raw []byte) (history, error) {
	var h history
	if len(raw) == 0 {
		return h, nil
	}
	if err := json.Unmarshal(raw, &h); err != nil {
		return history{}, fmt.Errorf("mvcc: decode history: %w", err)
	}
	return h, nil
}

func encode(h history) ([]byte, error) {
	raw, err := json.Marshal(h)
	if err != nil {
		return nil, fmt.Errorf("mvcc: encode history: %w", err)
	}
	return raw, nil
}

func (h history) head() (Version, bool) {
	if len(h.Versions) == 0 {
		return Version{}, false
	}
	return h.Versions[len(h.Versions)-1], true
}

// contains reports whether op already produced a version in the chain. Scanning
// from the tail is fastest for the common case (a recent retry). The chain is
// bounded by history GC in a later milestone; for now it stays small.
func (h history) contains(op OpID) bool {
	for i := len(h.Versions) - 1; i >= 0; i-- {
		if h.Versions[i].Op == op {
			return true
		}
	}
	return false
}

// Proposer is the consensus operation mvcc depends on: agree on a key's next
// value via a change function. *caspaxos.Proposer satisfies it directly for a
// single replica group; an agent Router satisfies it by routing each key to the
// proposer for its range, which is how one KV spans the whole keyspace.
type Proposer interface {
	Propose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error)
}

// LocalReader is an optional zero-RTT read source (the ownership manager's
// lease-guarded cache, W4). served=false falls back to the full linearizable
// round; a served read is trusted — its linearizability argument (lease guard
// + cache invalidation on full-path writes) lives with the implementation.
type LocalReader interface {
	ReadLocal(ctx context.Context, key []byte) (raw []byte, served bool, err error)
}

// KV is an MVCC key-value view backed by a CASPaxos proposer and an HLC clock.
type KV struct {
	prop   Proposer
	clock  *hlc.Clock
	local  LocalReader // optional; nil = every read is a full round
	nodeID uint64      // identity for minting OpIDs
	inc    uint64      // random incarnation for minting OpIDs; never 0
	opSeq  uint64      // atomic counter for OpIDs
}

// KVOption configures a KV.
type KVOption func(*KV)

// WithLocalReader lets reads try lr before the full round. Every read path
// (Get, GetAt, History, SnapshotAt) inherits it through read().
func WithLocalReader(lr LocalReader) KVOption { return func(kv *KV) { kv.local = lr } }

// WithIncarnation sets the incarnation of the KV's OpIDs in place of a random
// one. It is for tests that need two KVs to share an OpID space, as two
// processes did before issue #170. inc must not be 0.
func WithIncarnation(inc uint64) KVOption { return func(kv *KV) { kv.inc = inc } }

// New returns a KV that proposes through prop and stamps versions with clock.
//
// nodeID names the writer in the OpIDs this KV mints. It need not be unique:
// each KV draws a random 64-bit incarnation, and an OpID is (nodeID,
// incarnation, counter). Callers once had to make nodeID unique per process,
// and cask-apiserver used its PID, which is 1 in every pod. Two apiservers
// then minted the same OpIDs, and mvcc dropped one's writes as duplicates of
// the other's (issue #170).
func New(prop Proposer, clock *hlc.Clock, nodeID uint64, opts ...KVOption) *KV {
	//= docs/spec/fleet.md#3-storage-model
	//# Every write MUST carry an operation identity that no other writer and no earlier process of the same writer has used.
	kv := &KV{prop: prop, clock: clock, nodeID: nodeID, inc: newIncarnation()}
	for _, o := range opts {
		o(kv)
	}
	if kv.inc == 0 {
		panic("mvcc: incarnation 0 is reserved for OpIDs minted before incarnations existed")
	}
	return kv
}

// newIncarnation returns a random, nonzero incarnation. Two KVs collide with
// probability 2^-64.
func newIncarnation() uint64 {
	var b [8]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			panic(fmt.Sprintf("mvcc: read random incarnation: %v", err))
		}
		if inc := binary.LittleEndian.Uint64(b[:]); inc != 0 {
			return inc
		}
	}
}

func (kv *KV) nextOp() OpID {
	return OpID{Node: kv.nodeID, Inc: kv.inc, Seq: atomic.AddUint64(&kv.opSeq, 1)}
}

// appendOp builds a ChangeFunc that appends exactly one version, derived from
// the current head via mk and tagged with op. If op already appears in the
// chain (a retry of this same operation, or a carry-forward completion by
// another proposer), the change is a no-op — this is what makes the operation
// exactly-once and the register linearizable under retries.
func (kv *KV) appendOp(op OpID, mk func(head Version, present bool) (value []byte, tombstone bool, err error)) caspaxos.ChangeFunc {
	return func(current []byte) ([]byte, error) {
		h, err := decode(current)
		if err != nil {
			return nil, err
		}
		if h.contains(op) {
			return current, nil // already applied exactly once
		}
		head, present := h.head()
		value, tombstone, err := mk(head, present)
		if err != nil {
			return nil, err
		}
		// Stamp strictly after the previous version to keep per-key HLC
		// monotonic regardless of proposer retries or clock skew.
		ts := kv.clock.Update(head.HLC)
		h.Versions = append(h.Versions, Version{
			Seq:       head.Seq + 1,
			HLC:       ts,
			Value:     value,
			Tombstone: tombstone,
			Op:        op,
		})
		return encode(h)
	}
}

// Put writes value as a new version and returns it.
func (kv *KV) Put(ctx context.Context, key, value []byte) (Version, error) {
	op := kv.nextOp()
	return kv.commit(ctx, key, op, kv.appendOp(op, func(Version, bool) ([]byte, bool, error) {
		return value, false, nil
	}))
}

// Delete writes a tombstone version and returns it.
func (kv *KV) Delete(ctx context.Context, key []byte) (Version, error) {
	op := kv.nextOp()
	return kv.commit(ctx, key, op, kv.appendOp(op, func(Version, bool) ([]byte, bool, error) {
		return nil, true, nil
	}))
}

// CAS sets the key to value only if its current live value equals expected.
// A missing or tombstoned key is treated as the empty value (expected == nil
// matches an absent key). It returns caspaxos.ErrConflict if the precondition
// fails.
func (kv *KV) CAS(ctx context.Context, key, expected, value []byte) (Version, error) {
	op := kv.nextOp()
	return kv.commit(ctx, key, op, kv.appendOp(op, func(head Version, present bool) ([]byte, bool, error) {
		var cur []byte
		if present && head.Live() {
			cur = head.Value
		}
		if !bytes.Equal(cur, expected) {
			return nil, false, caspaxos.ErrConflict
		}
		return value, false, nil
	}))
}

// CASSeq sets the key to value only if its head version has sequence seq
// and is live. seq 0 matches an absent or tombstoned key. It returns
// caspaxos.ErrConflict if the precondition fails.
//
// CAS compares values, so a value that changes and changes back still
// matches. CASSeq compares sequences, which never repeat, so it detects
// every write since the caller read seq.
func (kv *KV) CASSeq(ctx context.Context, key []byte, seq uint64, value []byte) (Version, error) {
	op := kv.nextOp()
	return kv.commit(ctx, key, op, kv.appendOp(op, func(head Version, present bool) ([]byte, bool, error) {
		if !headIs(head, present, seq) {
			return nil, false, caspaxos.ErrConflict
		}
		return value, false, nil
	}))
}

// CreateAt sets the key to value only if the key is not live and its head
// has sequence seq: 0 for an absent key, or the sequence of the tombstone
// at the head. It returns caspaxos.ErrConflict if the precondition fails.
//
// CASSeq with 0 matches any tombstone. CreateAt matches one tombstone, so
// a caller that checked state against that tombstone knows no delete and
// create landed in between.
func (kv *KV) CreateAt(ctx context.Context, key []byte, seq uint64, value []byte) (Version, error) {
	op := kv.nextOp()
	return kv.commit(ctx, key, op, kv.appendOp(op, func(head Version, present bool) ([]byte, bool, error) {
		switch {
		case !present && seq == 0:
		case present && head.Tombstone && head.Seq == seq:
		default:
			return nil, false, caspaxos.ErrConflict
		}
		return value, false, nil
	}))
}

// DeleteSeq writes a tombstone only if the key's head version has
// sequence seq and is live. It returns caspaxos.ErrConflict if the
// precondition fails. seq must be nonzero: an absent key has nothing to
// delete.
func (kv *KV) DeleteSeq(ctx context.Context, key []byte, seq uint64) (Version, error) {
	if seq == 0 {
		return Version{}, errors.New("mvcc: DeleteSeq needs a live sequence, got 0")
	}
	op := kv.nextOp()
	return kv.commit(ctx, key, op, kv.appendOp(op, func(head Version, present bool) ([]byte, bool, error) {
		if !headIs(head, present, seq) {
			return nil, false, caspaxos.ErrConflict
		}
		return nil, true, nil
	}))
}

// headIs reports whether the head is the live version seq. seq 0 means
// the key is absent or tombstoned.
func headIs(head Version, present bool, seq uint64) bool {
	live := present && head.Live()
	if seq == 0 {
		return !live
	}
	return live && head.Seq == seq
}

// commit proposes change and returns the version produced by op. The version is
// located by OpID rather than by position, since other operations may commit
// before or after it in the chain.
func (kv *KV) commit(ctx context.Context, key []byte, op OpID, change caspaxos.ChangeFunc) (Version, error) {
	raw, err := kv.propose(ctx, key, change)
	if err != nil {
		return Version{}, err
	}
	h, err := decode(raw)
	if err != nil {
		return Version{}, err
	}
	for i := len(h.Versions) - 1; i >= 0; i-- {
		if h.Versions[i].Op == op {
			return h.Versions[i], nil
		}
	}
	head, _ := h.head()
	return head, nil
}

// unknownOutcomeRetries bounds how many times propose retries a change after
// caspaxos.ErrUnknownOutcome.
const unknownOutcomeRetries = 8

// propose runs change and retries it when the outcome is unknown. The retry
// is safe only because every change mvcc proposes detects its own earlier
// write: appendOp skips an OpID already in the chain, and Compact never
// moves the watermark back. The retry re-reads the register in its prepare
// phase and applies change to that value.
func (kv *KV) propose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error) {
	//= docs/spec/fleet.md#3-storage-model
	//# A retried write MUST be a compare-and-set, never a blind reapplication of a change.
	var err error
	for range unknownOutcomeRetries {
		var raw []byte
		raw, err = kv.prop.Propose(ctx, key, change)
		if !errors.Is(err, caspaxos.ErrUnknownOutcome) {
			return raw, err
		}
	}
	return nil, err
}

// Get performs a linearizable read of the current live value. found is false
// when the key is absent or its latest version is a tombstone.
func (kv *KV) Get(ctx context.Context, key []byte) (value []byte, found bool, err error) {
	h, err := kv.read(ctx, key)
	if err != nil {
		return nil, false, err
	}
	head, present := h.head()
	if !present || !head.Live() {
		return nil, false, nil
	}
	return head.Value, true, nil
}

// GetAt returns the version with the given sequence number, if it exists.
func (kv *KV) GetAt(ctx context.Context, key []byte, seq uint64) (Version, bool, error) {
	h, err := kv.read(ctx, key)
	if err != nil {
		return Version{}, false, err
	}
	for _, v := range h.Versions {
		if v.Seq == seq {
			return v, true, nil
		}
	}
	return Version{}, false, nil
}

// SnapshotAt returns the newest version whose commit timestamp is at or before
// t — the value the key held as of snapshot time t.
func (kv *KV) SnapshotAt(ctx context.Context, key []byte, t hlc.Timestamp) (Version, bool, error) {
	h, err := kv.read(ctx, key)
	if err != nil {
		return Version{}, false, err
	}
	var (
		out   Version
		found bool
	)
	for _, v := range h.Versions {
		if v.HLC.LessEqual(t) {
			out, found = v, true
			continue
		}
		break // versions are ordered by HLC ascending
	}
	return out, found, nil
}

// SnapshotRead returns each key's value as of HLC timestamp t — a consistent
// cross-key snapshot. Keys may live in different ranges; each is read
// independently and filtered to t. A key absent (or tombstoned) at t is omitted
// from the result.
func (kv *KV) SnapshotRead(ctx context.Context, keys [][]byte, t hlc.Timestamp) (map[string]Version, error) {
	out := make(map[string]Version, len(keys))
	for _, key := range keys {
		v, found, err := kv.SnapshotAt(ctx, key, t)
		if err != nil {
			return nil, err
		}
		if found && v.Live() {
			out[string(key)] = v
		}
	}
	return out, nil
}

// Chain is a key's retained version history plus the compaction watermark.
type Chain struct {
	Versions       []Version
	CompactedBelow uint64 // versions below this seq have been GC'd
}

// History returns a key's retained version chain (linearizable read). This is
// the change feed Watch replays from.
func (kv *KV) History(ctx context.Context, key []byte) (Chain, error) {
	h, err := kv.read(ctx, key)
	if err != nil {
		return Chain{}, err
	}
	return Chain{Versions: h.Versions, CompactedBelow: h.CompactedBelow}, nil
}

// Compact drops versions with Seq < keepFromSeq, advancing the compaction
// watermark. It never removes the head version (the current value is always
// retained), and never moves the watermark backwards. Callers (the GC) must keep
// keepFromSeq at or below the oldest active watcher's cursor so no in-progress
// watch is starved. Compaction is idempotent.
func (kv *KV) Compact(ctx context.Context, key []byte, keepFromSeq uint64) error {
	_, err := kv.propose(ctx, key, func(current []byte) ([]byte, error) {
		h, err := decode(current)
		if err != nil {
			return nil, err
		}
		head, present := h.head()
		if !present {
			return current, nil
		}
		if keepFromSeq > head.Seq {
			keepFromSeq = head.Seq // always retain the head
		}
		if keepFromSeq <= h.CompactedBelow {
			return current, nil // nothing new to compact
		}
		kept := h.Versions[:0:0]
		for _, v := range h.Versions {
			if v.Seq >= keepFromSeq {
				kept = append(kept, v)
			}
		}
		h.Versions = kept
		h.CompactedBelow = keepFromSeq
		return encode(h)
	})
	return err
}

// read returns the key's history: from the zero-RTT owner cache when one is
// installed and vouches for itself, else via a linearizable identity round.
func (kv *KV) read(ctx context.Context, key []byte) (history, error) {
	// BUGGIFY: skip the owner cache and take the full round, keeping the
	// fallback path continuously exercised alongside the fast one.
	if kv.local != nil && !buggify.Maybe("mvcc_skip_owner_cache", 0.05) {
		if raw, served, err := kv.local.ReadLocal(ctx, key); err == nil && served {
			return decode(raw)
		}
	}
	raw, err := kv.prop.Propose(ctx, key, caspaxos.Identity)
	if err != nil {
		return history{}, err
	}
	return decode(raw)
}
