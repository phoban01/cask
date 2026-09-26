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
	"encoding/json"
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
type OpID struct {
	Node uint64 `json:"node"`
	Seq  uint64 `json:"seq"`
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
	opSeq  uint64      // atomic counter for OpIDs
}

// KVOption configures a KV.
type KVOption func(*KV)

// WithLocalReader lets reads try lr before the full round. Every read path
// (Get, GetAt, History, SnapshotAt) inherits it through read().
func WithLocalReader(lr LocalReader) KVOption { return func(kv *KV) { kv.local = lr } }

// New returns a KV that proposes through prop and stamps versions with clock.
//
// nodeID namespaces the OpIDs this KV mints and MUST be unique per proposer
// incarnation: two live proposers must differ, and a restarted proposer must
// not reuse a previous incarnation's id (or it could mint an OpID that already
// exists in a chain, and its mutation would be silently deduplicated). In
// production this is a persisted node UUID combined with a boot epoch — the same
// uniqueness the ballot counter needs.
func New(prop Proposer, clock *hlc.Clock, nodeID uint64, opts ...KVOption) *KV {
	kv := &KV{prop: prop, clock: clock, nodeID: nodeID}
	for _, o := range opts {
		o(kv)
	}
	return kv
}

func (kv *KV) nextOp() OpID {
	return OpID{Node: kv.nodeID, Seq: atomic.AddUint64(&kv.opSeq, 1)}
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

// commit proposes change and returns the version produced by op. The version is
// located by OpID rather than by position, since other operations may commit
// before or after it in the chain.
func (kv *KV) commit(ctx context.Context, key []byte, op OpID, change caspaxos.ChangeFunc) (Version, error) {
	raw, err := kv.prop.Propose(ctx, key, change)
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
	_, err := kv.prop.Propose(ctx, key, func(current []byte) ([]byte, error) {
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
