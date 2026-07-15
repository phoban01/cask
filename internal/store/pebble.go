package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"

	"github.com/phoban01/cask/internal/caspaxos"
)

// Pebble is the durable [caspaxos.Storage] (roadmap §3.0). Every acknowledged
// Store is fsynced before returning — an acceptor reply implies durability,
// and a promise or accept acknowledged but lost to a crash would violate
// consensus safety. Crash recovery is Pebble's WAL: open the DB and serve;
// nothing to replay at this layer.
//
// Group commit is SELF-CLOCKED (no timer window): a write arriving while no
// sync is in flight commits immediately — idle writes pay zero added latency
// — and writes arriving DURING an in-flight sync coalesce into the next
// batch, which commits the moment the current sync finishes. The sync itself
// is the clock: the slower the disk, the more each fsync amortizes. Distinct
// keys' rounds already run concurrently (the acceptor stripes its locks per
// key), so under load one fsync covers many accepts. Misconfiguration is not
// possible — there is nothing to configure, and callers always block until
// THEIR batch has synced.
//
// (The roadmap §3.0 sketched a fixed fsync window; that design charges every
// founder the full window even when idle, measured 150x slower on fast
// storage. Self-clocking dominates it at both ends of the disk-speed range.)
type Pebble struct {
	db    *pebble.DB
	group bool

	mu       sync.Mutex
	inflight bool
	pending  *commitGroup
	flushes  uint64 // committed groups, for tests/telemetry (guarded by mu)
}

// commitGroup is one forming batch: joiners add their write and wait for the
// group's single synced commit.
type commitGroup struct {
	batch *pebble.Batch
	done  chan struct{}
	err   error
}

// PebbleOption configures a Pebble store.
type PebbleOption func(*Pebble)

// WithGroupCommit enables self-clocked fsync coalescing (see the type doc).
// Off, every Store is its own synced write — simplest, and what single-writer
// tests use for exactness.
func WithGroupCommit() PebbleOption {
	return func(p *Pebble) { p.group = true }
}

// NewPebble opens (or creates) the durable store rooted at dir.
func NewPebble(dir string, opts ...PebbleOption) (*Pebble, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("store: open pebble at %s: %w", dir, err)
	}
	p := &Pebble{db: db}
	for _, o := range opts {
		o(p)
	}
	return p, nil
}

// Close flushes and closes the database. Safe only once outstanding Stores
// have returned (their durability was already acknowledged by then).
func (p *Pebble) Close() error { return p.db.Close() }

// registerKey namespaces acceptor state in the keyspace, leaving room for
// future column families (mvcc history rows move here in a later milestone).
func registerKey(key []byte) []byte {
	return append([]byte("r/"), key...)
}

// encodeRegister packs a Register as four big-endian uint64s (promise and
// accepted ballots) followed by the raw value bytes.
func encodeRegister(r caspaxos.Register) []byte {
	out := make([]byte, 32+len(r.Value))
	binary.BigEndian.PutUint64(out[0:], r.Promise.Counter)
	binary.BigEndian.PutUint64(out[8:], r.Promise.NodeID)
	binary.BigEndian.PutUint64(out[16:], r.Accepted.Counter)
	binary.BigEndian.PutUint64(out[24:], r.Accepted.NodeID)
	copy(out[32:], r.Value)
	return out
}

func decodeRegister(raw []byte) (caspaxos.Register, error) {
	if len(raw) < 32 {
		return caspaxos.Register{}, fmt.Errorf("store: register record too short: %d bytes", len(raw))
	}
	r := caspaxos.Register{
		Promise:  caspaxos.Ballot{Counter: binary.BigEndian.Uint64(raw[0:]), NodeID: binary.BigEndian.Uint64(raw[8:])},
		Accepted: caspaxos.Ballot{Counter: binary.BigEndian.Uint64(raw[16:]), NodeID: binary.BigEndian.Uint64(raw[24:])},
	}
	if len(raw) > 32 {
		r.Value = append([]byte(nil), raw[32:]...)
	}
	return r, nil
}

// Load returns the register for key, or the zero Register if never stored.
func (p *Pebble) Load(_ context.Context, key []byte) (caspaxos.Register, error) {
	raw, closer, err := p.db.Get(registerKey(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return caspaxos.Register{}, nil
	}
	if err != nil {
		return caspaxos.Register{}, fmt.Errorf("store: load: %w", err)
	}
	defer closer.Close()
	return decodeRegister(raw)
}

// Store durably persists the register for key (fsynced before returning).
func (p *Pebble) Store(_ context.Context, key []byte, r caspaxos.Register) error {
	enc := encodeRegister(r)
	if !p.group {
		return p.db.Set(registerKey(key), enc, pebble.Sync)
	}

	p.mu.Lock()
	g := p.pending
	if g == nil {
		g = &commitGroup{batch: p.db.NewBatch(), done: make(chan struct{})}
		p.pending = g
	}
	err := g.batch.Set(registerKey(key), enc, nil)
	if err != nil {
		p.mu.Unlock()
		return fmt.Errorf("store: batch set: %w", err)
	}
	if !p.inflight {
		// No sync running: this writer's group commits immediately; a flusher
		// drains any groups that pile up while syncs are in flight.
		p.inflight = true
		go p.flusher()
	}
	p.mu.Unlock()

	<-g.done // returns only after the group's synced commit
	return g.err
}

// flusher commits pending groups back to back, each with one synced write,
// until none are waiting. The sync in flight is what accumulates the next
// group — self-clocking.
func (p *Pebble) flusher() {
	for {
		p.mu.Lock()
		g := p.pending
		p.pending = nil
		if g == nil {
			p.inflight = false
			p.mu.Unlock()
			return
		}
		p.flushes++
		p.mu.Unlock()

		g.err = g.batch.Commit(pebble.Sync)
		close(g.done)
	}
}

// Flushes reports how many group commits have run (telemetry/tests).
func (p *Pebble) Flushes() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.flushes
}

// Compile-time check.
var _ caspaxos.Storage = (*Pebble)(nil)
