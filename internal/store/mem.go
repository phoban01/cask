// Package store provides durable homes for acceptor state. Mem is an in-memory
// implementation used by tests and the deterministic simulator; the production
// pebble-backed store arrives in a later milestone.
package store

import (
	"context"
	"sync"

	"github.com/phoban01/cask/internal/buggify"
	"github.com/phoban01/cask/internal/caspaxos"
)

func init() {
	// Declared for catalog completeness. The slow-fsync effect is modelled by the
	// testutil/sim/faults SlowStore decorator today; a firing site lands here with
	// the Pebble store (§3.0).
	buggify.Register("store_slow_fsync",
		"acceptor Storage.Store pauses before persisting, modelling a slow fsync (decorator-driven today; site lands with §3.0)", 0.05)
}

// Mem is a goroutine-safe in-memory caspaxos.Storage. It copies values on the
// way in and out so callers can never alias stored register bytes. There is no
// real fsync; durability is simulated, which is sufficient for logical tests
// (crash/restart durability is modeled explicitly by the simulator instead).
type Mem struct {
	mu   sync.Mutex
	regs map[string]caspaxos.Register
}

// NewMem returns an empty in-memory store.
func NewMem() *Mem { return &Mem{regs: make(map[string]caspaxos.Register)} }

// Load returns the register for key, or the zero register if absent.
func (m *Mem) Load(_ context.Context, key []byte) (caspaxos.Register, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	reg, ok := m.regs[string(key)]
	if !ok {
		return caspaxos.Register{}, nil
	}
	reg.Value = clone(reg.Value)
	return reg, nil
}

// Store durably writes the register for key.
func (m *Mem) Store(_ context.Context, key []byte, r caspaxos.Register) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r.Value = clone(r.Value)
	m.regs[string(key)] = r
	return nil
}

// Keys returns every stored key (the store.Lister seam for carry-forward key
// enumeration, §4.3).
func (m *Mem) Keys(_ context.Context) ([][]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([][]byte, 0, len(m.regs))
	for k := range m.regs {
		out = append(out, []byte(k))
	}
	return out, nil
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
