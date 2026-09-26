package faults

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/testutil/sim"
)

// SlowStore wraps a caspaxos.Storage and, when armed, adds a small delay before
// each Store — modelling a slow fsync. The delay is bounded and fixed so the
// gate's seed budget stays within its wall-clock target; the point is to widen
// the window during which other goroutines observe an in-progress write, not to
// model realistic disk latency.
//
// Build the gate's stores as SlowStores for the slow_fsync fault to bite; the
// fault type-asserts to *SlowStore and arms them.
type SlowStore struct {
	inner caspaxos.Storage
	delay time.Duration
	armed int32 // atomic bool
}

// NewSlowStore wraps inner. delay is the per-Store pause applied while armed.
func NewSlowStore(inner caspaxos.Storage, delay time.Duration) *SlowStore {
	return &SlowStore{inner: inner, delay: delay}
}

// Load delegates unchanged.
func (s *SlowStore) Load(ctx context.Context, key []byte) (caspaxos.Register, error) {
	return s.inner.Load(ctx, key)
}

// Store pauses (when armed) before delegating, modelling a slow fsync.
func (s *SlowStore) Store(ctx context.Context, key []byte, r caspaxos.Register) error {
	if atomic.LoadInt32(&s.armed) == 1 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.inner.Store(ctx, key, r)
}

func (s *SlowStore) arm(on bool) {
	if on {
		atomic.StoreInt32(&s.armed, 1)
	} else {
		atomic.StoreInt32(&s.armed, 0)
	}
}

// Disarm clears the slow flag. The gate's heal step calls HealStores to undo a
// slow_fsync injection between fault dwells.
func (s *SlowStore) Disarm() { s.arm(false) }

// HealStores disarms every SlowStore in the sim. Compose it with sim.Network's
// Heal in the gate's heal hook when the stores are SlowStores.
func HealStores(s *sim.Sim) {
	for _, st := range s.Stores {
		if ss, ok := st.(*SlowStore); ok {
			ss.Disarm()
		}
	}
}

// SlowFsync arms a seeded subset of SlowStores so their next writes are slow. If
// the gate's stores are not SlowStores it is a documented no-op.
type SlowFsync struct{}

func (SlowFsync) Name() string { return sim.FaultSlowFsync }

func (SlowFsync) Inject(s *sim.Sim) {
	armed := 0
	for i, st := range s.Stores {
		ss, ok := st.(*SlowStore)
		if !ok {
			continue
		}
		if s.RNG.Intn(2) == 0 {
			ss.arm(true)
			armed++
			s.Trace.Add("step %d: slow_fsync armed on acceptor %d", s.Step(), i)
		}
	}
	if armed == 0 {
		s.Trace.Add("step %d: slow_fsync no-op (stores are not SlowStores)", s.Step())
	}
}
