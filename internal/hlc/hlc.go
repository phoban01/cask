// Package hlc implements a Hybrid Logical Clock (Kulkarni et al.): timestamps
// that track physical time closely while preserving causal (happens-before)
// order without any central sequencer. cask stamps every committed version with
// an HLC timestamp so that a read "as of T" yields a consistent cross-key
// snapshot, the way CockroachDB and YugabyteDB do distributed MVCC.
package hlc

import (
	"fmt"
	"sync"
)

// Timestamp is a hybrid logical clock reading: a physical component (typically
// unix nanoseconds) plus a logical counter that disambiguates events sharing a
// physical tick. The pair forms a total order.
type Timestamp struct {
	Physical int64
	Logical  uint32
}

// Less reports whether t orders strictly before other.
func (t Timestamp) Less(other Timestamp) bool {
	if t.Physical != other.Physical {
		return t.Physical < other.Physical
	}
	return t.Logical < other.Logical
}

// LessEqual reports whether t orders at or before other.
func (t Timestamp) LessEqual(other Timestamp) bool { return t == other || t.Less(other) }

// IsZero reports whether t is the zero timestamp.
func (t Timestamp) IsZero() bool { return t == Timestamp{} }

func (t Timestamp) String() string { return fmt.Sprintf("%d.%d", t.Physical, t.Logical) }

// PhysicalFunc returns the current physical time in the same unit used for
// Timestamp.Physical (unix nanoseconds in production). It is injected so tests
// can drive a deterministic clock.
type PhysicalFunc func() int64

// Clock is a thread-safe hybrid logical clock. The zero value is not usable;
// construct one with New.
type Clock struct {
	mu   sync.Mutex
	last Timestamp
	phys PhysicalFunc
}

// New returns a Clock that derives physical time from phys.
func New(phys PhysicalFunc) *Clock { return &Clock{phys: phys} }

// Now returns a timestamp for a local event. It never moves backwards and is
// strictly greater than every timestamp previously returned by this clock.
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.phys()
	if now > c.last.Physical {
		c.last = Timestamp{Physical: now, Logical: 0}
	} else {
		c.last.Logical++
	}
	return c.last
}

// Update advances the clock past a timestamp received from a peer and returns a
// local timestamp that happens-after remote. This is what keeps causal order
// across nodes without coordination.
func (c *Clock) Update(remote Timestamp) Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.phys()
	max := c.last
	if remote.Physical > max.Physical || (remote.Physical == max.Physical && remote.Logical > max.Logical) {
		max = remote
	}
	switch {
	case now > max.Physical:
		c.last = Timestamp{Physical: now, Logical: 0}
	default:
		c.last = Timestamp{Physical: max.Physical, Logical: max.Logical + 1}
	}
	return c.last
}
