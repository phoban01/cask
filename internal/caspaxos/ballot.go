// Package caspaxos implements the pure, deterministic core of the CASPaxos
// protocol (arXiv:1802.07000): a wait-free, linearizable, multi-writer register
// over an unreliable asynchronous network.
//
// The package is intentionally free of I/O, networking, wall-clock time and
// background goroutines. Durability is provided through the injected [Storage]
// interface and peer communication through the injected [AcceptorClient]
// interface, so an entire cluster can be driven by a deterministic, seeded
// simulator in tests.
package caspaxos

import "fmt"

// Ballot is a Paxos round number. The pair (Counter, NodeID) is a strict total
// order over every ballot ever generated in the system with no coordination:
// Counter is advanced locally by each proposer, and NodeID breaks ties so two
// proposers can never mint equal ballots.
type Ballot struct {
	Counter uint64
	NodeID  uint64
}

// ZeroBallot is the bottom of the ballot order; it is the implicit promise and
// accepted ballot of a register that has never been touched.
var ZeroBallot = Ballot{}

// IsZero reports whether b is the zero ballot.
func (b Ballot) IsZero() bool { return b == ZeroBallot }

// Less reports whether b orders strictly before other.
func (b Ballot) Less(other Ballot) bool {
	if b.Counter != other.Counter {
		return b.Counter < other.Counter
	}
	return b.NodeID < other.NodeID
}

// AtLeast reports whether b orders at or after other (i.e. !b.Less(other)).
func (b Ballot) AtLeast(other Ballot) bool { return !b.Less(other) }

// Max returns the greater of b and other.
func (b Ballot) Max(other Ballot) Ballot {
	if b.Less(other) {
		return other
	}
	return b
}

func (b Ballot) String() string { return fmt.Sprintf("%d.%d", b.Counter, b.NodeID) }
