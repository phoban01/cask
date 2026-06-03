package caspaxos

import "errors"

var (
	// ErrConflict is returned by a ChangeFunc to abort a round when its
	// precondition does not hold (e.g. a failed compare-and-set). The register
	// is left unchanged.
	ErrConflict = errors.New("caspaxos: change precondition failed")

	// ErrNoQuorum means a phase could not gather a majority of acceptor
	// responses (too many were unreachable or rejected the ballot).
	ErrNoQuorum = errors.New("caspaxos: no quorum")

	// ErrPreempted means the round kept losing to a competing proposer with a
	// higher ballot and exhausted its retry budget.
	ErrPreempted = errors.New("caspaxos: preempted")
)
