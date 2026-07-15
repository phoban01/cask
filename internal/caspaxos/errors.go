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

	// ErrRangeChanged means an acceptor rejected the request because the
	// proposer's claimed range-descriptor epoch is older than the acceptor's —
	// the range was reconfigured (or split) since the proposer last looked.
	// The proposer must refresh its routing and retry against the current
	// replica set (§4.1). Unlike a transport failure (a non-vote), this aborts
	// the round immediately: retrying against a stale replica set can never
	// succeed and, worse, can read stale state.
	ErrRangeChanged = errors.New("caspaxos: range descriptor changed; refresh routing")
)
