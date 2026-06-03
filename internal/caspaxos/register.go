package caspaxos

// Register is the per-key state held by an acceptor — the only durable
// consensus state in the system. It follows the standard Paxos formulation,
// keeping the highest promised ballot separate from the accepted ballot/value:
//
//   - Promise is the highest ballot this acceptor has answered a Prepare for;
//     it will reject any Prepare/Accept below it.
//   - Accepted is the ballot under which Value was accepted (phase 2).
//
// A never-touched register is the zero value: ZeroBallot promise, ZeroBallot
// accepted, nil value.
type Register struct {
	Promise  Ballot
	Accepted Ballot
	Value    []byte
}

// PrepareReply is an acceptor's response to phase 1.
type PrepareReply struct {
	// Promised is true when the acceptor adopted the proposer's ballot as its
	// new promise. When false the prepare was rejected and Conflict carries the
	// higher ballot that caused the rejection, so the proposer can jump ahead.
	Promised bool
	Conflict Ballot

	// Accepted/Value report what (if anything) the acceptor had already
	// accepted, so the proposer can carry the highest-accepted value forward.
	Accepted Ballot
	Value    []byte
}

// AcceptReply is an acceptor's response to phase 2.
type AcceptReply struct {
	// Accepted is true when the value was durably accepted. When false the
	// acceptor had promised a higher ballot, reported in Conflict.
	Accepted bool
	Conflict Ballot
}
