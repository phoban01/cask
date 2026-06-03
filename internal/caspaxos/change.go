package caspaxos

// ChangeFunc transforms the current value of a register into its next value. It
// is the single user-defined operation CASPaxos agrees on: a blind write
// ignores current; a compare-and-set inspects current and returns ErrConflict
// when its precondition fails; a linearizable read is the identity function.
//
// current is nil for a register that has never held a value. A ChangeFunc must
// be a pure function of current — it is invoked exactly once per successful
// round, between the prepare and accept phases, and may be retried on a fresh
// current if the round is preempted.
type ChangeFunc func(current []byte) (next []byte, err error)

// Identity returns the current value unchanged. Proposing it runs a full
// two-phase round, which is how a linearizable read is performed: the value is
// confirmed by a quorum and any in-flight write is completed.
func Identity(current []byte) ([]byte, error) { return current, nil }

// Write returns a ChangeFunc that unconditionally sets the register to value.
func Write(value []byte) ChangeFunc {
	return func([]byte) ([]byte, error) { return value, nil }
}
