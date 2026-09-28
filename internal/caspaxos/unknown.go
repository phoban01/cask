package caspaxos

import (
	"bytes"
	"context"
	"errors"
	"fmt"
)

// Proposing is anything that runs a CASPaxos round for a key: a *Proposer,
// or a layer that routes to one.
type Proposing interface {
	Propose(ctx context.Context, key []byte, change ChangeFunc) ([]byte, error)
}

// unknownRetries bounds how many times ProposeResolving retries after
// ErrUnknownOutcome.
const unknownRetries = 8

// ProposeResolving runs change through prop and resolves ErrUnknownOutcome.
// After an unknown outcome it proposes again. The next round reads the
// register in its prepare phase. If the register holds the exact value the
// earlier attempt wrote, that write landed: the round writes the value back
// unchanged and returns it. Otherwise change runs on the fresh value, so a
// compare-and-set in change sees any write that came in between.
//
// Use it only when change is a compare-and-set, or when an equal value
// means the same outcome. The test is on bytes: if another writer stored
// the same bytes, the call reports success, which is correct only when the
// two writes mean the same thing.
//
// After one unknown outcome, every later error except ErrConflict comes
// back as ErrUnknownOutcome, never as ErrPreempted. The earlier write may
// still land, and ErrPreempted reads as "nothing written". mvcc's propose
// follows the same rule.
func ProposeResolving(ctx context.Context, prop Proposing, key []byte, change ChangeFunc) ([]byte, error) {
	//= docs/spec/fleet.md#3-storage-model
	//# A retried write MUST be a compare-and-set, never a blind reapplication of a change.
	var (
		wrote   [][]byte // every value an attempt tried to write
		pending bool     // an earlier attempt's outcome is unknown
	)
	resolving := func(current []byte) ([]byte, error) {
		if pending {
			for _, w := range wrote {
				if bytes.Equal(current, w) {
					return current, nil // an earlier write landed; do not apply change again
				}
			}
		}
		next, err := change(current)
		if err == nil {
			wrote = append(wrote, next)
		}
		return next, err
	}
	for range unknownRetries {
		raw, err := prop.Propose(ctx, key, resolving)
		switch {
		case errors.Is(err, ErrUnknownOutcome):
			pending = true
		case err == nil, !pending, errors.Is(err, ErrConflict):
			return raw, err
		default:
			// An earlier attempt may still land. Keep err out of the
			// chain, so errors.Is does not match ErrPreempted.
			return nil, fmt.Errorf("%w: a later attempt failed: %v", ErrUnknownOutcome, err)
		}
	}
	return nil, ErrUnknownOutcome
}
