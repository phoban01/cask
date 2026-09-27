package caspaxos_test

import (
	"context"
	"errors"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
)

// landsLater is a fake proposer. Its first round stores the new value and
// reports ErrUnknownOutcome, as when a minority accept is chosen later.
// Later rounds run normally on the stored value.
type landsLater struct {
	value []byte
	calls int
}

func (l *landsLater) Propose(_ context.Context, _ []byte, change caspaxos.ChangeFunc) ([]byte, error) {
	l.calls++
	next, err := change(l.value)
	if err != nil {
		return nil, err
	}
	l.value = next
	if l.calls == 1 {
		return nil, caspaxos.ErrUnknownOutcome
	}
	return next, nil
}

// After an unknown outcome, a retry that finds its own write must not apply
// the change a second time.
func TestProposeResolvingDetectsLandedWrite(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A retried write MUST be a compare-and-set, never a blind reapplication of a change.
	prop := &landsLater{}
	applied := 0
	got, err := caspaxos.ProposeResolving(context.Background(), prop, []byte("k"), func(cur []byte) ([]byte, error) {
		applied++
		return append(append([]byte{}, cur...), 'x'), nil
	})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if applied != 1 || string(got) != "x" || string(prop.value) != "x" {
		t.Fatalf("applied %d times, got %q, register %q; want once, x, x", applied, got, prop.value)
	}
}

// lostThenOverwritten is a fake proposer. Its first round loses its write
// and reports ErrUnknownOutcome, and another writer then stores "other".
// Later rounds run normally.
type lostThenOverwritten struct {
	value []byte
	calls int
}

func (l *lostThenOverwritten) Propose(_ context.Context, _ []byte, change caspaxos.ChangeFunc) ([]byte, error) {
	l.calls++
	next, err := change(l.value)
	if err != nil {
		return nil, err
	}
	if l.calls == 1 {
		l.value = []byte("other")
		return nil, caspaxos.ErrUnknownOutcome
	}
	l.value = next
	return next, nil
}

// When the earlier write did not land, the retry runs the change on the
// fresh value, so its compare-and-set sees the other writer and fails.
func TestProposeResolvingRetriesUnlandedWrite(t *testing.T) {
	prop := &lostThenOverwritten{}
	applied := 0
	_, err := caspaxos.ProposeResolving(context.Background(), prop, []byte("k"), func(cur []byte) ([]byte, error) {
		applied++
		if len(cur) != 0 { // create-if-absent
			return nil, caspaxos.ErrConflict
		}
		return []byte("mine"), nil
	})
	if !errors.Is(err, caspaxos.ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
	if applied != 2 || string(prop.value) != "other" {
		t.Fatalf("applied %d times, register %q; want 2, other", applied, prop.value)
	}
}

