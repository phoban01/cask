package caspaxos_test

import (
	"context"
	"errors"
	"slices"
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

// slowAccept passes prepares through. Its accepts wait until the phase
// cancels them, as for a replica that is alive but lagging.
type slowAccept struct {
	inner    caspaxos.AcceptorClient
	released chan struct{}
}

func (s slowAccept) Prepare(ctx context.Context, key []byte, b caspaxos.Ballot) (caspaxos.PrepareReply, error) {
	return s.inner.Prepare(ctx, key, b)
}

func (s slowAccept) Accept(ctx context.Context, _ []byte, _ caspaxos.Ballot, _ []byte) (caspaxos.AcceptReply, error) {
	<-ctx.Done()
	close(s.released)
	return caspaxos.AcceptReply{}, ctx.Err()
}

// With three acceptors, the first rejection ends the accept phase while
// the third acceptor is still in flight. That acceptor may still take the value, so
// Propose must return ErrUnknownOutcome and must not run the change again.
func TestTwoRejectOneSlowReturnsUnknownOutcome(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A write that returned a conflict MAY have been committed.
	base := newCluster(3)
	slow := slowAccept{inner: base[2], released: make(chan struct{})}
	p := caspaxos.NewProposer(1, []caspaxos.AcceptorClient{
		acceptNack{base[0]}, acceptNack{base[1]}, slow,
	})

	calls := 0
	_, err := p.Propose(context.Background(), []byte("k"), func([]byte) ([]byte, error) {
		calls++
		return []byte("v"), nil
	})
	if !errors.Is(err, caspaxos.ErrUnknownOutcome) {
		t.Fatalf("err = %v, want ErrUnknownOutcome", err)
	}
	if calls != 1 {
		t.Fatalf("change ran %d times, want 1", calls)
	}
	<-slow.released // the phase cancelled the straggler
}

// scripted is a fake proposer that returns one scripted error per round
// and runs no change.
type scripted struct{ errs []error }

func (s *scripted) Propose(context.Context, []byte, caspaxos.ChangeFunc) ([]byte, error) {
	err := s.errs[0]
	s.errs = s.errs[1:]
	return nil, err
}

// A retry after an unknown outcome that is then preempted must still
// report the unknown outcome. The first write may still land, and
// ErrPreempted reads as "nothing written" (issue #180).
func TestProposeResolvingKeepsUnknownAfterPreempted(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A write that returned a conflict MAY have been committed.
	prop := &scripted{errs: []error{caspaxos.ErrUnknownOutcome, caspaxos.ErrPreempted}}
	_, err := caspaxos.ProposeResolving(context.Background(), prop, []byte("k"), caspaxos.Identity)
	if !errors.Is(err, caspaxos.ErrUnknownOutcome) || errors.Is(err, caspaxos.ErrPreempted) {
		t.Fatalf("err = %v, want ErrUnknownOutcome and not ErrPreempted", err)
	}
}

// retries is a fake proposer that records the retry number on each call
// and returns one scripted error per call.
type retries struct {
	errs []error
	seen []int
}

func (r *retries) Propose(ctx context.Context, _ []byte, _ caspaxos.ChangeFunc) ([]byte, error) {
	r.seen = append(r.seen, caspaxos.RetryOf(ctx))
	err := r.errs[0]
	r.errs = r.errs[1:]
	return nil, err
}

// ProposeResolving marks each call after an unknown outcome with its retry
// number, so the proposer's backoff grows across calls.
func TestProposeResolvingMarksRetries(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A write that retries after an unknown outcome MUST wait longer before each retry, up to a fixed limit.
	prop := &retries{errs: []error{caspaxos.ErrUnknownOutcome, caspaxos.ErrUnknownOutcome, nil}}
	if _, err := caspaxos.ProposeResolving(context.Background(), prop, []byte("k"), caspaxos.Identity); err != nil {
		t.Fatal(err)
	}
	if want := []int{0, 1, 2}; !slices.Equal(prop.seen, want) {
		t.Fatalf("retry numbers = %v, want %v", prop.seen, want)
	}
}
