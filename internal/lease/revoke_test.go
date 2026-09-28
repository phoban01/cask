package lease_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/lease"
	"github.com/phoban01/cask/testutil/sim"
)

// acceptRecorder wraps one acceptor link and sends every value that the
// acceptor accepts on accepted.
type acceptRecorder struct {
	caspaxos.AcceptorClient
	accepted chan []byte
}

func (r acceptRecorder) Accept(ctx context.Context, key []byte, b caspaxos.Ballot, val []byte) (caspaxos.AcceptReply, error) {
	reply, err := r.AcceptorClient.Accept(ctx, key, b, val)
	if err == nil && reply.Accepted {
		r.accepted <- val
	}
	return reply, err
}

// betweenAttempts wraps a proposer. In the first round that writes a new
// value, it downs the accept path to acceptors 1 and 2 after the change
// runs. So only acceptor 0 takes the write, and the round returns
// ErrUnknownOutcome. It then waits until acceptor 0 holds the write, and
// runs between. That models a round that commits the minority write and
// writes on top of it.
//
// The wait is needed. The round returns as soon as acceptors 1 and 2 fail,
// and the accept to acceptor 0 can still be in flight. Without the wait,
// between can read acceptor 0 before the write lands.
//
// Arming on the first write, not on a call number, makes the test work
// for any Revoke: one that reads first and one that writes at once.
type betweenAttempts struct {
	t        *testing.T
	prop     lease.Proposer
	nw       *sim.Network
	accepted chan []byte // values acceptor 0 accepted from prop
	fired    bool
	between  func()
}

func (b *betweenAttempts) Propose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error) {
	if b.fired {
		return b.prop.Propose(ctx, key, change)
	}
	var (
		armed bool
		wrote []byte
	)
	arm := func(cur []byte) ([]byte, error) {
		next, err := change(cur)
		if err == nil && !armed && !bytes.Equal(next, cur) {
			// The prepare phase is done. Down the accepts to 1 and 2.
			armed, wrote = true, next
			b.nw.SetAcceptDown(1, true)
			b.nw.SetAcceptDown(2, true)
		}
		return next, err
	}
	raw, err := b.prop.Propose(ctx, key, arm)
	if !armed {
		return raw, err
	}
	b.fired = true
	b.nw.SetAcceptDown(1, false)
	b.nw.SetAcceptDown(2, false)
	if !errors.Is(err, caspaxos.ErrUnknownOutcome) {
		b.t.Errorf("first write = %v, want ErrUnknownOutcome", err)
		return raw, err
	}
	timeout := time.After(10 * time.Second)
	for {
		select {
		case v := <-b.accepted:
			if bytes.Equal(v, wrote) {
				b.between()
				return raw, err
			}
		case <-timeout:
			b.t.Errorf("acceptor 0 did not accept the write %q", wrote)
			return raw, err
		}
	}
}

// A Grant lands between two Revoke attempts. The retry must see the new
// session and leave it alone.
func TestRevokeRetryKeepsNewSession(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A retried write MUST be a compare-and-set, never a blind reapplication of a change.
	ctx := context.Background()
	nw, now := fixture()
	atomic.StoreInt64(now, 1000)
	clock := func() int64 { return atomic.LoadInt64(now) }

	ssA := lease.NewSessions(caspaxos.NewProposer(1, nw.Clients()), clock)
	if _, err := ssA.Grant(ctx, "s", "agentA", 100); err != nil {
		t.Fatal(err)
	}

	// The revoker reaches acceptor 0 through a recorder. The buffer holds
	// every accept in the test, so a send never blocks.
	accepted := make(chan []byte, 64)
	clients := nw.Clients()
	clients[0] = acceptRecorder{AcceptorClient: clients[0], accepted: accepted}

	ssB := lease.NewSessions(caspaxos.NewProposer(2, nw.Clients()), clock)
	granted := false
	wrap := &betweenAttempts{
		t:        t,
		prop:     caspaxos.NewProposer(3, clients),
		nw:       nw,
		accepted: accepted,
		between: func() {
			// Hide acceptor 2 so the Grant's prepare quorum is {0, 1}.
			// The Grant then adopts the cleared record from acceptor 0,
			// commits it, and writes agentB's session on top.
			nw.SetReachable(2, false)
			defer nw.SetReachable(2, true)
			if _, err := ssB.Grant(ctx, "s", "agentB", 100); err != nil {
				t.Errorf("grant between attempts: %v", err)
				return
			}
			granted = true
		},
	}
	revoker := lease.NewSessions(wrap, clock)

	err := revoker.Revoke(ctx, "s")
	if err != nil && !errors.Is(err, caspaxos.ErrConflict) {
		t.Fatalf("revoke = %v, want nil or ErrConflict", err)
	}
	if !granted {
		t.Fatal("the grant between attempts did not run")
	}
	got, present, err := ssB.Info(ctx, "s")
	if err != nil {
		t.Fatal(err)
	}
	if !present || got.Owner != "agentB" {
		t.Fatalf("session = %+v (present %v), want agentB's session to survive", got, present)
	}
}
