package lease_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/lease"
	"github.com/phoban01/cask/testutil/sim"
)

// betweenAttempts wraps a proposer. On call number arm it downs the accept
// path to acceptors 1 and 2, so only acceptor 0 takes the write and the
// round returns ErrUnknownOutcome. It then runs between, which models a
// round that commits the minority write and writes on top of it.
type betweenAttempts struct {
	prop    lease.Proposer
	nw      *sim.Network
	arm     int
	calls   int
	between func()
}

func (b *betweenAttempts) Propose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error) {
	b.calls++
	if b.calls != b.arm {
		return b.prop.Propose(ctx, key, change)
	}
	b.nw.SetAcceptDown(1, true)
	b.nw.SetAcceptDown(2, true)
	raw, err := b.prop.Propose(ctx, key, change)
	b.nw.SetAcceptDown(1, false)
	b.nw.SetAcceptDown(2, false)
	if errors.Is(err, caspaxos.ErrUnknownOutcome) {
		b.between()
	}
	return raw, err
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

	ssB := lease.NewSessions(caspaxos.NewProposer(2, nw.Clients()), clock)
	granted := false
	wrap := &betweenAttempts{
		prop: caspaxos.NewProposer(3, nw.Clients()),
		nw:   nw,
		arm:  2, // call 1 is Revoke's read; call 2 is its first write
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
