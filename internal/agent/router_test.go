package agent_test

import (
	"context"
	"errors"
	"testing"

	"github.com/phoban01/cask/internal/agent"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/ranges"
	"github.com/phoban01/cask/internal/store"
)

// stale models an acceptor behind a §4.1 epoch check that rejects this
// proposer's routing as out of date.
type stale struct{}

func (stale) Prepare(context.Context, []byte, caspaxos.Ballot) (caspaxos.PrepareReply, error) {
	return caspaxos.PrepareReply{}, caspaxos.ErrRangeChanged
}

func (stale) Accept(context.Context, []byte, caspaxos.Ballot, []byte) (caspaxos.AcceptReply, error) {
	return caspaxos.AcceptReply{}, caspaxos.ErrRangeChanged
}

// On ErrRangeChanged the router re-resolves placement once and retries
// against the fresh replica set; persistent staleness propagates the error.
func TestRouterRefreshesOnRangeChanged(t *testing.T) {
	ctx := context.Background()
	dialer := agent.StaticDialer{
		0: stale{}, 1: stale{}, 2: stale{},
		10: caspaxos.NewAcceptor(store.NewMem()),
		11: caspaxos.NewAcceptor(store.NewMem()),
		12: caspaxos.NewAcceptor(store.NewMem()),
	}
	oldMap := ranges.NewMap([]ranges.Descriptor{{ID: 1, Replicas: []uint64{0, 1, 2}, Epoch: 1}})
	newMap := ranges.NewMap([]ranges.Descriptor{{ID: 1, Replicas: []uint64{10, 11, 12}, Epoch: 2}})

	refreshes := 0
	r := agent.NewRouter(7, oldMap, dialer, agent.WithRefresh(func() *ranges.Map {
		refreshes++
		return newMap
	}))

	got, err := r.Propose(ctx, []byte("k"), caspaxos.Write([]byte("v")))
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	if string(got) != "v" {
		t.Fatalf("propose = %q, want v", got)
	}
	if refreshes != 1 {
		t.Fatalf("refreshes = %d, want 1", refreshes)
	}

	// The refreshed map is retained: the next op routes straight to the new
	// replicas with no further refresh.
	if _, err := r.Propose(ctx, []byte("k"), caspaxos.Write([]byte("v2"))); err != nil {
		t.Fatalf("second propose: %v", err)
	}
	if refreshes != 1 {
		t.Fatalf("refreshes after second propose = %d, want 1 (map retained)", refreshes)
	}
}

// A refresh that yields the same stale placement must not loop: one retry,
// then the typed error reaches the caller.
func TestRouterPersistentStalenessPropagates(t *testing.T) {
	dialer := agent.StaticDialer{0: stale{}, 1: stale{}, 2: stale{}}
	m := ranges.NewMap([]ranges.Descriptor{{ID: 1, Replicas: []uint64{0, 1, 2}, Epoch: 1}})

	refreshes := 0
	r := agent.NewRouter(7, m, dialer, agent.WithRefresh(func() *ranges.Map {
		refreshes++
		return m
	}))

	_, err := r.Propose(context.Background(), []byte("k"), caspaxos.Write([]byte("v")))
	if !errors.Is(err, caspaxos.ErrRangeChanged) {
		t.Fatalf("propose = %v, want ErrRangeChanged", err)
	}
	if refreshes != 1 {
		t.Fatalf("refreshes = %d, want exactly 1 (no retry loop)", refreshes)
	}
}
