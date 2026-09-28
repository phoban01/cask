package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/backoff"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/store"
)

var errVoterDown = errors.New("voter down")

// stoppableVoter is an acceptor whose listener can stop and start again.
// Its store stays intact, as a StatefulSet pod keeps its volume when it
// scales to zero and back.
type stoppableVoter struct {
	acc  *caspaxos.Acceptor
	down atomic.Bool
}

func (v *stoppableVoter) Prepare(ctx context.Context, key []byte, b caspaxos.Ballot) (caspaxos.PrepareReply, error) {
	if v.down.Load() {
		return caspaxos.PrepareReply{}, errVoterDown
	}
	return v.acc.Prepare(ctx, key, b)
}

func (v *stoppableVoter) Accept(ctx context.Context, key []byte, b caspaxos.Ballot, val []byte) (caspaxos.AcceptReply, error) {
	if v.down.Load() {
		return caspaxos.AcceptReply{}, errVoterDown
	}
	return v.acc.Accept(ctx, key, b, val)
}

// outageRun runs the scenario of issue #170. Two apiservers, east and
// west, create devices at once. East then scales to zero: its voter stops
// and its process ends. West keeps creating on two voters. East scales
// back: the voter restarts with its store, a new process starts, and both
// create again. newKV builds the KV of one apiserver process. The run
// returns every create error.
func outageRun(t *testing.T, newKV func(prop mvcc.Proposer, clock *hlc.Clock) *mvcc.KV) []string {
	t.Helper()
	voters := make([]*stoppableVoter, 3)
	clients := make([]caspaxos.AcceptorClient, 3)
	for i := range voters {
		voters[i] = &stoppableVoter{acc: caspaxos.NewAcceptor(store.NewMem())}
		clients[i] = voters[i]
	}
	var now atomic.Int64
	clock := hlc.New(func() int64 { return now.Add(1) })
	start := func(member uint64) *Store {
		prop := caspaxos.NewProposer(member, clients,
			caspaxos.WithBackoff(backoff.FullJitter(time.Millisecond, 20*time.Millisecond)))
		return newStoreOn(t, newKV(prop, clock))
	}

	var (
		mu     sync.Mutex
		failed []string
		wg     sync.WaitGroup
	)
	burst := func(s *Store, prefix string) {
		wg.Go(func() {
			for i := range 8 {
				name := fmt.Sprintf("%s-%d", prefix, i)
				out := device("", "")
				err := s.Create(context.Background(), keyPrefix+name, device(name, "m"), out, 0)
				if err == nil && out.Name != name {
					err = fmt.Errorf("create returned object %q", out.Name)
				}
				if err != nil {
					mu.Lock()
					failed = append(failed, fmt.Sprintf("%s: %v", name, err))
					mu.Unlock()
				}
			}
		})
	}

	east, west := start(11), start(12)
	burst(east, "east-a")
	burst(west, "west-a")
	wg.Wait()

	voters[0].down.Store(true)
	burst(west, "west-b")
	wg.Wait()

	voters[0].down.Store(false)
	east = start(11)
	burst(east, "east-c")
	burst(west, "west-c")
	wg.Wait()

	// Every create must be in the index at its own entry.
	idx := mustIndex(t, west.kv, "devices")
	for _, p := range []string{"east-a", "west-a", "west-b", "east-c", "west-c"} {
		for i := range 8 {
			if name := fmt.Sprintf("%s-%d", p, i); !hasEntry(idx, name) {
				failed = append(failed, name+": not in the index")
			}
		}
	}
	return failed
}

func hasEntry(idx Index, name string) bool {
	_, ok := idx.Entries[name]
	return ok
}

// Issue #170: every cask-apiserver process named its KV writes by its PID,
// which is 1 in every pod. Two apiservers then minted the same operation
// ids, and mvcc dropped a new index write as a repeat of the other's old
// one. writeIndex saw its write "land" at the old version's sequence.
func TestApiserversWithOneNodeIDKeepEveryWrite(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# Every write MUST carry an operation identity that no other writer and no earlier process of the same writer has used.
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# Each resource type MUST have one index register that maps every object name to that object's latest sequence.
	failed := outageRun(t, func(prop mvcc.Proposer, clock *hlc.Clock) *mvcc.KV {
		return mvcc.New(prop, clock, 1)
	})
	if len(failed) > 0 {
		t.Fatalf("%d writes failed:\n%s", len(failed), strings.Join(failed, "\n"))
	}
}

// The control: when two processes share an operation id space, as before
// the fix, writeIndex detects the dropped index write. The check in
// writeIndex is right; the operation ids were wrong.
func TestSharedOperationIDsAreCaught(t *testing.T) {
	failed := outageRun(t, func(prop mvcc.Proposer, clock *hlc.Clock) *mvcc.KV {
		return mvcc.New(prop, clock, 1, mvcc.WithIncarnation(1))
	})
	for _, f := range failed {
		if strings.Contains(f, "index write landed at") {
			return
		}
	}
	t.Fatalf("shared operation ids went unnoticed; failures: %v", failed)
}
