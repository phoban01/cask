package storage

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/backoff"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/store"
	"k8s.io/apimachinery/pkg/watch"
)

// countingProposer counts the rounds that callers start on one key.
type countingProposer struct {
	next   caspaxos.Proposing
	key    []byte
	rounds atomic.Int64
}

func (p *countingProposer) Propose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error) {
	if bytes.Equal(key, p.key) {
		p.rounds.Add(1)
	}
	return p.next.Propose(ctx, key, change)
}

// newCountingStore returns a Store over a new in-process cask that counts
// the rounds on the devices index register.
func newCountingStore(t *testing.T) (*Store, *countingProposer) {
	t.Helper()
	acceptors := make([]caspaxos.AcceptorClient, 3)
	for i := range acceptors {
		acceptors[i] = caspaxos.NewAcceptor(store.NewMem())
	}
	var now atomic.Int64
	clock := hlc.New(func() int64 { return now.Add(1) })
	prop := caspaxos.NewProposer(1, acceptors,
		caspaxos.WithBackoff(backoff.FullJitter(time.Millisecond, 20*time.Millisecond)))
	counter := &countingProposer{next: prop, key: IndexKey("devices")}
	return newStoreOn(t, mvcc.New(counter, clock, 1)), counter
}

// Issue #149: each watch read the index history with its own round, so N
// watches cost N rounds on the index register per write. The rounds
// preempted the index writes. One feed per Store now reads for all of
// them.
func TestWatchesShareOneIndexReader(t *testing.T) {
	//= docs/spec/fleet.md#4-list-and-watch
	//= type=test
	//# A watch from a resourceVersion MUST deliver every index change after that version, in order, with no gaps.
	const watches, writes = 20, 10
	// indexRounds runs the writes with n open watches, waits until every
	// watch has every event, and returns the rounds on the index register
	// and the reads of the feed.
	indexRounds := func(n int) (rounds, reads int64) {
		s, counter := newCountingStore(t)
		// No polls: the feed reads only when a write wakes it.
		s.watchPoll = time.Hour
		ws := make([]watch.Interface, n)
		for i := range ws {
			ws[i] = mustWatch(t, s, keyPrefix, watchFrom(0))
		}
		startRounds, startReads := counter.rounds.Load(), s.feed.reads.Load()
		for i := range writes {
			mustCreate(t, s, device(fmt.Sprintf("gpu-%d", i), "a100"))
		}
		for _, w := range ws {
			for i := range writes {
				if got, want := short(next(t, w)), fmt.Sprintf("ADDED gpu-%d@%d", i, i+1); got != want {
					t.Fatalf("event %d = %q, want %q", i, got, want)
				}
			}
		}
		return counter.rounds.Load() - startRounds, s.feed.reads.Load() - startReads
	}

	base, _ := indexRounds(0)
	rounds, reads := indexRounds(watches)
	// The feed may start its first read after the count starts, so allow
	// one read more than the writes.
	if reads > writes+1 {
		t.Errorf("%d watches: the feed read the index %d times for %d writes, want at most %d",
			watches, reads, writes, writes+1)
	}
	// Per-watch reads would add watches*writes = 200 rounds. A lost round
	// can make a write start one more, so allow twice the feed's reads.
	if extra := rounds - base; extra > 2*(writes+1) {
		t.Errorf("%d watches added %d rounds on the index register for %d writes, want at most %d",
			watches, extra, writes, 2*(writes+1))
	}
	t.Logf("index rounds: %d with no watch, %d with %d watches; feed reads: %d", base, rounds, watches, reads)
}

// A watch that has not taken the last index history gets the newer one in
// its place, and still delivers every step in order.
func TestSlowWatchMissesNoStep(t *testing.T) {
	//= docs/spec/fleet.md#4-list-and-watch
	//= type=test
	//# A watch from a resourceVersion MUST deliver every index change after that version, in order, with no gaps.
	s := fastWatch(newTestStore(t))
	w := mustWatch(t, s, keyPrefix, watchFrom(0))
	const writes = 30
	for i := range writes {
		mustCreate(t, s, device(fmt.Sprintf("gpu-%02d", i), "a100"))
	}
	for i := range writes {
		if got, want := short(next(t, w)), fmt.Sprintf("ADDED gpu-%02d@%d", i, i+1); got != want {
			t.Fatalf("event %d = %q, want %q", i, got, want)
		}
	}
}
