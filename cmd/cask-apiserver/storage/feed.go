package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/phoban01/cask/internal/mvcc"
)

// indexFeed is the one reader of a Store's index history. It serves every
// open watch of the Store, so N watches cost one read of the index
// register per poll, not N (issue #149). Each read of the index register
// is a consensus round, and rounds on the index register compete with the
// index writes of every create, update, and delete.
//
// The feed runs while at least one watch is open. It reads the index
// history after each index write through the Store, and every watchPoll
// otherwise, and hands the chain to every watch.
//
// Each watch has a buffer of one chain. When a watch has not taken the
// last chain, the feed replaces it with the new one. No step is lost that
// way: the retained history only grows at the head, so the newer chain
// holds every step of the older one. The only way a step leaves the
// history is compaction, and a watch whose cursor falls below the
// retained history ends with 410 Gone, as it did before.
type indexFeed struct {
	s *Store

	mu   sync.Mutex
	subs map[*indexWatch]struct{}
	// last is the newest chain the feed read. A new watch starts from it.
	last *mvcc.Chain
	// stop ends the running feed. It is nil when the feed is not running.
	stop context.CancelFunc

	// reads counts the index history reads of the feed.
	reads atomic.Int64
}

func newIndexFeed(s *Store) *indexFeed {
	return &indexFeed{s: s, subs: map[*indexWatch]struct{}{}}
}

// subscribe adds w to the feed and starts the feed if it is not running.
// w gets the newest chain the feed has read, if any.
func (f *indexFeed) subscribe(w *indexWatch) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subs[w] = struct{}{}
	if f.last != nil {
		offer(w.chains, f.last)
	}
	if f.stop == nil {
		ctx, stop := context.WithCancel(context.Background())
		f.stop = stop
		go f.run(ctx)
	}
}

// unsubscribe removes w and stops the feed when no watch is left.
func (f *indexFeed) unsubscribe(w *indexWatch) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.subs, w)
	if len(f.subs) == 0 && f.stop != nil {
		f.stop()
		f.stop = nil
		// A later feed must not hand out a chain older than a new
		// watch's own read.
		f.last = nil
	}
}

// run reads the index history until ctx ends.
func (f *indexFeed) run(ctx context.Context) {
	//= docs/spec/fleet.md#4-list-and-watch
	//= type=exception
	//= reason=a write through another Store or cluster reaches the watch at the next poll of the index history; tracked in issue #148
	//# Watch events SHOULD be pushed from the index register's change feed rather than polled.
	for {
		// Take the wake-up channel before the read, so a write that lands
		// during the read still wakes the next wait.
		wake := f.s.indexChanged()
		chain, err := f.s.kv.History(ctx, IndexKey(f.s.resource))
		f.reads.Add(1)
		if err == nil {
			f.publish(ctx, &chain)
		}
		// A read that failed, for example one that lost its round to
		// the index writes, is tried again at the next wake or poll.
		// The watches keep their cursors, so they miss no step.
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-time.After(f.s.watchPoll):
		}
	}
}

// publish hands chain to every watch. A feed that was stopped while it
// read publishes nothing.
func (f *indexFeed) publish(ctx context.Context, chain *mvcc.Chain) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if ctx.Err() != nil {
		return
	}
	f.last = chain
	for w := range f.subs {
		offer(w.chains, chain)
	}
}

// offer puts chain in ch, a buffer of one, in place of any chain that the
// watch has not taken yet. Only the feed sends on ch, under its lock.
func offer(ch chan *mvcc.Chain, chain *mvcc.Chain) {
	select {
	case <-ch:
	default:
	}
	ch <- chain
}
