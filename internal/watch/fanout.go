package watch

import (
	"context"
	"sync"
)

// FanOut multiplexes one upstream KeyWatcher to many local subscribers. An agent
// keeps a single FanOut per watched key and dispatches each polled batch to all
// of its client watchers, so the storage tier sees one subscription regardless
// of how many clients are watching through this agent.
type FanOut struct {
	upstream *KeyWatcher

	mu     sync.Mutex
	nextID int
	subs   map[int]chan Event
}

// NewFanOut wraps an upstream watcher.
func NewFanOut(upstream *KeyWatcher) *FanOut {
	return &FanOut{upstream: upstream, subs: map[int]chan Event{}}
}

// Subscribe registers a client watcher and returns its event channel plus a
// cancel function. buffer sizes the per-subscriber channel.
func (f *FanOut) Subscribe(buffer int) (<-chan Event, func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := f.nextID
	f.nextID++
	ch := make(chan Event, buffer)
	f.subs[id] = ch
	return ch, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if c, ok := f.subs[id]; ok {
			close(c)
			delete(f.subs, id)
		}
	}
}

// Subscribers returns the current subscriber count.
func (f *FanOut) Subscribers() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.subs)
}

// Pump polls the single upstream watcher once and delivers every event to every
// subscriber. It returns the number of events dispatched (or an error, e.g.
// ErrCompacted from upstream). This is the O(agents)-not-O(clients) property: a
// single upstream read serves all local watchers.
func (f *FanOut) Pump(ctx context.Context) (int, error) {
	events, err := f.upstream.Poll(ctx)
	if err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ev := range events {
		for _, ch := range f.subs {
			ch <- ev
		}
	}
	return len(events), nil
}
