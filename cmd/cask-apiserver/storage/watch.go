package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/phoban01/cask/internal/mvcc"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	apistorage "k8s.io/apiserver/pkg/storage"
)

// watchExpired matches the message of the etcd store.
const watchExpired = "The resourceVersion for the provided watch is too old."

// Watch streams the changes of the index register after
// opts.ResourceVersion, which is an index sequence.
//
// Every index version after the start is one step of the stream, in
// sequence order, with no gaps. For each name that a step adds, changes,
// or removes, the watch sends ADDED, MODIFIED, or DELETED. The event
// carries the object at the sequence the index recorded. A DELETED event
// carries the last version that the index recorded before the removal.
//
// The resourceVersion of every event is the index sequence of its step.
// An ADDED or MODIFIED event carries the entry's index sequence, which is
// the step that wrote the entry. A DELETED event carries the step that
// removed the name. A client that resumes a watch from the
// resourceVersion of any event therefore neither replays nor skips a
// step.
//
// With resourceVersion "" or "0", or with SendInitialEvents, the watch
// first sends an ADDED event for each object in the current index. When
// the start version is compacted, the watch sends one 410 Gone error
// event and ends. A start version above the current index sequence gets
// a "too large resource version" error.
//
// When the predicate allows bookmarks, the watch sends a bookmark with
// the index sequence it has reached after each idle bookmark interval,
// and after the initial events when SendInitialEvents is set.
func (s *Store) Watch(ctx context.Context, key string, opts apistorage.ListOptions) (watch.Interface, error) {
	rv, err := s.versioner.ParseResourceVersion(opts.ResourceVersion)
	if err != nil {
		return nil, err
	}
	// A read that lost its round changed nothing, so it is retried.
	chain, err := indexHistory(ctx, s.kv, s.resource)
	if err != nil {
		return nil, err
	}
	current := headSeq(chain)
	if rv > current {
		return nil, apistorage.NewTooLargeResourceVersionError(rv, current, 1)
	}
	initial := (opts.SendInitialEvents == nil && rv == 0) ||
		(opts.SendInitialEvents != nil && *opts.SendInitialEvents)
	start := rv
	if initial || rv == 0 {
		// Initial events show the current state, which is not older
		// than rv. Without them, "0" means "from now".
		start = current
	}

	ctx, cancel := context.WithCancel(ctx)
	w := &indexWatch{
		s:      s,
		ctx:    ctx,
		cancel: cancel,
		chains: make(chan *mvcc.Chain, 1),
		result: make(chan watch.Event),
		pred:   opts.Predicate,
		marks:  opts.Predicate.AllowWatchBookmarks || opts.ProgressNotify,
	}
	if !opts.Recursive {
		w.name = key[strings.LastIndexByte(key, '/')+1:]
	}
	base, ok, err := indexAt(chain, start)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("cask storage: decode %s index at %d: %w", s.resource, start, err)
	}
	if !ok {
		//= docs/spec/fleet.md#4-list-and-watch
		//# A watch whose start version is compacted MUST end with 410 Gone.
		go w.fail(apierrors.NewResourceExpired(watchExpired))
		return w, nil
	}
	endMark := initial && opts.SendInitialEvents != nil && opts.Predicate.AllowWatchBookmarks
	go w.run(base, start, initial, endMark)
	return w, nil
}

// indexWatch is one watch on the index register of a Store.
type indexWatch struct {
	s      *Store
	ctx    context.Context
	cancel context.CancelFunc
	// chains holds the newest index history that the Store's feed read
	// and this watch has not taken yet.
	chains chan *mvcc.Chain
	result chan watch.Event
	pred   apistorage.SelectionPredicate
	// name limits the watch to one object. It is empty for a watch on
	// the whole resource.
	name string
	// marks is true when the client allows bookmark events.
	marks bool
}

// Stop ends the watch. ResultChan closes soon after.
func (w *indexWatch) Stop() { w.cancel() }

// ResultChan returns the channel of watch events.
func (w *indexWatch) ResultChan() <-chan watch.Event { return w.result }

// send delivers ev. It returns false when the watch has stopped.
func (w *indexWatch) send(ev watch.Event) bool {
	select {
	case w.result <- ev:
		return true
	case <-w.ctx.Done():
		return false
	}
}

// fail sends err as an error event and ends the watch. A stopped watch
// ends with no error event.
func (w *indexWatch) fail(err error) {
	defer close(w.result)
	defer w.cancel()
	if w.ctx.Err() != nil {
		return
	}
	var status apierrors.APIStatus
	if !errors.As(err, &status) {
		status = apierrors.NewInternalError(err)
	}
	st := status.Status()
	w.send(watch.Event{Type: watch.Error, Object: &st})
}

// bookmark sends a bookmark at index sequence seq.
func (w *indexWatch) bookmark(seq uint64, initialEnd bool) bool {
	obj := w.s.newFunc()
	if err := w.s.versioner.UpdateObject(obj, seq); err != nil {
		return false
	}
	if initialEnd {
		if err := apistorage.AnnotateInitialEventsEndBookmark(obj); err != nil {
			return false
		}
	}
	return w.send(watch.Event{Type: watch.Bookmark, Object: obj})
}

// run sends the initial events, if asked, and then every index step after
// cursor. base is the index at cursor.
func (w *indexWatch) run(base map[string]Entry, cursor uint64, initial, endMark bool) {
	if initial {
		for _, name := range sortedNames(w, base) {
			obj, err := w.s.objectAt(w.ctx, name, base[name], w.s.newFunc())
			if err != nil {
				w.fail(err)
				return
			}
			if ok, err := w.pred.Matches(obj); err != nil {
				w.fail(err)
				return
			} else if ok && !w.send(watch.Event{Type: watch.Added, Object: obj}) {
				w.fail(w.ctx.Err())
				return
			}
		}
	}
	if endMark && !w.bookmark(cursor, true) {
		w.fail(w.ctx.Err())
		return
	}

	// The Store's one index feed reads the history for every watch.
	w.s.feed.subscribe(w)
	defer w.s.feed.unsubscribe(w)
	prev := base
	lastSent := time.Now()
	for {
		var chain *mvcc.Chain
		select {
		case <-w.ctx.Done():
			w.fail(w.ctx.Err())
			return
		case chain = <-w.chains:
		}
		if cursor+1 < chain.CompactedBelow {
			w.fail(apierrors.NewResourceExpired(watchExpired))
			return
		}
		for _, v := range chain.Versions {
			if v.Seq <= cursor {
				continue
			}
			//= docs/spec/fleet.md#4-list-and-watch
			//# A watch from a resourceVersion MUST deliver every index change after that version, in order, with no gaps.
			if v.Seq != cursor+1 {
				// The history lost the step after the cursor.
				w.fail(apierrors.NewResourceExpired(watchExpired))
				return
			}
			next, err := versionEntries(v)
			if err != nil {
				w.fail(fmt.Errorf("cask storage: decode %s index at %d: %w", w.s.resource, v.Seq, err))
				return
			}
			sent, err := w.step(prev, next, v.Seq)
			if err != nil {
				w.fail(err)
				return
			}
			if sent {
				lastSent = time.Now()
			}
			prev, cursor = next, v.Seq
		}
		if w.marks && time.Since(lastSent) >= w.s.bookmarkEvery {
			if !w.bookmark(cursor, false) {
				w.fail(w.ctx.Err())
				return
			}
			lastSent = time.Now()
		}
	}
}

// step sends the events for one index step from prev to next, in name
// order. seq is the index sequence of the step. It reports whether it
// sent any event.
func (w *indexWatch) step(prev, next map[string]Entry, seq uint64) (bool, error) {
	changed := map[string]bool{}
	for name, e := range next {
		if old, ok := prev[name]; !ok || old != e {
			changed[name] = true
		}
	}
	for name := range prev {
		if _, ok := next[name]; !ok {
			changed[name] = true
		}
	}
	sent := false
	for _, name := range sortedNames(w, changed) {
		ev, ok, err := w.event(name, prev, next, seq)
		if err != nil {
			return sent, err
		}
		if !ok {
			continue
		}
		if !w.send(ev) {
			return sent, w.ctx.Err()
		}
		sent = true
	}
	return sent, nil
}

// event builds the event for name, which changed from prev to next. ok is
// false when the predicate filters the change out.
func (w *indexWatch) event(name string, prev, next map[string]Entry, seq uint64) (ev watch.Event, ok bool, err error) {
	//= docs/spec/fleet.md#4-list-and-watch
	//# Each watch event MUST carry the object at the sequence the index recorded.
	//= docs/spec/fleet.md#4-list-and-watch
	//# Every resourceVersion that a get, a list, or a watch event reports MUST be an index sequence.
	// An entry that this step wrote carries Idx == seq, so an ADDED or
	// MODIFIED event carries the step's index sequence.
	oldEntry, hadOld := prev[name]
	newEntry, hasNew := next[name]
	var oldObj, newObj runtime.Object
	oldMatch, newMatch := false, false
	if hasNew {
		if newObj, err = w.s.objectAt(w.ctx, name, newEntry, w.s.newFunc()); err != nil {
			return ev, false, err
		}
		if newMatch, err = w.pred.Matches(newObj); err != nil {
			return ev, false, err
		}
	}
	// The old object decides a DELETED event, and whether a change moved
	// the object into or out of the predicate.
	if hadOld && (!hasNew || !w.pred.Empty()) {
		if oldObj, err = w.s.objectAt(w.ctx, name, oldEntry, w.s.newFunc()); err != nil {
			return ev, false, err
		}
		if oldMatch, err = w.pred.Matches(oldObj); err != nil {
			return ev, false, err
		}
	} else if hadOld {
		oldMatch = true
	}
	switch {
	case newMatch && oldMatch:
		return watch.Event{Type: watch.Modified, Object: newObj}, true, nil
	case newMatch:
		return watch.Event{Type: watch.Added, Object: newObj}, true, nil
	case oldMatch:
		// The old object carries the index sequence of the step that
		// removed it, or that moved it out of the predicate, as the etcd
		// store does. A resume from it does not replay the step.
		//= docs/spec/fleet.md#4-list-and-watch
		//# A watch that resumes from the resourceVersion of any event MUST NOT skip or replay a change.
		if err := w.s.versioner.UpdateObject(oldObj, seq); err != nil {
			return ev, false, err
		}
		return watch.Event{Type: watch.Deleted, Object: oldObj}, true, nil
	}
	return ev, false, nil
}

// sortedNames returns the keys of m in order, limited to w.name when the
// watch is on one object.
func sortedNames[V any](w *indexWatch, m map[string]V) []string {
	out := make([]string, 0, len(m))
	for name := range m {
		if w.name == "" || name == w.name {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}
