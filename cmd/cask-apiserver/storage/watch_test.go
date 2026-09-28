package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
	"github.com/phoban01/cask/internal/caspaxos"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/watch"
	apistorage "k8s.io/apiserver/pkg/storage"
)

// fastWatch makes watches on s poll and bookmark quickly.
func fastWatch(s *Store) *Store {
	s.watchPoll = 20 * time.Millisecond
	s.bookmarkEvery = time.Hour
	return s
}

func mustWatch(t *testing.T, s *Store, key string, opts apistorage.ListOptions) watch.Interface {
	t.Helper()
	w, err := s.Watch(context.Background(), key, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
	return w
}

func watchFrom(rv uint64) apistorage.ListOptions {
	opts := listOpts()
	opts.ResourceVersion = strconv.FormatUint(rv, 10)
	return opts
}

// next returns the next event, or fails the test after a timeout.
func next(t *testing.T, w watch.Interface) watch.Event {
	t.Helper()
	select {
	case ev, ok := <-w.ResultChan():
		if !ok {
			t.Fatal("watch closed")
		}
		return ev
	case <-time.After(10 * time.Second):
		t.Fatal("no watch event within 10s")
	}
	return watch.Event{}
}

// short describes an event as "TYPE name@rv".
func short(ev watch.Event) string {
	switch o := ev.Object.(type) {
	case *v1alpha1.Device:
		return fmt.Sprintf("%s %s@%s", ev.Type, o.Name, o.ResourceVersion)
	case *metav1.Status:
		return fmt.Sprintf("%s %d", ev.Type, o.Code)
	}
	return fmt.Sprintf("%s %T", ev.Type, ev.Object)
}

// expectedSteps derives, from the retained index history, the event that
// each index step after from must produce.
func expectedSteps(t *testing.T, s *Store, from, to uint64) []string {
	t.Helper()
	chain, err := s.kv.History(context.Background(), IndexKey(s.resource))
	if err != nil {
		t.Fatal(err)
	}
	prev, ok, err := indexAt(chain, from)
	if err != nil || !ok {
		t.Fatalf("index at %d: ok=%v err=%v", from, ok, err)
	}
	var out []string
	for seq := from + 1; seq <= to; seq++ {
		cur, ok, err := indexAt(chain, seq)
		if err != nil || !ok {
			t.Fatalf("index at %d: ok=%v err=%v", seq, ok, err)
		}
		n := 0
		for name, e := range cur {
			if p, had := prev[name]; (!had || p != e) && e.Idx != seq {
				t.Fatalf("index step %d wrote %s at index sequence %d", seq, name, e.Idx)
			}
			if p, had := prev[name]; !had {
				out = append(out, fmt.Sprintf("ADDED %s@%d", name, e.Idx))
				n++
			} else if p != e {
				out = append(out, fmt.Sprintf("MODIFIED %s@%d", name, e.Idx))
				n++
			}
		}
		for name := range prev {
			if _, has := cur[name]; !has {
				out = append(out, fmt.Sprintf("DELETED %s@%d", name, seq))
				n++
			}
		}
		if n != 1 {
			t.Fatalf("index step %d changed %d names, want 1", seq, n)
		}
		prev = cur
	}
	return out
}

// untilLanded runs op, a mutation, until it lands. The reads of the
// open watches, and the index reads of every get, update, and delete,
// compete with its rounds (issue #149).
//
// A mutation that lost too many rounds before its object write committed
// wrote nothing, so untilLanded runs it again. The Store retries an index
// write that lost a round, so "index write failed" is a test failure.
// Any other error comes back unchanged.
func untilLanded(op func() error) error {
	for {
		err := op()
		if err == nil || !errors.Is(err, caspaxos.ErrPreempted) || strings.Contains(err.Error(), "index write failed") {
			return err
		}
	}
}

func TestWatchFromOldVersionDeliversEveryChangeInOrder(t *testing.T) {
	//= docs/spec/fleet.md#4-list-and-watch
	//= type=test
	//# A watch from a resourceVersion MUST deliver every index change after that version, in order, with no gaps.
	//= docs/spec/fleet.md#4-list-and-watch
	//= type=test
	//# Each watch event MUST carry the object at the sequence the index recorded.
	ctx := context.Background()
	cluster := newCask(t)
	kv := cluster(1)
	s := fastWatch(newStoreOn(t, kv))
	// A second Store with its own proposer stands in for another cluster.
	// Its writes do not wake the watch; the watch finds them in the index
	// history.
	other := newStoreOn(t, cluster(2))

	mustCreate(t, s, device("seed-0", "a100"))
	mustCreate(t, s, device("seed-1", "a100"))
	start := listRV(t, mustList(t, s, listOpts()))

	// Watch from the start, and a second watch that starts before the
	// seeds were written.
	w := mustWatch(t, s, keyPrefix, watchFrom(start))
	early := mustWatch(t, s, keyPrefix, watchFrom(1))
	if err := s.Delete(ctx, keyPrefix+"seed-0", &v1alpha1.Device{}, nil, nil, nil, apistorage.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	const writers, rounds = 3, 5
	var wg sync.WaitGroup
	for i := range writers {
		st := s
		if i%2 == 1 {
			st = other
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := fmt.Sprintf("gpu-%d", i)
			for r := range rounds {
				err := untilLanded(func() error {
					return st.Create(ctx, keyPrefix+name, device(name, "a100"), nil, 0)
				})
				if err == nil {
					err = untilLanded(func() error {
						return st.GuaranteedUpdate(ctx, keyPrefix+name, &v1alpha1.Device{}, false, nil,
							mutate(func(d *v1alpha1.Device) { d.Spec.Model = fmt.Sprint(r) }), nil)
					})
				}
				if err == nil {
					err = untilLanded(func() error {
						return st.Delete(ctx, keyPrefix+name, &v1alpha1.Device{}, nil, nil, nil, apistorage.DeleteOptions{})
					})
				}
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	// Each round is three mutations. A repaired index write can fold two
	// of them into one index step, so the count is a bound.
	end := mustIndex(t, kv, "devices").Seq
	if lo, hi := start+1+writers*rounds*2, start+1+writers*rounds*3; end < lo || end > hi {
		t.Fatalf("index sequence = %d, want %d to %d", end, lo, hi)
	}
	for _, tc := range []struct {
		name string
		w    watch.Interface
		from uint64
	}{{"from list", w, start}, {"from 1", early, 1}} {
		want := expectedSteps(t, s, tc.from, end)
		for i, exp := range want {
			if got := short(next(t, tc.w)); got != exp {
				t.Fatalf("%s: event %d = %q, want %q", tc.name, i, got, exp)
			}
		}
	}
}

func TestWatchCompactedStartEndsWithGone(t *testing.T) {
	//= docs/spec/fleet.md#4-list-and-watch
	//= type=test
	//# A watch whose start version is compacted MUST end with 410 Gone.
	ctx := context.Background()
	s := fastWatch(newTestStore(t))
	for _, n := range []string{"gpu-0", "gpu-1", "gpu-2"} {
		mustCreate(t, s, device(n, "a100"))
	}
	if err := s.kv.Compact(ctx, IndexKey("devices"), 3); err != nil {
		t.Fatal(err)
	}

	for _, rv := range []uint64{1, 2} {
		w := mustWatch(t, s, keyPrefix, watchFrom(rv))
		ev := next(t, w)
		st, ok := ev.Object.(*metav1.Status)
		if ev.Type != watch.Error || !ok || st.Code != http.StatusGone {
			t.Fatalf("watch from %d: first event = %s, want ERROR 410", rv, short(ev))
		}
		if _, open := <-w.ResultChan(); open {
			t.Fatalf("watch from %d: channel open after 410", rv)
		}
	}

	// Version 3 is retained, so a watch from 3 sees the next change.
	w := mustWatch(t, s, keyPrefix, watchFrom(3))
	mustCreate(t, s, device("gpu-3", "a100"))
	if got := short(next(t, w)); got != "ADDED gpu-3@4" {
		t.Fatalf("watch from 3: %s", got)
	}
}

func TestWatchFutureVersionIsTooLarge(t *testing.T) {
	s := newTestStore(t)
	mustCreate(t, s, device("gpu-0", "a100"))
	_, err := s.Watch(context.Background(), keyPrefix, watchFrom(5))
	if !apistorage.IsTooLargeResourceVersion(err) {
		t.Fatalf("error = %v, want too large resource version", err)
	}
}

func TestWatchInitialEventsAndBookmarks(t *testing.T) {
	s := fastWatch(newTestStore(t))
	mustCreate(t, s, device("gpu-1", "a100"))
	mustCreate(t, s, device("gpu-0", "a100"))

	// resourceVersion "0" starts with the current state.
	w := mustWatch(t, s, keyPrefix, watchFrom(0))
	for _, want := range []string{"ADDED gpu-0@2", "ADDED gpu-1@1"} {
		if got := short(next(t, w)); got != want {
			t.Fatalf("initial event = %q, want %q", got, want)
		}
	}
	mustCreate(t, s, device("gpu-2", "a100"))
	if got := short(next(t, w)); got != "ADDED gpu-2@3" {
		t.Fatalf("after initial events: %s", got)
	}

	// SendInitialEvents with bookmarks ends the initial events with an
	// annotated bookmark at the index sequence.
	yes := true
	opts := watchFrom(0)
	opts.SendInitialEvents = &yes
	opts.ResourceVersionMatch = metav1.ResourceVersionMatchNotOlderThan
	opts.Predicate.AllowWatchBookmarks = true
	w = mustWatch(t, s, keyPrefix, opts)
	for range 3 {
		if ev := next(t, w); ev.Type != watch.Added {
			t.Fatalf("initial event = %s", short(ev))
		}
	}
	ev := next(t, w)
	d, ok := ev.Object.(*v1alpha1.Device)
	if ev.Type != watch.Bookmark || !ok || d.ResourceVersion != "3" ||
		d.Annotations[metav1.InitialEventsAnnotationKey] != "true" {
		t.Fatalf("end of initial events = %s %+v", short(ev), ev.Object)
	}

	// An idle watch that allows bookmarks sends the index sequence. A new
	// Store on the same cask gets the short interval, so the setting does
	// not race with the watches above, which still run.
	idle := fastWatch(newStoreOn(t, s.kv))
	idle.bookmarkEvery = 10 * time.Millisecond
	opts = watchFrom(3)
	opts.Predicate.AllowWatchBookmarks = true
	w = mustWatch(t, idle, keyPrefix, opts)
	if ev := next(t, w); ev.Type != watch.Bookmark || ev.Object.(*v1alpha1.Device).ResourceVersion != "3" {
		t.Fatalf("idle bookmark = %s", short(ev))
	}
}

func TestWatchPredicateAndOneObject(t *testing.T) {
	ctx := context.Background()
	s := fastWatch(newTestStore(t))
	east := func(d *v1alpha1.Device) *v1alpha1.Device {
		d.Labels = map[string]string{"zone": "east"}
		return d
	}
	mustCreate(t, s, east(device("gpu-0", "a100")))

	opts := watchFrom(1)
	opts.Predicate.Label = labels.SelectorFromSet(labels.Set{"zone": "east"})
	sel := mustWatch(t, s, keyPrefix, opts)
	one := mustWatch(t, s, keyPrefix+"gpu-1", apistorage.ListOptions{
		ResourceVersion: "1", Predicate: everything(),
	})

	mustCreate(t, s, device("gpu-1", "a100")) // not east
	if err := s.GuaranteedUpdate(ctx, keyPrefix+"gpu-1", &v1alpha1.Device{}, false, nil,
		mutate(func(d *v1alpha1.Device) { d.Labels = map[string]string{"zone": "east"} }), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.GuaranteedUpdate(ctx, keyPrefix+"gpu-0", &v1alpha1.Device{}, false, nil,
		mutate(func(d *v1alpha1.Device) { d.Labels = nil }), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, keyPrefix+"gpu-1", &v1alpha1.Device{}, nil, nil, nil, apistorage.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	// gpu-1 moves into the selector, gpu-0 moves out, then gpu-1 goes.
	for _, want := range []string{"ADDED gpu-1@3", "DELETED gpu-0@4", "DELETED gpu-1@5"} {
		if got := short(next(t, sel)); got != want {
			t.Fatalf("selector watch: %q, want %q", got, want)
		}
	}
	for _, want := range []string{"ADDED gpu-1@2", "MODIFIED gpu-1@3", "DELETED gpu-1@5"} {
		if got := short(next(t, one)); got != want {
			t.Fatalf("one-object watch: %q, want %q", got, want)
		}
	}
}
