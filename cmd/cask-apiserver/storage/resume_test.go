package storage

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
	"k8s.io/apimachinery/pkg/watch"
	apistorage "k8s.io/apiserver/pkg/storage"
)

// churn runs random creates, updates, and deletes on a few names through
// s until stop closes. It ignores the errors that a race between writers
// gives: exists, not found, and a lost index write, which the next write
// to the same name repairs.
func churn(ctx context.Context, s *Store, seed uint64, stop <-chan struct{}, writes *atomic.Int64) {
	r := rand.New(rand.NewPCG(seed, seed))
	for {
		select {
		case <-stop:
			return
		default:
		}
		name := fmt.Sprintf("dev-%d", r.IntN(4))
		key := keyPrefix + name
		var err error
		switch r.IntN(4) {
		case 0:
			err = s.Create(ctx, key, device(name, "m0"), &v1alpha1.Device{}, 0)
		case 1:
			err = s.Delete(ctx, key, &v1alpha1.Device{}, nil, nil, nil, apistorage.DeleteOptions{})
		default:
			model := "m" + strconv.Itoa(r.IntN(1000))
			err = s.GuaranteedUpdate(ctx, key, &v1alpha1.Device{}, false, nil,
				mutate(func(d *v1alpha1.Device) { d.Spec.Model = model }), nil)
		}
		if err == nil {
			writes.Add(1)
		}
	}
}

// reflectorState is the cache that a client-go reflector keeps: each
// object by name, as the last event or list left it.
type reflectorState map[string]v1alpha1.Device

func (st reflectorState) String() string {
	out := ""
	for _, n := range sortedKeys(st) {
		d := st[n]
		out += fmt.Sprintf("%s@%s=%s ", n, d.ResourceVersion, d.Spec.Model)
	}
	return out
}

func sortedKeys(st reflectorState) []string {
	out := make([]string, 0, len(st))
	for n := range st {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func stateOf(list *v1alpha1.DeviceList) reflectorState {
	st := reflectorState{}
	for _, d := range list.Items {
		st[d.Name] = d
	}
	return st
}

// TestReflectorResumesFromEventResourceVersion does what a client-go
// reflector does. It lists, watches from the list resourceVersion, and
// then keeps stopping the watch and resuming it from the resourceVersion
// of the last event it got. Writers on two clusters run all the time.
//
// Every event must carry a resourceVersion above the one the watch
// resumed from, or the watch replayed a change. The number of events must
// equal the number of index steps, or the watch skipped a change. At the
// end, the cache must equal a fresh list.
func TestReflectorResumesFromEventResourceVersion(t *testing.T) {
	//= docs/spec/fleet.md#4-list-and-watch
	//= type=test
	//# A watch that resumes from the resourceVersion of any event MUST NOT skip or replay a change.
	//= docs/spec/fleet.md#4-list-and-watch
	//= type=test
	//# Every resourceVersion that a get, a list, or a watch event reports MUST be an index sequence.
	ctx := context.Background()
	cask := newCask(t)
	east := fastWatch(newStoreOn(t, cask(1)))
	west := fastWatch(newStoreOn(t, cask(2)))
	mustCreate(t, east, device("dev-0", "m0"))

	list := mustList(t, east, listOpts())
	start := listRV(t, list)
	st := stateOf(list)
	rv := start

	stop := make(chan struct{})
	var writes atomic.Int64
	var wg sync.WaitGroup
	for i, s := range []*Store{east, west, east} {
		wg.Go(func() { churn(ctx, s, uint64(i+1), stop, &writes) })
	}
	deadline := time.After(3 * time.Second)
	stopped := false

	events, resumes := 0, 0
	r := rand.New(rand.NewPCG(7, 7))
	for {
		if !stopped {
			select {
			case <-deadline:
				close(stop)
				wg.Wait()
				stopped = true
			default:
			}
		}
		if stopped {
			idx, err := ReadIndex(ctx, east.kv, east.resource)
			if err != nil {
				t.Fatal(err)
			}
			if rv == idx.Seq {
				break
			}
		}

		// Take one to three events, then drop the watch, as a reflector
		// does when its connection ends.
		w, err := east.Watch(ctx, keyPrefix, watchFrom(rv))
		if err != nil {
			t.Fatalf("resume %d from %d: %v", resumes, rv, err)
		}
		resumes++
		want := 1 + r.IntN(3)
	take:
		for range want {
			select {
			case ev, ok := <-w.ResultChan():
				if !ok {
					t.Fatalf("watch from %d closed", rv)
				}
				if ev.Type == watch.Error {
					t.Fatalf("watch from %d: error event %v", rv, ev.Object)
				}
				d := ev.Object.(*v1alpha1.Device)
				evRV := rvOf(t, d)
				if evRV <= rv {
					t.Fatalf("watch from %d replayed %s", rv, short(ev))
				}
				switch ev.Type {
				case watch.Added, watch.Modified:
					st[d.Name] = *d
				case watch.Deleted:
					delete(st, d.Name)
				}
				rv = evRV
				events++
			case <-time.After(100 * time.Millisecond):
				break take
			}
		}
		w.Stop()
	}

	if events != int(rv-start) {
		t.Errorf("got %d events for index steps %d..%d, want %d: the watch skipped or merged a change",
			events, start+1, rv, rv-start)
	}
	fresh := mustList(t, east, listOpts())
	if got := listRV(t, fresh); got != rv {
		t.Errorf("fresh list resourceVersion = %d, reflector reached %d", got, rv)
	}
	if want := stateOf(fresh); st.String() != want.String() {
		t.Errorf("reflector cache differs from a fresh list\ncache: %s\nlist:  %s", st, want)
	}
	if writes.Load() == 0 || resumes < 10 {
		t.Fatalf("weak run: %d writes, %d resumes", writes.Load(), resumes)
	}
	t.Logf("%d writes, %d events, %d resumes", writes.Load(), events, resumes)
}
