package storage

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	apistorage "k8s.io/apiserver/pkg/storage"
)

// writeResult is one successful write and the object it returned.
type writeResult struct {
	op   string // create, update, or delete
	name string
	// wrote is the model the write stored. For a delete it is the model
	// of the deleted version.
	wrote string
	uid   types.UID
	out   v1alpha1.Device
}

// versionAt returns the object that the index names for name at index
// sequence rv, and whether the index names it there.
func versionAt(t *testing.T, s *Store, name string, rv uint64) (*v1alpha1.Device, bool) {
	t.Helper()
	ctx := context.Background()
	chain, err := s.kv.History(ctx, IndexKey(s.resource))
	if err != nil {
		t.Fatal(err)
	}
	entries, ok, err := indexAt(chain, rv)
	if err != nil || !ok {
		t.Fatalf("index at %d: ok=%v err=%v", rv, ok, err)
	}
	e, named := entries[name]
	if !named {
		return nil, false
	}
	obj, err := s.objectAt(ctx, name, e, &v1alpha1.Device{})
	if err != nil {
		t.Fatal(err)
	}
	return obj.(*v1alpha1.Device), true
}

// TestConcurrentWritersGetTheirOwnVersion is the adversarial run from the
// review of issue #150. Four writers on two clusters create, update, and
// delete two names for three seconds. Each create gives the object a new
// UID. One watcher runs all the time.
//
// Every write response must carry the version that the write stored, at
// the index sequence where the index recorded it. A delete response must
// carry the index sequence that removed the name. No MODIFIED event may
// change the UID of an object: a delete and a create are two events.
func TestConcurrentWritersGetTheirOwnVersion(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A create or an update MUST return the object version that it wrote, with the index sequence at which the index register recorded that version.
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A delete MUST return the index sequence at which the index register removed the name.
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A create MUST NOT write over a tombstone while the index register still names the deleted object.
	//= docs/spec/fleet.md#4-list-and-watch
	//= type=test
	//# A watch MUST report the deletion of an object and the creation of a new object with the same name as separate events.
	ctx := context.Background()
	cask := newCask(t)
	east := fastWatch(newStoreOn(t, cask(1)))
	west := fastWatch(newStoreOn(t, cask(2)))
	// The seed makes the list resourceVersion 1. The drain below resumes a
	// watch from the last resourceVersion it saw. Resuming from "0" means
	// "from now" and skips every step before it, so the drain then waited
	// for steps that never came (issue #172).
	mustCreate(t, east, device("seed", "m0"))
	start := listRV(t, mustList(t, east, listOpts()))
	if start == 0 {
		t.Fatal("list resourceVersion is 0 after the seed")
	}
	w := mustWatch(t, east, keyPrefix, watchFrom(start))

	var (
		mu      sync.Mutex
		results []writeResult
		serial  atomic.Int64
		wg      sync.WaitGroup
	)
	stop := time.After(3 * time.Second)
	done := make(chan struct{})
	for i, s := range []*Store{east, west, east, west} {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(i+1), 99))
			for {
				select {
				case <-done:
					return
				default:
				}
				name := fmt.Sprintf("dev-%d", r.IntN(2))
				key := keyPrefix + name
				n := serial.Add(1)
				out := &v1alpha1.Device{}
				var res writeResult
				var err error
				switch r.IntN(3) {
				case 0:
					d := device(name, fmt.Sprintf("c%d", n))
					d.UID = types.UID(fmt.Sprintf("uid-%d", n))
					err = s.Create(ctx, key, d, out, 0)
					res = writeResult{op: "create", name: name, wrote: d.Spec.Model, uid: d.UID}
				case 1:
					err = s.Delete(ctx, key, out, nil, nil, nil, apistorage.DeleteOptions{})
					res = writeResult{op: "delete", name: name, wrote: out.Spec.Model, uid: out.UID}
				default:
					model := fmt.Sprintf("u%d", n)
					var uid types.UID
					err = s.GuaranteedUpdate(ctx, key, out, false, nil,
						mutate(func(d *v1alpha1.Device) { d.Spec.Model = model; uid = d.UID }), nil)
					res = writeResult{op: "update", name: name, wrote: model, uid: uid}
				}
				if err != nil {
					// Exists, not found, and lost rounds are expected.
					continue
				}
				if res.op == "delete" {
					res.wrote, res.uid = out.Spec.Model, out.UID
				}
				res.out = *out
				mu.Lock()
				results = append(results, res)
				mu.Unlock()
			}
		})
	}
	<-stop
	close(done)
	wg.Wait()

	for _, res := range results {
		rv := rvOf(t, &res.out)
		switch res.op {
		case "create", "update":
			if res.out.Spec.Model != res.wrote || res.out.UID != res.uid {
				t.Errorf("%s of %s wrote %s/%s but the response carries %s/%s@%d",
					res.op, res.name, res.wrote, res.uid, res.out.Spec.Model, res.out.UID, rv)
				continue
			}
			got, ok := versionAt(t, east, res.name, rv)
			if !ok || got.Spec.Model != res.wrote || got.UID != res.uid {
				t.Errorf("%s of %s wrote %s, but the index at its resourceVersion %d holds %v", res.op, res.name, res.wrote, rv, got)
			}
		case "delete":
			if _, named := versionAt(t, east, res.name, rv); named {
				t.Errorf("delete of %s returned %d, where the index still names it", res.name, rv)
				continue
			}
			prev, ok := versionAt(t, east, res.name, rv-1)
			if !ok || prev.UID != res.uid || prev.Spec.Model != res.wrote {
				t.Errorf("delete of %s/%s returned %d, which is not the step that removed it (before: %v)",
					res.name, res.wrote, rv, prev)
			}
		}
	}

	// Drain the watch to the end of the index, and check that no MODIFIED
	// event changes a UID.
	end := mustIndex(t, east.kv, "devices").Seq
	cache := map[string]types.UID{}
	rv, events := start, 0
	for rv < end {
		ev := next(t, w)
		d, ok := ev.Object.(*v1alpha1.Device)
		if !ok {
			// A watch read can lose its rounds to the writers (issue
			// #149). Resume from the last event, as a reflector does.
			if st, isStatus := ev.Object.(*metav1.Status); isStatus && strings.Contains(st.Message, "preempted") {
				w = mustWatch(t, east, keyPrefix, watchFrom(rv))
				continue
			}
			t.Fatalf("watch ended with %s: %+v", short(ev), ev.Object)
		}
		switch ev.Type {
		case watch.Added:
			cache[d.Name] = d.UID
		case watch.Modified:
			if cache[d.Name] != d.UID {
				t.Errorf("MODIFIED %s@%s changed the UID from %s to %s: a delete and a create became one event",
					d.Name, d.ResourceVersion, cache[d.Name], d.UID)
			}
			cache[d.Name] = d.UID
		case watch.Deleted:
			delete(cache, d.Name)
		default:
			t.Fatalf("unexpected event %s", short(ev))
		}
		rv = rvOf(t, d)
		events++
	}
	if len(results) < 50 {
		t.Fatalf("weak run: %d writes", len(results))
	}
	t.Logf("%d writes, %d events", len(results), events)
}

// A delete whose index write was lost leaves the index naming the deleted
// object. A create of the same name, or an update that creates it, must
// first record the removal. The watch then sends DELETED and ADDED, not
// one MODIFIED event with a new UID.
func TestCreateAfterLostDeleteIndexWrite(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A create MUST NOT write over a tombstone while the index register still names the deleted object.
	//= docs/spec/fleet.md#4-list-and-watch
	//= type=test
	//# A watch MUST report the deletion of an object and the creation of a new object with the same name as separate events.
	ctx := context.Background()
	for _, via := range []string{"create", "update"} {
		t.Run(via, func(t *testing.T) {
			s := fastWatch(newTestStore(t))
			first := device("gpu-0", "a100")
			first.UID = "uid-1"
			created := mustCreate(t, s, first)
			w := mustWatch(t, s, keyPrefix, watchFrom(rvOf(t, created)))

			// The delete tombstones the object and then loses its index
			// write.
			e, _ := indexEntry(t, s, "gpu-0")
			if _, err := s.kv.DeleteSeq(ctx, ObjectKey("devices", "gpu-0"), e.Obj); err != nil {
				t.Fatal(err)
			}

			second := device("gpu-0", "h100")
			second.UID = "uid-2"
			out := &v1alpha1.Device{}
			var err error
			if via == "create" {
				err = s.Create(ctx, keyPrefix+"gpu-0", second, out, 0)
			} else {
				err = s.GuaranteedUpdate(ctx, keyPrefix+"gpu-0", out, true, nil,
					func(runtime.Object, apistorage.ResponseMeta) (runtime.Object, *uint64, error) {
						return second.DeepCopy(), nil, nil
					}, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			if out.UID != "uid-2" || rvOf(t, out) != 3 {
				t.Fatalf("%s returned %s@%s, want uid-2@3", via, out.UID, out.ResourceVersion)
			}
			for _, want := range []string{"DELETED gpu-0@2", "ADDED gpu-0@3"} {
				ev := next(t, w)
				if got := short(ev); got != want {
					t.Fatalf("event %q, want %q", got, want)
				}
			}
		})
	}
}
