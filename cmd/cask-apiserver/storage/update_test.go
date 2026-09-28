package storage

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	apistorage "k8s.io/apiserver/pkg/storage"
)

// mutate returns an UpdateFunc that applies f to the device it is given.
func mutate(f func(d *v1alpha1.Device)) apistorage.UpdateFunc {
	return func(in runtime.Object, _ apistorage.ResponseMeta) (runtime.Object, *uint64, error) {
		d := in.(*v1alpha1.Device)
		if d.Spec.Attributes == nil {
			d.Spec.Attributes = map[string]string{}
		}
		f(d)
		return d, nil, nil
	}
}

func mustCreate(t *testing.T, s *Store, d *v1alpha1.Device) *v1alpha1.Device {
	t.Helper()
	out := &v1alpha1.Device{}
	if err := s.Create(context.Background(), keyPrefix+d.Name, d, out, 0); err != nil {
		t.Fatal(err)
	}
	return out
}

func mustGet(t *testing.T, s *Store, name string) *v1alpha1.Device {
	t.Helper()
	out := &v1alpha1.Device{}
	if err := s.Get(context.Background(), keyPrefix+name, apistorage.GetOptions{}, out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestGuaranteedUpdateConcurrentUpdatersBothApply(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# An update MUST use a compare-and-set on the resourceVersion the client supplied.
	ctx := context.Background()
	cask := newCask(t)
	east, west := newStoreOn(t, cask(1)), newStoreOn(t, cask(2))
	mustCreate(t, east, device("gpu-0", "a100"))

	// Each updater adds one to a shared counter. A lost update would
	// leave the counter short.
	const rounds = 5
	var wg sync.WaitGroup
	errs := make(chan error, 2*rounds)
	for _, s := range []*Store{east, west} {
		wg.Go(func() {
			for range rounds {
				err := s.GuaranteedUpdate(ctx, keyPrefix+"gpu-0", &v1alpha1.Device{}, false, nil,
					mutate(func(d *v1alpha1.Device) {
						n, _ := strconv.Atoi(d.Spec.Attributes["n"])
						d.Spec.Attributes["n"] = strconv.Itoa(n + 1)
					}), nil)
				if err != nil {
					errs <- err
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	got := mustGet(t, west, "gpu-0")
	if got.Spec.Attributes["n"] != strconv.Itoa(2*rounds) {
		t.Fatalf("counter = %s, want %d: an update was lost", got.Spec.Attributes["n"], 2*rounds)
	}
	seq, _, err := objectHead(ctx, east.kv, "devices", "gpu-0")
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1+2*rounds {
		t.Fatalf("object sequence = %d, want %d", seq, 1+2*rounds)
	}
	if e, _ := indexEntry(t, east, "gpu-0"); e.Obj != seq || rvOf(t, got) != e.Idx {
		t.Fatalf("index entry = %+v, resourceVersion = %s, want the entry to record %d at the resourceVersion",
			e, got.ResourceVersion, seq)
	}
}

func TestGuaranteedUpdateRereadsAfterConflict(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# The extension server MUST re-read an object before it retries a write that returned a conflict.
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A retried write MUST be a compare-and-set, never a blind reapplication of a change.
	ctx := context.Background()
	cask := newCask(t)
	east, west := newStoreOn(t, cask(1)), newStoreOn(t, cask(2))
	mustCreate(t, east, device("gpu-0", "a100"))

	var seen []uint64
	dest := &v1alpha1.Device{}
	err := east.GuaranteedUpdate(ctx, keyPrefix+"gpu-0", dest, false, nil,
		func(in runtime.Object, res apistorage.ResponseMeta) (runtime.Object, *uint64, error) {
			seen = append(seen, res.ResourceVersion)
			if len(seen) == 1 {
				// West writes after east's read and before east's write.
				if err := west.GuaranteedUpdate(ctx, keyPrefix+"gpu-0", &v1alpha1.Device{}, false, nil,
					mutate(func(d *v1alpha1.Device) { d.Spec.Attributes["west"] = "yes" }), nil); err != nil {
					return nil, nil, err
				}
			}
			d := in.(*v1alpha1.Device)
			d.Spec.Zone = "eu-west"
			return d, nil, nil
		}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || seen[0] != 1 || seen[1] != 2 {
		t.Fatalf("tryUpdate saw resourceVersions %v, want [1 2]", seen)
	}
	got := mustGet(t, west, "gpu-0")
	if got.Spec.Zone != "eu-west" || got.Spec.Attributes["west"] != "yes" {
		t.Fatalf("got zone %q attributes %v, want both writes", got.Spec.Zone, got.Spec.Attributes)
	}
	if rvOf(t, dest) != 3 || rvOf(t, got) != 3 {
		t.Fatalf("destination rv %s, stored rv %s, want 3", dest.ResourceVersion, got.ResourceVersion)
	}
}

func TestGuaranteedUpdatePreconditions(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# An update whose compare-and-set fails MUST return a conflict.
	ctx := context.Background()
	s := newTestStore(t)
	d := device("gpu-0", "a100")
	d.UID = types.UID("uid-1")
	mustCreate(t, s, d)
	mustUpdate := mutate(func(d *v1alpha1.Device) { d.Spec.Model = "h100" })

	otherUID := types.UID("uid-2")
	err := s.GuaranteedUpdate(ctx, keyPrefix+"gpu-0", &v1alpha1.Device{}, false,
		&apistorage.Preconditions{UID: &otherUID}, mustUpdate, nil)
	if !apistorage.IsInvalidObj(err) {
		t.Fatalf("UID precondition: err = %v, want invalid object", err)
	}

	// Move the object to sequence 2, then update with a precondition on
	// sequence 1.
	if err := s.GuaranteedUpdate(ctx, keyPrefix+"gpu-0", &v1alpha1.Device{}, false, nil,
		mutate(func(d *v1alpha1.Device) { d.Spec.Zone = "a" }), nil); err != nil {
		t.Fatal(err)
	}
	stale := "1"
	err = s.GuaranteedUpdate(ctx, keyPrefix+"gpu-0", &v1alpha1.Device{}, false,
		&apistorage.Preconditions{ResourceVersion: &stale}, mustUpdate, nil)
	if !apistorage.IsInvalidObj(err) {
		t.Fatalf("stale resourceVersion precondition: err = %v, want invalid object", err)
	}
	if got := mustGet(t, s, "gpu-0"); got.Spec.Model != "a100" || rvOf(t, got) != 2 {
		t.Fatalf("a failed precondition changed the object: %s rv %s", got.Spec.Model, got.ResourceVersion)
	}

	current := "2"
	uid := types.UID("uid-1")
	dest := &v1alpha1.Device{}
	if err := s.GuaranteedUpdate(ctx, keyPrefix+"gpu-0", dest, false,
		&apistorage.Preconditions{UID: &uid, ResourceVersion: &current}, mustUpdate, nil); err != nil {
		t.Fatal(err)
	}
	if dest.Spec.Model != "h100" || rvOf(t, dest) != 3 {
		t.Fatalf("destination = %s rv %s, want h100 rv 3", dest.Spec.Model, dest.ResourceVersion)
	}
}

func TestGuaranteedUpdateNotFound(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	setModel := mutate(func(d *v1alpha1.Device) { d.Name = "gpu-0"; d.Spec.Model = "a100" })

	err := s.GuaranteedUpdate(ctx, keyPrefix+"gpu-0", &v1alpha1.Device{}, false, nil, setModel, nil)
	if !apistorage.IsNotFound(err) {
		t.Fatalf("err = %v, want not found", err)
	}
	if _, ok := indexEntry(t, s, "gpu-0"); ok {
		t.Fatal("a failed update reached the index")
	}

	// With ignoreNotFound, the update creates the object.
	dest := &v1alpha1.Device{}
	if err := s.GuaranteedUpdate(ctx, keyPrefix+"gpu-0", dest, true, nil, setModel, nil); err != nil {
		t.Fatal(err)
	}
	if dest.Spec.Model != "a100" || rvOf(t, dest) != 1 {
		t.Fatalf("destination = %s rv %s, want a100 rv 1", dest.Spec.Model, dest.ResourceVersion)
	}
	if e, _ := indexEntry(t, s, "gpu-0"); e != (Entry{Obj: 1, Idx: 1}) {
		t.Fatalf("index entry = %+v, want {1 1}", e)
	}
}

func TestGuaranteedUpdateNoChangeSkipsWrite(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	mustCreate(t, s, device("gpu-0", "a100"))

	dest := &v1alpha1.Device{}
	noop := func(in runtime.Object, _ apistorage.ResponseMeta) (runtime.Object, *uint64, error) {
		return in, nil, nil
	}
	if err := s.GuaranteedUpdate(ctx, keyPrefix+"gpu-0", dest, false, nil, noop, nil); err != nil {
		t.Fatal(err)
	}
	if rvOf(t, dest) != 1 || dest.Spec.Model != "a100" {
		t.Fatalf("destination = %s rv %s, want a100 rv 1", dest.Spec.Model, dest.ResourceVersion)
	}
	if seq, _, _ := objectHead(ctx, s.kv, "devices", "gpu-0"); seq != 1 {
		t.Fatalf("object sequence = %d after a no-op update, want 1", seq)
	}
}

func TestPreconditionUsesIndexSequence(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A resourceVersion precondition MUST be checked against the index sequence of the object's index entry.
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# When the index write of a mutation did not complete, the next index write for that object MUST record the object register's current sequence.
	ctx := context.Background()
	s := newTestStore(t)
	// gpu-1 takes index sequence 1. gpu-0 is at object sequence 1 and
	// index sequence 2.
	mustCreate(t, s, device("gpu-1", "a100"))
	mustCreate(t, s, device("gpu-0", "a100"))
	setModel := mutate(func(d *v1alpha1.Device) { d.Spec.Model = "h100" })

	// The object sequence is not a resourceVersion.
	objSeq := "1"
	err := s.GuaranteedUpdate(ctx, keyPrefix+"gpu-0", &v1alpha1.Device{}, false,
		&apistorage.Preconditions{ResourceVersion: &objSeq}, setModel, nil)
	if !apistorage.IsInvalidObj(err) {
		t.Fatalf("precondition on the object sequence: err = %v, want invalid object", err)
	}
	err = s.Delete(ctx, keyPrefix+"gpu-0", &v1alpha1.Device{}, &apistorage.Preconditions{ResourceVersion: &objSeq},
		nil, nil, apistorage.DeleteOptions{})
	if !apistorage.IsInvalidObj(err) {
		t.Fatalf("delete precondition on the object sequence: err = %v, want invalid object", err)
	}
	idxSeq := "2"
	dest := &v1alpha1.Device{}
	if err := s.GuaranteedUpdate(ctx, keyPrefix+"gpu-0", dest, false,
		&apistorage.Preconditions{ResourceVersion: &idxSeq}, setModel, nil); err != nil {
		t.Fatalf("precondition on the index sequence: %v", err)
	}
	if rvOf(t, dest) != 3 {
		t.Fatalf("destination rv %s, want index sequence 3", dest.ResourceVersion)
	}

	// An object write whose index write did not complete. The update
	// first sees the indexed version, fails its compare-and-set, catches
	// the index up, and then runs on the fresh copy.
	putRaw(t, s, device("gpu-0", "b200"))
	var seen []uint64
	dest = &v1alpha1.Device{}
	err = s.GuaranteedUpdate(ctx, keyPrefix+"gpu-0", dest, false, nil,
		func(in runtime.Object, res apistorage.ResponseMeta) (runtime.Object, *uint64, error) {
			seen = append(seen, res.ResourceVersion)
			d := in.(*v1alpha1.Device)
			d.Spec.Zone = d.Spec.Model
			return d, nil, nil
		}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || seen[0] != 3 || seen[1] != 4 {
		t.Fatalf("tryUpdate saw resourceVersions %v, want [3 4]", seen)
	}
	if dest.Spec.Zone != "b200" || rvOf(t, dest) != 5 {
		t.Fatalf("destination = zone %s rv %s, want b200 at 5", dest.Spec.Zone, dest.ResourceVersion)
	}

	// A tombstone whose index write did not complete. Get still serves
	// the indexed version. Delete fails its compare-and-set, catches the
	// index up, and then finds nothing to delete.
	if _, err := s.kv.Delete(ctx, ObjectKey("devices", "gpu-0")); err != nil {
		t.Fatal(err)
	}
	if got := mustGet(t, s, "gpu-0"); rvOf(t, got) != 5 {
		t.Fatalf("get before repair: rv %s, want the indexed version at 5", got.ResourceVersion)
	}
	err = s.Delete(ctx, keyPrefix+"gpu-0", &v1alpha1.Device{}, nil, nil, nil, apistorage.DeleteOptions{})
	if !apistorage.IsNotFound(err) {
		t.Fatalf("delete over an unindexed tombstone: err = %v, want not found", err)
	}
	if _, ok := indexEntry(t, s, "gpu-0"); ok {
		t.Fatal("index still names the tombstoned object")
	}
}

// An update lands at object sequence 2. Before its index write runs, a
// repair records 2 and a second writer overwrites it at 3 and deletes it
// at 4. The first update must still answer with its own version at the
// index sequence that recorded 2, and a delete of 3 must answer with the
// step that removed it.
func TestWriteResponseIsOwnVersionAfterOverwrite(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A create or an update MUST return the object version that it wrote, with the index sequence at which the index register recorded that version.
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A delete MUST return the index sequence at which the index register removed the name.
	ctx := context.Background()
	s := newTestStore(t)
	mustCreate(t, s, device("gpu-0", "a100")) // object 1, index 1
	reg := ObjectKey("devices", "gpu-0")
	put := func(seq uint64, model string) []byte {
		t.Helper()
		raw, err := s.encode(device("gpu-0", model))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.kv.CASSeq(ctx, reg, seq, raw); err != nil {
			t.Fatal(err)
		}
		return raw
	}
	mine := put(1, "mine")                              // object 2
	if err := s.repairIndex(ctx, "gpu-0"); err != nil { // index 2 records object 2
		t.Fatal(err)
	}
	put(2, "theirs")                     // object 3
	w, err := s.writeIndex(ctx, "gpu-0") // index 3 records object 3
	if err != nil {
		t.Fatal(err)
	}
	out := &v1alpha1.Device{}
	if err := s.written(ctx, keyPrefix+"gpu-0", "gpu-0", 2, mine, w, out); err != nil {
		t.Fatal(err)
	}
	if out.Spec.Model != "mine" || rvOf(t, out) != 2 {
		t.Fatalf("response = %s@%s, want the update's own version mine@2", out.Spec.Model, out.ResourceVersion)
	}

	// A delete of object 3 whose own index write comes after another
	// index write removed the name, and after a new object was created.
	if _, err := s.kv.DeleteSeq(ctx, reg, 3); err != nil { // object 4
		t.Fatal(err)
	}
	if err := s.repairIndex(ctx, "gpu-0"); err != nil { // index 4 removes it
		t.Fatal(err)
	}
	mustCreate(t, s, device("gpu-0", "next")) // object 5, index 5
	rv, err := RemovedAt(ctx, s.kv, s.resource, "gpu-0", 3)
	if err != nil || rv != 4 {
		t.Fatalf("RemovedAt(3) = %d, %v, want 4", rv, err)
	}
	if rv, err := RecordedAt(ctx, s.kv, s.resource, "gpu-0", 3); err != nil || rv != 3 {
		t.Fatalf("RecordedAt(3) = %d, %v, want 3", rv, err)
	}
	if _, err := RecordedAt(ctx, s.kv, s.resource, "gpu-0", 4); err == nil {
		t.Fatal("RecordedAt(4) found a tombstone")
	}
}
