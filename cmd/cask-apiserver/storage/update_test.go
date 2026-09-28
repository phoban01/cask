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
	if rvOf(t, got) != seq || seq != 1+2*rounds {
		t.Fatalf("resourceVersion = %s, object sequence = %d, want both %d", got.ResourceVersion, seq, 1+2*rounds)
	}
	if entry, _ := indexEntry(t, east, "gpu-0"); entry != seq {
		t.Fatalf("index entry = %d, want %d", entry, seq)
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
	if seq, _ := indexEntry(t, s, "gpu-0"); seq != 1 {
		t.Fatalf("index entry = %d, want 1", seq)
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
