package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	apistorage "k8s.io/apiserver/pkg/storage"
)

func TestDeleteTombstonesThenRemovesIndexEntry(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A delete MUST tombstone the object register before it removes the name from the index register.
	ctx := context.Background()
	s := newTestStore(t)
	mustCreate(t, s, device("gpu-0", "a100"))
	mustCreate(t, s, device("gpu-1", "h100"))

	out := &v1alpha1.Device{}
	if err := s.Delete(ctx, keyPrefix+"gpu-0", out, nil, apistorage.ValidateAllObjectFunc, nil, apistorage.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if out.Name != "gpu-0" || out.Spec.Model != "a100" || rvOf(t, out) != 3 {
		t.Fatalf("out = %s/%s rv %s, want the deleted gpu-0/a100 at the removal index sequence 3",
			out.Name, out.Spec.Model, out.ResourceVersion)
	}

	err := s.Get(ctx, keyPrefix+"gpu-0", apistorage.GetOptions{}, &v1alpha1.Device{})
	if !apistorage.IsNotFound(err) {
		t.Fatalf("get after delete: err = %v, want not found", err)
	}
	chain, err := s.kv.History(ctx, ObjectKey("devices", "gpu-0"))
	if err != nil {
		t.Fatal(err)
	}
	if head := chain.Versions[len(chain.Versions)-1]; !head.Tombstone || head.Seq != 2 {
		t.Fatalf("register head = %+v, want a tombstone at sequence 2", head)
	}
	if _, ok := indexEntry(t, s, "gpu-0"); ok {
		t.Fatal("index still names the deleted object")
	}
	if _, ok := indexEntry(t, s, "gpu-1"); !ok {
		t.Fatal("delete removed another object's index entry")
	}

	err = s.Delete(ctx, keyPrefix+"gpu-0", &v1alpha1.Device{}, nil, nil, nil, apistorage.DeleteOptions{})
	if !apistorage.IsNotFound(err) {
		t.Fatalf("second delete: err = %v, want not found", err)
	}
}

func TestDeleteChecksPreconditionsAndValidation(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	d := device("gpu-0", "a100")
	d.UID = types.UID("uid-1")
	mustCreate(t, s, d)

	otherUID := types.UID("uid-2")
	err := s.Delete(ctx, keyPrefix+"gpu-0", &v1alpha1.Device{}, &apistorage.Preconditions{UID: &otherUID},
		nil, nil, apistorage.DeleteOptions{})
	if !apistorage.IsInvalidObj(err) {
		t.Fatalf("UID precondition: err = %v, want invalid object", err)
	}
	stale := "7"
	err = s.Delete(ctx, keyPrefix+"gpu-0", &v1alpha1.Device{}, &apistorage.Preconditions{ResourceVersion: &stale},
		nil, nil, apistorage.DeleteOptions{})
	if !apistorage.IsInvalidObj(err) {
		t.Fatalf("resourceVersion precondition: err = %v, want invalid object", err)
	}
	refuse := errors.New("refused")
	err = s.Delete(ctx, keyPrefix+"gpu-0", &v1alpha1.Device{}, nil,
		func(context.Context, runtime.Object) error { return refuse }, nil, apistorage.DeleteOptions{})
	if !errors.Is(err, refuse) {
		t.Fatalf("validateDeletion: err = %v, want %v", err, refuse)
	}
	if got := mustGet(t, s, "gpu-0"); rvOf(t, got) != 1 {
		t.Fatalf("a refused delete changed the object: rv %s", got.ResourceVersion)
	}
	if _, ok := indexEntry(t, s, "gpu-0"); !ok {
		t.Fatal("a refused delete removed the index entry")
	}

	uid := types.UID("uid-1")
	current := "1"
	if err := s.Delete(ctx, keyPrefix+"gpu-0", &v1alpha1.Device{},
		&apistorage.Preconditions{UID: &uid, ResourceVersion: &current}, nil, nil, apistorage.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteRereadsAfterConflict(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# The extension server MUST re-read an object before it retries a write that returned a conflict.
	ctx := context.Background()
	cask := newCask(t)
	east, west := newStoreOn(t, cask(1)), newStoreOn(t, cask(2))
	mustCreate(t, east, device("gpu-0", "a100"))

	var seen []string
	validate := func(_ context.Context, obj runtime.Object) error {
		d := obj.(*v1alpha1.Device)
		seen = append(seen, d.ResourceVersion)
		if len(seen) == 1 {
			// West updates after east's read and before east's tombstone.
			return west.GuaranteedUpdate(ctx, keyPrefix+"gpu-0", &v1alpha1.Device{}, false, nil,
				mutate(func(d *v1alpha1.Device) { d.Spec.Model = "h100" }), nil)
		}
		return nil
	}
	out := &v1alpha1.Device{}
	if err := east.Delete(ctx, keyPrefix+"gpu-0", out, nil, validate, nil, apistorage.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || seen[0] != "1" || seen[1] != "2" {
		t.Fatalf("validateDeletion saw resourceVersions %v, want [1 2]", seen)
	}
	if out.Spec.Model != "h100" || rvOf(t, out) != 3 {
		t.Fatalf("out = %s rv %s, want the fresh h100 at removal index sequence 3", out.Spec.Model, out.ResourceVersion)
	}
	if _, ok := indexEntry(t, west, "gpu-0"); ok {
		t.Fatal("index still names the deleted object")
	}
}
