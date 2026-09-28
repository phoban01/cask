package storage

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
	"github.com/phoban01/cask/internal/mvcc"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	apistorage "k8s.io/apiserver/pkg/storage"
)

// newTestStore returns a Store for devices over a new in-process cask.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	return newStoreOn(t, newKV(t))
}

// newStoreOn returns a Store for devices over kv.
func newStoreOn(t *testing.T, kv *mvcc.KV) *Store {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	codec := serializer.NewCodecFactory(scheme).LegacyCodec(v1alpha1.SchemeGroupVersion)
	return New(kv, codec, "devices", func() runtime.Object { return &v1alpha1.Device{} })
}

const keyPrefix = "/fleet.cask.dev/devices/"

func device(name, model string) *v1alpha1.Device {
	return &v1alpha1.Device{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1alpha1.DeviceSpec{Model: model},
	}
}

// putRaw writes obj straight to its object register, with no index write,
// and returns the register sequence.
func putRaw(t *testing.T, s *Store, obj *v1alpha1.Device) uint64 {
	t.Helper()
	raw, err := runtime.Encode(s.codec, obj)
	if err != nil {
		t.Fatal(err)
	}
	v, err := s.kv.Put(context.Background(), ObjectKey(s.resource, obj.Name), raw)
	if err != nil {
		t.Fatal(err)
	}
	return v.Seq
}

func rvOf(t *testing.T, obj *v1alpha1.Device) uint64 {
	t.Helper()
	rv, err := strconv.ParseUint(obj.ResourceVersion, 10, 64)
	if err != nil {
		t.Fatalf("resourceVersion %q: %v", obj.ResourceVersion, err)
	}
	return rv
}

func TestGetReturnsIndexedVersionAtIndexSequence(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# An object's resourceVersion MUST be the index sequence at which the index register recorded that object version.
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A get MUST serve an object only once the index register records it.
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A get MUST read the object at the object register sequence that the index entry records.
	//= docs/spec/fleet.md#4-list-and-watch
	//= type=test
	//# Every resourceVersion that a get, a list, or a watch event reports MUST be an index sequence.
	ctx := context.Background()
	s := newTestStore(t)

	putRaw(t, s, device("gpu-1", "a100"))
	putRaw(t, s, device("gpu-0", "a100"))
	seq := putRaw(t, s, device("gpu-0", "h100"))

	// No index write yet: the object is not served.
	if err := s.Get(ctx, keyPrefix+"gpu-0", apistorage.GetOptions{}, &v1alpha1.Device{}); !apistorage.IsNotFound(err) {
		t.Fatalf("unindexed get: err = %v, want not found", err)
	}

	// gpu-1 goes in at index sequence 1, gpu-0 at 2.
	if _, _, err := WriteIndex(ctx, s.kv, s.resource, "gpu-1"); err != nil {
		t.Fatal(err)
	}
	e, live, err := WriteIndex(ctx, s.kv, s.resource, "gpu-0")
	if err != nil || !live || e != (Entry{Obj: seq, Idx: 2}) {
		t.Fatalf("WriteIndex = %+v live=%v err=%v, want {Obj:%d Idx:2}", e, live, err, seq)
	}

	// A later object write that the index does not record is not served.
	putRaw(t, s, device("gpu-0", "b200"))

	got := &v1alpha1.Device{}
	if err := s.Get(ctx, keyPrefix+"gpu-0", apistorage.GetOptions{}, got); err != nil {
		t.Fatal(err)
	}
	if rvOf(t, got) != 2 {
		t.Fatalf("resourceVersion = %s, want index sequence 2, not object sequence %d", got.ResourceVersion, seq)
	}
	if got.Name != "gpu-0" || got.Spec.Model != "h100" {
		t.Fatalf("got %s/%s, want the indexed version gpu-0/h100", got.Name, got.Spec.Model)
	}
}

func TestGetNotFound(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	err := s.Get(ctx, keyPrefix+"missing", apistorage.GetOptions{}, &v1alpha1.Device{})
	if !apistorage.IsNotFound(err) {
		t.Fatalf("err = %v, want not found", err)
	}

	got := device("stale", "x")
	if err := s.Get(ctx, keyPrefix+"missing", apistorage.GetOptions{IgnoreNotFound: true}, got); err != nil {
		t.Fatalf("IgnoreNotFound: %v", err)
	}
	if !equality.Semantic.DeepEqual(got, &v1alpha1.Device{}) {
		t.Fatalf("IgnoreNotFound left %+v, want the zero object", got)
	}

	// A tombstoned register reads as not found.
	putRaw(t, s, device("gpu-0", "a100"))
	if _, err := s.kv.Delete(ctx, ObjectKey("devices", "gpu-0")); err != nil {
		t.Fatal(err)
	}
	err = s.Get(ctx, keyPrefix+"gpu-0", apistorage.GetOptions{}, &v1alpha1.Device{})
	if !apistorage.IsNotFound(err) {
		t.Fatalf("after tombstone: err = %v, want not found", err)
	}
}

// indexEntry returns the entry the index holds for name, and whether the
// index names it.
func indexEntry(t *testing.T, s *Store, name string) (Entry, bool) {
	t.Helper()
	idx, err := ReadIndex(context.Background(), s.kv, s.resource)
	if err != nil {
		t.Fatal(err)
	}
	e, ok := idx.Entries[name]
	return e, ok
}

func TestCreateIsCreateOnly(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A create MUST use a compare-and-set that requires the object register to be absent.
	ctx := context.Background()
	s := newTestStore(t)

	out := &v1alpha1.Device{}
	if err := s.Create(ctx, keyPrefix+"gpu-0", device("gpu-0", "a100"), out, 0); err != nil {
		t.Fatal(err)
	}
	if out.Name != "gpu-0" || out.Spec.Model != "a100" || rvOf(t, out) != 1 {
		t.Fatalf("out = %s/%s rv %s, want gpu-0/a100 rv 1", out.Name, out.Spec.Model, out.ResourceVersion)
	}

	err := s.Create(ctx, keyPrefix+"gpu-0", device("gpu-0", "h100"), &v1alpha1.Device{}, 0)
	if !apistorage.IsExist(err) {
		t.Fatalf("second create: err = %v, want key exists", err)
	}
	got := &v1alpha1.Device{}
	if err := s.Get(ctx, keyPrefix+"gpu-0", apistorage.GetOptions{}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Model != "a100" || rvOf(t, got) != 1 {
		t.Fatalf("second create changed the object: %s rv %s", got.Spec.Model, got.ResourceVersion)
	}
}

func TestCreateWritesObjectThenIndex(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A mutation MUST write the object register before the index register.
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# Each index entry MUST record both the object register sequence and the index sequence at which the index register recorded it.
	ctx := context.Background()
	s := newTestStore(t)

	out := &v1alpha1.Device{}
	if err := s.Create(ctx, keyPrefix+"gpu-0", device("gpu-0", "a100"), out, 0); err != nil {
		t.Fatal(err)
	}
	if e, ok := indexEntry(t, s, "gpu-0"); !ok || e != (Entry{Obj: 1, Idx: rvOf(t, out)}) {
		t.Fatalf("index entry = %+v (named %v), want {Obj:1 Idx:%s}", e, ok, out.ResourceVersion)
	}

	// A create over a tombstone succeeds at a higher object sequence. The
	// tombstone never reached the index, so the create is the second
	// index write.
	if _, err := s.kv.Delete(ctx, ObjectKey("devices", "gpu-0")); err != nil {
		t.Fatal(err)
	}
	again := &v1alpha1.Device{}
	if err := s.Create(ctx, keyPrefix+"gpu-0", device("gpu-0", "h100"), again, 0); err != nil {
		t.Fatalf("create over tombstone: %v", err)
	}
	if rvOf(t, again) != 2 {
		t.Fatalf("resourceVersion = %s, want index sequence 2", again.ResourceVersion)
	}
	if e, _ := indexEntry(t, s, "gpu-0"); e != (Entry{Obj: 3, Idx: 2}) {
		t.Fatalf("index entry = %+v, want {Obj:3 Idx:2} (create, tombstone, create)", e)
	}
}

func TestCreateRejectsResourceVersionAndTTL(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	withRV := device("gpu-0", "a100")
	withRV.ResourceVersion = "5"
	if err := s.Create(ctx, keyPrefix+"gpu-0", withRV, nil, 0); !errors.Is(err, apistorage.ErrResourceVersionSetOnCreate) {
		t.Fatalf("err = %v, want ErrResourceVersionSetOnCreate", err)
	}
	if err := s.Create(ctx, keyPrefix+"gpu-0", device("gpu-0", "a100"), nil, 30); err == nil {
		t.Fatal("create with a TTL must fail")
	}
	if _, ok := indexEntry(t, s, "gpu-0"); ok {
		t.Fatal("a rejected create reached the index")
	}
}

func TestObjectKeyUsesResourceAndName(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# Each object MUST be stored in one cask register keyed by resource type and name.
	s := newTestStore(t)
	reg, name, err := s.objectKey(keyPrefix + "gpu-0")
	if err != nil {
		t.Fatal(err)
	}
	if string(reg) != "fleet/devices/gpu-0" || name != "gpu-0" {
		t.Fatalf("objectKey = %q, %q", reg, name)
	}
	if _, _, err := s.objectKey(keyPrefix); err == nil {
		t.Fatal("a key with no name must fail")
	}
}
