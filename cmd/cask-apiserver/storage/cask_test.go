package storage

import (
	"context"
	"strconv"
	"testing"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	apistorage "k8s.io/apiserver/pkg/storage"
)

// newTestStore returns a Store for devices over an in-process cask.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	codec := serializer.NewCodecFactory(scheme).LegacyCodec(v1alpha1.SchemeGroupVersion)
	return New(newKV(t), codec, "devices")
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

func TestGetReturnsObjectAtRegisterSequence(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# An object's resourceVersion MUST be the sequence of its object register.
	ctx := context.Background()
	s := newTestStore(t)

	putRaw(t, s, device("gpu-0", "a100"))
	seq := putRaw(t, s, device("gpu-0", "h100"))

	got := &v1alpha1.Device{}
	if err := s.Get(ctx, keyPrefix+"gpu-0", apistorage.GetOptions{}, got); err != nil {
		t.Fatal(err)
	}
	if rvOf(t, got) != seq {
		t.Fatalf("resourceVersion = %s, want register sequence %d", got.ResourceVersion, seq)
	}
	if got.Name != "gpu-0" || got.Spec.Model != "h100" {
		t.Fatalf("got %s/%s, want gpu-0/h100", got.Name, got.Spec.Model)
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
