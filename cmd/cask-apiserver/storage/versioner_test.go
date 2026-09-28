package storage

import (
	"context"
	"testing"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
	apistorage "k8s.io/apiserver/pkg/storage"
)

func TestVersionerParse(t *testing.T) {
	v := newTestStore(t).Versioner()
	if v == nil {
		t.Fatal("Versioner() = nil")
	}
	for _, tc := range []struct {
		in   string
		want uint64
	}{
		{"", 0},
		{"0", 0},
		{"1", 1},
		{"18446744073709551615", 18446744073709551615},
	} {
		got, err := v.ParseResourceVersion(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("ParseResourceVersion(%q) = %d, %v; want %d", tc.in, got, err, tc.want)
		}
	}
	for _, in := range []string{"abc", "-1", "1.5", "0x10", "18446744073709551616"} {
		_, err := v.ParseResourceVersion(in)
		if !apistorage.IsInvalidError(err) {
			t.Errorf("ParseResourceVersion(%q) error = %v; want a storage InvalidError", in, err)
		}
	}
}

func TestVersionerUpdateObjectMatchesRegisterSequence(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# An object's resourceVersion MUST be the sequence of its object register.
	ctx := context.Background()
	s := newTestStore(t)
	v := s.Versioner()

	created := mustCreate(t, s, device("gpu-0", "a100"))
	updated := &v1alpha1.Device{}
	if err := s.GuaranteedUpdate(ctx, keyPrefix+"gpu-0", updated, false, nil,
		mutate(func(d *v1alpha1.Device) { d.Spec.Model = "h100" }), nil); err != nil {
		t.Fatal(err)
	}
	head, ok, err := s.head(ctx, ObjectKey("devices", "gpu-0"))
	if err != nil || !ok {
		t.Fatalf("head: ok=%v err=%v", ok, err)
	}
	cRV, err := v.ObjectResourceVersion(created)
	if err != nil {
		t.Fatal(err)
	}
	uRV, err := v.ObjectResourceVersion(updated)
	if err != nil {
		t.Fatal(err)
	}
	if cRV != 1 || uRV != 2 || uRV != head.Seq {
		t.Fatalf("resourceVersions = %d, %d; register head = %d; want 1, 2, 2", cRV, uRV, head.Seq)
	}

	// UpdateObject writes the sequence as a decimal string, and 0 clears it.
	d := device("gpu-1", "a100")
	if err := v.UpdateObject(d, 7); err != nil || d.ResourceVersion != "7" {
		t.Fatalf("UpdateObject(7): rv=%q err=%v", d.ResourceVersion, err)
	}
	if err := v.UpdateObject(d, 0); err != nil || d.ResourceVersion != "" {
		t.Fatalf("UpdateObject(0): rv=%q err=%v", d.ResourceVersion, err)
	}
	if err := v.PrepareObjectForStorage(created); err != nil || created.ResourceVersion != "" {
		t.Fatalf("PrepareObjectForStorage: rv=%q err=%v", created.ResourceVersion, err)
	}
}

func TestVersionerUpdateList(t *testing.T) {
	v := newTestStore(t).Versioner()
	list := &v1alpha1.DeviceList{}
	if err := v.UpdateList(list, 42, "", nil); err != nil {
		t.Fatal(err)
	}
	if list.ResourceVersion != "42" {
		t.Fatalf("list resourceVersion = %q, want 42", list.ResourceVersion)
	}
	// Sequence 0 never comes from a register write, so it is refused.
	if err := v.UpdateList(list, 0, "", nil); err == nil {
		t.Fatal("UpdateList(0) returned no error")
	}
}
