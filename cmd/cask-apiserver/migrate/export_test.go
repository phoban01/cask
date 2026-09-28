package migrate

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	fakediscovery "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
)

const group = "fleet.cask.dev"

var (
	devicesGVR = schema.GroupVersionResource{Group: group, Version: "v1alpha1", Resource: "devices"}
	claimsGVR  = schema.GroupVersionResource{Group: group, Version: "v1alpha1", Resource: "deviceclaims"}
	resources  = []Resource{
		{GVR: claimsGVR, Kind: "DeviceClaim"},
		{GVR: devicesGVR, Kind: "Device"},
	}
)

func device(name, uid string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "fleet.cask.dev/v1alpha1",
		"kind":       "Device",
		"metadata": map[string]any{
			"name":              name,
			"uid":               uid,
			"resourceVersion":   "1200",
			"generation":        int64(3),
			"creationTimestamp": "2026-01-02T03:04:05Z",
			"labels":            map[string]any{"zone": "eu-west-a"},
			"annotations":       map[string]any{"note": "a <b> & c"},
			"finalizers":        []any{"fleet.cask.dev/lease"},
			"ownerReferences": []any{map[string]any{
				"apiVersion": "fleet.cask.dev/v1alpha1",
				"kind":       "DeviceClaim",
				"name":       "claim-" + name,
				"uid":        "owner-" + uid,
				"controller": true,
			}},
		},
		"spec": map[string]any{
			"model":      "gpu-a",
			"attributes": map[string]any{"mem": "80Gi"},
		},
		"status": map[string]any{
			"phase": "Leased",
			"lease": map[string]any{"cluster": "east", "claim": "claim-" + name, "fence": int64(9007199254740993)},
		},
	}}
}

func claim(name, uid string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "fleet.cask.dev/v1alpha1",
		"kind":       "DeviceClaim",
		"metadata": map[string]any{
			"name":              name,
			"uid":               uid,
			"resourceVersion":   "1210",
			"creationTimestamp": "2026-01-02T03:04:06Z",
		},
		"spec":   map[string]any{"deviceName": "dev-a", "ttlSeconds": int64(30)},
		"status": map[string]any{"phase": "Bound", "cluster": "east", "fence": int64(7)},
	}}
}

func list(rv, cont string, items ...*unstructured.Unstructured) *unstructured.UnstructuredList {
	l := &unstructured.UnstructuredList{Object: map[string]any{
		"apiVersion": "fleet.cask.dev/v1alpha1",
		"kind":       "List",
	}}
	l.SetResourceVersion(rv)
	l.SetContinue(cont)
	for _, o := range items {
		l.Items = append(l.Items, *o.DeepCopy())
	}
	return l
}

// fakeClient serves devices in two pages and claims in one. The pages
// have a different resourceVersion, so a test sees which one Export keeps.
// The fake client drops the continue option, so the reactor counts calls.
func fakeClient(devices, claims []*unstructured.Unstructured) *dynamicfake.FakeDynamicClient {
	c := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{devicesGVR: "DeviceList", claimsGVR: "DeviceClaimList"})
	var calls int
	c.PrependReactor("list", "devices", func(clienttesting.Action) (bool, runtime.Object, error) {
		calls++
		if calls%2 == 1 {
			return true, list("1234", "page-2", devices[:2]...), nil
		}
		return true, list("9999", "", devices[2:]...), nil
	})
	c.PrependReactor("list", "deviceclaims", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, list("1240", "", claims...), nil
	})
	return c
}

func TestExportRoundTripsEveryField(t *testing.T) {
	//= docs/spec/fleet.md#7-migration
	//= type=test
	//# The migration MUST preserve each object's uid.
	//= docs/spec/fleet.md#7-migration
	//= type=test
	//# The migration MUST preserve each object's creationTimestamp.
	// The list returns the devices out of name order.
	devices := []*unstructured.Unstructured{
		device("dev-c", "uid-c"), device("dev-a", "uid-a"), device("dev-b", "uid-b"),
	}
	claims := []*unstructured.Unstructured{claim("claim-a", "uid-claim-a")}
	c := fakeClient(devices, claims)

	var buf bytes.Buffer
	h, err := Export(context.Background(), c, group, resources, &buf)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range c.Actions() {
		if a.GetVerb() != "list" {
			t.Errorf("export sent a %s request; it must only read", a.GetVerb())
		}
	}

	wantHeader := Header{
		Format:   Format,
		Group:    group,
		Revision: 1240,
		Resources: []ResourceHeader{
			{Version: "v1alpha1", Resource: "deviceclaims", Kind: "DeviceClaim", ListResourceVersion: 1240, Count: 1},
			{Version: "v1alpha1", Resource: "devices", Kind: "Device", ListResourceVersion: 1234, Count: 3},
		},
	}
	if !reflect.DeepEqual(h, wantHeader) {
		t.Fatalf("header = %+v, want %+v", h, wantHeader)
	}

	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != 1+4 {
		t.Fatalf("got %d lines, want a header and one line per object:\n%s", len(lines), buf.String())
	}
	for _, l := range lines[1:] {
		if !strings.Contains(l, `"uid":"uid-`) {
			t.Errorf("object line has no uid: %s", l)
		}
	}

	gotHeader, got, err := Read(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotHeader, wantHeader) {
		t.Fatalf("read header = %+v, want %+v", gotHeader, wantHeader)
	}
	want := []*unstructured.Unstructured{claims[0], devices[1], devices[2], devices[0]}
	if len(got) != len(want) {
		t.Fatalf("read %d objects, want %d", len(got), len(want))
	}
	for i := range want {
		if !reflect.DeepEqual(got[i].Object, want[i].Object) {
			t.Errorf("object %d changed in the round trip:\ngot  %v\nwant %v", i, got[i].Object, want[i].Object)
		}
		if got[i].GetUID() != want[i].GetUID() {
			t.Errorf("object %d uid = %q, want %q", i, got[i].GetUID(), want[i].GetUID())
		}
		gotTS, wantTS := got[i].GetCreationTimestamp(), want[i].GetCreationTimestamp()
		if wantTS.IsZero() || !gotTS.Time.Equal(wantTS.Time) {
			t.Errorf("object %d creationTimestamp = %v, want %v", i, gotTS, wantTS)
		}
	}

	// The same objects give the same bytes.
	var again bytes.Buffer
	if _, err := Export(context.Background(), fakeClient(devices, claims), group, resources, &again); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), again.Bytes()) {
		t.Errorf("two exports of the same objects differ:\n%s\n%s", buf.String(), again.String())
	}
}

func TestExportRefusesNamespacedResource(t *testing.T) {
	//= docs/spec/fleet.md#2-resources
	//= type=test
	//# Every fleet resource MUST be cluster-scoped.
	c := fakeClient(nil, nil)
	rs := []Resource{{GVR: devicesGVR, Kind: "Device", Namespaced: true}}
	var buf bytes.Buffer
	if _, err := Export(context.Background(), c, group, rs, &buf); err == nil || !strings.Contains(err.Error(), "namespaced") {
		t.Fatalf("Export(namespaced resource) error = %v, want a namespaced error", err)
	}
	if len(c.Actions()) != 0 || buf.Len() != 0 {
		t.Errorf("export listed or wrote before it refused: %d actions, %d bytes", len(c.Actions()), buf.Len())
	}
}

func TestExportRefusesNamespacedObject(t *testing.T) {
	d := device("dev-a", "uid-a")
	d.SetNamespace("default")
	c := fakeClient(nil, []*unstructured.Unstructured{claim("claim-a", "uid-claim-a")})
	c.PrependReactor("list", "deviceclaims", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, list("1240", "", d), nil
	})
	if _, err := Export(context.Background(), c, group, resources[:1], &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "namespace") {
		t.Fatalf("Export(object with namespace) error = %v, want a namespace error", err)
	}
}

func TestExportRefusesObjectWithoutUID(t *testing.T) {
	//= docs/spec/fleet.md#7-migration
	//= type=test
	//# The migration MUST preserve each object's uid.
	c := fakeClient(nil, []*unstructured.Unstructured{claim("claim-a", "")})
	if _, err := Export(context.Background(), c, group, resources[:1], &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "uid") {
		t.Fatalf("Export(object without uid) error = %v, want a uid error", err)
	}
}

func TestExportRefusesNonNumericRevision(t *testing.T) {
	c := fakeClient(nil, nil)
	c.PrependReactor("list", "deviceclaims", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, list("abc", ""), nil
	})
	if _, err := Export(context.Background(), c, group, resources[:1], &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "etcd revision") {
		t.Fatalf("Export(non-numeric revision) error = %v, want a revision error", err)
	}
}

// A server that returns the same continue token forever must not make the
// export page forever. An early version of this test did that and ran the
// machine out of memory.
func TestExportStopsOnRepeatedContinueToken(t *testing.T) {
	c := fakeClient(nil, nil)
	c.PrependReactor("list", "deviceclaims", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, list("1240", "same", claim("claim-a", "uid-claim-a")), nil
	})
	if _, err := Export(context.Background(), c, group, resources[:1], &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "continue token") {
		t.Fatalf("Export(repeated continue token) error = %v, want a continue token error", err)
	}
	if n := len(c.Actions()); n != 2 {
		t.Errorf("export sent %d list requests, want 2", n)
	}
}

func TestReadRejectsWrongCount(t *testing.T) {
	c := fakeClient(nil, []*unstructured.Unstructured{claim("claim-a", "uid-claim-a")})
	var buf bytes.Buffer
	if _, err := Export(context.Background(), c, group, resources[:1], &buf); err != nil {
		t.Fatal(err)
	}
	lines := strings.SplitAfter(buf.String(), "\n")
	if _, _, err := Read(strings.NewReader(lines[0])); err == nil {
		t.Error("Read accepted a file with a missing object")
	}
	if _, _, err := Read(strings.NewReader(buf.String() + lines[1])); err == nil {
		t.Error("Read accepted a file with an extra object")
	}
	if _, _, err := Read(strings.NewReader(strings.Replace(buf.String(), Format, "cask-export/v0", 1))); err == nil {
		t.Error("Read accepted an unknown format")
	}
}

func TestDiscoverListsClusterResources(t *testing.T) {
	d := &fakediscovery.FakeDiscovery{Fake: &clienttesting.Fake{Resources: []*metav1.APIResourceList{
		{GroupVersion: "fleet.cask.dev/v1alpha1", APIResources: []metav1.APIResource{
			{Name: "devices", Kind: "Device", Verbs: []string{"get", "list", "watch"}},
			{Name: "devices/status", Kind: "Device", Verbs: []string{"get", "update"}},
			{Name: "deviceclaims", Kind: "DeviceClaim", Verbs: []string{"list"}},
			{Name: "tokenreviews", Kind: "TokenReview", Verbs: []string{"create"}},
		}},
		{GroupVersion: "v1", APIResources: []metav1.APIResource{
			{Name: "pods", Kind: "Pod", Namespaced: true, Verbs: []string{"list"}},
		}},
	}}}
	got, err := Discover(d, group)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, resources) {
		t.Errorf("Discover = %+v, want %+v", got, resources)
	}
	if _, err := Discover(d, "other.example.com"); err == nil {
		t.Error("Discover accepted a group the server does not serve")
	}
}
