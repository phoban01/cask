package migrate

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
	"github.com/phoban01/cask/cmd/cask-apiserver/storage"
	"github.com/phoban01/cask/internal/backoff"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/store"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	apistorage "k8s.io/apiserver/pkg/storage"
)

// newKV returns one proposer over a new in-process cask of three
// in-memory acceptors.
func newKV(t *testing.T) *mvcc.KV {
	t.Helper()
	acceptors := make([]caspaxos.AcceptorClient, 3)
	for i := range acceptors {
		acceptors[i] = caspaxos.NewAcceptor(store.NewMem())
	}
	var now atomic.Int64
	clock := hlc.New(func() int64 { return now.Add(1) })
	prop := caspaxos.NewProposer(1, acceptors,
		caspaxos.WithBackoff(backoff.FullJitter(time.Millisecond, 20*time.Millisecond)))
	return mvcc.New(prop, clock, 1)
}

// exportFixture exports three devices and one claim through the fake
// client and returns the file.
func exportFixture(t *testing.T) []byte {
	t.Helper()
	// claim-a is Bound to dev-a at fence 7, and dev-a advertises it, so
	// the import keeps the claim as it is.
	devA := device("dev-a", "uid-a")
	if err := unstructured.SetNestedMap(devA.Object, map[string]any{
		"cluster": "east", "claim": "claim-a", "fence": int64(7),
	}, "status", "lease"); err != nil {
		t.Fatal(err)
	}
	devices := []*unstructured.Unstructured{
		device("dev-c", "uid-c"), devA, device("dev-b", "uid-b"),
	}
	claims := []*unstructured.Unstructured{claim("claim-a", "uid-claim-a")}
	var buf bytes.Buffer
	if _, err := Export(context.Background(), fakeClient(devices, claims), group, resources, &buf); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func importFile(t *testing.T, kv *mvcc.KV, file []byte) (Result, error) {
	t.Helper()
	h, objects, err := Read(bytes.NewReader(file))
	if err != nil {
		t.Fatal(err)
	}
	return Import(context.Background(), kv, &fakeLocks{}, h, objects)
}

// newStore returns a storage.Store for resource over kv, as the API
// server builds it.
func newStore(t *testing.T, kv *mvcc.KV, resource string, newFunc func() runtime.Object) *storage.Store {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	codec := serializer.NewCodecFactory(scheme).LegacyCodec(v1alpha1.SchemeGroupVersion)
	return storage.New(kv, codec, resource, newFunc)
}

// snapshot returns the history of every register that the import can
// touch.
func snapshot(t *testing.T, kv *mvcc.KV, objects []*unstructured.Unstructured) map[string]mvcc.Chain {
	t.Helper()
	keys := [][]byte{MarkerKey, storage.IndexKey("devices"), storage.IndexKey("deviceclaims")}
	for _, o := range objects {
		keys = append(keys, storage.ObjectKey(resourceOf(o), o.GetName()))
	}
	out := map[string]mvcc.Chain{}
	for _, k := range keys {
		c, err := kv.History(context.Background(), k)
		if err != nil {
			t.Fatal(err)
		}
		out[string(k)] = c
	}
	return out
}

func resourceOf(o *unstructured.Unstructured) string {
	if o.GetKind() == "Device" {
		return "devices"
	}
	return "deviceclaims"
}

func TestImportRoundTripsEveryFieldButResourceVersion(t *testing.T) {
	//= docs/spec/fleet.md#7-migration
	//= type=test
	//# The migration MUST preserve each object's uid.
	//= docs/spec/fleet.md#7-migration
	//= type=test
	//# The migration MUST preserve each object's creationTimestamp.
	ctx := context.Background()
	kv := newKV(t)
	file := exportFixture(t)

	res, err := importFile(t, kv, file)
	if err != nil {
		t.Fatal(err)
	}
	if res != (Result{Written: 4, Renewed: 1}) {
		t.Fatalf("result = %+v, want 4 written and 1 renewed", res)
	}

	_, objects, err := Read(bytes.NewReader(file))
	if err != nil {
		t.Fatal(err)
	}
	devices := newStore(t, kv, "devices", func() runtime.Object { return &v1alpha1.Device{} })
	claims := newStore(t, kv, "deviceclaims", func() runtime.Object { return &v1alpha1.DeviceClaim{} })
	for _, o := range objects {
		var got, want runtime.Object
		s := devices
		if o.GetKind() == "Device" {
			got, want = &v1alpha1.Device{}, &v1alpha1.Device{}
		} else {
			got, want, s = &v1alpha1.DeviceClaim{}, &v1alpha1.DeviceClaim{}, claims
		}
		if err := s.Get(ctx, "/fleet.cask.dev/"+resourceOf(o)+"/"+o.GetName(), apistorage.GetOptions{}, got); err != nil {
			t.Fatalf("get %s: %v", o.GetName(), err)
		}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(o.Object, want); err != nil {
			t.Fatal(err)
		}
		gotMeta, wantMeta := got.(interface{ GetResourceVersion() string }), want.(interface {
			GetResourceVersion() string
			SetResourceVersion(string)
		})
		// The resourceVersion is the new index sequence, not the source
		// etcd revision.
		if rv := gotMeta.GetResourceVersion(); rv == "" || rv == wantMeta.GetResourceVersion() {
			t.Errorf("%s resourceVersion = %q, want a new index sequence", o.GetName(), rv)
		}
		wantMeta.SetResourceVersion(gotMeta.GetResourceVersion())
		if !equality.Semantic.DeepEqual(got, want) {
			t.Errorf("%s differs after import:\n got %+v\nwant %+v", o.GetName(), got, want)
		}
	}

	// Check uid and creationTimestamp by name, so a converter bug cannot
	// hide them.
	d := &v1alpha1.Device{}
	if err := devices.Get(ctx, "/fleet.cask.dev/devices/dev-a", apistorage.GetOptions{}, d); err != nil {
		t.Fatal(err)
	}
	if d.UID != "uid-a" || d.CreationTimestamp.UTC().Format(time.RFC3339) != "2026-01-02T03:04:05Z" {
		t.Fatalf("dev-a uid = %q, creationTimestamp = %s", d.UID, d.CreationTimestamp)
	}

	// The marker is written.
	if _, found, err := kv.Get(ctx, MarkerKey); err != nil || !found {
		t.Fatalf("marker found = %v, err = %v", found, err)
	}
}

func TestImportTwiceChangesNothing(t *testing.T) {
	kv := newKV(t)
	file := exportFixture(t)
	if _, err := importFile(t, kv, file); err != nil {
		t.Fatal(err)
	}
	_, objects, err := Read(bytes.NewReader(file))
	if err != nil {
		t.Fatal(err)
	}
	before := snapshot(t, kv, objects)

	res, err := importFile(t, kv, file)
	if err != nil {
		t.Fatal(err)
	}
	if res != (Result{Unchanged: 4}) {
		t.Fatalf("second import result = %+v, want 4 unchanged", res)
	}
	if after := snapshot(t, kv, objects); !reflect.DeepEqual(before, after) {
		t.Fatal("the second import changed a register")
	}
}

func TestImportRepairsLostIndexWrite(t *testing.T) {
	ctx := context.Background()
	kv := newKV(t)
	file := exportFixture(t)
	h, objects, err := Read(bytes.NewReader(file))
	if err != nil {
		t.Fatal(err)
	}
	// A crash after the first object write and before its index write.
	items, _, err := prepare(h, objects)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.CreateAt(ctx, storage.ObjectKey(items[0].resource, items[0].name), 0, items[0].raw); err != nil {
		t.Fatal(err)
	}

	res, err := Import(ctx, kv, &fakeLocks{}, h, objects)
	if err != nil {
		t.Fatal(err)
	}
	if res != (Result{Written: 3, Unchanged: 1, Renewed: 1}) {
		t.Fatalf("result = %+v, want 3 written, 1 unchanged, 1 renewed", res)
	}
	idx, err := storage.ReadIndex(ctx, kv, items[0].resource)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := idx.Entries[items[0].name]; !ok {
		t.Fatalf("the index does not name %s", items[0].name)
	}
}

func TestImportRefusesConflictingUID(t *testing.T) {
	ctx := context.Background()
	kv := newKV(t)
	file := exportFixture(t)

	// cask already holds dev-b with another uid.
	other := device("dev-b", "uid-other")
	other.SetResourceVersion("")
	raw, err := other.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.CreateAt(ctx, storage.ObjectKey("devices", "dev-b"), 0, raw); err != nil {
		t.Fatal(err)
	}
	if _, _, err := storage.WriteIndex(ctx, kv, "devices", "dev-b"); err != nil {
		t.Fatal(err)
	}
	_, objects, err := Read(bytes.NewReader(file))
	if err != nil {
		t.Fatal(err)
	}
	before := snapshot(t, kv, objects)

	_, err = importFile(t, kv, file)
	if !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "uid-other") {
		t.Fatalf("import err = %v, want ErrConflict naming uid-other", err)
	}
	if after := snapshot(t, kv, objects); !reflect.DeepEqual(before, after) {
		t.Fatal("a refused import wrote to cask")
	}
}

func TestImportRefusesInvalidFileBeforeWriting(t *testing.T) {
	kv := newKV(t)
	file := exportFixture(t)
	h, objects, err := Read(bytes.NewReader(file))
	if err != nil {
		t.Fatal(err)
	}
	before := snapshot(t, kv, objects)

	// The last object has no uid. The objects before it are valid, but
	// none of them may be written.
	bad := make([]*unstructured.Unstructured, len(objects))
	for i, o := range objects {
		bad[i] = o.DeepCopy()
	}
	bad[len(bad)-1].SetUID("")
	if _, err := Import(context.Background(), kv, &fakeLocks{}, h, bad); err == nil || !strings.Contains(err.Error(), "no uid") {
		t.Fatalf("import err = %v, want a missing uid", err)
	}

	// A file that Read rejects never reaches Import.
	truncated := file[:bytes.LastIndexByte(file[:len(file)-1], '\n')+1]
	if _, _, err := Read(bytes.NewReader(truncated)); err == nil {
		t.Fatal("Read accepted a truncated file")
	}

	if after := snapshot(t, kv, objects); !reflect.DeepEqual(before, after) {
		t.Fatal("an invalid import wrote to cask")
	}
}
