package main

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
	"github.com/phoban01/cask/cmd/cask-apiserver/migrate"
	"github.com/phoban01/cask/internal/mvcc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/server/healthz"
	apistorage "k8s.io/apiserver/pkg/storage"
)

// testRevision is the source etcd revision of the test exports.
const testRevision = 4200

// noLocks is a migrate.Locks for an import with no Bound claims.
type noLocks struct{}

func (noLocks) Seed(context.Context, migrate.LockSeed) error  { return nil }
func (noLocks) Renew(context.Context, migrate.LockSeed) error { return nil }

func exportedDevice(name, uid string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "fleet.cask.dev/v1alpha1",
		"kind":       "Device",
		"metadata": map[string]any{
			"name":              name,
			"uid":               uid,
			"resourceVersion":   "1200",
			"creationTimestamp": "2026-01-02T03:04:05Z",
		},
		"spec": map[string]any{"class": "gpu"},
	}}
}

// exportedClaim returns a Pending claim that device ownerName with
// ownerUID owns.
func exportedClaim(name, uid, ownerName, ownerUID string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "fleet.cask.dev/v1alpha1",
		"kind":       "DeviceClaim",
		"metadata": map[string]any{
			"name":              name,
			"uid":               uid,
			"resourceVersion":   "1210",
			"creationTimestamp": "2026-01-02T03:04:06Z",
			"ownerReferences": []any{map[string]any{
				"apiVersion": "fleet.cask.dev/v1alpha1",
				"kind":       "Device",
				"name":       ownerName,
				"uid":        ownerUID,
			}},
		},
		"spec": map[string]any{"deviceName": ownerName, "ttlSeconds": int64(30)},
	}}
}

// importObjects imports devices and claims into kv, as --import-file does.
func importObjects(t *testing.T, kv *mvcc.KV, devices, claims []*unstructured.Unstructured) {
	t.Helper()
	h := migrate.Header{
		Format:   migrate.Format,
		Group:    v1alpha1.GroupName,
		Revision: testRevision,
		Resources: []migrate.ResourceHeader{
			{Version: v1alpha1.Version, Resource: "devices", Kind: "Device", Count: len(devices)},
			{Version: v1alpha1.Version, Resource: "deviceclaims", Kind: "DeviceClaim", Count: len(claims)},
		},
	}
	objs := append(append([]*unstructured.Unstructured{}, devices...), claims...)
	if _, err := migrate.Import(context.Background(), kv, noLocks{}, h, objs); err != nil {
		t.Fatalf("import: %v", err)
	}
}

// startImportServer starts a server with the checks of --expect-import at
// revision. It does not wait for readiness.
func startImportServer(t *testing.T, revision uint64) *testServer {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return startTestServerWith(t, "eu-west-a", false, testServerConfig{
		noWait: true,
		checks: func(kv *mvcc.KV) []healthz.HealthChecker {
			ic := newImportCheck(kv, fleetStores(kv), revision)
			go ic.run(ctx, 50*time.Millisecond)
			return []healthz.HealthChecker{newStorageCheck(kv), ic}
		},
	})
}

// staysNotReady fails the test when /readyz reports ready within a few
// verifications of the import check.
func staysNotReady(t *testing.T, ts *testServer, why string) {
	t.Helper()
	for range 10 {
		if code := ts.readyz(); code == http.StatusOK {
			t.Fatalf("readyz %s: got 200, want not ready", why)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestReadyzWaitsForImport starts a server with --expect-import and no
// import. It reports not ready. After the import, it reports ready.
func TestReadyzWaitsForImport(t *testing.T) {
	//= docs/spec/fleet.md#2-resources
	//= type=test
	//# The extension server MUST report not ready until any pending migration import is complete.
	//= docs/spec/fleet.md#7-migration
	//= type=test
	//# The APIService MUST NOT become available before the import has completed.
	ts := startImportServer(t, testRevision)
	staysNotReady(t, ts, "before the import")

	importObjects(t, ts.kv,
		[]*unstructured.Unstructured{exportedDevice("dev-a", "uid-a")},
		[]*unstructured.Unstructured{exportedClaim("claim-a", "uid-claim-a", "dev-a", "uid-a")})
	ts.waitReadyz(t, func(code int) bool { return code == http.StatusOK })
}

// TestReadyzWithoutExpectImport starts a server without --expect-import
// and no import. It reports ready.
func TestReadyzWithoutExpectImport(t *testing.T) {
	ts := startTestServer(t, "eu-west-a", false)
	if code := ts.readyz(); code != http.StatusOK {
		t.Fatalf("readyz without --expect-import: got %d, want 200", code)
	}
}

// TestReadyzWaitsForImportedOwner imports a claim whose owner device is
// not in the export. The server reports not ready until the owner exists
// with the uid of the reference.
func TestReadyzWaitsForImportedOwner(t *testing.T) {
	//= docs/spec/fleet.md#7-migration
	//= type=test
	//# The APIService MUST NOT become available while any imported object that other objects reference by ownerReference is missing.
	ts := startImportServer(t, testRevision)
	importObjects(t, ts.kv, nil,
		[]*unstructured.Unstructured{exportedClaim("claim-a", "uid-claim-a", "dev-a", "uid-a")})
	staysNotReady(t, ts, "with a missing owner")

	// A device with the name but another uid is not the owner.
	stores := fleetStores(ts.kv)
	create := func(uid string) {
		t.Helper()
		d := &v1alpha1.Device{ObjectMeta: metav1.ObjectMeta{Name: "dev-a", UID: types.UID(uid)}}
		if err := stores["devices"].Create(context.Background(), "/devices/dev-a", d, &v1alpha1.Device{}, 0); err != nil {
			t.Fatalf("create dev-a: %v", err)
		}
	}
	create("uid-other")
	staysNotReady(t, ts, "with an owner of another uid")
	if err := stores["devices"].Delete(context.Background(), "/devices/dev-a", &v1alpha1.Device{}, nil,
		apistorage.ValidateAllObjectFunc, nil, apistorage.DeleteOptions{}); err != nil {
		t.Fatalf("delete dev-a: %v", err)
	}

	create("uid-a")
	ts.waitReadyz(t, func(code int) bool { return code == http.StatusOK })
}

// TestImportCheckRejectsOtherRevision reads a marker of another source
// revision. The check fails and names both revisions.
func TestImportCheckRejectsOtherRevision(t *testing.T) {
	//= docs/spec/fleet.md#2-resources
	//= type=test
	//# The extension server MUST report not ready until any pending migration import is complete.
	ts := startImportServer(t, testRevision+1)
	importObjects(t, ts.kv, []*unstructured.Unstructured{exportedDevice("dev-a", "uid-a")}, nil)
	staysNotReady(t, ts, "with a marker of another revision")

	ic := newImportCheck(ts.kv, fleetStores(ts.kv), testRevision+1)
	err := ic.verify(context.Background())
	if err == nil || !strings.Contains(err.Error(), "revision 4200, want 4201") {
		t.Fatalf("verify: got %v, want a revision mismatch", err)
	}
}
