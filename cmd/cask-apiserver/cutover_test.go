package main

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/phoban01/cask/cmd/cask-apiserver/migrate"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// cutoverExport is an export with one device and one claim. The claim
// "mapper" of cluster-a is Bound to lidar-7 at fence 7, and the device
// advertises that lease.
func cutoverExport() (migrate.Header, []*unstructured.Unstructured) {
	h := migrate.Header{
		Format:   migrate.Format,
		Group:    apiGroup,
		Revision: 500,
		Resources: []migrate.ResourceHeader{
			{Version: apiVersion, Resource: "devices", Kind: "Device", ListResourceVersion: 500, Count: 1},
			{Version: apiVersion, Resource: "deviceclaims", Kind: "DeviceClaim", ListResourceVersion: 500, Count: 1},
		},
	}
	dev := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": apiGroup + "/" + apiVersion,
		"kind":       "Device",
		"metadata": map[string]any{
			"name": "lidar-7", "uid": "uid-lidar-7", "resourceVersion": "480",
			"creationTimestamp": "2026-01-02T03:04:05Z",
		},
		"spec": map[string]any{"model": "lidar"},
		"status": map[string]any{
			"phase": DeviceLeased,
			"lease": map[string]any{"cluster": "cluster-a", "claim": "mapper", "fence": int64(7)},
		},
	}}
	claim := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": apiGroup + "/" + apiVersion,
		"kind":       "DeviceClaim",
		"metadata": map[string]any{
			"name": "mapper", "uid": "uid-mapper", "resourceVersion": "490",
			"creationTimestamp": "2026-01-02T03:04:06Z",
		},
		"spec":   map[string]any{"deviceName": "lidar-7", "ttlSeconds": int64(10)},
		"status": map[string]any{"phase": ClaimBound, "cluster": "cluster-a", "fence": int64(7)},
	}}
	return h, []*unstructured.Unstructured{dev, claim}
}

// noSeed is the import without lock seeding: the state before issue #196.
type noSeed struct{}

func (noSeed) Seed(context.Context, migrate.LockSeed) error  { return nil }
func (noSeed) Renew(context.Context, migrate.LockSeed) error { return nil }

// slowImport delegates to the fleet store and advances the fake clock by
// delay after each seed, as if the object writes that follow took that
// long.
type slowImport struct {
	*fleetStore
	f     *fleetFixture
	delay time.Duration
}

func (s slowImport) Seed(ctx context.Context, seed migrate.LockSeed) error {
	err := s.fleetStore.Seed(ctx, seed)
	s.f.now.Add(int64(s.delay))
	return err
}

// A slow import outlasts the claim's 10 s TTL. The import grace keeps the
// restored session alive, and the renewal after the marker gives the
// claim's controller a whole TTL again. Another cluster's claim cannot take
// the lock in that window.
func TestCutoverSlowImportKeepsClaimBound(t *testing.T) {
	//= docs/spec/fleet.md#7-migration
	//= type=test
	//# The import MUST grant a restored claim's session for the claim's TTL plus an import grace.
	//= docs/spec/fleet.md#7-migration
	//= type=test
	//# The import MUST renew each restored claim's session after it writes the import marker.
	f := newFleetFixture(t)
	ctx := context.Background()
	f.a.api.store.importGrace = 10 * time.Minute
	h, objects := cutoverExport()
	res, err := migrate.Import(ctx, f.a.api.store.kv, slowImport{f.a.api.store, f, 2 * time.Minute}, h, objects)
	if err != nil {
		t.Fatal(err)
	}
	if res.Renewed != 1 || res.Lapsed != 0 {
		t.Fatalf("result = %+v, want 1 renewed", res)
	}

	// cluster-b runs already. Its claim waits while cluster-a starts.
	doReq(t, f.b.ts, http.MethodPost, groupPrefix+"/deviceclaims",
		`{"metadata":{"name":"surveyor"},"spec":{"deviceName":"lidar-7","ttlSeconds":10}}`)
	f.now.Add(int64(15 * time.Second)) // past the claim TTL
	f.b.api.claims.reconcileOnce(ctx)
	if c := getClaim(t, f.b, "surveyor"); c.Status.Phase != ClaimPending {
		t.Fatalf("second claim during the start of cluster-a = %+v, want Pending", c.Status)
	}

	// cluster-a starts and renews. The imported claim stays Bound at 7.
	f.a.api.claims.reconcileOnce(ctx)
	if c := getClaim(t, f.a, "mapper"); c.Status.Phase != ClaimBound || c.Status.Fence != 7 {
		t.Fatalf("imported claim after a slow import = %+v, want Bound at 7", c.Status)
	}
}

// Control: with no import grace, the same slow import outlasts the
// session. The claim goes to Lost, and the next acquisition still mints
// above the seeded fence. Liveness is lost, safety is kept.
func TestCutoverSlowImportWithoutGraceLosesClaimNotFence(t *testing.T) {
	f := newFleetFixture(t)
	ctx := context.Background()
	h, objects := cutoverExport()
	res, err := migrate.Import(ctx, f.a.api.store.kv, slowImport{f.a.api.store, f, 2 * time.Minute}, h, objects)
	if err != nil {
		t.Fatal(err)
	}
	if res.Lapsed != 1 {
		t.Fatalf("result = %+v, want 1 lapsed", res)
	}
	doReq(t, f.b.ts, http.MethodPost, groupPrefix+"/deviceclaims",
		`{"metadata":{"name":"surveyor"},"spec":{"deviceName":"lidar-7","ttlSeconds":10}}`)
	f.b.api.claims.reconcileOnce(ctx)
	if c := getClaim(t, f.b, "surveyor"); c.Status.Phase != ClaimBound || c.Status.Fence != 8 {
		t.Fatalf("takeover = %+v, want Bound at 8", c.Status)
	}
	f.a.api.claims.reconcileOnce(ctx)
	if c := getClaim(t, f.a, "mapper"); c.Status.Phase != ClaimLost {
		t.Fatalf("imported claim = %+v, want Lost", c.Status)
	}
}

// After the import, the Bound claim still holds the lock at fence 7 and
// stays Bound. When it lets go, the next acquisition mints fence 8.
func TestCutoverKeepsBoundClaimAndMintsAbove(t *testing.T) {
	//= docs/spec/fleet.md#7-migration
	//= type=test
	//# The import MUST NOT seed an object's lock at a fence below the highest fence that the export recorded for that object.
	//= docs/spec/fleet.md#7-migration
	//= type=test
	//# The import MUST restore a Bound claim as Bound only when that claim alone holds the highest fence that the export recorded for its object.
	//= docs/spec/fleet.md#7-migration
	//= type=test
	//# The import MUST grant a restored claim's session and seed its lock before it writes the claim.
	f := newFleetFixture(t)
	ctx := context.Background()
	h, objects := cutoverExport()
	if _, err := migrate.Import(ctx, f.a.api.store.kv, f.a.api.store, h, objects); err != nil {
		t.Fatal(err)
	}

	session, live, fence, err := f.a.api.store.locks.Owner(ctx, deviceLockName("lidar-7"))
	if err != nil || session != claimSessionID("cluster-a", "mapper") || !live || fence != 7 {
		t.Fatalf("lock owner = %q live=%v fence=%d err=%v, want mapper live at 7", session, live, fence, err)
	}

	// cluster-a renews the imported claim. It stays Bound at fence 7.
	f.a.api.claims.reconcileOnce(ctx)
	if c := getClaim(t, f.a, "mapper"); c.Status.Phase != ClaimBound || c.Status.Fence != 7 {
		t.Fatalf("imported claim after renew = %+v, want Bound at 7", c.Status)
	}

	// A second claim from cluster-b waits: the imported claim holds the lock.
	doReq(t, f.b.ts, http.MethodPost, groupPrefix+"/deviceclaims",
		`{"metadata":{"name":"surveyor"},"spec":{"deviceName":"lidar-7","ttlSeconds":10}}`)
	f.b.api.claims.reconcileOnce(ctx)
	if c := getClaim(t, f.b, "surveyor"); c.Status.Phase != ClaimPending {
		t.Fatalf("second claim = %+v, want Pending", c.Status)
	}

	// Deleting the imported claim releases the lock. The next acquisition
	// mints above every fence from before the cutover.
	if code, raw := doReq(t, f.a.ts, http.MethodDelete, groupPrefix+"/deviceclaims/mapper", ""); code != http.StatusOK {
		t.Fatalf("delete = %d %s", code, raw)
	}
	f.b.api.claims.reconcileOnce(ctx)
	c := getClaim(t, f.b, "surveyor")
	if c.Status.Phase != ClaimBound || c.Status.Fence != 8 {
		t.Fatalf("next acquisition = %+v, want Bound at 8", c.Status)
	}
	if d := getDevice(t, f.a, "lidar-7"); d.Status.Lease == nil || d.Status.Lease.Fence != 8 {
		t.Fatalf("device lease = %+v, want fence 8", d.Status)
	}
}

// Control: without the seed, the imported claim has no lock and goes to
// Lost. The next acquisition mints fence 1, and the device status, a
// receiver that accepted fence 7, rejects it.
func TestCutoverWithoutSeedRestartsFenceAtOne(t *testing.T) {
	f := newFleetFixture(t)
	ctx := context.Background()
	h, objects := cutoverExport()
	if _, err := migrate.Import(ctx, f.a.api.store.kv, noSeed{}, h, objects); err != nil {
		t.Fatal(err)
	}

	f.a.api.claims.reconcileOnce(ctx)
	if c := getClaim(t, f.a, "mapper"); c.Status.Phase != ClaimLost {
		t.Fatalf("imported claim without a seed = %+v, want Lost", c.Status)
	}

	doReq(t, f.b.ts, http.MethodPost, groupPrefix+"/deviceclaims",
		`{"metadata":{"name":"surveyor"},"spec":{"deviceName":"lidar-7","ttlSeconds":10}}`)
	f.b.api.claims.reconcileOnce(ctx)
	c := getClaim(t, f.b, "surveyor")
	if c.Status.Phase != ClaimBound || c.Status.Fence != 1 {
		t.Fatalf("acquisition without a seed = %+v, want Bound at 1", c.Status)
	}
	d := getDevice(t, f.a, "lidar-7")
	if d.Status.Lease == nil || d.Status.Lease.Fence != 7 || d.Status.Lease.Claim != "mapper" {
		t.Fatalf("device lease = %+v, want the fence-7 lease kept and fence 1 rejected", d.Status)
	}
}
