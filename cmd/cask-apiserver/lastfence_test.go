package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/phoban01/cask/cmd/cask-apiserver/migrate"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

// exportFrom lists the devices and claims that cs serves, as the export
// does. The plain mux drops uid and creationTimestamp, so this adds them.
func exportFrom(t *testing.T, cs *clusterServer) (migrate.Header, []*unstructured.Unstructured) {
	t.Helper()
	h := migrate.Header{Format: migrate.Format, Group: apiGroup, Revision: 500}
	var objects []*unstructured.Unstructured
	for _, r := range []struct{ resource, kind string }{{"devices", "Device"}, {"deviceclaims", "DeviceClaim"}} {
		code, raw := doReq(t, cs.ts, http.MethodGet, groupPrefix+"/"+r.resource, "")
		if code != http.StatusOK {
			t.Fatalf("list %s = %d %s", r.resource, code, raw)
		}
		var list struct {
			Items []map[string]any `json:"items"`
		}
		if err := json.Unmarshal(raw, &list); err != nil {
			t.Fatal(err)
		}
		for _, item := range list.Items {
			o := &unstructured.Unstructured{Object: item}
			o.SetUID(types.UID("uid-" + o.GetName()))
			_ = unstructured.SetNestedField(o.Object, "2026-01-02T03:04:05Z", "metadata", "creationTimestamp")
			objects = append(objects, o)
		}
		h.Resources = append(h.Resources, migrate.ResourceHeader{
			Version: apiVersion, Resource: r.resource, Kind: r.kind,
			ListResourceVersion: 500, Count: len(list.Items),
		})
	}
	return h, objects
}

// bindAndRelease binds a claim on lidar-9 and deletes it, n times. The
// i-th bind mints fence i. After each delete, the device keeps the fence
// in lastFence.
func bindAndRelease(t *testing.T, f *fleetFixture, n uint64) {
	t.Helper()
	ctx := context.Background()
	doReq(t, f.a.ts, http.MethodPost, groupPrefix+"/devices", `{"metadata":{"name":"lidar-9"},"spec":{}}`)
	for i := uint64(1); i <= n; i++ {
		name := fmt.Sprintf("job-%d", i)
		doReq(t, f.a.ts, http.MethodPost, groupPrefix+"/deviceclaims",
			`{"metadata":{"name":"`+name+`"},"spec":{"deviceName":"lidar-9","ttlSeconds":10}}`)
		f.a.api.claims.reconcileOnce(ctx)
		if c := getClaim(t, f.a, name); c.Status.Phase != ClaimBound || c.Status.Fence != i {
			t.Fatalf("claim %s = %+v, want Bound at %d", name, c.Status, i)
		}
		if code, raw := doReq(t, f.a.ts, http.MethodDelete, groupPrefix+"/deviceclaims/"+name, ""); code != http.StatusOK {
			t.Fatalf("delete %s = %d %s", name, code, raw)
		}
		d := getDevice(t, f.a, "lidar-9")
		if d.Status.Phase != DeviceAvailable || d.Status.Lease != nil || d.Status.LastFence != i {
			t.Fatalf("device after release %d = %+v, want Available with lastFence %d", i, d.Status, i)
		}
	}
}

// Issue #200: a claim binds at fence 7, is released, and is deleted. No
// claim and no lease shows fence 7 at the export. The device lastFence
// does, so the import seeds the lock at 7 and the next bind mints 8.
func TestCutoverKeepsReleasedFence(t *testing.T) {
	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# A Device status MUST keep in its lastFence field the highest fence that the Device has advertised.
	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# Deleting a claim MUST NOT complete before the lastFence of its Device is at least the claim's fence.
	//= docs/spec/fleet.md#7-migration
	//= type=test
	//# The fences that the export records for a Device MUST include the lastFence in its status.
	src := newFleetFixture(t)
	bindAndRelease(t, src, 7)
	h, objects := exportFrom(t, src.a)

	dst := newFleetFixture(t)
	ctx := context.Background()
	if _, err := migrate.Import(ctx, dst.a.api.store.kv, dst.a.api.store, h, objects); err != nil {
		t.Fatal(err)
	}
	doReq(t, dst.b.ts, http.MethodPost, groupPrefix+"/deviceclaims",
		`{"metadata":{"name":"surveyor"},"spec":{"deviceName":"lidar-9","ttlSeconds":10}}`)
	dst.b.api.claims.reconcileOnce(ctx)
	if c := getClaim(t, dst.b, "surveyor"); c.Status.Phase != ClaimBound || c.Status.Fence != 8 {
		t.Fatalf("first bind after the cutover = %+v, want Bound at 8", c.Status)
	}
	if d := getDevice(t, dst.a, "lidar-9"); d.Status.LastFence != 8 || d.Status.Lease == nil || d.Status.Lease.Fence != 8 {
		t.Fatalf("device after the bind = %+v, want lease and lastFence 8", d.Status)
	}
}

// Control: an export without lastFence, as before issue #200. The import
// seeds no lock, and the next bind mints fence 1, below the fence 7 that
// a receiver already accepted.
func TestCutoverWithoutLastFenceRestartsAtOne(t *testing.T) {
	src := newFleetFixture(t)
	bindAndRelease(t, src, 7)
	h, objects := exportFrom(t, src.a)
	for _, o := range objects {
		unstructured.RemoveNestedField(o.Object, "status", "lastFence")
	}

	dst := newFleetFixture(t)
	ctx := context.Background()
	if _, err := migrate.Import(ctx, dst.a.api.store.kv, dst.a.api.store, h, objects); err != nil {
		t.Fatal(err)
	}
	doReq(t, dst.b.ts, http.MethodPost, groupPrefix+"/deviceclaims",
		`{"metadata":{"name":"surveyor"},"spec":{"deviceName":"lidar-9","ttlSeconds":10}}`)
	dst.b.api.claims.reconcileOnce(ctx)
	if c := getClaim(t, dst.b, "surveyor"); c.Status.Phase != ClaimBound || c.Status.Fence != 1 {
		t.Fatalf("first bind without lastFence = %+v, want Bound at 1", c.Status)
	}
}

// A zombie cannot lower lastFence. Its stale lease write, its delete, and
// a client update that drops lastFence all leave it at the highest fence.
func TestZombieCannotLowerLastFence(t *testing.T) {
	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# A write to a Device MUST NOT lower its lastFence.
	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# A status write MUST NOT lower an advertised fence.
	f := newFleetFixture(t)
	ctx := context.Background()
	doReq(t, f.a.ts, http.MethodPost, groupPrefix+"/devices", `{"metadata":{"name":"lidar-3"},"spec":{}}`)
	doReq(t, f.a.ts, http.MethodPost, groupPrefix+"/deviceclaims",
		`{"metadata":{"name":"mapper"},"spec":{"deviceName":"lidar-3","ttlSeconds":1}}`)
	doReq(t, f.b.ts, http.MethodPost, groupPrefix+"/deviceclaims",
		`{"metadata":{"name":"surveyor"},"spec":{"deviceName":"lidar-3","ttlSeconds":10}}`)

	f.a.api.claims.reconcileOnce(ctx)
	zombie := getClaim(t, f.a, "mapper")
	if zombie.Status.Phase != ClaimBound || zombie.Status.Fence != 1 {
		t.Fatalf("A = %+v, want Bound at 1", zombie.Status)
	}
	f.now.Add(2_000_000_000) // A's session lapses
	f.b.api.claims.reconcileOnce(ctx)
	if c := getClaim(t, f.b, "surveyor"); c.Status.Phase != ClaimBound || c.Status.Fence != 2 {
		t.Fatalf("takeover = %+v, want Bound at 2", c.Status)
	}

	// The zombie is still Bound in its status. Deleting it must not clear
	// the successor's lease.
	if code, raw := doReq(t, f.a.ts, http.MethodDelete, groupPrefix+"/deviceclaims/mapper", ""); code != http.StatusOK {
		t.Fatalf("delete zombie = %d %s", code, raw)
	}
	d := getDevice(t, f.a, "lidar-3")
	if d.Status.Lease == nil || d.Status.Lease.Fence != 2 || d.Status.LastFence != 2 {
		t.Fatalf("device after the zombie's delete = %+v, want the fence-2 lease", d.Status)
	}

	// The successor lets go. Its fence stays in lastFence.
	if code, raw := doReq(t, f.b.ts, http.MethodDelete, groupPrefix+"/deviceclaims/surveyor", ""); code != http.StatusOK {
		t.Fatalf("delete surveyor = %d %s", code, raw)
	}

	// The zombie's stale lease write comes in late.
	if err := f.a.api.claims.setDeviceLease(ctx, "lidar-3",
		&LeaseRef{Cluster: "cluster-a", Claim: "mapper", Fence: 1}); err != nil {
		t.Fatal(err)
	}
	d = getDevice(t, f.a, "lidar-3")
	if d.Status.Lease != nil || d.Status.LastFence != 2 {
		t.Fatalf("device after the stale write = %+v, want no lease and lastFence 2", d.Status)
	}

	// A stale re-advertise of the released fence 2 is refused too.
	if err := f.b.api.claims.setDeviceLease(ctx, "lidar-3",
		&LeaseRef{Cluster: "cluster-b", Claim: "surveyor", Fence: 2}); err != nil {
		t.Fatal(err)
	}
	if d = getDevice(t, f.a, "lidar-3"); d.Status.Lease != nil {
		t.Fatalf("device after a stale re-advertise = %+v, want no lease", d.Status)
	}

	// A client update that drops lastFence keeps it.
	body := fmt.Sprintf(`{"metadata":{"name":"lidar-3","resourceVersion":%q},"spec":{"model":"v2"},"status":{"phase":"Available"}}`,
		d.ResourceVersion)
	if code, raw := doReq(t, f.a.ts, http.MethodPut, groupPrefix+"/devices/lidar-3", body); code != http.StatusOK {
		t.Fatalf("update = %d %s", code, raw)
	}
	if d = getDevice(t, f.a, "lidar-3"); d.Spec.Model != "v2" || d.Status.LastFence != 2 {
		t.Fatalf("device after a client update = %+v, want lastFence 2", d)
	}
}
