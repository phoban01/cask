package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/lease"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/store"
)

// fleetFixture is two clusters' apiservers sharing ONE cask consensus group —
// the multi-cluster topology, in-process. The shared registers are what make
// a device's lease globally exclusive; the fake clock makes lease expiry and
// zombie scenarios deterministic.
type fleetFixture struct {
	now  atomic.Int64
	a, b *clusterServer
}

type clusterServer struct {
	api *apiServer
	ts  *httptest.Server
}

func newFleetFixture(t *testing.T) *fleetFixture {
	t.Helper()
	f := &fleetFixture{}
	f.now.Store(1_000)

	acceptors := make([]caspaxos.AcceptorClient, 3)
	for i := range acceptors {
		acceptors[i] = caspaxos.NewAcceptor(store.NewMem())
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	clock := func() int64 { return f.now.Load() }

	mk := func(cluster string, id uint64) *clusterServer {
		prop := caspaxos.NewProposer(id, acceptors)
		sessions := lease.NewSessions(prop, clock)
		fs := &fleetStore{
			kv:       mvcc.New(prop, hlc.New(func() int64 { return f.now.Add(1) }), id),
			sessions: sessions,
			locks:    lease.NewLocks(prop, sessions),
		}
		api := newAPIServer(cluster, fs, log)
		api.watchPoll = 10 * time.Millisecond
		ts := httptest.NewServer(api.routes())
		t.Cleanup(ts.Close)
		return &clusterServer{api: api, ts: ts}
	}
	f.a = mk("cluster-a", 11)
	f.b = mk("cluster-b", 12)
	return f
}

func doReq(t *testing.T, ts *httptest.Server, method, path, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func getClaim(t *testing.T, cs *clusterServer, name string) DeviceClaim {
	t.Helper()
	code, raw := doReq(t, cs.ts, http.MethodGet, groupPrefix+"/deviceclaims/"+name, "")
	if code != http.StatusOK {
		t.Fatalf("get claim %s = %d %s", name, code, raw)
	}
	var c DeviceClaim
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

func getDevice(t *testing.T, cs *clusterServer, name string) Device {
	t.Helper()
	code, raw := doReq(t, cs.ts, http.MethodGet, groupPrefix+"/devices/"+name, "")
	if code != http.StatusOK {
		t.Fatalf("get device %s = %d %s", name, code, raw)
	}
	var d Device
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatal(err)
	}
	return d
}

// CRUD basics: create/get/list, resourceVersion round trip, stale-RV
// conflict, delete. A device created via cluster A is immediately visible —
// same bytes, same RV — from cluster B: one fleet, one truth.
func TestDeviceCRUDAcrossClusters(t *testing.T) {
	f := newFleetFixture(t)

	code, raw := doReq(t, f.a.ts, http.MethodPost, groupPrefix+"/devices",
		`{"metadata":{"name":"cam-1"},"spec":{"model":"axis-q1656","zone":"eu-west"}}`)
	if code != http.StatusCreated {
		t.Fatalf("create = %d %s", code, raw)
	}

	dA, dB := getDevice(t, f.a, "cam-1"), getDevice(t, f.b, "cam-1")
	if dA.ResourceVersion == "" || dA.ResourceVersion != dB.ResourceVersion || dB.Spec.Model != "axis-q1656" {
		t.Fatalf("cross-cluster views differ: A=%+v B=%+v", dA, dB)
	}
	if dA.Status.Phase != DeviceAvailable {
		t.Fatalf("new device phase = %q, want Available", dA.Status.Phase)
	}

	// Duplicate create conflicts; update with a stale RV conflicts.
	if code, _ := doReq(t, f.b.ts, http.MethodPost, groupPrefix+"/devices",
		`{"metadata":{"name":"cam-1"},"spec":{}}`); code != http.StatusConflict {
		t.Fatalf("duplicate create = %d, want 409", code)
	}
	upd := fmt.Sprintf(`{"metadata":{"name":"cam-1","resourceVersion":"%s"},"spec":{"model":"axis-q1656","zone":"eu-central"}}`, dA.ResourceVersion)
	if code, _ := doReq(t, f.b.ts, http.MethodPut, groupPrefix+"/devices/cam-1", upd); code != http.StatusOK {
		t.Fatalf("update = %d, want 200", code)
	}
	if code, _ := doReq(t, f.a.ts, http.MethodPut, groupPrefix+"/devices/cam-1", upd); code != http.StatusConflict {
		t.Fatalf("stale-RV update = %d, want 409", code)
	}

	code, raw = doReq(t, f.a.ts, http.MethodGet, groupPrefix+"/devices", "")
	if code != http.StatusOK || !strings.Contains(string(raw), `"cam-1"`) {
		t.Fatalf("list = %d %s", code, raw)
	}
	if code, _ := doReq(t, f.b.ts, http.MethodDelete, groupPrefix+"/devices/cam-1", ""); code != http.StatusOK {
		t.Fatalf("delete = %d", code)
	}
	if code, _ := doReq(t, f.a.ts, http.MethodGet, groupPrefix+"/devices/cam-1", ""); code != http.StatusNotFound {
		t.Fatalf("get after delete = %d, want 404", code)
	}
}

// A create needs an absent register, an update needs the client's
// resourceVersion, and a stale resourceVersion is a 409.
func TestUpdateWithStaleResourceVersionConflicts(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A create MUST use a compare-and-set that requires the object register to be absent.
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# An update MUST use a compare-and-set on the resourceVersion the client supplied.
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# An update whose compare-and-set fails MUST return a conflict.
	f := newFleetFixture(t)

	create := `{"metadata":{"name":"cam-stale"},"spec":{"zone":"eu-west"}}`
	if code, raw := doReq(t, f.a.ts, http.MethodPost, groupPrefix+"/devices", create); code != http.StatusCreated {
		t.Fatalf("create = %d %s", code, raw)
	}
	if code, _ := doReq(t, f.a.ts, http.MethodPost, groupPrefix+"/devices", create); code != http.StatusConflict {
		t.Fatalf("create over an existing object = %d, want 409", code)
	}

	stale := getDevice(t, f.a, "cam-stale").ResourceVersion
	body := func(rv, zone string) string {
		return fmt.Sprintf(`{"metadata":{"name":"cam-stale","resourceVersion":"%s"},"spec":{"zone":"%s"}}`, rv, zone)
	}
	if code, raw := doReq(t, f.a.ts, http.MethodPut, groupPrefix+"/devices/cam-stale", body(stale, "eu-central")); code != http.StatusOK {
		t.Fatalf("update at the current resourceVersion = %d %s", code, raw)
	}

	// The first update moved the object on, so stale no longer matches.
	if code, _ := doReq(t, f.a.ts, http.MethodPut, groupPrefix+"/devices/cam-stale", body(stale, "us-east")); code != http.StatusConflict {
		t.Fatalf("update at a stale resourceVersion = %d, want 409", code)
	}
	if got := getDevice(t, f.a, "cam-stale"); got.Spec.Zone != "eu-central" || got.ResourceVersion == stale {
		t.Fatalf("after the conflict: zone=%q rv=%s, want eu-central at a new rv", got.Spec.Zone, got.ResourceVersion)
	}
}

// THE demo property: two clusters race to claim one device; exactly one
// binds, the other stays Pending. Releasing the winner's claim hands the
// device over with a STRICTLY HIGHER fencing token.
func TestSingleGlobalLease(t *testing.T) {
	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# At most one claim MUST be Bound to an object at the object's current fence.
	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# Every successful acquisition MUST mint a fence strictly greater than every fence previously minted for that object.
	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# Deleting a Bound claim MUST release the object's lock.
	f := newFleetFixture(t)
	ctx := context.Background()

	doReq(t, f.a.ts, http.MethodPost, groupPrefix+"/devices",
		`{"metadata":{"name":"gpu-7"},"spec":{"model":"h100"}}`)
	doReq(t, f.a.ts, http.MethodPost, groupPrefix+"/deviceclaims",
		`{"metadata":{"name":"train-job"},"spec":{"deviceName":"gpu-7","ttlSeconds":10}}`)
	doReq(t, f.b.ts, http.MethodPost, groupPrefix+"/deviceclaims",
		`{"metadata":{"name":"render-job"},"spec":{"deviceName":"gpu-7","ttlSeconds":10}}`)

	// A reconciles first and wins; B stays Pending no matter how often it tries.
	f.a.api.claims.reconcileOnce(ctx)
	f.b.api.claims.reconcileOnce(ctx)
	f.b.api.claims.reconcileOnce(ctx)

	ca, cb := getClaim(t, f.a, "train-job"), getClaim(t, f.b, "render-job")
	if ca.Status.Phase != ClaimBound || ca.Status.Fence == 0 {
		t.Fatalf("A's claim = %+v, want Bound with a fence", ca.Status)
	}
	if cb.Status.Phase != ClaimPending {
		t.Fatalf("B's claim = %+v, want Pending (device leased elsewhere)", cb.Status)
	}
	dev := getDevice(t, f.b, "gpu-7")
	if dev.Status.Phase != DeviceLeased || dev.Status.Lease == nil || dev.Status.Lease.Cluster != "cluster-a" {
		t.Fatalf("device status = %+v, want Leased by cluster-a", dev.Status)
	}
	firstFence := ca.Status.Fence

	// A releases (deletes its claim); B's next reconcile takes over with a
	// strictly higher fence — the monotonic token downstreams key on.
	if code, _ := doReq(t, f.a.ts, http.MethodDelete, groupPrefix+"/deviceclaims/train-job", ""); code != http.StatusOK {
		t.Fatal("delete claim failed")
	}
	f.b.api.claims.reconcileOnce(ctx)
	cb = getClaim(t, f.b, "render-job")
	if cb.Status.Phase != ClaimBound {
		t.Fatalf("B's claim after release = %+v, want Bound", cb.Status)
	}
	if cb.Status.Fence <= firstFence {
		t.Fatalf("fence did not increase across handover: %d then %d", firstFence, cb.Status.Fence)
	}
	dev = getDevice(t, f.a, "gpu-7")
	if dev.Status.Lease == nil || dev.Status.Lease.Cluster != "cluster-b" {
		t.Fatalf("device status after handover = %+v, want cluster-b", dev.Status)
	}
}

// The zombie scenario: cluster A's apiserver stops renewing (network parted,
// process paused). The lease lapses, B takes over at a higher fence, and A —
// waking up — discovers its claim is Lost. The device's advertised lease
// never regresses to the zombie's lower fence.
func TestZombieHolderIsFenced(t *testing.T) {
	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# A status write MUST NOT lower an advertised fence.
	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# When a claim's session lapses, the controller MUST set the claim to Lost.
	f := newFleetFixture(t)
	ctx := context.Background()

	doReq(t, f.a.ts, http.MethodPost, groupPrefix+"/devices",
		`{"metadata":{"name":"lidar-3"},"spec":{}}`)
	doReq(t, f.a.ts, http.MethodPost, groupPrefix+"/deviceclaims",
		`{"metadata":{"name":"mapper"},"spec":{"deviceName":"lidar-3","ttlSeconds":1}}`)
	doReq(t, f.b.ts, http.MethodPost, groupPrefix+"/deviceclaims",
		`{"metadata":{"name":"surveyor"},"spec":{"deviceName":"lidar-3","ttlSeconds":10}}`)

	f.a.api.claims.reconcileOnce(ctx)
	zombieFence := getClaim(t, f.a, "mapper").Status.Fence
	if zombieFence == 0 {
		t.Fatal("A never bound")
	}

	// A goes silent; its 1s TTL lapses on the shared (fake) clock.
	f.now.Add(2_000_000_000)

	f.b.api.claims.reconcileOnce(ctx)
	cb := getClaim(t, f.b, "surveyor")
	if cb.Status.Phase != ClaimBound || cb.Status.Fence <= zombieFence {
		t.Fatalf("takeover = %+v, want Bound with fence > %d", cb.Status, zombieFence)
	}

	// A wakes up: its renewal fails and the claim is terminally Lost.
	f.a.api.claims.reconcileOnce(ctx)
	ca := getClaim(t, f.a, "mapper")
	if ca.Status.Phase != ClaimLost {
		t.Fatalf("zombie's claim = %+v, want Lost", ca.Status)
	}

	// The zombie's stale device-status write cannot mask the live lease.
	if err := f.a.api.claims.setDeviceLease(ctx, "lidar-3",
		&LeaseRef{Cluster: "cluster-a", Claim: "mapper", Fence: zombieFence}); err != nil {
		t.Fatal(err)
	}
	dev := getDevice(t, f.b, "lidar-3")
	if dev.Status.Lease == nil || dev.Status.Lease.Fence <= zombieFence || dev.Status.Lease.Cluster != "cluster-b" {
		t.Fatalf("device lease regressed to the zombie: %+v", dev.Status)
	}
}

// Watch delivers ADDED / MODIFIED / DELETED events from a cross-cluster
// writer, in order, over the k8s line-delimited stream framing.
func TestWatchStreamsCrossClusterEvents(t *testing.T) {
	f := newFleetFixture(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.a.ts.URL+groupPrefix+"/devices?watch=true", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := f.a.ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	events := make(chan WatchEvent, 8)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			var ev WatchEvent
			if json.Unmarshal(sc.Bytes(), &ev) == nil {
				events <- ev
			}
		}
	}()
	next := func(wantType string) WatchEvent {
		t.Helper()
		select {
		case ev := <-events:
			if ev.Type != wantType {
				t.Fatalf("event = %s %s, want %s", ev.Type, ev.Object, wantType)
			}
			return ev
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %s event", wantType)
			return WatchEvent{}
		}
	}

	// Writes arrive through CLUSTER B; the watch is served by CLUSTER A.
	doReq(t, f.b.ts, http.MethodPost, groupPrefix+"/devices", `{"metadata":{"name":"cam-9"},"spec":{"zone":"us-east"}}`)
	ev := next("ADDED")
	var dev Device
	if json.Unmarshal(ev.Object, &dev) != nil || dev.Name != "cam-9" {
		t.Fatalf("ADDED object = %s", ev.Object)
	}

	got := getDevice(t, f.b, "cam-9")
	upd := fmt.Sprintf(`{"metadata":{"name":"cam-9","resourceVersion":"%s"},"spec":{"zone":"us-west"}}`, got.ResourceVersion)
	doReq(t, f.b.ts, http.MethodPut, groupPrefix+"/devices/cam-9", upd)
	next("MODIFIED")

	doReq(t, f.b.ts, http.MethodDelete, groupPrefix+"/devices/cam-9", "")
	next("DELETED")
}

// Responses kubectl decodes as objects/Status must carry an
// application/json content-type. A delete or error that emits text/plain
// makes real kubectl fail with "serializer for text/plain ... doesn't
// exist" — a bug invisible to status-code-only assertions.
func TestResponsesAreJSON(t *testing.T) {
	f := newFleetFixture(t)
	if code, _ := doReq(t, f.a.ts, http.MethodPost, groupPrefix+"/devices",
		`{"metadata":{"name":"cam-json"},"spec":{}}`); code != http.StatusCreated {
		t.Fatalf("seed device = %d", code)
	}

	check := func(method, path, body string, wantCode int) {
		t.Helper()
		req, err := http.NewRequest(method, f.a.ts.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := f.a.ts.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != wantCode {
			t.Fatalf("%s %s = %d, want %d (%s)", method, path, resp.StatusCode, wantCode, raw)
		}
		if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Fatalf("%s %s content-type = %q, want application/json", method, path, ct)
		}
		var obj map[string]any
		if err := json.Unmarshal(raw, &obj); err != nil {
			t.Fatalf("%s %s body not JSON: %v (%s)", method, path, err, raw)
		}
	}
	// The delete response: a Status kubectl must decode.
	check(http.MethodDelete, groupPrefix+"/devices/cam-json", "", http.StatusOK)
	// A 404 must also be a JSON Status, not text/plain.
	check(http.MethodGet, groupPrefix+"/devices/cam-json", "", http.StatusNotFound)
}

// Discovery documents exist in the shapes APIService registration needs.
func TestDiscoveryEndpoints(t *testing.T) {
	f := newFleetFixture(t)
	for _, path := range []string{"/apis", "/apis/" + apiGroup, groupPrefix} {
		code, raw := doReq(t, f.a.ts, http.MethodGet, path, "")
		if code != http.StatusOK {
			t.Fatalf("GET %s = %d", path, code)
		}
		if path == groupPrefix && !strings.Contains(string(raw), `"deviceclaims"`) {
			t.Fatalf("resource list missing deviceclaims: %s", raw)
		}
	}
}

// A value that changes and changes back between update's read and its
// write must not let the update through. The sequence moved on, so the
// resourceVersion the client read is stale.
func TestUpdateDetectsABA(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# An update MUST use a compare-and-set on the resourceVersion the client supplied.
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# An update whose compare-and-set fails MUST return a conflict.
	f := newFleetFixture(t)
	fs := f.a.api.store
	ctx := context.Background()

	a := []byte(`{"metadata":{"name":"cam-aba"},"spec":{"zone":"eu-west"}}`)
	b := []byte(`{"metadata":{"name":"cam-aba"},"spec":{"zone":"us-east"}}`)
	_, rv, err := fs.create(ctx, "devices", "cam-aba", a)
	if err != nil {
		t.Fatal(err)
	}

	// In the gap after the read, another writer moves the value from A to
	// B and back to A.
	fs.afterRead = func() {
		fs.afterRead = nil
		_, rvB, err := fs.update(ctx, "devices", "cam-aba", b, rv)
		if err != nil {
			t.Fatalf("A to B: %v", err)
		}
		if _, _, err := fs.update(ctx, "devices", "cam-aba", a, rvB); err != nil {
			t.Fatalf("B to A: %v", err)
		}
	}
	c := []byte(`{"metadata":{"name":"cam-aba"},"spec":{"zone":"ap-south"}}`)
	if _, _, err := fs.update(ctx, "devices", "cam-aba", c, rv); !errors.Is(err, errConflict) {
		t.Fatalf("update at the first resourceVersion after A-B-A = %v, want conflict", err)
	}
	if got := getDevice(t, f.a, "cam-aba"); got.Spec.Zone != "eu-west" {
		t.Fatalf("zone = %q, want eu-west", got.Spec.Zone)
	}
}

// An update that lands between delete's read and its tombstone must make
// the delete fail with a conflict. The DELETE handler takes no
// precondition, so the caller re-reads and decides again.
func TestDeleteConflictsWithConcurrentUpdate(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A delete MUST tombstone the object register before it removes the name from the index register.
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# The extension server MUST re-read an object before it retries a write that returned a conflict.
	f := newFleetFixture(t)
	fs := f.a.api.store
	ctx := context.Background()

	_, rv, err := fs.create(ctx, "devices", "cam-del", []byte(`{"metadata":{"name":"cam-del"},"spec":{"zone":"eu-west"}}`))
	if err != nil {
		t.Fatal(err)
	}
	fs.afterRead = func() {
		fs.afterRead = nil
		next := []byte(`{"metadata":{"name":"cam-del"},"spec":{"zone":"us-east"}}`)
		if _, _, err := fs.update(ctx, "devices", "cam-del", next, rv); err != nil {
			t.Fatalf("concurrent update: %v", err)
		}
	}
	if err := fs.delete(ctx, "devices", "cam-del"); !errors.Is(err, errConflict) {
		t.Fatalf("delete across a concurrent update = %v, want conflict", err)
	}
	if got := getDevice(t, f.a, "cam-del"); got.Spec.Zone != "us-east" {
		t.Fatalf("zone = %q, want the concurrent update us-east", got.Spec.Zone)
	}

	// The caller re-reads, sees the new value, and deletes again.
	if err := fs.delete(ctx, "devices", "cam-del"); err != nil {
		t.Fatalf("delete after a fresh read: %v", err)
	}
	if code, _ := doReq(t, f.a.ts, http.MethodGet, groupPrefix+"/devices/cam-del", ""); code != http.StatusNotFound {
		t.Fatalf("get after delete = %d, want 404", code)
	}
}
