package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/lease"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/store"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

var (
	devicesGVR = v1alpha1.SchemeGroupVersion.WithResource("devices")
	claimsGVR  = v1alpha1.SchemeGroupVersion.WithResource("deviceclaims")
)

// The fake kube-apiserver knows two tokens. alice may do anything. bob is
// authenticated but may do nothing.
const (
	aliceToken = "alice-token"
	bobToken   = "bob-token"
)

// fakeKubeAPIServer answers the TokenReview and SubjectAccessReview calls
// of the delegated authentication and authorization. It records the users
// it was asked to authorize.
type fakeKubeAPIServer struct {
	mu    sync.Mutex
	asked []string
}

func (f *fakeKubeAPIServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var review map[string]any
	if err := json.Unmarshal(body, &review); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	spec, _ := review["spec"].(map[string]any)
	switch r.URL.Path {
	case "/apis/authentication.k8s.io/v1/tokenreviews":
		status := map[string]any{"authenticated": false}
		switch spec["token"] {
		case aliceToken:
			status = map[string]any{"authenticated": true, "user": map[string]any{"username": "alice"}}
		case bobToken:
			status = map[string]any{"authenticated": true, "user": map[string]any{"username": "bob"}}
		}
		review["status"] = status
	case "/apis/authorization.k8s.io/v1/subjectaccessreviews":
		user, _ := spec["user"].(string)
		f.mu.Lock()
		f.asked = append(f.asked, user)
		f.mu.Unlock()
		review["status"] = map[string]any{"allowed": user == "alice"}
	default:
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(review)
}

// askedAbout reports whether the fake was asked to authorize user.
func (f *fakeKubeAPIServer) askedAbout(user string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.asked {
		if u == user {
			return true
		}
	}
	return false
}

// testServer is a generic server over an in-memory cask, with its claim
// controller.
type testServer struct {
	url    string
	claims *claimController
	kube   *fakeKubeAPIServer
}

// startTestServer starts the generic server for cluster over a
// one-acceptor cask. With delegated set, it delegates authentication and
// authorization to a fake kube-apiserver.
func startTestServer(t *testing.T, cluster string, delegated bool) *testServer {
	t.Helper()
	return startTestServerWith(t, cluster, delegated, nil)
}

// startTestServerWith is startTestServer with a hook that changes the
// server options before the server starts. A nil hook changes nothing.
func startTestServerWith(t *testing.T, cluster string, delegated bool, hook func(*serverOptions)) *testServer {
	t.Helper()
	acc := caspaxos.NewAcceptor(store.NewMem())
	prop := caspaxos.NewProposer(1, []caspaxos.AcceptorClient{acc})
	kv := mvcc.New(prop, hlc.New(func() int64 { return time.Now().UnixNano() }), 1)
	sessions := lease.NewSessions(prop, func() int64 { return time.Now().UnixNano() })
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	stores := fleetStores(kv)
	ts := &testServer{claims: &claimController{
		cluster:  cluster,
		store:    storageClaims{devices: stores["devices"], claims: stores["deviceclaims"]},
		sessions: sessions,
		locks:    lease.NewLocks(prop, sessions),
		log:      log,
	}}

	opts := newServerOptions()
	opts.delegatedAuth = delegated
	rec := opts.recommended
	// No kube-apiserver backs the core informers, so the test runs without
	// admission and without priority and fairness.
	rec.CoreAPI = nil
	rec.Admission = nil
	rec.Features.EnablePriorityAndFairness = false
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rec.SecureServing.Listener = ln
	rec.SecureServing.BindPort = ln.Addr().(*net.TCPAddr).Port
	if delegated {
		ts.kube = &fakeKubeAPIServer{}
		kube := httptest.NewServer(ts.kube)
		t.Cleanup(kube.Close)
		kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
		cfg := fmt.Sprintf(`apiVersion: v1
kind: Config
clusters: [{name: kube, cluster: {server: %q}}]
users: [{name: cask, user: {token: cask}}]
contexts: [{name: kube, context: {cluster: kube, user: cask}}]
current-context: kube
`, kube.URL)
		if err := os.WriteFile(kubeconfig, []byte(cfg), 0o600); err != nil {
			t.Fatal(err)
		}
		rec.Authentication.RemoteKubeConfigFile = kubeconfig
		rec.Authentication.SkipInClusterLookup = true
		rec.Authorization.RemoteKubeConfigFile = kubeconfig
	}

	if hook != nil {
		hook(opts)
	}
	srv, err := opts.newFleetServer(cluster, stores, ts.claims.release)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.PrepareRun().RunWithContext(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	ts.url = "https://" + ln.Addr().String()

	// /readyz needs no authorization, but a server that has not started
	// refuses the connection.
	hc := &http.Client{Transport: insecureTransport(), Timeout: time.Second}
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := hc.Get(ts.url + "/readyz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return ts
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("server not ready: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func insecureTransport() http.RoundTripper {
	cfg := &rest.Config{TLSClientConfig: rest.TLSClientConfig{Insecure: true}}
	rt, err := rest.TransportFor(cfg)
	if err != nil {
		panic(err)
	}
	return rt
}

// client returns a dynamic client that presents token. An empty token
// sends no credentials.
func (ts *testServer) client(t *testing.T, token string) dynamic.Interface {
	t.Helper()
	dc, err := dynamic.NewForConfig(&rest.Config{
		Host:            ts.url,
		BearerToken:     token,
		TLSClientConfig: rest.TLSClientConfig{Insecure: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	return dc
}

func device(name, model string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "fleet.cask.dev/v1alpha1",
		"kind":       "Device",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"model": model},
	}}
}

func claim(name, dev string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "fleet.cask.dev/v1alpha1",
		"kind":       "DeviceClaim",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"deviceName": dev, "ttlSeconds": int64(30)},
	}}
}

// nextEvent returns the next watch event, or fails after five seconds.
func nextEvent(t *testing.T, w watch.Interface) watch.Event {
	t.Helper()
	select {
	case ev, ok := <-w.ResultChan():
		if !ok {
			t.Fatal("watch ended")
		}
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("no watch event within 5s")
	}
	return watch.Event{}
}

// expectEvent reads the next event and checks its type and object name.
// It returns the resourceVersion of the object.
func expectEvent(t *testing.T, w watch.Interface, typ watch.EventType, name string) string {
	t.Helper()
	ev := nextEvent(t, w)
	u, ok := ev.Object.(*unstructured.Unstructured)
	if !ok {
		t.Fatalf("event %s carries %T: %v", ev.Type, ev.Object, ev.Object)
	}
	if ev.Type != typ || u.GetName() != name {
		t.Fatalf("event = %s %s, want %s %s", ev.Type, u.GetName(), typ, name)
	}
	return u.GetResourceVersion()
}

func TestGenericServerServesDevices(t *testing.T) {
	//= docs/spec/fleet.md#2-resources
	//= type=test
	//# The extension server MUST be built on the generic server in k8s.io/apiserver.
	//= docs/spec/fleet.md#4-list-and-watch
	//= type=test
	//# A watch that resumes from the resourceVersion of any event MUST NOT skip or replay a change.
	ts := startTestServer(t, "east", false)
	ctx := context.Background()
	devices := ts.client(t, "").Resource(devicesGVR)

	created, err := devices.Create(ctx, device("gpu-1", "h100"), metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// The registry fills in the metadata that the legacy mux dropped.
	if created.GetUID() == "" || created.GetCreationTimestamp().Time.IsZero() || created.GetResourceVersion() == "" {
		t.Fatalf("created device lacks uid, creationTimestamp, or resourceVersion: %v", created.Object["metadata"])
	}
	if phase, _, _ := unstructured.NestedString(created.Object, "status", "phase"); phase != v1alpha1.DeviceAvailable {
		t.Fatalf("new device phase = %q, want %q", phase, v1alpha1.DeviceAvailable)
	}
	got, err := devices.Get(ctx, "gpu-1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetUID() != created.GetUID() || got.GetResourceVersion() != created.GetResourceVersion() {
		t.Fatalf("get = uid %s rv %s, want uid %s rv %s",
			got.GetUID(), got.GetResourceVersion(), created.GetUID(), created.GetResourceVersion())
	}

	list, err := devices.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || list.Items[0].GetName() != "gpu-1" || list.GetResourceVersion() == "" {
		t.Fatalf("list = %d items at rv %q, want gpu-1", len(list.Items), list.GetResourceVersion())
	}

	w, err := devices.Watch(ctx, metav1.ListOptions{ResourceVersion: list.GetResourceVersion()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := devices.Create(ctx, device("gpu-2", "a100"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	addedRV := expectEvent(t, w, watch.Added, "gpu-2")
	got.Object["spec"] = map[string]any{"model": "h200"}
	updated, err := devices.Update(ctx, got, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	expectEvent(t, w, watch.Modified, "gpu-1")
	w.Stop()

	// A watch that resumes from the ADDED event sees the update once and
	// does not replay the create.
	w, err = devices.Watch(ctx, metav1.ListOptions{ResourceVersion: addedRV})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	if rv := expectEvent(t, w, watch.Modified, "gpu-1"); rv != updated.GetResourceVersion() {
		t.Fatalf("resumed MODIFIED at rv %s, want %s", rv, updated.GetResourceVersion())
	}

	// An update from the old resourceVersion is a conflict.
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# An update whose compare-and-set fails MUST return a conflict.
	got.Object["spec"] = map[string]any{"model": "stale"}
	if _, err := devices.Update(ctx, got, metav1.UpdateOptions{}); !apierrors.IsConflict(err) {
		t.Fatalf("stale update err = %v, want 409 Conflict", err)
	}

	// A field selector on the name filters the list.
	sel, err := devices.List(ctx, metav1.ListOptions{FieldSelector: "metadata.name=gpu-2"})
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Items) != 1 || sel.Items[0].GetName() != "gpu-2" {
		t.Fatalf("field selector list = %d items, want gpu-2 only", len(sel.Items))
	}

	if err := devices.Delete(ctx, "gpu-1", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	expectEvent(t, w, watch.Deleted, "gpu-1")
	if _, err := devices.Get(ctx, "gpu-1", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("get after delete err = %v, want 404", err)
	}
}

func TestGenericServerDelegatesAuth(t *testing.T) {
	//= docs/spec/fleet.md#2-resources
	//= type=test
	//# The extension server MUST delegate authentication and authorization to the local kube-apiserver.
	//= docs/spec/fleet.md#8-security
	//= type=test
	//# The cask client API and control endpoints MUST NOT be reachable outside the pod without authentication.
	ts := startTestServer(t, "east", true)
	ctx := context.Background()

	// No credentials: the request is anonymous, and the kube-apiserver
	// does not allow it.
	_, err := ts.client(t, "").Resource(devicesGVR).List(ctx, metav1.ListOptions{})
	if !apierrors.IsForbidden(err) {
		t.Fatalf("anonymous list err = %v, want 403 Forbidden", err)
	}
	if !ts.kube.askedAbout("system:anonymous") {
		t.Fatal("the server did not ask the kube-apiserver about the anonymous user")
	}
	// A token the kube-apiserver does not know.
	_, err = ts.client(t, "bad-token").Resource(devicesGVR).List(ctx, metav1.ListOptions{})
	if !apierrors.IsUnauthorized(err) {
		t.Fatalf("bad token list err = %v, want 401 Unauthorized", err)
	}
	// A known user without permission.
	_, err = ts.client(t, bobToken).Resource(devicesGVR).Create(ctx, device("gpu-1", "h100"), metav1.CreateOptions{})
	if !apierrors.IsForbidden(err) {
		t.Fatalf("bob create err = %v, want 403 Forbidden", err)
	}
	// A known user with permission.
	if _, err := ts.client(t, aliceToken).Resource(devicesGVR).Create(ctx, device("gpu-1", "h100"),
		metav1.CreateOptions{}); err != nil {
		t.Fatalf("alice create: %v", err)
	}
}

func TestGenericServerClaimLifecycle(t *testing.T) {
	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# Deleting a Bound claim MUST release the object's lock.
	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# A Bound claim MUST carry its fence in its status.
	ts := startTestServer(t, "east", false)
	ctx := context.Background()
	dc := ts.client(t, "")
	devices, claims := dc.Resource(devicesGVR), dc.Resource(claimsGVR)

	if _, err := devices.Create(ctx, device("gpu-7", "h100"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	c, err := claims.Create(ctx, claim("train", "gpu-7"), metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if phase, cluster := claimPhase(c); phase != v1alpha1.ClaimPending || cluster != "east" {
		t.Fatalf("new claim = %s in %s, want Pending in east", phase, cluster)
	}
	// A claim needs a device name.
	if _, err := claims.Create(ctx, claim("empty", ""), metav1.CreateOptions{}); !apierrors.IsInvalid(err) {
		t.Fatalf("claim without deviceName err = %v, want 422 Invalid", err)
	}
	if _, err := claims.Create(ctx, claim("render", "gpu-7"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	ts.claims.reconcileOnce(ctx)
	train := mustGet(t, claims, "train")
	render := mustGet(t, claims, "render")
	// The reconcile visits the claims in name order, so render binds.
	if phase, _ := claimPhase(render); phase != v1alpha1.ClaimBound {
		t.Fatalf("render = %s, want Bound", phase)
	}
	if phase, _ := claimPhase(train); phase != v1alpha1.ClaimPending {
		t.Fatalf("train = %s, want Pending", phase)
	}
	fence, _, _ := unstructured.NestedInt64(render.Object, "status", "fence")
	if fence == 0 {
		t.Fatal("the Bound claim carries no fence")
	}
	lease, _, _ := unstructured.NestedMap(mustGet(t, devices, "gpu-7").Object, "status", "lease")
	if lease["claim"] != "render" {
		t.Fatalf("device lease = %v, want claim render", lease)
	}

	// A user update of the claim does not touch its status.
	render.Object["status"] = map[string]any{"phase": "Lost"}
	render.SetLabels(map[string]string{"team": "vision"})
	after, err := claims.Update(ctx, render, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if phase, _ := claimPhase(after); phase != v1alpha1.ClaimBound {
		t.Fatalf("user update changed the status to %s", phase)
	}

	// Deleting the Bound claim releases the lock, so train binds next.
	if err := claims.Delete(ctx, "render", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	ts.claims.reconcileOnce(ctx)
	train = mustGet(t, claims, "train")
	if phase, _ := claimPhase(train); phase != v1alpha1.ClaimBound {
		t.Fatalf("train after release = %s, want Bound", phase)
	}
	if next, _, _ := unstructured.NestedInt64(train.Object, "status", "fence"); next <= fence {
		t.Fatalf("fence after handover = %d, want above %d", next, fence)
	}
}

func claimPhase(u *unstructured.Unstructured) (phase, cluster string) {
	phase, _, _ = unstructured.NestedString(u.Object, "status", "phase")
	cluster, _, _ = unstructured.NestedString(u.Object, "status", "cluster")
	return phase, cluster
}

func mustGet(t *testing.T, r dynamic.ResourceInterface, name string) *unstructured.Unstructured {
	t.Helper()
	u, err := r.Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// deviceFences returns the lease fence (0 without a lease) and the
// lastFence of a device.
func deviceFences(t *testing.T, r dynamic.ResourceInterface, name string) (lease, last int64, u *unstructured.Unstructured) {
	t.Helper()
	u = mustGet(t, r, name)
	lease, _, _ = unstructured.NestedInt64(u.Object, "status", "lease", "fence")
	last, _, _ = unstructured.NestedInt64(u.Object, "status", "lastFence")
	return lease, last, u
}

// The #206 rules hold on the generic server path: a bind advertises its
// fence in lastFence, a delete keeps it there before the claim goes, a
// zombie's release does not clear a successor's lease, and no client
// write lowers lastFence or the lease fence.
func TestGenericServerKeepsLastFence(t *testing.T) {
	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# A Device status MUST keep in its lastFence field the highest fence that the Device has advertised.
	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# Deleting a claim MUST NOT complete before the lastFence of its Device is at least the claim's fence.
	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# A write to a Device MUST NOT lower its lastFence.
	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# A status write MUST NOT lower an advertised fence.
	ts := startTestServer(t, "east", false)
	ctx := context.Background()
	dc := ts.client(t, "")
	devices, claims := dc.Resource(devicesGVR), dc.Resource(claimsGVR)

	if _, err := devices.Create(ctx, device("lidar-9", "v1"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := claims.Create(ctx, claim("job-1", "lidar-9"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	ts.claims.reconcileOnce(ctx)
	if lease, last, _ := deviceFences(t, devices, "lidar-9"); lease != 1 || last != 1 {
		t.Fatalf("after bind: lease fence %d, lastFence %d; want 1 and 1", lease, last)
	}
	// The delete keeps the fence in lastFence and clears the lease.
	if err := claims.Delete(ctx, "job-1", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if lease, last, u := deviceFences(t, devices, "lidar-9"); lease != 0 || last != 1 {
		t.Fatalf("after delete: lease fence %d, lastFence %d, status %v; want no lease, lastFence 1",
			lease, last, u.Object["status"])
	}

	// job-2 binds at fence 2. A zombie release at fence 1, from a claim
	// that still shows Bound, must not clear it.
	if _, err := claims.Create(ctx, claim("job-2", "lidar-9"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	ts.claims.reconcileOnce(ctx)
	zombie := &v1alpha1.DeviceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "job-1"},
		Spec:       v1alpha1.DeviceClaimSpec{DeviceName: "lidar-9"},
		Status:     v1alpha1.DeviceClaimStatus{Phase: v1alpha1.ClaimBound, Cluster: "east", Fence: 1},
	}
	if err := ts.claims.release(ctx, zombie); err != nil {
		t.Fatal(err)
	}
	lease, last, u := deviceFences(t, devices, "lidar-9")
	if lease != 2 || last != 2 {
		t.Fatalf("after zombie release: lease fence %d, lastFence %d; want 2 and 2", lease, last)
	}

	// A client update of the device keeps its status.
	u.Object["spec"] = map[string]any{"model": "v2"}
	u.Object["status"] = map[string]any{"phase": "Available"}
	updated, err := devices.Update(ctx, u, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if l, _, _ := unstructured.NestedInt64(updated.Object, "status", "lastFence"); l != 2 {
		t.Fatalf("client update changed lastFence to %d", l)
	}

	// A client status write may not lower the lease fence or drop the
	// lease.
	_, _, u = deviceFences(t, devices, "lidar-9")
	_ = unstructured.SetNestedField(u.Object, int64(1), "status", "lease", "fence")
	if _, err := devices.UpdateStatus(ctx, u, metav1.UpdateOptions{}); !apierrors.IsInvalid(err) {
		t.Fatalf("status write at a lower lease fence: err = %v, want 422 Invalid", err)
	}
	_, _, u = deviceFences(t, devices, "lidar-9")
	unstructured.RemoveNestedField(u.Object, "status", "lease")
	if _, err := devices.UpdateStatus(ctx, u, metav1.UpdateOptions{}); !apierrors.IsInvalid(err) {
		t.Fatalf("status write that drops the lease: err = %v, want 422 Invalid", err)
	}

	// Once the lease is gone, a status write that drops lastFence keeps it.
	if err := claims.Delete(ctx, "job-2", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	_, _, u = deviceFences(t, devices, "lidar-9")
	unstructured.RemoveNestedField(u.Object, "status", "lastFence")
	if _, err := devices.UpdateStatus(ctx, u, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if lease, last, _ := deviceFences(t, devices, "lidar-9"); lease != 0 || last != 2 {
		t.Fatalf("after a status write without lastFence: lease fence %d, lastFence %d; want none and 2", lease, last)
	}
}
