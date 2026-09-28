package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/ranges"
	"github.com/phoban01/cask/internal/roster"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/internal/transport"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
)

// acceptRoster makes acc accept a roster value with ConfigGen gen, the way
// a roster round does.
func acceptRoster(t *testing.T, acc caspaxos.AcceptorClient, counter, gen uint64) {
	t.Helper()
	ctx := context.Background()
	b := caspaxos.Ballot{Counter: counter, NodeID: 9}
	if r, err := acc.Prepare(ctx, roster.Key, b); err != nil || !r.Promised {
		t.Fatalf("roster prepare: %+v, %v", r, err)
	}
	val := fmt.Appendf(nil, `{"epoch":1,"core":[1],"cfg_gen":%d}`, gen)
	if r, err := acc.Accept(ctx, roster.Key, b, val); err != nil || !r.Accepted {
		t.Fatalf("roster accept: %+v, %v", r, err)
	}
}

// The fence reads the roster value the acceptor has accepted: durable
// state, not a member's view. It holds on the local path and on the wire,
// and a fresh acceptor over the same store enforces it at once, as after a
// restart.
func TestVoterRejectsWriteFromOlderCore(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# A voter MUST reject a data write that names an older core configuration than the roster value it has accepted.
	st := store.NewMem()
	fence := newFencedAcceptor(caspaxos.NewAcceptor(st), st)
	acceptRoster(t, fence, 1, 5)

	// A restarted voter: a new acceptor over the same store.
	restarted := newFencedAcceptor(caspaxos.NewAcceptor(st), st)
	mux := http.NewServeMux()
	mux.Handle(transport.ConnectHandler(restarted))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	remote := &fencedClient{AcceptorClient: transport.NewConnectClient(srv.URL, srv.Client())}
	local := &fencedClient{AcceptorClient: fence}

	ctx := context.Background()
	key := []byte("fleet/devices/d")
	for name, c := range map[string]caspaxos.AcceptorClient{"local": local, "remote": remote} {
		b := caspaxos.Ballot{Counter: 10, NodeID: 2}
		stale := ranges.WithClaimedEpoch(ctx, 4)
		if _, err := c.Prepare(stale, key, b); !errors.Is(err, caspaxos.ErrRangeChanged) {
			t.Errorf("%s: prepare from an older core: err = %v, want ErrRangeChanged", name, err)
		}
		if _, err := c.Prepare(ctx, key, b); !errors.Is(err, caspaxos.ErrRangeChanged) {
			t.Errorf("%s: prepare that names no core: err = %v, want ErrRangeChanged", name, err)
		}
		if _, err := c.Accept(stale, key, b, []byte("v")); !errors.Is(err, errStaleAccept) {
			t.Errorf("%s: accept from an older core: err = %v, want a missing vote", name, err)
		}
		if _, err := c.Prepare(ranges.WithClaimedEpoch(ctx, 5), key, b); err != nil {
			t.Errorf("%s: prepare from the current core: %v", name, err)
		}
	}
	// The roster key has its own guard and passes without a claim.
	acceptRoster(t, local, 20, 6)
}

// A key listing reads the fence and the keys in one step. A data write that
// passed the fence is in the listing, or it runs after the listing and
// meets the new fence.
func TestKeyListingIsAtomicWithTheFence(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# A voter MUST NOT list its data keys while a data write that passed its fence is still in progress.
	st := store.NewMem()
	fence := newFencedAcceptor(caspaxos.NewAcceptor(st), st)
	acceptRoster(t, fence, 1, 1)
	ctx := ranges.WithClaimedEpoch(context.Background(), 1)

	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := map[string]bool{}
	for i := range 200 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Appendf(nil, "fleet/devices/k%03d", i)
			b := caspaxos.Ballot{Counter: 1, NodeID: 2}
			if _, err := fence.Prepare(ctx, key, b); err != nil {
				return
			}
			if r, err := fence.Accept(ctx, key, b, []byte("v")); err == nil && r.Accepted {
				mu.Lock()
				accepted[string(key)] = true
				mu.Unlock()
			}
		}(i)
		if i == 100 {
			acceptRoster(t, fence, 2, 2) // the joint value lands mid-stream
		}
	}
	// List while writes still run.
	l, err := fence.listKeys(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if l.Gen != 2 {
		t.Fatalf("listing gen = %d, want 2", l.Gen)
	}
	listed := map[string]bool{}
	for _, k := range l.Keys {
		if slices.Equal(k, roster.Key) {
			t.Fatal("the listing has the roster key")
		}
		listed[string(k)] = true
	}
	for k := range accepted {
		if !listed[k] {
			t.Errorf("key %s was accepted under claim 1 after a listing that saw gen 2", k)
		}
	}
}

// serveListing answers the keys endpoint with a fixed listing.
func serveListing(t *testing.T, l keyListing) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(l)
	}))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

// The key listing counts only old voters that hold the joint roster value.
// A voter that does not may still take a stale write after the listing, so
// its keys cannot stand in for a majority.
func TestKeyListingSkipsVotersWithoutTheJointValue(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# A core change MUST list data keys only on old voters that have accepted the joint roster value.
	m, _ := newTestMemberServer(t, 1, true, nil, time.Second)
	acceptRoster(t, m.fence, 1, 2)
	ctx := ranges.WithClaimedEpoch(context.Background(), 2)
	b := caspaxos.Ballot{Counter: 1, NodeID: 1}
	if _, err := m.fence.Prepare(ctx, []byte("z"), b); err != nil {
		t.Fatal(err)
	}
	if _, err := m.fence.Accept(ctx, []byte("z"), b, []byte("v")); err != nil {
		t.Fatal(err)
	}
	behind := serveListing(t, keyListing{Gen: 1, Keys: [][]byte{[]byte("x")}})
	fresh := serveListing(t, keyListing{Gen: 2, Keys: [][]byte{[]byte("y")}})
	v := roster.Value{ConfigGen: 2, Joint: &roster.Joint{Old: []uint64{1, 2, 3}, New: []uint64{1, 2, 3, 4, 5}}}

	m.learn([]roster.Member{{NodeID: 2, Addr: behind}, {NodeID: 3, Addr: fresh}})
	keys, err := m.listOnce(context.Background(), v)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, k := range keys {
		got = append(got, string(k))
	}
	slices.Sort(got)
	if !slices.Equal(got, []string{"y", "z"}) {
		t.Fatalf("listed %v, want [y z]: voter 2 lacks the joint value", got)
	}

	// With only one voter holding the joint value, there is no majority.
	m.learn([]roster.Member{{NodeID: 3, Addr: behind}})
	if _, err := m.listOnce(context.Background(), v); err == nil {
		t.Fatal("listing succeeded with one of three old voters holding the joint value")
	}
}

// A write that a voter fenced did not apply, so the API answers 503 with
// Retry-After, not 500.
func TestStaleWriteGetsRetryableStatus(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# The extension server MUST answer a data write that a voter rejected as stale with a retryable status.
	rec := httptest.NewRecorder()
	httpStoreErr(rec, fmt.Errorf("update: %w", caspaxos.ErrRangeChanged))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("no Retry-After header")
	}
}

// The generic server answers a fenced write with 503 and Retry-After too.
func TestStaleWriteGetsRetryableStatusFromGenericServer(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# The extension server MUST answer a data write that a voter rejected as stale with a retryable status.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, groupPrefix+"/devices/gpu-1", nil)
	err := retryable(fmt.Errorf("update: %w", caspaxos.ErrRangeChanged))
	responsewriters.ErrorNegotiated(err, fleetCodecs, v1alpha1.SchemeGroupVersion, rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After = %q, want 1", rec.Header().Get("Retry-After"))
	}
	// Other errors pass through unchanged.
	other := errors.New("boom")
	if got := retryable(other); got != other {
		t.Fatalf("retryable(%v) = %v", other, got)
	}
}
