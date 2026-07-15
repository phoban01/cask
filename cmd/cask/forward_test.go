package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/phoban01/cask/internal/agent"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/lease"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/owner"
	"github.com/phoban01/cask/internal/placement"
	"github.com/phoban01/cask/internal/ranges"
	"github.com/phoban01/cask/internal/roster"
	"github.com/phoban01/cask/internal/store"
)

type fixedBook map[uint64]string

func (b fixedBook) addr(node uint64) (string, bool) {
	a, ok := b[node]
	return a, ok
}

// node is one cask node's client-API stack over the shared acceptors.
type node struct {
	id     uint64
	mgr    *owner.Manager
	kv     *mvcc.KV
	srv    *server
	router *agent.Router
}

// twoNodeFixture builds the M7 shape: node A is the HRW owner hint of the
// single range; node B is another replica whose server forwards to A.
type twoNodeFixture struct {
	now        atomic.Int64
	val        roster.Value
	rmap       *ranges.Map
	dialer     agent.StaticDialer
	a, b       *node
	aSrv       *httptest.Server
	ttl, skew  int64
	sessionsAt func(self uint64) (*lease.Sessions, *lease.Locks)
}

func newTwoNodeFixture(t *testing.T) *twoNodeFixture {
	t.Helper()
	f := &twoNodeFixture{ttl: 1_000_000, skew: 1_000}
	f.now.Store(1_000)
	clock := func() int64 { return f.now.Load() }

	members := []roster.Member{{NodeID: 0}, {NodeID: 1}, {NodeID: 2}}
	f.val = roster.Value{Epoch: 1, Members: members, Core: []uint64{0, 1, 2}}
	f.rmap = placeRange(f.val)
	d, ok := f.rmap.Lookup([]byte("k"))
	if !ok {
		t.Fatal("no range")
	}
	hint, ok := placement.Owner(ranges.RangeKey(d.ID), d.Replicas)
	if !ok {
		t.Fatal("no hint")
	}
	var other uint64
	for _, r := range d.Replicas {
		if r != hint {
			other = r
			break
		}
	}

	f.dialer = agent.StaticDialer{}
	for i := range 3 {
		f.dialer[uint64(i)] = caspaxos.NewAcceptor(store.NewMem())
	}
	f.sessionsAt = func(self uint64) (*lease.Sessions, *lease.Locks) {
		ctl := caspaxos.NewProposer(self, []caspaxos.AcceptorClient{f.dialer[0], f.dialer[1], f.dialer[2]})
		s := lease.NewSessions(ctl, clock)
		return s, lease.NewLocks(ctl, s)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	mk := func(self uint64, fwd *forwarder) *node {
		sess, locks := f.sessionsAt(self)
		mgr := owner.New(self, f.dialer, sess, locks,
			owner.WithClock(clock), owner.WithSessionTTL(f.ttl), owner.WithMaxOffset(f.skew))
		router := agent.NewRouter(self, f.rmap, f.dialer, agent.WithFastPath(mgr))
		kv := mvcc.New(router, hlc.New(func() int64 { return f.now.Add(1) }), self, mvcc.WithLocalReader(mgr))
		return &node{
			id: self, mgr: mgr, kv: kv, router: router,
			srv: &server{kv: kv, sessions: sess, locks: locks, fwd: fwd, log: log},
		}
	}

	// Node A (the hint) serves its API over HTTP so B can forward to it.
	f.a = mk(hint, nil)
	muxA := http.NewServeMux()
	muxA.HandleFunc("/kv/", f.a.srv.handleKV)
	muxA.HandleFunc("/cas/", f.a.srv.handleCAS)
	f.aSrv = httptest.NewServer(muxA)
	t.Cleanup(f.aSrv.Close)
	if err := f.a.mgr.Maintain(context.Background(), f.rmap); err != nil {
		t.Fatalf("A maintain: %v", err)
	}

	// Node B forwards to A's address.
	snapB := newRosterSnap(other)
	snapB.store(f.val)
	book := fixedBook{hint: strings.TrimPrefix(f.aSrv.URL, "http://")}
	fwdB := newForwarder(other, f.aSrv.Client(), book, snapB, log)
	f.b = mk(other, fwdB)
	return f
}

func do(t *testing.T, h http.HandlerFunc, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec
}

// A write entering node B reaches the owner A (one hop), lands on A's fast
// path, and A's zero-RTT read cache stays coherent — the cluster-wide
// writes-via-owner discipline M7 exists to provide.
func TestForwardWriteReachesOwner(t *testing.T) {
	f := newTwoNodeFixture(t)

	if rec := do(t, f.b.srv.handleKV, http.MethodPut, "/kv/k", "v1"); rec.Code != http.StatusNoContent {
		t.Fatalf("PUT via B = %d %s", rec.Code, rec.Body)
	}
	if s := f.a.mgr.Stats(); s.FastWrites == 0 {
		t.Fatal("write did not reach A's fast path (not forwarded?)")
	}

	// A serves the value from its lease-guarded cache; B's GET forwards too.
	got, found, err := f.a.kv.Get(context.Background(), []byte("k"))
	if err != nil || !found || string(got) != "v1" {
		t.Fatalf("A read = %q found=%v err=%v", got, found, err)
	}
	if rec := do(t, f.b.srv.handleKV, http.MethodGet, "/kv/k", ""); rec.Code != http.StatusOK || rec.Body.String() != "v1" {
		t.Fatalf("GET via B = %d %q", rec.Code, rec.Body)
	}
}

// When the owner is unreachable, a write must NOT silently run around its
// live read lease: the local path's gate turns it into a retryable 503.
// Once the owner's session lapses past the MaxOffset margin, the local full
// path proceeds.
func TestUnreachableOwnerGatesThenLapses(t *testing.T) {
	f := newTwoNodeFixture(t)

	if rec := do(t, f.b.srv.handleKV, http.MethodPut, "/kv/k", "v1"); rec.Code != http.StatusNoContent {
		t.Fatalf("seed PUT via B = %d %s", rec.Code, rec.Body)
	}

	// Owner becomes unreachable (its API server stops answering).
	f.aSrv.Close()

	rec := do(t, f.b.srv.handleKV, http.MethodPut, "/kv/k", "v2")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("PUT with live-but-unreachable owner = %d %s, want 503", rec.Code, rec.Body)
	}

	// The owner's session lapses; past the read margin the write may proceed
	// locally (and fences the dead owner's epoch as a side effect).
	f.now.Store(1_000 + f.ttl + f.skew + 1)
	if rec := do(t, f.b.srv.handleKV, http.MethodPut, "/kv/k", "v2"); rec.Code != http.StatusNoContent {
		t.Fatalf("PUT after lease lapse = %d %s", rec.Code, rec.Body)
	}
	got, found, err := f.b.kv.Get(context.Background(), []byte("k"))
	if err != nil || !found || string(got) != "v2" {
		t.Fatalf("B read = %q found=%v err=%v, want v2", got, found, err)
	}
}

// A forwarded request is never re-forwarded (loop guard): a node that gets a
// forwarded write for a range it does not own handles it locally under the
// gate rather than bouncing it back.
func TestForwardedRequestNotReforwarded(t *testing.T) {
	f := newTwoNodeFixture(t)

	req := httptest.NewRequest(http.MethodPut, "/kv/k", strings.NewReader("v"))
	req.Header.Set(forwardedHeader, "1")
	rec := httptest.NewRecorder()
	f.b.srv.handleKV(rec, req)
	// B does not own and A's lease is live: the gate refuses locally (503) —
	// crucially it did NOT proxy (no second hop), which would loop.
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("forwarded PUT on non-owner = %d %s, want 503 (handled locally, gated)", rec.Code, rec.Body)
	}
}
