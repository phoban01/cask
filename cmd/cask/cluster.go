package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/phoban01/cask/internal/agent"
	"github.com/phoban01/cask/internal/backoff"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/lease"
	"github.com/phoban01/cask/internal/owner"
	"github.com/phoban01/cask/internal/placement"
	"github.com/phoban01/cask/internal/ranges"
	"github.com/phoban01/cask/internal/roster"
	"github.com/phoban01/cask/internal/transport"
	"github.com/phoban01/cask/internal/transport/nebula"
)

// rangeKey identifies the single full-keyspace range this binary serves.
// Range splitting exists in internal/ranges but is not yet driven from cmd/cask;
// the node hosts one range whose replica set is chosen by zone-aware placement.
var rangeKey = []byte("\x00range-0")

const replicationFactor = 3

// reconcileInterval is how often the node re-runs discovery and reconciles the
// consensus roster, re-placing the range if membership changed.
const reconcileInterval = 5 * time.Second

// join bootstrap pacing: how long a joining node waits to find a live cluster
// member to learn the current roster core from, and how often it retries.
const (
	joinTimeout = 60 * time.Second
	joinPoll    = 1 * time.Second
)

// nebulaCluster builds a self-forming consensus engine over a Nebula overlay.
//
// There is no static genesis member set: exactly one node (started with
// --bootstrap, so bootstrap==true) FOUNDS the roster as a single-node register;
// every other node JOINS by learning the current acceptor core from a live peer
// (its /roster snapshot) and adding itself. The register's core then grows and
// shrinks by joint-consensus reconfiguration driven from the reconcile loop, so
// membership is fully dynamic.
//
// disco surfaces candidate members for the reconcile loop (e.g. DNS-SRV); it may
// be nil. Returns a proposer for mvcc/lease, the overlay listener, the local
// member, a health monitor (nil for client-only), and a roster snapshot holder
// the caller serves at /roster so joining peers can learn the core.
func nebulaCluster(ctx context.Context, log *slog.Logger, configYAML string, caskPort int, local *caspaxos.Acceptor, clientOnly, bootstrap bool, disco roster.Discovery) (*dynamicProposer, net.Listener, roster.Member, *healthMonitor, *rosterSnap, error) {
	network, err := nebula.New(configYAML, log)
	if err != nil {
		return nil, nil, roster.Member{}, nil, nil, err
	}

	self, err := nebula.SelfMember(configYAML, caskPort)
	if err != nil {
		return nil, nil, roster.Member{}, nil, nil, err
	}
	// seeds are the lighthouse hosts (+self) — Nebula rendezvous points that are
	// also where a joining node looks first for a live cask member to learn the
	// core from. They are NOT auto-added to the roster (a lighthouse need not be a
	// cask member); querying one that isn't simply fails and is skipped.
	seeds, err := nebula.SeedMembers(configYAML, caskPort)
	if err != nil {
		return nil, nil, roster.Member{}, nil, nil, err
	}
	log.Info("nebula self-forming", "node", self.NodeID, "addr", self.Addr, "zone", self.Zone, "bootstrap", bootstrap, "clientOnly", clientOnly)

	dialer := newOverlayDialer(self.NodeID, local, network.HTTPClient())
	dialer.learn(seeds)
	dialer.learn([]roster.Member{self})

	// The roster proposes against its current acceptor core, resolved to overlay
	// acceptor clients at call time — so a reconfiguration automatically retargets.
	rost := roster.New(self.NodeID, func(groups [][]uint64) roster.Proposer {
		return proposerForGroups(self.NodeID, groups, dialer)
	})

	// candidates a joiner polls for the core: seeds plus whatever discovery finds.
	candidates := func() []roster.Member {
		out := append([]roster.Member(nil), seeds...)
		if disco != nil {
			if d, derr := disco.Discover(ctx); derr == nil {
				out = append(out, d...)
			}
		}
		return out
	}

	var val roster.Value
	switch {
	case bootstrap:
		v, err := rost.Founder(ctx, self)
		if err != nil {
			return nil, nil, roster.Member{}, nil, nil, fmt.Errorf("roster founder: %w", err)
		}
		val = v
		log.Info("founded roster", "node", self.NodeID)
	default:
		// joinCluster returns the snapshot it observed (with self in membership),
		// so the node never issues a startup consensus read that could race the
		// driver's reconfiguration.
		v, err := joinCluster(ctx, log, rost, dialer, network.HTTPClient(), self, candidates, !clientOnly)
		if err != nil {
			return nil, nil, roster.Member{}, nil, nil, fmt.Errorf("roster join: %w", err)
		}
		val = v
	}
	dialer.learn(val.Members)

	snap := newRosterSnap(self.NodeID)
	snap.store(val)

	dyn := &dynamicProposer{}

	// The ownership manager's control-plane traffic (session, range locks)
	// rides the same dynamic proposer it serves; the rown key exclusion in
	// FastPropose breaks the recursion. Client-only nodes are never replicas,
	// so HRW never selects them and they run no manager.
	var mgr *owner.Manager
	if !clientOnly {
		osess := lease.NewSessions(dyn, func() int64 { return time.Now().UnixNano() })
		olocks := lease.NewLocks(dyn, osess, lease.WithAcquireBackoff(contentionBackoff()))
		mgr = owner.New(self.NodeID, dialer, osess, olocks)
	}
	dyn.set(routerFor(self.NodeID, val, dialer, mgr))

	ln, err := network.Listen(ctx, fmt.Sprintf(":%d", caskPort))
	if err != nil {
		return nil, nil, roster.Member{}, nil, nil, fmt.Errorf("overlay listen: %w", err)
	}

	if clientOnly {
		go reconcileLoopReadOnly(ctx, log, rost, dialer, dyn, snap, candidates, network.HTTPClient(), val)
		return dyn, ln, self, nil, snap, nil
	}

	mon := newHealthMonitor(self.NodeID, network.HTTPClient())
	go reconcileLoop(ctx, log, rost, dialer, dyn, mon, snap, disco, candidates, network.HTTPClient(), mgr, val)
	return dyn, ln, self, mon, snap, nil
}

// isDriver reports whether self is the single node responsible for driving
// consensus roster changes: the highest-id current core member. Restricting
// consensus writes to one node prevents reconcile loops from dueling — CASPaxos
// reads are write-imposing, so many nodes reading/writing the register at once
// would starve the multi-round reconfiguration.
func isDriver(self uint64, core []uint64) bool {
	if len(core) == 0 {
		return false
	}
	hi := core[0]
	for _, id := range core[1:] {
		if id > hi {
			hi = id
		}
	}
	return self == hi
}

// joinCluster brings a new node into an existing cluster WITHOUT proposing to the
// register itself (which would duel with the driver). It polls candidate peers
// for a roster snapshot, adopts the current acceptor core, and — unless
// client-only — asks the driver to add it via /roster/join, retrying until it
// observes itself in the membership or the join deadline passes.
func joinCluster(ctx context.Context, log *slog.Logger, rost *roster.Roster, dialer *overlayDialer, hc *http.Client, self roster.Member, candidates func() []roster.Member, addSelf bool) (roster.Value, error) {
	ctx, cancel := context.WithTimeout(ctx, joinTimeout)
	defer cancel()
	for {
		cands := candidates()
		dialer.learn(cands)
		if v, ok := fetchCoreHint(ctx, hc, cands); ok && len(v.Core) > 0 {
			dialer.learn(v.Members)
			rost.AdoptCore(v.Core)
			if !addSelf {
				return v, nil // client-only: tracking the core is enough
			}
			if containsMember(v.Members, self.NodeID) {
				log.Info("joined roster", "node", self.NodeID, "core", v.Core)
				return v, nil
			}
			requestJoin(ctx, hc, fetchTargets(cands, v.Members), self)
		}
		select {
		case <-ctx.Done():
			return roster.Value{}, fmt.Errorf("no live cluster member found to join (is a --bootstrap node up?): %w", ctx.Err())
		case <-time.After(joinPoll):
		}
	}
}

// requestJoin posts the member to peers' /roster/join. The current driver queues
// it; non-drivers reject with a redirect, which is harmless — posting to all
// targets each retry guarantees the driver eventually receives it, and the
// driver dedups by node id.
func requestJoin(ctx context.Context, hc *http.Client, targets []roster.Member, self roster.Member) {
	body, _ := json.Marshal(self)
	for _, m := range targets {
		if m.Addr == "" {
			continue
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+m.Addr+"/roster/join", bytes.NewReader(body))
		if err != nil {
			continue
		}
		if resp, err := hc.Do(req); err == nil {
			resp.Body.Close()
		}
	}
}

func containsMember(members []roster.Member, id uint64) bool {
	for _, m := range members {
		if m.NodeID == id {
			return true
		}
	}
	return false
}

// fetchCoreHint queries each candidate's /roster endpoint and returns the value
// with the highest ConfigGen — a non-consensus hint that lets a joiner discover
// the current acceptor core before it can read the register itself.
func fetchCoreHint(ctx context.Context, hc *http.Client, candidates []roster.Member) (roster.Value, bool) {
	var best roster.Value
	found := false
	for _, m := range candidates {
		if m.Addr == "" {
			continue
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+m.Addr+"/roster", nil)
		if err != nil {
			continue
		}
		resp, err := hc.Do(req)
		if err != nil {
			continue
		}
		var v roster.Value
		derr := json.NewDecoder(resp.Body).Decode(&v)
		resp.Body.Close()
		if derr != nil || v.ConfigGen == 0 {
			continue
		}
		// Prefer the freshest snapshot: higher config generation (newest core)
		// first, then higher epoch (newest membership). The driver holds the max.
		if !found || v.ConfigGen > best.ConfigGen || (v.ConfigGen == best.ConfigGen && v.Epoch > best.Epoch) {
			best, found = v, true
		}
	}
	return best, found
}

// proposerForGroups resolves node-id groups to overlay acceptor clients and
// builds the matching CASPaxos proposer: a single group is ordinary consensus,
// multiple groups give joint consensus for roster reconfiguration.
func proposerForGroups(self uint64, groups [][]uint64, d *overlayDialer) roster.Proposer {
	acl := make([][]caspaxos.AcceptorClient, len(groups))
	for i, ids := range groups {
		acl[i] = d.clients(ids)
	}
	if len(acl) == 1 {
		return caspaxos.NewProposer(self, acl[0], caspaxos.WithBackoff(contentionBackoff()))
	}
	return caspaxos.NewJointProposer(self, acl, caspaxos.WithBackoff(contentionBackoff()))
}

// contentionBackoff is the production contention policy: full jitter, LAN-ish
// base, bounded cap. Misconfiguration costs latency, never liveness — the
// single-driver convention still minimizes roster contention, but liveness no
// longer depends on it (W3 in docs/plans/quepaxa-learnings-implementation.md).
func contentionBackoff() func(ctx context.Context, attempt int) error {
	return backoff.FullJitter(5*time.Millisecond, 500*time.Millisecond)
}

// rosterSnap holds this node's latest known roster value (served at /roster so
// joining peers learn the current acceptor core) and a queue of pending joins it
// accepts at /roster/join. Joins are mediated so that the DRIVER is the single
// consensus writer of the register: a joining node never proposes to the
// register itself (which would duel with the driver's reconfiguration); it posts
// its membership here and the driver drains the queue into its next reconcile.
type rosterSnap struct {
	self    uint64
	v       atomic.Pointer[roster.Value]
	mu      sync.Mutex
	pending map[uint64]roster.Member
}

func newRosterSnap(self uint64) *rosterSnap {
	return &rosterSnap{self: self, pending: map[uint64]roster.Member{}}
}

func (s *rosterSnap) store(v roster.Value) { s.v.Store(&v) }

func (s *rosterSnap) load() (roster.Value, bool) {
	if v := s.v.Load(); v != nil {
		return *v, true
	}
	return roster.Value{}, false
}

func (s *rosterSnap) serve(w http.ResponseWriter, _ *http.Request) {
	v := s.v.Load()
	if v == nil {
		http.Error(w, "roster not ready", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// serveJoin accepts a membership join. If this node is the current driver it
// queues the member for its reconcile loop to add; otherwise it points the
// caller at the driver (whichever it currently believes that to be).
func (s *rosterSnap) serveJoin(w http.ResponseWriter, r *http.Request) {
	var m roster.Member
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil || m.NodeID == 0 || m.Addr == "" {
		http.Error(w, "bad member", http.StatusBadRequest)
		return
	}
	v, ok := s.load()
	if !ok || !isDriver(s.self, v.Core) {
		// Not the driver: tell the caller who is, so it can retarget.
		driver := ""
		if ok {
			driver = addrOfMax(v.Core, v.Members)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMisdirectedRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"driver": driver})
		return
	}
	s.mu.Lock()
	s.pending[m.NodeID] = m
	s.mu.Unlock()
	w.WriteHeader(http.StatusAccepted)
}

// drainJoins returns and clears the queued pending joins.
func (s *rosterSnap) drainJoins() []roster.Member {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.pending) == 0 {
		return nil
	}
	out := make([]roster.Member, 0, len(s.pending))
	for _, m := range s.pending {
		out = append(out, m)
	}
	s.pending = map[uint64]roster.Member{}
	return out
}

// addrOfMax returns the address of the highest-id core member (the driver).
func addrOfMax(core []uint64, members []roster.Member) string {
	if len(core) == 0 {
		return ""
	}
	hi := core[0]
	for _, id := range core[1:] {
		if id > hi {
			hi = id
		}
	}
	for _, m := range members {
		if m.NodeID == hi {
			return m.Addr
		}
	}
	return ""
}

// reconcileLoop keeps the consensus roster in step with discovery and the
// failure detector, and tracks the register's own acceptor core. Every tick it
// runs the failure-detector scan (so its suspicion vector is published at
// /health for the driver to aggregate). But only the DRIVER — the highest-id
// current core member — issues consensus operations (Add/Remove/Reconfigure);
// every other replica is a follower that learns the roster from the driver's
// /roster snapshot, never issuing a write-imposing consensus read. This keeps
// exactly one writer on the register at steady state, so reconfiguration is not
// starved by dueling reads. The range is re-placed when the membership epoch
// advances, on driver and follower alike.
func reconcileLoop(ctx context.Context, log *slog.Logger, rost *roster.Roster, dialer *overlayDialer, dyn *dynamicProposer, mon *healthMonitor, snap *rosterSnap, disco roster.Discovery, candidates func() []roster.Member, hc *http.Client, mgr *owner.Manager, val roster.Value) {
	ctrl := roster.NewController(rost).EnableCoreReconfig(roster.RegisterRF)
	self := dialer.self
	epoch := val.Epoch
	cur := val
	t := time.NewTicker(reconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			down := mon.scan(ctx, cur.Members, time.Now().UnixNano())
			if isDriver(self, cur.Core) {
				// The driver is the SOLE consensus writer: it adds queued joins and
				// discovered members, removes condemned ones, and reconfigures the
				// core — all serialized through this one loop, so nothing duels on
				// the register.
				discovered := snap.drainJoins()
				if disco != nil {
					if d, derr := disco.Discover(ctx); derr == nil {
						discovered = append(discovered, d...)
					}
				}
				if len(down) > 0 {
					log.Info("failure detector condemned members", "down", down)
				}
				v, err := ctrl.Reconcile(ctx, discovered, down)
				if err != nil {
					log.Warn("roster reconcile", "err", err)
					continue
				}
				cur = applyView(log, dialer, dyn, snap, mon, mgr, cur, v, &epoch, "driver")
			} else if v, ok := fetchCoreHint(ctx, hc, fetchTargets(candidates(), cur.Members)); ok && len(v.Core) > 0 {
				// Follower: adopt the driver's view without touching consensus, so
				// it can take over cleanly if it later becomes the driver.
				rost.AdoptCore(v.Core)
				cur = applyView(log, dialer, dyn, snap, mon, mgr, cur, v, &epoch, "follower")
			}
			// Ownership grants ride the reconcile cadence: renew the session,
			// acquire/release range locks per current HRW eligibility. Never on
			// the write path; failures cost fast-path coverage, not correctness.
			if mgr != nil {
				if err := mgr.Maintain(ctx, placeRange(cur)); err != nil {
					log.Warn("owner maintain", "err", err)
				}
			}
		}
	}
}

// reconcileLoopReadOnly is the client-only counterpart: a follower that never
// joins consensus. It tracks the roster purely from peer /roster snapshots and
// re-places the range on an epoch change. It proposes nothing and runs no
// failure detector.
func reconcileLoopReadOnly(ctx context.Context, log *slog.Logger, rost *roster.Roster, dialer *overlayDialer, dyn *dynamicProposer, snap *rosterSnap, candidates func() []roster.Member, hc *http.Client, val roster.Value) {
	epoch := val.Epoch
	cur := val
	t := time.NewTicker(reconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			v, ok := fetchCoreHint(ctx, hc, fetchTargets(candidates(), cur.Members))
			if !ok || len(v.Core) == 0 {
				continue
			}
			rost.AdoptCore(v.Core)
			cur = applyView(log, dialer, dyn, snap, nil, nil, cur, v, &epoch, "client")
		}
	}
}

// applyView records a freshly observed roster value: it updates the snapshot,
// learns member addresses, and — when the membership epoch advanced — re-places
// the range and forgets departed peers. It returns the value as the new current.
func applyView(log *slog.Logger, dialer *overlayDialer, dyn *dynamicProposer, snap *rosterSnap, mon *healthMonitor, mgr *owner.Manager, prev, v roster.Value, epoch *uint64, who string) roster.Value {
	snap.store(v)
	dialer.learn(v.Members)
	if v.Epoch != *epoch {
		if mon != nil {
			mon.forget(departed(prev.Members, v.Members))
		}
		dyn.set(routerFor(dialer.self, v, dialer, mgr))
		log.Info("roster changed; range re-placed", "by", who, "epoch", v.Epoch, "members", len(v.Members), "core", v.Core)
		*epoch = v.Epoch
	}
	return v
}

// fetchTargets is the set of peers to poll for a roster snapshot: the current
// members plus the static/discovered candidates (deduped by node id).
func fetchTargets(candidates, members []roster.Member) []roster.Member {
	seen := map[uint64]bool{}
	var out []roster.Member
	for _, m := range append(append([]roster.Member(nil), members...), candidates...) {
		if m.Addr == "" || seen[m.NodeID] {
			continue
		}
		seen[m.NodeID] = true
		out = append(out, m)
	}
	return out
}

// departed returns the ids present in old but not in cur.
func departed(old, cur []roster.Member) []uint64 {
	keep := make(map[uint64]bool, len(cur))
	for _, m := range cur {
		keep[m.NodeID] = true
	}
	var gone []uint64
	for _, m := range old {
		if !keep[m.NodeID] {
			gone = append(gone, m.NodeID)
		}
	}
	return gone
}

// routerFor builds a range router for self over the placement computed from
// val, with contention backoff and — when an ownership manager exists — the
// W1 1-RTT fast path.
func routerFor(self uint64, val roster.Value, dialer *overlayDialer, mgr *owner.Manager) *agent.Router {
	opts := []agent.RouterOption{agent.WithBackoff(contentionBackoff())}
	if mgr != nil {
		opts = append(opts, agent.WithFastPath(mgr))
	}
	return agent.NewRouter(self, placeRange(val), dialer, opts...)
}

// placeRange builds the one-range map whose replicas are chosen by zone-aware
// HRW placement over the current membership.
func placeRange(val roster.Value) *ranges.Map {
	nodes := make([]placement.Node, len(val.Members))
	for i, m := range val.Members {
		nodes[i] = placement.Node{ID: m.NodeID, Zone: m.Zone}
	}
	replicas := placement.TargetReplicas(rangeKey, nodes, replicationFactor)
	return ranges.NewMap([]ranges.Descriptor{{
		ID:       1,
		Replicas: replicas,
		Epoch:    val.Epoch,
	}})
}

// dynamicProposer delegates to the current router, swappable atomically so the
// range can be re-placed under live mvcc/lease traffic.
type dynamicProposer struct {
	cur atomic.Pointer[agent.Router]
}

func (d *dynamicProposer) set(r *agent.Router) { d.cur.Store(r) }

func (d *dynamicProposer) Propose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error) {
	return d.cur.Load().Propose(ctx, key, change)
}

// overlayDialer resolves a node id to an acceptor client, dialing peers over the
// Nebula overlay and short-circuiting our own id to the local acceptor. It
// satisfies agent.Dialer.
type overlayDialer struct {
	self  uint64
	local caspaxos.AcceptorClient
	hc    *http.Client

	mu    sync.Mutex
	addrs map[uint64]string
	cache map[uint64]caspaxos.AcceptorClient
}

func newOverlayDialer(self uint64, local caspaxos.AcceptorClient, hc *http.Client) *overlayDialer {
	return &overlayDialer{
		self:  self,
		local: local,
		hc:    hc,
		addrs: map[uint64]string{},
		cache: map[uint64]caspaxos.AcceptorClient{},
	}
}

// learn records the overlay address of each member for later dialing.
func (d *overlayDialer) learn(members []roster.Member) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, m := range members {
		if m.Addr != "" {
			d.addrs[m.NodeID] = m.Addr
		}
	}
}

func (d *overlayDialer) Acceptor(node uint64) (caspaxos.AcceptorClient, bool) {
	if node == d.self {
		return d.local, true
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if c, ok := d.cache[node]; ok {
		return c, true
	}
	addr, ok := d.addrs[node]
	if !ok {
		return nil, false
	}
	c := transport.NewConnectClient("http://"+addr, d.hc)
	d.cache[node] = c
	return c, true
}

// clients returns acceptor clients for the given node ids (skipping any that
// cannot be resolved).
func (d *overlayDialer) clients(ids []uint64) []caspaxos.AcceptorClient {
	out := make([]caspaxos.AcceptorClient, 0, len(ids))
	for _, id := range ids {
		if c, ok := d.Acceptor(id); ok {
			out = append(out, c)
		}
	}
	return out
}
