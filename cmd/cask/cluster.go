package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/phoban01/cask/internal/agent"
	"github.com/phoban01/cask/internal/backoff"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/cluster"
	"github.com/phoban01/cask/internal/lease"
	"github.com/phoban01/cask/internal/owner"
	"github.com/phoban01/cask/internal/placement"
	"github.com/phoban01/cask/internal/ranges"
	"github.com/phoban01/cask/internal/roster"
	"github.com/phoban01/cask/internal/store"
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
func nebulaCluster(ctx context.Context, log *slog.Logger, configYAML string, caskPort int, local *caspaxos.Acceptor, localStore caspaxos.Storage, clientOnly, bootstrap bool, disco roster.Discovery) (*dynamicProposer, net.Listener, roster.Member, *healthMonitor, *cluster.Snap, *forwarder, *owner.Manager, *adminAPI, error) {
	var admin *adminAPI
	fail := func(err error) (*dynamicProposer, net.Listener, roster.Member, *healthMonitor, *cluster.Snap, *forwarder, *owner.Manager, *adminAPI, error) {
		return nil, nil, roster.Member{}, nil, nil, nil, nil, nil, err
	}
	network, err := nebula.New(configYAML, log)
	if err != nil {
		return fail(err)
	}

	self, err := nebula.SelfMember(configYAML, caskPort)
	if err != nil {
		return fail(err)
	}
	// seeds are the lighthouse hosts (+self) — Nebula rendezvous points that are
	// also where a joining node looks first for a live cask member to learn the
	// core from. They are NOT auto-added to the roster (a lighthouse need not be a
	// cask member); querying one that isn't simply fails and is skipped.
	seeds, err := nebula.SeedMembers(configYAML, caskPort)
	if err != nil {
		return fail(err)
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
	var joinedDescs []ranges.State
	switch {
	case bootstrap:
		v, err := rost.Founder(ctx, self)
		if err != nil {
			return fail(fmt.Errorf("roster founder: %w", err))
		}
		val = v
		log.Info("founded roster", "node", self.NodeID)
	default:
		// cluster.Join returns the snapshot it observed (with self in membership),
		// so the node never issues a startup consensus read that could race the
		// driver's reconfiguration.
		v, err := cluster.Join(ctx, log, rost, dialer.learn, network.HTTPClient(), self, candidates, !clientOnly, joinTimeout, joinPoll)
		if err != nil {
			return fail(fmt.Errorf("roster join: %w", err))
		}
		val, joinedDescs = v.Value, v.Descriptors
	}
	dialer.learn(val.Members)

	snap := cluster.NewSnap(self.NodeID)
	snap.Store(val)
	snap.StoreDescs(joinedDescs)

	dyn := &dynamicProposer{}

	// The ownership manager's control-plane traffic (session, range locks)
	// rides the same dynamic proposer it serves; the internal-keyspace
	// exclusion in FastPropose breaks the recursion. Client-only nodes are
	// never replicas, so HRW never selects them and they run no manager.
	//
	// With M7 forwarding in place (the forwarder below + the router's
	// FullPathGate), the writes-via-owner discipline holds cluster-wide for
	// client keys, so main.go enables lease-guarded local reads
	// (mvcc.WithLocalReader) over this manager.
	var mgr *owner.Manager
	if !clientOnly {
		osess := lease.NewSessions(dyn, func() int64 { return time.Now().UnixNano() })
		olocks := lease.NewLocks(dyn, osess, lease.WithAcquireBackoff(contentionBackoff()))
		mgr = owner.New(self.NodeID, dialer, osess, olocks)
	}
	dyn.set(routerFor(self.NodeID, val, dialer, mgr, snap))

	ln, err := network.Listen(ctx, fmt.Sprintf(":%d", caskPort))
	if err != nil {
		return fail(fmt.Errorf("overlay listen: %w", err))
	}

	fwd := newForwarder(self.NodeID, network.HTTPClient(), dialer, snap, log)

	if clientOnly {
		go reconcileLoopReadOnly(ctx, log, rost, dialer, dyn, snap, candidates, network.HTTPClient(), val)
		return dyn, ln, self, nil, snap, fwd, nil, nil, nil
	}

	// §4.3: the descriptor store proposes against the current Core; the
	// orchestrator's settle waits out the routing lease — two reconcile ticks
	// covers the driver's own observation plus every follower's next poll,
	// after which no pre-joint writer can commit (§4.1 fencing).
	dstore := descriptorStore(self.NodeID, snap, dialer)
	settle := func(ctx context.Context) error {
		select {
		case <-time.After(2*reconcileInterval + time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	orch := ranges.NewOrchestrator(self.NodeID, dstore, dialer,
		keyLister(self.NodeID, localStore, dialer, network.HTTPClient()), settle)
	// Split/merge cutovers commit through the roster's RangeIDs — the routing
	// switch clients observe on their next snapshot poll.
	orch.SetCutover(func(ctx context.Context, remove, add []uint64) error {
		_, err := rost.UpdateRangeIDs(ctx, func(ids []uint64) []uint64 {
			keep := ids[:0:0]
			for _, id := range ids {
				drop := false
				for _, r := range remove {
					if id == r {
						drop = true
						break
					}
				}
				if !drop {
					keep = append(keep, id)
				}
			}
			return append(keep, add...)
		})
		return err
	})

	mon := newHealthMonitor(self.NodeID, network.HTTPClient())
	go reconcileLoop(ctx, log, rost, dialer, dyn, mon, snap, disco, candidates, network.HTTPClient(), mgr, dstore, orch, val)
	admin = &adminAPI{self: self.NodeID, orch: orch, snap: snap, log: log}
	return dyn, ln, self, mon, snap, fwd, mgr, admin, nil
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
func reconcileLoop(ctx context.Context, log *slog.Logger, rost *roster.Roster, dialer *overlayDialer, dyn *dynamicProposer, mon *healthMonitor, snap *cluster.Snap, disco roster.Discovery, candidates func() []roster.Member, hc *http.Client, mgr *owner.Manager, dstore *ranges.Store, orch *ranges.Orchestrator, val roster.Value) {
	ctrl := roster.NewController(rost).EnableCoreReconfig(roster.RegisterRF)
	self := dialer.self
	epoch := val.Epoch
	cur := val
	lastFP := ""
	var reconfigBusy atomic.Bool
	t := time.NewTicker(reconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			down := mon.scan(ctx, cur.Members, time.Now().UnixNano())
			if cluster.IsDriver(self, cur.Core) {
				// The driver is the SOLE consensus writer: it adds queued joins and
				// discovered members, removes condemned ones, and reconfigures the
				// core — all serialized through this one loop, so nothing duels on
				// the register.
				discovered := snap.DrainJoins()
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
				// §4.3: the driver is also the sole descriptor reader/writer —
				// it seeds, triggers reconfigurations, and refreshes the
				// snapshot's descriptor states, which followers pick up on
				// their next /roster poll.
				snap.StoreDescs(driveRanges(ctx, log, dstore, orch, cur, replicationFactor, &reconfigBusy))
			} else if v, ok := cluster.FetchCoreHint(ctx, hc, cluster.FetchTargets(candidates(), cur.Members)); ok && len(v.Core) > 0 {
				// Follower: adopt the driver's view without touching consensus, so
				// it can take over cleanly if it later becomes the driver.
				rost.AdoptCore(v.Core)
				snap.StoreDescs(v.Descriptors)
				cur = applyView(log, dialer, dyn, snap, mon, mgr, cur, v.Value, &epoch, "follower")
			}
			// Rebuild routing whenever descriptor state moved: descriptor
			// epochs (joint publish, release, splits) advance WITHOUT a roster
			// epoch bump, and the routing lease the orchestrator's settle
			// waits out is exactly this loop observing the change per tick.
			if fp := descFingerprint(snap, cur); fp != lastFP {
				lastFP = fp
				dyn.set(routerFor(self, cur, dialer, mgr, snap))
			}
			// Ownership grants ride the reconcile cadence: renew the session,
			// acquire/release range locks per current HRW eligibility (joint
			// ranges take no grant). Never on the write path; failures cost
			// fast-path coverage, not correctness.
			if mgr != nil {
				if err := mgr.Maintain(ctx, rmapFromSnap(snap, cur)); err != nil {
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
func reconcileLoopReadOnly(ctx context.Context, log *slog.Logger, rost *roster.Roster, dialer *overlayDialer, dyn *dynamicProposer, snap *cluster.Snap, candidates func() []roster.Member, hc *http.Client, val roster.Value) {
	epoch := val.Epoch
	cur := val
	lastFP := ""
	t := time.NewTicker(reconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			v, ok := cluster.FetchCoreHint(ctx, hc, cluster.FetchTargets(candidates(), cur.Members))
			if !ok || len(v.Core) == 0 {
				continue
			}
			rost.AdoptCore(v.Core)
			snap.StoreDescs(v.Descriptors)
			cur = applyView(log, dialer, dyn, snap, nil, nil, cur, v.Value, &epoch, "client")
			// Descriptor moves must retarget this client's routing too.
			if fp := descFingerprint(snap, cur); fp != lastFP {
				lastFP = fp
				dyn.set(routerFor(dialer.self, cur, dialer, nil, snap))
			}
		}
	}
}

// applyView records a freshly observed roster value: it updates the snapshot,
// learns member addresses, and — when the membership epoch advanced — re-places
// the range and forgets departed peers. It returns the value as the new current.
func applyView(log *slog.Logger, dialer *overlayDialer, dyn *dynamicProposer, snap *cluster.Snap, mon *healthMonitor, mgr *owner.Manager, prev, v roster.Value, epoch *uint64, who string) roster.Value {
	snap.Store(v)
	dialer.learn(v.Members)
	if v.Epoch != *epoch {
		if mon != nil {
			mon.forget(departed(prev.Members, v.Members))
		}
		dyn.set(routerFor(dialer.self, v, dialer, mgr, snap))
		log.Info("roster changed; range re-placed", "by", who, "epoch", v.Epoch, "members", len(v.Members), "core", v.Core)
		*epoch = v.Epoch
	}
	return v
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
// val, with contention backoff, the W1 fast path (when an ownership manager
// exists), and §4.1 stale-routing recovery (when a snapshot source exists):
// an ErrRangeChanged rejection re-resolves placement from the latest roster
// snapshot and retries once.
func routerFor(self uint64, val roster.Value, dialer *overlayDialer, mgr *owner.Manager, snap *cluster.Snap) *agent.Router {
	opts := []agent.RouterOption{agent.WithBackoff(contentionBackoff())}
	if mgr != nil {
		opts = append(opts, agent.WithFastPath(mgr))
	}
	if snap != nil {
		opts = append(opts, agent.WithRefresh(func() *ranges.Map {
			v, ok := snap.Load()
			if !ok {
				return nil
			}
			return placeRange(v)
		}))
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

// addr returns the learned overlay address of node, if any.
func (d *overlayDialer) addr(node uint64) (string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	a, ok := d.addrs[node]
	return a, ok
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

// --- §4.3: descriptor-driven placement ------------------------------------

// proposerFunc adapts a closure to the ranges.Proposer seam.
type proposerFunc func(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error)

func (f proposerFunc) Propose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error) {
	return f(ctx, key, change)
}

// descriptorStore builds a ranges.Store whose proposals target the roster's
// CURRENT Core, resolved from the snapshot at call time — the same late-
// binding pattern the roster itself uses, so a Core reconfiguration
// retargets automatically.
func descriptorStore(self uint64, snap *cluster.Snap, dialer *overlayDialer) *ranges.Store {
	return ranges.NewStore(proposerFunc(func(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error) {
		v, ok := snap.Load()
		if !ok || len(v.Core) == 0 {
			return nil, fmt.Errorf("descriptor store: no known core yet")
		}
		return proposerForGroups(self, [][]uint64{v.Core}, dialer).Propose(ctx, key, change)
	}))
}

// rangeIDsOf returns the roster's live range ids; empty means the pre-§4.3
// implicit single range.
func rangeIDsOf(v roster.Value) []uint64 {
	if len(v.RangeIDs) > 0 {
		return v.RangeIDs
	}
	return []uint64{1}
}

// nodesOf converts roster members to placement candidates.
func nodesOf(v roster.Value) []placement.Node {
	nodes := make([]placement.Node, len(v.Members))
	for i, m := range v.Members {
		nodes[i] = placement.Node{ID: m.NodeID, Zone: m.Zone}
	}
	return nodes
}

// rmapFromSnap builds the routing map from the latest known descriptor
// states, falling back to legacy roster-derived HRW placement until the first
// descriptor is known (bootstrap, and clusters predating §4.3).
func rmapFromSnap(snap *cluster.Snap, val roster.Value) *ranges.Map {
	if snap != nil {
		if descs := snap.LoadDescs(); len(descs) > 0 {
			ds := make([]ranges.Descriptor, 0, len(descs))
			for _, s := range descs {
				if !s.Tombstoned {
					ds = append(ds, s.Descriptor)
				}
			}
			if len(ds) > 0 {
				return ranges.NewMap(ds)
			}
		}
	}
	return placeRange(val)
}

// descFingerprint summarizes routing-relevant state; a change means the
// routers must be rebuilt (descriptor epochs move without roster epochs).
func descFingerprint(snap *cluster.Snap, val roster.Value) string {
	var b strings.Builder
	fmt.Fprintf(&b, "e%d", val.Epoch)
	for _, s := range snap.LoadDescs() {
		fmt.Fprintf(&b, "|%d:%d:%v", s.ID, s.Epoch, s.Tombstoned)
	}
	return b.String()
}

// readDescriptors linearizably reads every named descriptor from the Core.
func readDescriptors(ctx context.Context, dstore *ranges.Store, ids []uint64) ([]ranges.State, error) {
	out := make([]ranges.State, 0, len(ids))
	for _, id := range ids {
		st, ok, err := dstore.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, st)
		}
	}
	return out, nil
}

// keyLister enumerates a node's acceptor-store keys for carry-forward: the
// local store directly, remote nodes via their /rangekeys endpoint.
func keyLister(self uint64, local caspaxos.Storage, dialer *overlayDialer, hc *http.Client) ranges.KeyLister {
	return func(ctx context.Context, node uint64) ([][]byte, error) {
		if node == self {
			l, ok := local.(store.Lister)
			if !ok {
				return nil, fmt.Errorf("local store cannot enumerate keys")
			}
			return l.Keys(ctx)
		}
		addr, ok := dialer.addr(node)
		if !ok {
			return nil, fmt.Errorf("no address for node %d", node)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/rangekeys", nil)
		if err != nil {
			return nil, err
		}
		resp, err := hc.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("rangekeys %d: %s", node, resp.Status)
		}
		var keys [][]byte
		if err := json.NewDecoder(resp.Body).Decode(&keys); err != nil {
			return nil, err
		}
		return keys, nil
	}
}

// adminAPI exposes the §4.3 lifecycle operations. Split/merge policy is an
// operator decision for now (split-point selection heuristics are a deferred
// sub-decision); like joins, the endpoints are driver-gated — a non-driver
// answers 421 with the driver's address.
type adminAPI struct {
	self uint64
	orch *ranges.Orchestrator
	snap *cluster.Snap
	log  *slog.Logger
}

// serveSplit handles POST /admin/split?range=<id>&at=<key>.
func (a *adminAPI) serveSplit(w http.ResponseWriter, r *http.Request) {
	v, ok := a.gate(w)
	if !ok {
		return
	}
	id := uint64(queryInt64(r, "range", 0))
	at := r.URL.Query().Get("at")
	if id == 0 || at == "" {
		http.Error(w, "need range=<id>&at=<key>", http.StatusBadRequest)
		return
	}
	next := maxRangeID(v) + 1
	left, right, err := a.orch.Split(r.Context(), id, []byte(at), next, next+1)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	a.log.Info("range split", "range", id, "at", at, "left", left.ID, "right", right.ID)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]uint64{"left": left.ID, "right": right.ID})
}

// serveMerge handles POST /admin/merge?left=<id>&right=<id>.
func (a *adminAPI) serveMerge(w http.ResponseWriter, r *http.Request) {
	v, ok := a.gate(w)
	if !ok {
		return
	}
	leftID := uint64(queryInt64(r, "left", 0))
	rightID := uint64(queryInt64(r, "right", 0))
	if leftID == 0 || rightID == 0 {
		http.Error(w, "need left=<id>&right=<id>", http.StatusBadRequest)
		return
	}
	merged, err := a.orch.Merge(r.Context(), leftID, rightID, maxRangeID(v)+1)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	a.log.Info("ranges merged", "left", leftID, "right", rightID, "into", merged.ID)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]uint64{"range": merged.ID})
}

// gate enforces the driver-only rule, mirroring serveJoin.
func (a *adminAPI) gate(w http.ResponseWriter) (roster.Value, bool) {
	v, ok := a.snap.Load()
	if !ok || !cluster.IsDriver(a.self, v.Core) {
		driver := ""
		if ok {
			driver = cluster.DriverAddr(v.Core, v.Members)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMisdirectedRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"driver": driver})
		return roster.Value{}, false
	}
	return v, true
}

func maxRangeID(v roster.Value) uint64 {
	max := uint64(0)
	for _, id := range rangeIDsOf(v) {
		if id > max {
			max = id
		}
	}
	return max
}

func queryInt64(r *http.Request, key string, def int64) int64 {
	if s := r.URL.Query().Get(key); s != "" {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n
		}
	}
	return def
}

// driveRanges is the driver's per-tick §4.3 duty: seed the genesis descriptor
// if missing, trigger replica reconfiguration when placement wants it (or an
// in-flight joint needs completing), and return the fresh descriptor states
// for the snapshot. Reconfigurations run single-flight in the background —
// they block on the routing-lease settle and the data carry.
func driveRanges(ctx context.Context, log *slog.Logger, dstore *ranges.Store, orch *ranges.Orchestrator, cur roster.Value, rf int, busy *atomic.Bool) []ranges.State {
	var out []ranges.State
	for _, id := range rangeIDsOf(cur) {
		st, ok, err := dstore.Get(ctx, id)
		if err != nil {
			log.Warn("descriptor read", "range", id, "err", err)
			continue
		}
		if !ok {
			if id != 1 {
				log.Error("roster names a range with no descriptor", "range", id)
				continue
			}
			seeded, err := orch.Seed(ctx, ranges.Descriptor{
				ID:       id,
				Replicas: placement.TargetReplicas(ranges.RangeKey(id), nodesOf(cur), rf),
				Epoch:    1,
			})
			if err != nil {
				log.Warn("descriptor seed", "range", id, "err", err)
				continue
			}
			st = seeded
			log.Info("seeded genesis descriptor", "range", id, "replicas", st.Replicas)
		}
		out = append(out, st)

		// Resume interrupted split/merge protocols first (their intents are
		// recorded in the register), then placement-driven reconfiguration.
		switch {
		case st.Split != nil && busy.CompareAndSwap(false, true):
			go func(id uint64, in ranges.SplitIntent) {
				defer busy.Store(false)
				if _, _, err := orch.Split(ctx, id, in.At, in.Left, in.Right); err != nil {
					log.Warn("split resume", "range", id, "err", err)
				}
			}(id, *st.Split)
		case st.Merge != nil && busy.CompareAndSwap(false, true):
			go func(id uint64, in ranges.MergeIntent) {
				defer busy.Store(false)
				if _, err := orch.Merge(ctx, id, in.With, in.Into); err != nil {
					log.Warn("merge resume", "range", id, "err", err)
				}
			}(id, *st.Merge)
		default:
			target, need := placement.NeedsReconfig(ranges.RangeKey(id), st.Replicas, nodesOf(cur), rf)
			if (need || st.Joint != nil) && busy.CompareAndSwap(false, true) {
				go func(id uint64, target []uint64) {
					defer busy.Store(false)
					released, err := orch.ReconfigReplicas(ctx, id, target)
					if err != nil {
						log.Warn("range reconfig", "range", id, "target", target, "err", err)
						return
					}
					log.Info("range reconfigured", "range", id, "replicas", released.Replicas, "epoch", released.Epoch)
				}(id, target)
			}
		}
	}
	return out
}
