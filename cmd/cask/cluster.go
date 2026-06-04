package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/phoban01/cask/internal/agent"
	"github.com/phoban01/cask/internal/caspaxos"
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

// nebulaCluster builds a self-forming consensus engine over a Nebula overlay:
// it brings up the overlay, derives seeds from the config (the node's own cert
// plus its lighthouse hosts), installs the roster by consensus, and routes a
// single placement-driven range. It returns a proposer for mvcc/lease, the
// overlay listener the node serves its acceptor on, and the local member.
//
// The returned proposer is dynamic: a background loop reconciles the roster with
// discovery and, on a membership change, recomputes placement and swaps in a
// fresh router — so the range's replica set tracks the cluster.
// clientOnly nodes join the overlay and route KV/lock traffic to the roster's
// replicas, but never add themselves to the consensus roster — so HRW placement
// never selects them and no peer ever dials them. They only make outbound overlay
// connections to the lighthouses, which is exactly what a NAT'd node can do.
func nebulaCluster(ctx context.Context, log *slog.Logger, configYAML string, caskPort int, local *caspaxos.Acceptor, clientOnly bool) (*dynamicProposer, net.Listener, roster.Member, *healthMonitor, error) {
	network, err := nebula.New(configYAML, log)
	if err != nil {
		return nil, nil, roster.Member{}, nil, err
	}

	self, err := nebula.SelfMember(configYAML, caskPort)
	if err != nil {
		return nil, nil, roster.Member{}, nil, err
	}
	// genesis is the identical lighthouse set every node installs; seeds is this
	// node's discovery view (self + lighthouses) used by the reconcile loop.
	genesis, err := nebula.GenesisMembers(configYAML, caskPort)
	if err != nil {
		return nil, nil, roster.Member{}, nil, err
	}
	seeds, err := nebula.SeedMembers(configYAML, caskPort)
	if err != nil {
		return nil, nil, roster.Member{}, nil, err
	}
	log.Info("nebula self-forming", "node", self.NodeID, "addr", self.Addr, "zone", self.Zone, "lighthouses", len(genesis))

	dialer := newOverlayDialer(self.NodeID, local, network.HTTPClient())
	dialer.learn(genesis)
	dialer.learn([]roster.Member{self})

	// The roster register lives on the lighthouse (genesis) acceptor set; every
	// node proposes to it through that same set, so the bootstrap is consistent.
	rost := roster.New(caspaxos.NewProposer(self.NodeID, dialer.clients(memberIDs(genesis))))
	if _, err := rost.Genesis(ctx, genesis); err != nil {
		return nil, nil, roster.Member{}, nil, fmt.Errorf("roster genesis: %w", err)
	}
	// A non-lighthouse node joins by adding itself to the consensus roster. A
	// client-only node skips this: it routes to the replicas without becoming one.
	if !clientOnly && !containsID(genesis, self.NodeID) {
		if _, err := rost.Add(ctx, self); err != nil {
			return nil, nil, roster.Member{}, nil, fmt.Errorf("roster join: %w", err)
		}
	}
	val, err := rost.Get(ctx)
	if err != nil {
		return nil, nil, roster.Member{}, nil, fmt.Errorf("roster get: %w", err)
	}
	dialer.learn(val.Members)

	dyn := &dynamicProposer{}
	dyn.set(routerFor(self.NodeID, val, dialer))

	ln, err := network.Listen(ctx, fmt.Sprintf(":%d", caskPort))
	if err != nil {
		return nil, nil, roster.Member{}, nil, fmt.Errorf("overlay listen: %w", err)
	}

	// A client-only node has no peers to monitor and proposes no roster changes;
	// it just re-reads the roster and re-places the range as membership shifts.
	if clientOnly {
		go reconcileLoopReadOnly(ctx, log, rost, dialer, dyn, val)
		return dyn, ln, self, nil, nil
	}

	mon := newHealthMonitor(self.NodeID, network.HTTPClient())
	go reconcileLoop(ctx, log, rost, dialer, dyn, mon, seeds, val)
	return dyn, ln, self, mon, nil
}

// reconcileLoopReadOnly is the client-only counterpart to reconcileLoop: it
// re-reads the consensus roster each tick and, when the epoch advances, learns
// the new members' overlay addresses and re-places the range so proposals keep
// routing to the current replica set. It never proposes Add/Remove and runs no
// failure detector.
func reconcileLoopReadOnly(ctx context.Context, log *slog.Logger, rost *roster.Roster, dialer *overlayDialer, dyn *dynamicProposer, val roster.Value) {
	epoch := val.Epoch
	t := time.NewTicker(reconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			v, err := rost.Get(ctx)
			if err != nil {
				log.Warn("client roster get", "err", err)
				continue
			}
			if v.Epoch != epoch {
				dialer.learn(v.Members)
				dyn.set(routerFor(dialer.self, v, dialer))
				log.Info("client re-placed range", "epoch", v.Epoch, "members", len(v.Members))
				epoch = v.Epoch
			}
		}
	}
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

func containsID(members []roster.Member, id uint64) bool {
	for _, m := range members {
		if m.NodeID == id {
			return true
		}
	}
	return false
}

// reconcileLoop keeps the consensus roster in step with discovery and re-places
// the range when the membership epoch advances. Each tick it both adds members
// surfaced by discovery and removes any condemned by the health monitor's
// majority cut detector, so the roster tracks both joins and failures.
func reconcileLoop(ctx context.Context, log *slog.Logger, rost *roster.Roster, dialer *overlayDialer, dyn *dynamicProposer, mon *healthMonitor, seeds []roster.Member, val roster.Value) {
	ctrl := roster.NewController(rost)
	disco := roster.SeedDiscovery(seeds)
	epoch := val.Epoch
	members := val.Members
	t := time.NewTicker(reconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			discovered, err := disco.Discover(ctx)
			if err != nil {
				continue
			}
			down := mon.scan(ctx, members, time.Now().UnixNano())
			if len(down) > 0 {
				log.Info("failure detector condemned members", "down", down)
			}
			val, err := ctrl.Reconcile(ctx, discovered, down)
			if err != nil {
				log.Warn("roster reconcile", "err", err)
				continue
			}
			if val.Epoch != epoch {
				mon.forget(departed(members, val.Members))
				dialer.learn(val.Members)
				dyn.set(routerFor(dialer.self, val, dialer))
				log.Info("roster changed; range re-placed", "epoch", val.Epoch, "members", len(val.Members))
				epoch = val.Epoch
			}
			members = val.Members
		}
	}
}

// routerFor builds a range router for self over the placement computed from val.
func routerFor(self uint64, val roster.Value, dialer *overlayDialer) *agent.Router {
	return agent.NewRouter(self, placeRange(val), dialer)
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
		d.addrs[m.NodeID] = m.Addr
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

func memberIDs(members []roster.Member) []uint64 {
	ids := make([]uint64, len(members))
	for i, m := range members {
		ids[i] = m.NodeID
	}
	return ids
}
