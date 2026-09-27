package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/phoban01/cask/internal/backoff"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/cluster"
	"github.com/phoban01/cask/internal/ranges"
	"github.com/phoban01/cask/internal/roster"
	"github.com/phoban01/cask/internal/transport"
)

// membershipConfig configures the dynamic roster path of one apiserver.
type membershipConfig struct {
	ID        uint64             // this member's node id (--id)
	Advertise string             // this member's consensus address (--advertise-consensus)
	Bootstrap bool               // found a new fleet (--bootstrap)
	Seeds     []string           // addresses of live members to join through (--seed)
	Local     *caspaxos.Acceptor // the embedded acceptor
	HTTP      *http.Client       // client for peer consensus and roster traffic

	// Keys lists the keys the embedded acceptor holds. A core change uses
	// it to find every data register it must carry to the new core.
	Keys func(ctx context.Context) ([][]byte, error)

	// Interval is the reconcile tick and the join poll. JoinTimeout bounds
	// the wait for a live member to add this one. Settle is how long a
	// core change waits for every member to see the joint core before it
	// carries the data registers (default: three intervals).
	Interval    time.Duration
	JoinTimeout time.Duration
	Settle      time.Duration
}

// membership is this apiserver's place in the fleet. It founds the roster or
// joins it through the same code cmd/cask uses (internal/cluster), serves
// GET /roster and POST /roster/join next to the acceptor, and proposes data
// writes against the roster's current core.
//
// A joined member is a participant: it is in the roster members but not in
// the core. The driver never grows the core here. Voters change only through
// changeCore, which the promote endpoint (issue #45) will call.
//
// Every data register uses the core as its acceptor set, so a core change
// moves the data registers too (corechange.go).
type membership struct {
	cfg   membershipConfig
	log   *slog.Logger
	self  roster.Member
	seeds []roster.Member
	rost  *roster.Roster
	snap  *cluster.Snap

	viewMu sync.Mutex // orders snapshot stores so the view never moves back

	local    *fencedClient   // the embedded acceptor behind the write fence
	coreReqs chan coreChange // core changes for the run loop to drive

	mu      sync.Mutex
	addrs   map[uint64]string
	conns   map[uint64]caspaxos.AcceptorClient
	prop    *caspaxos.Proposer
	propGen uint64
}

func newMembership(cfg membershipConfig, log *slog.Logger) *membership {
	if cfg.Interval == 0 {
		cfg.Interval = time.Second
	}
	if cfg.JoinTimeout == 0 {
		cfg.JoinTimeout = 60 * time.Second
	}
	if cfg.Settle == 0 {
		cfg.Settle = 3 * cfg.Interval
	}
	m := &membership{
		cfg:   cfg,
		log:   log,
		self:  roster.Member{NodeID: cfg.ID, Addr: cfg.Advertise},
		snap:  cluster.NewSnap(cfg.ID),
		addrs: map[uint64]string{},
		conns: map[uint64]caspaxos.AcceptorClient{},
	}
	for _, a := range cfg.Seeds {
		m.seeds = append(m.seeds, roster.Member{Addr: a})
	}
	m.rost = roster.New(cfg.ID, func(groups [][]uint64) roster.Proposer {
		acl := make([][]caspaxos.AcceptorClient, len(groups))
		for i, ids := range groups {
			acl[i] = m.clients(ids)
		}
		if len(acl) == 1 {
			return caspaxos.NewProposer(cfg.ID, acl[0], caspaxos.WithBackoff(contentionBackoff()))
		}
		return caspaxos.NewJointProposer(cfg.ID, acl, caspaxos.WithBackoff(contentionBackoff()))
	})
	m.local = &fencedClient{AcceptorClient: cfg.Local, gen: m.coreGen}
	m.coreReqs = make(chan coreChange)
	m.rost.SetCarry(m.carry)
	m.learn([]roster.Member{m.self})
	return m
}

func contentionBackoff() func(ctx context.Context, attempt int) error {
	return backoff.FullJitter(5*time.Millisecond, 500*time.Millisecond)
}

// handler serves the acceptor and the roster endpoints on one listener.
func (m *membership) handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle(transport.ConnectHandler(m.cfg.Local, transport.WithEpochOf(m.coreGen)))
	mux.HandleFunc("/roster", m.snap.Serve)
	mux.HandleFunc("/roster/join", m.snap.ServeJoin)
	mux.HandleFunc(dataKeysPath, m.serveDataKeys)
	return mux
}

// start founds the roster or joins it, then runs the reconcile loop until ctx
// ends. It returns once this member is in the roster.
func (m *membership) start(ctx context.Context) error {
	//= docs/spec/fleet.md#6-membership
	//# The extension server MUST join the fleet through the dynamic roster path.
	var val roster.Value
	if m.cfg.Bootstrap {
		v, err := m.rost.Founder(ctx, m.self)
		if err != nil {
			return fmt.Errorf("roster founder: %w", err)
		}
		val = v
		m.log.Info("founded roster", "node", m.self.NodeID)
	} else {
		v, err := cluster.Join(ctx, m.log, m.rost, m.learn, m.cfg.HTTP, m.self,
			func() []roster.Member { return m.seeds }, true, m.cfg.JoinTimeout, m.cfg.Interval)
		if err != nil {
			return fmt.Errorf("roster join: %w", err)
		}
		val = v.Value
	}
	m.learn(val.Members)
	m.store(val)
	go m.run(ctx)
	return nil
}

// run keeps the roster snapshot current. The driver adds queued joins by
// consensus. Every other member learns the roster from a peer's snapshot and
// never writes the register.
func (m *membership) run(ctx context.Context) {
	ctrl := roster.NewController(m.rost)
	t := time.NewTicker(m.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-m.coreReqs:
			// A core change runs here, so the driver stays the one roster
			// writer and does not duel with its own reconcile.
			req.done <- m.runCoreChange(ctx, req.target)
			continue
		case <-t.C:
		}
		cur, _ := m.snap.Load()
		if cluster.IsDriver(m.self.NodeID, cur.Core) {
			v, err := ctrl.Reconcile(ctx, m.snap.DrainJoins(), nil)
			if err != nil {
				m.log.Warn("roster reconcile", "err", err)
				continue
			}
			m.apply(cur, v)
			continue
		}
		if v, ok := cluster.FetchCoreHint(ctx, m.cfg.HTTP, cluster.FetchTargets(m.seeds, cur.Members)); ok && len(v.Core) > 0 {
			m.rost.AdoptCore(v.Core)
			m.apply(cur, v.Value)
		}
	}
}

func (m *membership) apply(prev, v roster.Value) {
	m.learn(v.Members)
	m.store(v)
	if v.Epoch != prev.Epoch || v.ConfigGen != prev.ConfigGen {
		m.log.Info("roster changed", "epoch", v.Epoch, "members", len(v.Members), "core", v.Core)
	}
}

// current returns the latest known roster value.
func (m *membership) current() (roster.Value, bool) { return m.snap.Load() }

// store publishes v as this member's roster view. It drops a value older
// than the view it holds, so a slow reconcile never moves the core view,
// and with it the write fence, back.
func (m *membership) store(v roster.Value) {
	m.viewMu.Lock()
	defer m.viewMu.Unlock()
	if cur, ok := m.snap.Load(); ok && olderView(v, cur) {
		return
	}
	m.snap.Store(v)
}

// olderView reports whether a is an older roster value than b. ConfigGen
// orders core changes and Epoch orders member changes; both only grow.
func olderView(a, b roster.Value) bool {
	return a.ConfigGen < b.ConfigGen || (a.ConfigGen == b.ConfigGen && a.Epoch < b.Epoch)
}

// staleRetries bounds how often Propose refreshes its core view after a
// voter rejects it as stale.
const staleRetries = 8

// Propose runs a CASPaxos round on key against the roster's current core.
// Only voters hold register replicas; a participant proposes to them. The
// round names the core configuration (ConfigGen) it uses. A voter that
// knows a newer one rejects it, and Propose waits for a newer view and
// runs again. The rejection comes in the prepare phase, before change runs;
// a rejected accept counts as a missing vote (see fencedClient).
func (m *membership) Propose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error) {
	for attempt := 0; ; attempt++ {
		p, gen, err := m.proposer()
		if err != nil {
			return nil, err
		}
		out, err := p.Propose(ranges.WithClaimedEpoch(ctx, gen), key, change)
		if !errors.Is(err, caspaxos.ErrRangeChanged) || attempt >= staleRetries {
			return out, err
		}
		if err := m.awaitNewerCore(ctx, gen); err != nil {
			return nil, err
		}
	}
}

// awaitNewerCore waits until this member's view names a core configuration
// newer than gen, or a few intervals pass.
func (m *membership) awaitNewerCore(ctx context.Context, gen uint64) error {
	t := time.NewTicker(m.cfg.Interval / 4)
	defer t.Stop()
	deadline := time.Now().Add(4 * m.cfg.Interval)
	for time.Now().Before(deadline) {
		if v, ok := m.snap.Load(); ok && v.ConfigGen > gen {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	return nil
}

// proposer returns a proposer over the current core and the ConfigGen it
// was built for. It keeps one proposer per ConfigGen so the ballot counter
// is not reset on every call.
func (m *membership) proposer() (*caspaxos.Proposer, uint64, error) {
	v, ok := m.snap.Load()
	if !ok || len(v.Core) == 0 {
		return nil, 0, fmt.Errorf("membership: no roster core yet")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.prop != nil && m.propGen == v.ConfigGen {
		return m.prop, v.ConfigGen, nil
	}
	groups := [][]uint64{v.Core}
	if v.Joint != nil {
		//= docs/spec/fleet.md#6-membership
		//# During a core change, a data write MUST reach a quorum of both the old and the new core.
		groups = [][]uint64{v.Joint.Old, v.Joint.New}
	}
	acl := make([][]caspaxos.AcceptorClient, len(groups))
	for i, ids := range groups {
		c, err := m.allClientsLocked(ids)
		if err != nil {
			return nil, 0, err
		}
		acl[i] = c
	}
	if len(acl) == 1 {
		m.prop = caspaxos.NewProposer(m.self.NodeID, acl[0], caspaxos.WithBackoff(contentionBackoff()))
	} else {
		m.prop = caspaxos.NewJointProposer(m.self.NodeID, acl, caspaxos.WithBackoff(contentionBackoff()))
	}
	m.propGen = v.ConfigGen
	return m.prop, v.ConfigGen, nil
}

// learn records member addresses. A seed has no node id yet, so it is skipped.
func (m *membership) learn(members []roster.Member) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, x := range members {
		if x.NodeID != 0 && x.Addr != "" && m.addrs[x.NodeID] != x.Addr {
			m.addrs[x.NodeID] = x.Addr
			delete(m.conns, x.NodeID)
		}
	}
}

func (m *membership) clients(ids []uint64) []caspaxos.AcceptorClient {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.clientsLocked(ids)
}

// clientsLocked resolves node ids to acceptor clients. Its own id resolves to
// the local acceptor, behind the same write fence that remote callers meet.
// An id with no known address is skipped.
func (m *membership) clientsLocked(ids []uint64) []caspaxos.AcceptorClient {
	out := make([]caspaxos.AcceptorClient, 0, len(ids))
	for _, id := range ids {
		if c, ok := m.clientLocked(id); ok {
			out = append(out, c)
		}
	}
	return out
}

// allClientsLocked is clientsLocked for a quorum group: it fails if an id
// has no known address. A group with a missing member has a smaller
// majority, and that majority need not be a majority of the real group.
func (m *membership) allClientsLocked(ids []uint64) ([]caspaxos.AcceptorClient, error) {
	out := make([]caspaxos.AcceptorClient, 0, len(ids))
	for _, id := range ids {
		c, ok := m.clientLocked(id)
		if !ok {
			return nil, fmt.Errorf("membership: no address for core member %d", id)
		}
		out = append(out, c)
	}
	return out, nil
}

func (m *membership) allClients(ids []uint64) ([]caspaxos.AcceptorClient, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.allClientsLocked(ids)
}

// clientLocked returns the cached client for id. The joint proposer dedups
// acceptors by identity, so one node must always map to one client value.
func (m *membership) clientLocked(id uint64) (caspaxos.AcceptorClient, bool) {
	if id == m.self.NodeID {
		return m.local, true
	}
	c, ok := m.conns[id]
	if !ok {
		addr, known := m.addrs[id]
		if !known {
			return nil, false
		}
		c = &fencedClient{AcceptorClient: transport.NewConnectClient("http://"+addr, m.cfg.HTTP)}
		m.conns[id] = c
	}
	return c, true
}

// parseSeeds splits a comma-separated --seed value into addresses.
func parseSeeds(csv string) []string {
	var out []string
	for _, s := range strings.Split(csv, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}
