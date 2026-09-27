package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/phoban01/cask/internal/backoff"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/cluster"
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

	// Interval is the reconcile tick and the join poll. JoinTimeout bounds
	// the wait for a live member to add this one.
	Interval    time.Duration
	JoinTimeout time.Duration
}

// membership is this apiserver's place in the fleet. It founds the roster or
// joins it through the same code cmd/cask uses (internal/cluster), serves
// GET /roster and POST /roster/join next to the acceptor, and proposes data
// writes against the roster's current core.
//
// A joined member is a participant: it is in the roster members but not in
// the core. The driver never grows the core here. Voters change only through
// the promote endpoint (issue #45).
type membership struct {
	cfg   membershipConfig
	log   *slog.Logger
	self  roster.Member
	seeds []roster.Member
	rost  *roster.Roster
	snap  *cluster.Snap

	mu       sync.Mutex
	addrs    map[uint64]string
	conns    map[uint64]caspaxos.AcceptorClient
	prop     *caspaxos.Proposer
	propCore []uint64
}

func newMembership(cfg membershipConfig, log *slog.Logger) *membership {
	if cfg.Interval == 0 {
		cfg.Interval = time.Second
	}
	if cfg.JoinTimeout == 0 {
		cfg.JoinTimeout = 60 * time.Second
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
	m.learn([]roster.Member{m.self})
	return m
}

func contentionBackoff() func(ctx context.Context, attempt int) error {
	return backoff.FullJitter(5*time.Millisecond, 500*time.Millisecond)
}

// handler serves the acceptor and the roster endpoints on one listener.
func (m *membership) handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle(transport.ConnectHandler(m.cfg.Local))
	mux.HandleFunc("/roster", m.snap.Serve)
	mux.HandleFunc("/roster/join", m.snap.ServeJoin)
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
	m.snap.Store(val)
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
	m.snap.Store(v)
	if v.Epoch != prev.Epoch || v.ConfigGen != prev.ConfigGen {
		m.log.Info("roster changed", "epoch", v.Epoch, "members", len(v.Members), "core", v.Core)
	}
}

// current returns the latest known roster value.
func (m *membership) current() (roster.Value, bool) { return m.snap.Load() }

// Propose runs a CASPaxos round on key against the roster's current core.
// Only voters hold register replicas; a participant proposes to them.
func (m *membership) Propose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error) {
	p, err := m.proposer()
	if err != nil {
		return nil, err
	}
	return p.Propose(ctx, key, change)
}

// proposer returns a proposer over the current core. It keeps one proposer
// per core so the ballot counter is not reset on every call.
func (m *membership) proposer() (*caspaxos.Proposer, error) {
	v, ok := m.snap.Load()
	if !ok || len(v.Core) == 0 {
		return nil, fmt.Errorf("membership: no roster core yet")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.prop == nil || !slices.Equal(m.propCore, v.Core) {
		m.prop = caspaxos.NewProposer(m.self.NodeID, m.clientsLocked(v.Core), caspaxos.WithBackoff(contentionBackoff()))
		m.propCore = slices.Clone(v.Core)
	}
	return m.prop, nil
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
// the local acceptor. An id with no known address is skipped.
func (m *membership) clientsLocked(ids []uint64) []caspaxos.AcceptorClient {
	out := make([]caspaxos.AcceptorClient, 0, len(ids))
	for _, id := range ids {
		if id == m.self.NodeID {
			out = append(out, m.cfg.Local)
			continue
		}
		c, ok := m.conns[id]
		if !ok {
			addr, known := m.addrs[id]
			if !known {
				continue
			}
			c = transport.NewConnectClient("http://"+addr, m.cfg.HTTP)
			m.conns[id] = c
		}
		out = append(out, c)
	}
	return out
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
