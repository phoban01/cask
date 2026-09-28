// Package cluster holds the dynamic roster path that cmd/cask and
// cmd/cask-apiserver share: one node founds the roster, every other node
// joins by learning the current core from a live peer's GET /roster and
// asking the driver to add it through POST /roster/join.
//
// The driver is the highest-id core member. It is the only node that writes
// the roster register, so joiners never duel with it.
package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/phoban01/cask/internal/ranges"
	"github.com/phoban01/cask/internal/roster"
)

// Snapshot is the GET /roster wire shape: the roster value flat (embedded, so
// a decoder expecting only roster.Value still works) plus the descriptor
// states the driver last read from the Core. The snapshot poll is how
// followers learn descriptor changes without issuing write-imposing
// consensus reads of their own.
type Snapshot struct {
	roster.Value
	Descriptors []ranges.State `json:"descriptors,omitempty"`
}

// Snap holds this node's latest known roster value (served at GET /roster so
// joining peers learn the current acceptor core) and a queue of pending joins
// it accepts at POST /roster/join. Joins are mediated so that the driver is
// the single consensus writer of the register: a joining node never proposes
// to the register itself; it posts its membership here and the driver drains
// the queue into its next reconcile.
type Snap struct {
	self    uint64
	v       atomic.Pointer[roster.Value]
	descs   atomic.Pointer[[]ranges.State] // §4.3: descriptor states, driver-refreshed
	mu      sync.Mutex
	pending map[uint64]roster.Member
}

// NewSnap returns an empty snapshot holder for node self.
func NewSnap(self uint64) *Snap {
	return &Snap{self: self, pending: map[uint64]roster.Member{}}
}

// Store records v as the latest known roster value.
func (s *Snap) Store(v roster.Value) { s.v.Store(&v) }

// Load returns the latest known roster value, if any.
func (s *Snap) Load() (roster.Value, bool) {
	if v := s.v.Load(); v != nil {
		return *v, true
	}
	return roster.Value{}, false
}

// StoreDescs records the latest known descriptor states (nil-safe no-op).
func (s *Snap) StoreDescs(descs []ranges.State) {
	if descs != nil {
		s.descs.Store(&descs)
	}
}

// LoadDescs returns the latest known descriptor states.
func (s *Snap) LoadDescs() []ranges.State {
	if d := s.descs.Load(); d != nil {
		return *d
	}
	return nil
}

// Serve handles GET /roster.
func (s *Snap) Serve(w http.ResponseWriter, _ *http.Request) {
	v := s.v.Load()
	if v == nil {
		http.Error(w, "roster not ready", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(Snapshot{Value: *v, Descriptors: s.LoadDescs()})
}

// ServeJoin handles POST /roster/join. If this node is the current driver it
// queues the member for its reconcile loop to add; otherwise it points the
// caller at the driver (whichever it currently believes that to be).
func (s *Snap) ServeJoin(w http.ResponseWriter, r *http.Request) {
	var m roster.Member
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil || m.NodeID == 0 || m.Addr == "" {
		http.Error(w, "bad member", http.StatusBadRequest)
		return
	}
	v, ok := s.Load()
	if !ok || !IsDriver(s.self, v.Core) {
		// Not the driver: tell the caller who is, so it can retarget.
		driver := ""
		if ok {
			driver = DriverAddr(v.Core, v.Members)
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

// DrainJoins returns and clears the queued pending joins.
func (s *Snap) DrainJoins() []roster.Member {
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

// IsDriver reports whether self is the single node responsible for driving
// consensus roster changes: the highest-id current core member. Restricting
// consensus writes to one node prevents reconcile loops from dueling. CASPaxos
// reads are write-imposing, so many nodes reading and writing the register at
// once would starve the multi-round reconfiguration.
func IsDriver(self uint64, core []uint64) bool {
	if len(core) == 0 {
		return false
	}
	return self == maxID(core)
}

// DriverAddr returns the address of the highest-id core member (the driver).
func DriverAddr(core []uint64, members []roster.Member) string {
	if len(core) == 0 {
		return ""
	}
	hi := maxID(core)
	for _, m := range members {
		if m.NodeID == hi {
			return m.Addr
		}
	}
	return ""
}

// maxID returns the largest id in a non-empty set.
func maxID(ids []uint64) uint64 {
	hi := ids[0]
	for _, id := range ids[1:] {
		if id > hi {
			hi = id
		}
	}
	return hi
}

// Join brings a new node into an existing cluster WITHOUT proposing to the
// register itself (which would duel with the driver). It polls candidate
// peers for a roster snapshot, adopts the current acceptor core, and, when
// addSelf is true, asks the driver to add it through POST /roster/join. It
// retries every poll until it observes itself in the membership or timeout
// passes. learn, when not nil, receives every member address Join sees.
func Join(ctx context.Context, log *slog.Logger, rost *roster.Roster, learn func([]roster.Member), hc *http.Client, self roster.Member, candidates func() []roster.Member, addSelf bool, timeout, poll time.Duration) (Snapshot, error) {
	if learn == nil {
		learn = func([]roster.Member) {}
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		cands := candidates()
		learn(cands)
		if v, ok := FetchCoreHint(ctx, hc, cands); ok && len(v.Core) > 0 {
			learn(v.Members)
			rost.AdoptCore(v.Core)
			if !addSelf {
				return v, nil // client-only: tracking the core is enough
			}
			if containsMember(v.Members, self.NodeID) {
				log.Info("joined roster", "node", self.NodeID, "core", v.Core)
				return v, nil
			}
			RequestJoin(ctx, hc, FetchTargets(cands, v.Members), self)
		}
		select {
		case <-ctx.Done():
			return Snapshot{}, fmt.Errorf("no live cluster member found to join (is a --bootstrap node up?): %w", ctx.Err())
		case <-time.After(poll):
		}
	}
}

// RequestJoin posts the member to each target's POST /roster/join. The
// current driver queues it; non-drivers reject with a redirect, which is
// harmless. Posting to all targets each retry guarantees the driver
// eventually receives it, and the driver dedups by node id.
func RequestJoin(ctx context.Context, hc *http.Client, targets []roster.Member, self roster.Member) {
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

// FetchCoreHint queries each candidate's GET /roster and returns the value
// with the highest ConfigGen. It is a non-consensus hint that lets a joiner
// discover the current acceptor core before it can read the register itself.
//
// It asks every candidate at once and waits for all answers or for ctx, so
// one hung peer costs at most the caller's deadline, not every tick.
func FetchCoreHint(ctx context.Context, hc *http.Client, candidates []roster.Member) (Snapshot, bool) {
	answers := make(chan Snapshot, len(candidates))
	asked := 0
	for _, m := range candidates {
		if m.Addr == "" {
			continue
		}
		asked++
		go func(addr string) {
			var v Snapshot
			defer func() { answers <- v }()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/roster", nil)
			if err != nil {
				return
			}
			resp, err := hc.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
				v = Snapshot{}
			}
		}(m.Addr)
	}
	var best Snapshot
	found := false
	for range asked {
		var v Snapshot
		select {
		case v = <-answers:
		case <-ctx.Done():
			return best, found
		}
		if v.ConfigGen == 0 {
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

// FetchTargets is the set of peers to poll for a roster snapshot: the current
// members plus the static or discovered candidates, deduped by node id. A
// candidate with node id 0 is a bare seed address, so it is deduped by
// address instead.
func FetchTargets(candidates, members []roster.Member) []roster.Member {
	seen := map[uint64]bool{}
	seenAddr := map[string]bool{}
	var out []roster.Member
	for _, m := range append(append([]roster.Member(nil), members...), candidates...) {
		if m.Addr == "" {
			continue
		}
		if m.NodeID == 0 {
			if seenAddr[m.Addr] {
				continue
			}
			seenAddr[m.Addr] = true
		} else {
			if seen[m.NodeID] {
				continue
			}
			seen[m.NodeID] = true
		}
		out = append(out, m)
	}
	return out
}
