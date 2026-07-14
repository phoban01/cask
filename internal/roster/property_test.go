package roster

import (
	"context"
	"math/rand"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/testutil/sim"
)

// TestReconfigurationProperties drives many randomized sequences of joins,
// departures, and reconfigurations through the reflexive roster and asserts the
// safety invariants hold after every step. It is the property-based companion to
// the hand-written scenarios in reconfig_test.go: the randomized orderings
// exercise interleavings (a departure that drops the driver, a join that grows
// the core, churn that forces repeated reconfiguration) that fixed cases miss.
//
// Invariants checked after each settled step:
//   - Core is a subset of Members (you can never be an acceptor without being a member).
//   - |Core| == min(RegisterRF, |Members|) once settled (the core is kept full).
//   - Membership is exactly the set the workload intends — no committed member
//     is ever lost across a reconfiguration (the no-lost-value property).
//   - No reconfiguration is left half-done (Joint is nil at rest).
func TestReconfigurationProperties(t *testing.T) {
	t.Parallel()
	const rf = 3
	for seed := int64(0); seed < 300; seed++ {
		seed := seed
		t.Run("", func(t *testing.T) {
			t.Parallel()
			newScenario(t, seed, rf).run()
		})
	}
}

// scenario is one randomized run over a private sim network. Node id == sim
// acceptor index (0..n-1). reader is a non-member handle with a very high id,
// so its read ballots dominate any writer's. It reads via the tracked believed
// core (like a real client), not via all n acceptors: a quorum over a superset
// group does not necessarily intersect the register's real core quorums, so an
// all-nodes read could return stale state — the pre-W2 sequential proposer
// masked this by happening to collect every reply before returning, but the
// W2 early-quorum return makes the mismatched-group read the protocol
// violation it always was.
type scenario struct {
	t      *testing.T
	seed   int64
	rf     int
	n      int
	nw     *sim.Network
	mkFor  func(self uint64) ProposerFactory
	hs     map[uint64]*Roster
	reader *Roster
	want   map[uint64]bool
	core   []uint64 // last observed acceptor core (the reader's belief)
}

func newScenario(t *testing.T, seed int64, rf int) *scenario {
	const n = 7
	stores := make([]caspaxos.Storage, n)
	for i := range stores {
		stores[i] = store.NewMem()
	}
	nw := sim.NewNetwork(stores)
	mkFor := func(self uint64) ProposerFactory {
		return func(groups [][]uint64) Proposer {
			acl := make([][]caspaxos.AcceptorClient, len(groups))
			for g, ids := range groups {
				for _, id := range ids {
					acl[g] = append(acl[g], nw.Client(int(id)))
				}
			}
			if len(acl) == 1 {
				return caspaxos.NewProposer(self, acl[0])
			}
			return caspaxos.NewJointProposer(self, acl)
		}
	}
	hs := make(map[uint64]*Roster, n)
	for id := uint64(0); id < n; id++ {
		hs[id] = New(id, mkFor(id))
	}
	const readerID = uint64(1) << 62
	return &scenario{
		t: t, seed: seed, rf: rf, n: n, nw: nw, mkFor: mkFor, hs: hs,
		reader: New(readerID, mkFor(readerID)),
		want:   map[uint64]bool{},
	}
}

func (s *scenario) run() {
	ctx := context.Background()
	if _, err := s.hs[0].Founder(ctx, mem(0)); err != nil {
		s.t.Fatalf("seed %d: founder: %v", s.seed, err)
	}
	s.want[0] = true
	s.core = []uint64{0}

	rng := rand.New(rand.NewSource(s.seed))
	steps := 4 + rng.Intn(12)
	for i := 0; i < steps; i++ {
		switch rng.Intn(3) {
		case 0, 1: // join a new member (weighted toward growth)
			if cands := s.absent(); len(cands) > 0 {
				id := cands[rng.Intn(len(cands))]
				s.want[id] = true
				s.drive(ctx, []Member{mem(id)}, nil)
			}
		case 2: // a member departs (never the last)
			if present := keysOf(s.want); len(present) > 1 {
				id := present[rng.Intn(len(present))]
				delete(s.want, id)
				s.drive(ctx, nil, []uint64{id})
			}
		}
		s.settle(ctx)
		s.check(ctx)
	}
}

// drive runs one reconcile from the current driver (highest-id core member).
func (s *scenario) drive(ctx context.Context, discovered []Member, down []uint64) {
	core := s.read(ctx).Core
	d := s.hs[maxID(core)]
	d.AdoptCore(core)
	if _, err := NewController(d).EnableCoreReconfig(s.rf).Reconcile(ctx, discovered, down); err != nil {
		s.t.Fatalf("seed %d: reconcile: %v", s.seed, err)
	}
}

// settle drives no-op reconciles until the core reaches its target with no joint
// in flight — modelling steady reconcile ticks, including the hand-off when a
// reconfiguration moves the driver to a different node.
func (s *scenario) settle(ctx context.Context) {
	for i := 0; i < 25; i++ {
		v := s.read(ctx)
		if v.Joint == nil && idsEqual(v.Core, nextCore(v.Core, v.Members, s.rf)) {
			return
		}
		s.drive(ctx, nil, nil)
	}
	s.t.Fatalf("seed %d: core never settled", s.seed)
}

func (s *scenario) check(ctx context.Context) {
	v := s.read(ctx)
	got := map[uint64]bool{}
	for _, m := range v.Members {
		got[m.NodeID] = true
	}
	if len(got) != len(s.want) {
		s.t.Fatalf("seed %d: members=%v want=%v", s.seed, keysOf(got), keysOf(s.want))
	}
	for id := range s.want {
		if !got[id] {
			s.t.Fatalf("seed %d: lost member %d (have %v)", s.seed, id, keysOf(got))
		}
	}
	for _, c := range v.Core {
		if !got[c] {
			s.t.Fatalf("seed %d: core member %d not in membership %v", s.seed, c, keysOf(got))
		}
	}
	if wantSize := min(s.rf, len(s.want)); len(v.Core) != wantSize {
		s.t.Fatalf("seed %d: |core|=%d want %d", s.seed, len(v.Core), wantSize)
	}
	if v.Joint != nil {
		s.t.Fatalf("seed %d: joint left in flight: %+v", s.seed, v.Joint)
	}
}

// read returns the current register value via the high-id reader against the
// tracked believed core, then updates the belief from the value read — the
// production client pattern (AdoptCore from a snapshot, refresh on read).
// This is sound in the test's lockstep flow: a release commits under a joint
// quorum, so a read via the pre-release core still observes it and learns the
// new core before the next write happens.
func (s *scenario) read(ctx context.Context) Value {
	s.reader.AdoptCore(append([]uint64(nil), s.core...))
	v, err := s.reader.Get(ctx)
	if err != nil {
		s.t.Fatalf("seed %d: read: %v", s.seed, err)
	}
	if len(v.Core) > 0 {
		s.core = append([]uint64(nil), v.Core...)
	}
	return v
}

func (s *scenario) absent() []uint64 {
	var out []uint64
	for id := uint64(0); id < uint64(s.n); id++ {
		if !s.want[id] {
			out = append(out, id)
		}
	}
	return out
}

func keysOf(m map[uint64]bool) []uint64 { return mapKeys(m) }
