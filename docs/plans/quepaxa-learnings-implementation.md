# QuePaxa learnings — detailed implementation plan

Companion to `quepaxa-meerkat-review.md` (the comparison and migration verdict: stay on
CASPaxos, adopt the learnings). This document turns workstreams W1–W5 into
implementable, ordered work items, plus W6 (paper + blog + Kubernetes multi-cluster
story). Grounded against the code as of 2026-07-14 (commit `4d82117` + working tree).

Guiding stance (the QuePaxa lesson): **mechanisms whose misconfiguration costs
performance, never liveness or safety.**

## Sequencing

| Order | Item | Size | Depends on |
|---|---|---|---|
| 1 | W0 — ballot-space discipline fix (safety pre-req, discovered during planning) | S | — |
| 2 | W2 — parallel fan-out + early-quorum | M | — |
| 3 | W3 — randomized backoff + dueling-proposers fault | S | — |
| 4 | W1 — ownership manager, fast path on the prod write path | L | W0; W2 recommended first (touches same files) |
| 5 | W4 — owner-cached reads | M | W1 |
| 6 | W5 — contention profile, S13, L1, CI wiring | M | lands incrementally with W0–W4 |
| 7 | W6 — paper, blog post, k8s aggregated-API prototype | L | W1/W2/W4 benches feed the paper |

W0, W2, W3 are independent of each other and individually landable. Nothing here
depends on §3.0 (Pebble); interactions are noted where they exist.

---

## W0 — Ballot-space discipline (fix a latent lost-update hazard)

**Why.** Planning W1 surfaced a real safety bug that fires the moment `OwnedProposer`
shares a key with a full `Proposer` — which is exactly what W1's fallback path does.

The hazard: an owner at epoch `e` has written up to seq `s` (promises at
`e<<40|s`). A full proposer's prepare is rejected with `Conflict = e<<40|s`
(`acceptor.go:66`); `nextBallot` bumps to `Counter = e<<40|s+1` (`proposer.go:75-83`)
— **inside the owner's epoch space**. It commits a new value there. The owner's next
`Write` also mints `e<<40|s+1` (`owned.go:126-127`) — the *same counter*. The tie is
broken by NodeID (`ballot.go`), so if the owner's NodeID is higher, its accept
satisfies `!b.Less(reg.Promise)` (`acceptor.go:78`) and **overwrites the full
proposer's committed value with one derived from the owner's stale cache**. A lost
update — silent, quorum-accepted, ~50% likely depending on NodeID order.

Today this is unreachable in production only because `OwnedProposer` is unused outside
the simulator, and the existing `epoch_old_owner_write` fault only tests owner-vs-owner
at different epochs, never owner-vs-full-proposer.

**Fix.** Full proposers must never mint ballots inside an epoch space someone may
seq-bump into. Two coordinated changes in `internal/caspaxos`:

1. `Proposer.nextBallot` (`proposer.go:75`): when jumping past a conflict whose epoch
   bits are nonzero (`atLeast.Counter >= 1<<epochShift`), round up to the **next epoch
   boundary** instead of `+1`:
   ```go
   if atLeast.Counter >= 1<<epochShift {
       atLeast.Counter = ((atLeast.Counter >> epochShift) + 1) << epochShift
   }
   ```
   The interloper's ballot `(e+1)<<40|0…` now strictly dominates the owner's entire
   space; the owner's next `Write` NACKs cleanly → `ErrLostOwnership` → re-acquire.
   Within the synthetic space, full-proposer-vs-full-proposer contention re-triggers
   the jump (one epoch burned per preemption round). Budget note: 2^24 epochs per key;
   sustained adversarial contention burns ≤ `maxRounds` epochs per failed Propose —
   document alongside the existing 2^24-handoffs bound in `owned.go:25-29`.

2. `OwnedProposer.TakeOwnership` (`owned.go:71`): track the max `Conflict` from
   rejected prepares and, on quorum failure, return a typed error
   ```go
   type EpochBehindError struct{ Observed uint64 } // errors.Is → ErrLostOwnership
   ```
   so the caller can fast-forward its fence past a synthetic epoch instead of blindly
   retrying at a fence that can never win (fence `e+1` prepares at `(e+1)<<40|0`,
   which loses to any promise `(e+1)<<40|k, k≥1` — it must go to `e+2`).

3. `lease.Locks`: add `Bump(ctx, name, sessionID, minFence) (uint64, error)` — CAS the
   fence upward for the *existing holder* (today `Acquire` returns the existing token
   unchanged when re-acquiring your own lock, `lock.go:50-52`, so a holder cannot
   raise its fence). Also extend the takeover path: `newFence = max(observed+1,
   minFence)`. Fence stays strictly monotonic — S7/`quint/lease.qnt` FenceMonotone
   unaffected (verify with TLC; the change only widens the increment).

**Sim.** New fault `owner_vs_full_proposer` in `testutil/sim/faults/protocol.go`,
modeled on `EpochOldOwnerWrite`: establish an owner (epoch 100), commit a marker via
owner `Write`; concurrently drive a full `Proposer` Put through `mvcc.KV`; then have
the owner attempt another `Write`. Run both NodeID orders (owner higher / lower).
Assert both OpIDs appear in the final chain — caught structurally by the new S13
invariant (W5) even if the fault's own assertion is skipped. Add the fault name
constant to `testutil/sim/profile.go` and to the `consensus` + `contention` profiles.

**Files.** `internal/caspaxos/proposer.go`, `owned.go`, `errors.go`;
`internal/lease/lock.go`; `testutil/sim/faults/protocol.go`, `testutil/sim/profile.go`;
`quint/owned_register.qnt` (new), `quint/lease.qnt` (Bump action).

**Done when.** The new fault run *without* the nextBallot fix reproduces the lost
update (keep as a commented regression note or a unit test asserting the old behavior
fails); with the fix, `consensus` profile passes its seed budget; a unit test in
`caspaxos` covers the exact tie scenario at both NodeID orders.

**Implementation notes (landed 2026-07-14).** As specified above, plus three
things discovered while landing it:

- `TakeOwnership` also rejects epoch 0 (`ErrEpochReserved`) — counters below
  `1<<epochShift` are the plain-proposer space, and an owner minting there
  would break the discipline from the other side.
- The gate could not see the bug through snapshot invariants alone: the duel
  completes within one round, and snapshot-pair invariants only compare
  quiescent endpoints. Two additions close this: **S13** (committed MVCC
  history is append-only, comparing quorum-committed chains only, tolerant of
  compaction and abandoned partial accepts) for the cross-round class, and
  **FAULT-ASSERT** (any new `WARNING` trace event fails the gate) so a fault's
  own observation of a mid-round safety break is a violation, not log noise.
- Validation was run in both directions: with the jump rule temporarily
  disabled, the unit test, the fault test, and the gate (seed 1, round 5,
  FAULT-ASSERT) all reproduce the lost update; with it enabled, everything is
  green. `stepBumpRule` in `quint/owned_register.qnt` is the permanent negative control
  (JumpRule = FALSE must violate NoLostUpdate).
- TLC was actually run for the first time in this repo's history (JDK via
  nix-shell; see quint/PARITY.md). `OwnedRegister.cfg` passes (~1M distinct
  states); the negative control finds the exact predicted trace (owner and
  full proposer tie at counter (epoch,seq) = (2,1), owner wins the NodeID
  tiebreak, full proposer's committed value vanishes). The first-ever full
  suite run also surfaced four latent defects in pre-existing specs (fixed —
  see quint/PARITY.md) and one genuine design constraint: the §4.4 snapshot
  contract requires read timestamps at or below the range's applied HLC.
  **W4 must carry this rule** (it is the same MaxOffset/read-freshness
  reasoning as the owner-cache guard), and §4.6 GetReadVersion is its
  general mechanism.

---

## W1 — Ownership manager: 1-RTT writes on the production path

**Why.** `OwnedProposer` (the QuePaxa-leader-equivalent fast path) exists but only the
simulator uses it. `mvcc.KV`, `lease`, and the `agent.Router` all take the 2-RTT
`Proposer.Propose` path. This is the single biggest write-latency win and the
`agent/router.go` package comment already names it as the intended M7 shape.

**Design decisions.**

- **Ownership granularity: per range** (not per key). One ownership *grant* per range;
  per-key `OwnedProposer` instances are lazily created under that grant, all fenced by
  the same epoch. Rationale: one lock acquisition + one session amortizes over every
  key in the range; per-key locks would be O(keys) control-plane load.
- **Epoch source: a range-ownership lock's fencing token.** Key
  `\x00rown\x00<be64 rangeID>`, living in the control-plane keyspace (the Core range,
  per design C'; in today's single-range world, the same replica set). Fence = epoch.
  This satisfies `OwnedProposer`'s at-most-one-writer-per-epoch requirement *by
  construction* — HRW alone cannot (two nodes with different roster views could both
  believe they are HRW owner at the same epoch → the W0 lost-update scenario at equal
  epochs, which epoch fencing does not repair).
- **Eligibility vs safety:** only the HRW owner hint (`Descriptor.Owner`,
  `ranges.go:42`) *attempts* acquisition — a routing optimization to avoid lock
  contention. Safety never depends on who acquires: the fence does the work.
- **Non-owners keep proposing via the full path.** After W0 this is safe: a non-owner
  write fences the owner out (owner sees `ErrLostOwnership`, re-acquires with `Bump`).
  Owner forwarding (routing non-owner writes to the owner over the wire) is explicitly
  out of scope here — it needs the node-to-node proposer RPC and can land later; the
  fast path already wins because the HRW hint concentrates writes per key on one node.
- **CAS conflicts fall back to the full path.** `OwnedProposer.Write` evaluates the
  change against its cache and returns `ErrConflict` *without a network round*
  (`owned.go:121-123`). A deposed-but-unaware owner could therefore judge a CAS
  against a stale cache. Until W4's lease guard exists, the manager must treat
  `ErrConflict` from the fast path as a miss and re-run the op through full `Propose`
  (which write-backs and judges linearizably, `proposer.go:124-143`). Conflicts are
  the rare path; correctness over cleverness.

**What.**

1. New package `internal/owner`:
   ```go
   type Manager struct {
       nodeID   uint64
       rmap     *ranges.Map            // range → descriptor (replicas, epoch)
       dialer   agent.Dialer           // node → AcceptorClient
       ctl      lease.Proposer         // control-plane proposer for locks/sessions
       sessions *lease.Sessions        // node session ("node-<id>", TTL 10s, keepalive TTL/3)
       locks    *lease.Locks
       // per range: grant{fence uint64, perKey map[string]*caspaxos.OwnedProposer}
   }
   func (m *Manager) FastPropose(ctx, key []byte, change caspaxos.ChangeFunc) ([]byte, bool, error)
   ```
   `FastPropose` returns `handled=false` (fall back) when: this node doesn't hold the
   range grant; `TakeOwnership`/`Write` returns `ErrLostOwnership` (after one
   re-acquire attempt via `Bump` with `EpochBehindError.Observed+1`); or the change
   returned `ErrConflict` (see above). All fallbacks and hits are counted — this
   telemetry is the input the deferred bandit-placement idea needs.
2. `agent.Router` (`router.go:68`): accept an optional `FastProposer` (the manager);
   `Propose` tries it first, falls back to the cached full proposer. The
   `mvcc.Proposer` seam (`mvcc.go:104-106`) is unchanged — mvcc, lease, watch, roster
   need **zero changes**.
3. Grant lifecycle: acquire on becoming HRW owner (reconcile tick), `Bump` on
   `EpochBehindError`, drop grant + invalidate all per-key proposers on: session
   lapse, descriptor epoch change (`cachedProposer`-style check, `router.go:92`), or
   roster-driven replica change. Wire the reconcile hook in `cmd/cask/cluster.go`.
4. Exclusions: the roster register (`\x00roster`) and reconfig carry-forward keep the
   full/joint path unconditionally — joint quorums don't fit single-group
   `OwnedProposer`, and the single-driver convention already serializes them.

**Sim/tests.** Raise `owned_proposer_force_full_round` to 0.05 under the `contention`
profile so fast/slow interleave heavily. Extend `MVCCWorkload` (`workload.go`) with an
ownership-enabled variant (manager over the sim network + SimClock-driven session
clock). S11 (owner-epoch dominance) and new S13 (append-only history) are the safety
net; add invariant assertion that a `handled` fast write's OpID appears in the chain.

**Files.** `internal/owner/manager.go` (new, ~200 LOC), `internal/agent/router.go`,
`cmd/cask/cluster.go`, `internal/lease/lock.go` (from W0), `testutil/sim/workload.go`,
`testutil/sim/stores.go` as needed.

**Done when.** Steady-state Put on an owned key issues exactly one Accept round on the
wire (assert via a counting `AcceptorClient` decorator in tests); mixed
owner/non-owner writes pass the `contention` profile seed budget; bench
`bench_put_owned` shows ~2× throughput / ~half latency vs full-round Put on the same
sim network; lock/session churn during a 100-round ownership-flapping scenario never
violates S1/S2/S11/S13.

**Implementation notes (landed 2026-07-14).** As designed, plus three things
the tests surfaced:

- **Retry-after-bump is load-bearing, not an optimization.** A fence-bump
  followed by a full-path fallback livelocks against ITSELF: the fallback
  write epoch-jumps into exactly the epoch the manager just bumped to, and
  being minted by the same node id, the next TakeOwnership collides with it
  (equal ballot) and forces another bump, forever. FastPropose therefore
  retries the fast path once after a successful bump — the retried take
  lands cleanly because nothing occupied the fresh epoch's boundary.
- **TakeOwnership is now idempotent per epoch** (`owning && epoch == e` →
  no-op): concurrent writers racing to the first take would otherwise
  re-prepare at the same epoch and be rejected by their own promise — a
  false self-deposition. Combined with the rule that every take/write
  failure funnels through a strictly-fence-raising recovery, retries can
  never self-conflict with residue from failed attempts.
- **Recursion guard**: control-plane sessions/locks ride the same dynamic
  proposer the manager serves; FastPropose declines ownership-lock keys
  (`\x00lock\x00rown/` prefix) so lock traffic cannot re-enter the manager.
  Covered by a test wiring the exact production shape (late-bound proposer).
- Grant lifecycle rides the reconcile loop (`Maintain`: session renew, HRW
  eligibility, descriptor-epoch invalidation); never on the write path.
  Telemetry (fast writes / fallbacks / bumps / grants) feeds the deferred
  bandit-placement idea. The gate now runs every profile through BOTH
  topologies (`MVCCWorkload` and `OwnershipWorkload`).
- Test hygiene note: W2's early-quorum return means phase RPCs can land
  after the call returns — wire-counting tests must drain stragglers before
  resetting counters.

---

## W2 — Parallel fan-out, early-quorum return, cancellation

**Why.** `Proposer.prepare`/`accept` iterate acceptors **serially**
(`proposer.go:151-198`), as do `OwnedProposer.TakeOwnership`/`Write`
(`owned.go:83-93, 128-135`). Latency is Σ(replica RTTs) instead of max(quorum RTT),
and a dead peer stalls each phase for the full 4 s overlay RPC timeout
(`nebula.go:99`) — serially, per phase. This is cask's own small tyranny of timeouts;
roadmap §3.6 already names early-quorum as the fix.

**What.**

1. A `fanout` helper in `internal/caspaxos` (used by both proposer types):
   - Launch one goroutine per acceptor with a per-phase `context.WithCancel`.
   - Aggregate on the caller's goroutine: per-group promise/accept counts, running
     `best`/`current` (prepare), running max `Conflict`.
   - Return early when `quorumInAllGroups` is satisfiable no matter the stragglers
     (success) **or** provably unsatisfiable (enough failures/rejections in some
     group) — then cancel the rest. Early success with a quorum subset is safe:
     CASPaxos needs any quorum, and the highest-accepted-value rule only requires the
     quorum actually counted.
   - Cancelled/late accepts may still land on stragglers — Paxos tolerates this;
     the `duplicate_delivery` fault already exercises the acceptor idempotency this
     relies on (`network.go:193-219`).
2. **Buggify determinism rule:** `buggify.Maybe` sites currently sit inside the
   acceptor loop (`proposer_drop_vote`, `proposer.go:157`) and would race the
   unguarded sim RNG once the loop is concurrent (`sim.go:27-32`). Pre-draw all
   per-acceptor decisions serially on the calling goroutine into a `[]bool` *before*
   launching goroutines. Document the rule in `internal/buggify`: **no `Maybe` call
   from a goroutine the round spawned.**
3. Phase deadline: keep the 4 s transport ceiling as the backstop; early-quorum makes
   it irrelevant when a quorum is healthy. No new tunable — a slow replica now costs
   nothing (the QuePaxa stance: the timeout can only hurt performance, never block a
   healthy quorum).
4. Verify the sim `Network` under concurrency: `link` methods are already
   mutex-guarded and `slow_link` uses ctx-aware `time.After` (`network.go:224-235`) —
   delays now overlap instead of summing; adjust any gate timing assumptions.
   `keyedMutex` striping on the acceptor side is already concurrency-correct.

**Files.** `internal/caspaxos/proposer.go`, `owned.go`, new `fanout.go`;
`internal/buggify/buggify.go` (doc + optional debug assertion); no transport changes
(`http.go` uses `NewRequestWithContext`, cancellation already propagates).

**Done when.** §3.6's done-when: under a sim nemesis delaying 1-of-3 replicas by
50 ms (`slow_link`), p99 Propose latency ≈ no-nemesis baseline (was: +100 ms —
50 ms × 2 phases). With 1-of-3 crashed, Propose completes in ~1 quorum RTT per phase,
not 4 s. `consensus` profile passes; `go test -race ./internal/caspaxos/...` clean.

**Implementation notes (landed 2026-07-14).** As specified, plus what landing
it surfaced:

- The buggify hook now has its own seeded, mutex-guarded RNG stream and an
  atomic hook slot (`buggify.SetHook`): acceptor-side sites fire from fan-out
  workers, which would have raced both the shared adversary RNG and the
  scenario-swap of the hook global. Stream isolation is a strict improvement —
  buggify draw counts can no longer perturb the fault schedule.
- Early-quorum return exposed a latent protocol misuse in
  `roster/property_test.go`: its reader proposed over ALL nodes while the
  register's group was the 3-node core; a 4-of-7 quorum need not intersect a
  2-of-3 core quorum. The sequential proposer masked it by collecting every
  reply. The test now reads via a tracked believed core — the production
  client pattern. Lesson recorded: **a proposer's group must be the
  register's group**; superset reads were never sound.
- Impossibility detection (`quorumStillPossible`) fails a phase as soon as a
  group provably cannot reach quorum — a fully-down joint group no longer
  waits out the stragglers.
- New latency regression tests: a blocked (not yet timed-out) peer and a
  300 ms-slow replica are both off the critical path for `Propose` and
  `OwnedProposer.Write`; early-quorum value carry is asserted against a slow
  value-holder; joint impossibility fails fast.

---

## W3 — Randomized backoff on preemption

**Why.** Contention is resolved by ballot bumping with a retry budget
(`maxRounds = 12`) and **no delay** — two symmetric proposers can duel to
`ErrPreempted` in microseconds. The bootstrap livelock was fixed by *convention*
(single driver, adopt-don't-read, `cluster.go:143-154`); conventions don't compose.
QuePaxa gets guaranteed probabilistic termination from randomization; jittered backoff
is the register-protocol equivalent, and it belongs in the protocol layer, not in each
caller.

**What.**

1. `internal/backoff` (new, ~40 LOC): `Policy` interface + `FullJitter{Base, Cap,
   Rand}` — sleep `rand(0, min(Cap, Base<<attempt))`, ctx-aware. Defaults: Base 5 ms,
   Cap 500 ms (LAN-appropriate; a WAN profile can widen it — misconfiguration costs
   latency, never liveness).
2. `caspaxos.Proposer`: injected hook, keeping the core pure of clocks:
   `WithBackoff(f func(ctx context.Context, attempt int) error)` (option on
   `NewProposer`/`NewJointProposer`). Called between rounds only after a preemption
   (`proposer.go:119-121, 137-139`), never on the first attempt — the fast path pays
   nothing. Nil = today's behavior (unit tests unchanged).
3. Wiring: production proposers (`agent.Router.proposerFor`, roster controller,
   `cmd/cask/cluster.go`) get `FullJitter` with a `math/rand` source; unify the
   ad-hoc jittered retry that commit `10e1d3f` added in the command layer onto
   `internal/backoff`. The sim wires a deterministic policy that advances `SimClock`
   instead of sleeping, with a `rand.Rand` seeded from the scenario seed **created on
   the gate goroutine per proposer** (respecting the RNG-ownership rule).
4. `lease.Locks.Acquire` retry loop (`lock.go:45`): apply the same policy between
   attempts (currently a tight re-read loop; 8 attempts can exhaust in one contention
   burst).

**Sim.** New fault `dueling_proposers` (`faults/protocol.go`): K=4 concurrent
`mvcc.KV` writers (distinct proposer NodeIDs *and* distinct KV OpID node IDs —
`mvcc.go:120-123`) hammer one key with Put/CAS, no driver convention, network healed.
Liveness assertion (first of its kind in the gate): all K complete within the round
budget with zero `ErrPreempted`. This empirically demonstrates the property QuePaxa
has by construction, in the regime it was designed for.

**Files.** `internal/backoff/` (new), `internal/caspaxos/proposer.go`,
`internal/agent/router.go`, `internal/lease/lock.go`, `internal/roster/controller.go`,
`cmd/cask/cluster.go`, `testutil/sim/faults/protocol.go`, `testutil/sim/profile.go`.

**Done when.** `dueling_proposers` passes its seed budget with backoff on and
demonstrably fails (ErrPreempted observed) with backoff off at K=4/maxRounds=12
(regression-style proof the mechanism is load-bearing); single-driver rule remains for
the roster (message-load reasons) but a gate scenario with the convention disabled now
passes.

**Implementation notes (landed 2026-07-14).** As specified, with one honest
correction to the done-when: the "fails with backoff off" regression proof
does NOT reproduce at unit-test scale — 20 runs of 6 writers × 5 ops over
in-memory acceptors converged every time without backoff, because
near-zero-RTT rounds break symmetry by scheduler noise alone. The livelock
class needs real network RTTs (as in the multi-lighthouse bootstrap
incident), so backoff is defense-in-depth validated by: convergence +
exactly-once accounting under contention (unit test), the standing
`dueling_proposers` gate fault (WARNING → FAULT-ASSERT on any preemption
exhaustion), and the bootstrap history. Wiring: `caspaxos.WithBackoff`
option (called only between preempted rounds), `agent.Router` WithBackoff,
`lease.Locks` WithAcquireBackoff (Acquire + Bump retries), production
policy `backoff.FullJitter(5ms, 500ms)` in cmd; the simulator uses
`backoff.Seeded` streams drawn on the gate goroutine.

---

## W4 — Owner-cached reads (zero-RTT linearizable reads)

**Why.** Every read is a full write-imposing 2-RTT round (`mvcc.go:339`,
`change.go` Identity) — the biggest gap vs etcd (roadmap §3.1: reads ≥10× writes in a
coordination store), and the reason startup reads duelled the bootstrap seeder.
Meerkat offers local stale reads; cask can do better — *linearizable* local reads
under the ownership lease.

**Design.** Epoch fencing alone cannot make local reads safe: a deposed owner detects
deposition only when a *write* NACKs; a read has no round to detect it. The guard must
be temporal — this is the classic leader-lease read, and it needs the double-sided
MaxOffset discipline (introduces `MaxOffset`, default 500 ms, the design-A constant
from §4.4):

- **Owner side:** serve `ReadLocal` only while `now_owner + MaxOffset < Expiry` of its
  ownership session (the freshest Expiry confirmed by its last successful
  `KeepAlive`, which returns the committed Session — `session.go:83-95`).
- **Taker side:** the ownership manager (only — user-facing `Locks` semantics are
  untouched) treats a lapsed prior holder as takeable only after
  `now_taker ≥ Expiry + MaxOffset`.

Under |skew| ≤ MaxOffset the old owner stops serving (true time ≤ Expiry) before the
new owner can start (true time ≥ Expiry); beyond MaxOffset the contract is the
documented uncertainty window of design A. Skew beyond bound = stale reads possible,
never lost writes (writes stay epoch-fenced).

**What.**

1. `OwnedProposer.ReadLocal(key) ([]byte, error)`: return the cached value iff
   `owning` **and** an injected `leaseValid func() bool` (supplied by the owner
   manager, which owns clocks — the caspaxos core stays time-free) reports true; else
   `ErrLostOwnership`.
2. `owner.Manager.ReadLocal(ctx, key) ([]byte, bool /*served*/, error)` implementing
   the guard; takeover delay in the manager's acquire path.
3. `mvcc.KV`: optional `LocalReader` (functional option). The single seam is
   `read()` (`mvcc.go:338-344`): try the local reader first — **finally activating
   the already-registered `mvcc_skip_owner_cache` buggify site (`mvcc.go:24-29`)** to
   force the full-round fallback probabilistically. `Get`, `GetAt`, `History`,
   `SnapshotAt` all inherit it through `read()`.
4. `StaleOK` reads (any replica, zero consensus — Meerkat's "read from any replica's
   local data"): requires a read RPC on the transport (`/v1/read` returning the local
   accepted register). Stretch goal — separate PR, documented as
   possibly-stale-by-design.
5. `quint/lease.qnt`: add the `LocalRead(owner, key)` action per §3.1 and re-run TLC
   with the double-sided guard modeled.

**Sim.** `hlc_skew_forward` buggify site plus a new `clock_skew_owner_read` fault:
skew the old owner's SimClock backward within/beyond MaxOffset across an ownership
handoff and assert reads within bound never observe pre-handoff values after the new
owner's first write (a read-after-write flip test — the Jepsen fencing scenario of
§3.1's done-when, run in the sim first).

**Files.** `internal/caspaxos/owned.go`, `internal/owner/manager.go`,
`internal/mvcc/mvcc.go`, `internal/lease/session.go` (expose confirmed Expiry),
`quint/lease.qnt`, `testutil/sim/faults/` (+ profile).

**Done when.** Owned-key `Get` issues zero RPCs (counting-decorator assertion); the
skew fault passes within MaxOffset and the violation beyond MaxOffset is *observed and
documented* (proves the bound is real, not vestigial); porcupine linearizability run
including ReadLocal-served reads passes under partition nemesis.

**Depends on:** W1. Interplay with §3.0 (Pebble): none for correctness — the cache is
process-local and dies with the process (`owning=false` on restart); durable acceptor
state is §3.0's separate concern.

**Implementation notes (landed 2026-07-15).** The design survived contact with
one major scoping discovery and shipped with it handled honestly:

- **Epoch fencing cannot protect reads at all** — a write detects deposition
  by its accept NACKing; a read has no round. Worse, the owner's own
  CAS-conflict fallbacks commit via the full path AROUND its own cache:
  staleness needs no second node and no clock skew. Two mechanisms close
  this: the double-sided MaxOffset lease guard (owner stops serving
  MaxOffset before its confirmed session expiry; a takeover of a lapsed
  holder waits MaxOffset past it), and **cache invalidation on every
  full-path write the router carries** (`agent.LocalInvalidator`). Together
  they make local reads linearizable under the writes-via-owner discipline.
- **That discipline holds per-node today, not cluster-wide**: a write
  entering through another node's router bypasses this node's invalidation.
  Hence local reads are wired in the sim topology (single router — the
  discipline holds, the gate exercises the path fully) but stay OFF in
  cmd/cask until M7 forwarding. `TestCrossNodeWriteReadGapIsDocumented`
  pins the gap as a regression marker: when forwarding closes it, that test
  fails and the caveats come out together.
  *(Closed 2026-07-15 by M7 — see the M7 notes below; the marker test was
  repurposed as `TestBareProposerWriteIsOutOfContract`.)*

### M7 — write-forwarding to the range owner (landed 2026-07-15)

Three mechanisms make the writes-via-owner discipline hold cluster-wide,
which turns W4's local reads on in production (`cmd/cask`):

1. **Forwarding** (`cmd/cask/forward.go`): client `/kv/` and `/cas/` requests
   entering a non-owner node are proxied one hop to the range-owner hint over
   the overlay (the overlay listener already serves the full API mux). An
   `X-Cask-Forwarded` header guarantees at most one hop — a mis-addressed
   forward is handled locally under the gate, never bounced.
2. **The full-path gate** (`owner.GateFullPath`, consulted by the Router):
   when forwarding fails, a local full-path write for a client key is
   REFUSED with `ErrOwnerLive` (HTTP 503, retryable) while another node's
   ownership session is live or within MaxOffset of lapse — the Raft-lease
   rule: you cannot commit around a live read lease. Once the lease lapses
   past the margin, local writes proceed (and fence the dead owner's epoch).
3. **Keyspace split**: the fast path (writes AND cached reads) now covers
   ONLY client keys (no `\x00` prefix). Internal registers — sessions,
   locks, roster — are written full-path by every node by design and are
   never cached, which also subsumes the old rown-lock recursion exclusion.

Scope notes: reads forward opportunistically (the owner serves them with
zero rounds) but any node may always serve a full-round read locally —
quorum reads are linearizable regardless. Bare `caspaxos.Proposer` writers
bypass all three mechanisms and are out of contract (the repurposed test
documents the resulting staleness and its recovery). The gate costs two
control-plane reads per gated propose — only on the forward-failed path;
cache the verdict if that ever shows up in profiles.

### §4.1 — ErrRangeChanged (landed 2026-07-15)

Stale routing is now a typed, recoverable condition instead of a silent
round against the wrong replicas:

- The router stamps each proposal with the descriptor epoch it routed under
  (`ranges.WithClaimedEpoch`, carried as the `Cask-Range-Epoch` header on
  both transports — no protobuf change needed).
- The serving node compares it against its CURRENT placement (from the
  roster snapshot) and rejects OLDER claims with `caspaxos.ErrRangeChanged`
  before touching the acceptor. Newer or absent claims pass: the acceptor
  itself is deliberately range-agnostic, and a behind server is harmless.
- The proposer treats `ErrRangeChanged` unlike a transport failure: it
  aborts the round immediately (a stale replica set can never yield a
  trustworthy quorum) instead of counting a non-vote.
- The router re-resolves placement (`agent.WithRefresh` → `placeRange` of
  the latest snapshot in cmd), drops its proposer cache, and retries ONCE;
  persistent staleness propagates to the caller.

**Known hole this does NOT fix (tracked)**: `reconfig.CarryForwardKeys` is
never driven from cmd — when HRW re-places the range's replicas on a roster
change, committed data is not carried to the new acceptor set. §4.1 is the
client-refresh half; the carry-forward driver is §4.3's replica-set-reconfig
scope and is release-blocking for any multi-epoch churn claim.
*(Closed 2026-07-15 by the §4.3 wiring below.)*

### §4.3 — descriptor-driven placement + sound replica reconfig (landed 2026-07-15)

The data-loss hole is closed end to end. Foundation (commit `9dcbf97`):
descriptor registers at `\x00rd/<id>` on the Core, `Descriptor.Joint` making
in-flight reconfigurations visible to routing (joint proposers), and
`Orchestrator.ReconfigReplicas` — publish-joint → settle → majority-union
carry-forward → release, resumable via the register's Joint marker. cmd
wiring (this commit):

- **Distribution channel**: descriptor states ride the existing `/roster`
  snapshot (`snapPayload` embeds the roster value flat for wire-compat). The
  DRIVER is the sole consensus reader/writer of descriptors (`driveRanges`
  per tick: seed genesis from placement, trigger reconfig via
  `placement.NeedsReconfig`, refresh states); followers and joiners learn
  them from their normal poll — no consensus-read storms, no dueling.
- **Routing rebuilds on a descriptor fingerprint** (ids+epochs+tombstones):
  descriptor epochs move without roster epochs, and the orchestrator's
  settle (2 reconcile ticks + slack) is calibrated to exactly this
  observation latency. `rmapFromSnap` falls back to legacy HRW placement
  until the genesis descriptor exists, so pre-§4.3 clusters upgrade in
  place. `epochOf` (§4.1) and the M7 forwarder's owner hint now come from
  descriptors too.
- **Key enumeration**: `/rangekeys` endpoint (store.Lister) + majority-union
  over the old replicas.
- **Owner-manager hardening the analysis demanded**: fast-path rounds stamp
  their grant's descriptor epoch (an owner that misses the joint publish is
  fenced by §4.1 like any stale writer — owned writes previously carried NO
  epoch), and `Maintain` takes no grant on a joint range (owned single-set
  accepts cannot satisfy joint quorums).

Tested: the full driver flow (seed from placement → membership change →
background reconfig → all data served from the new set → fingerprint
retarget) plus the ranges-layer suite from the foundation commit.

**Split/merge (Variant 1) — landed 2026-07-15, completing §4.3:**

- `Orchestrator.Split(id, at, left, right)`: record the `SplitIntent` on the
  old descriptor (CAS — serializes concurrent splits AND makes every later
  step resumable), create both descriptors on the Core, CUTOVER via the
  injected roster commit (`roster.UpdateRangeIDs` swaps the ids; clients
  route to the halves from their next snapshot poll), tombstone the old
  descriptor with `ReplacedBy`. Data moves nowhere — both halves inherit the
  replica set; later `ReconfigReplicas` calls rebalance them independently.
- **Epoch lineage rule (deviation from the roadmap's `Epoch = 1`)**: new
  descriptors start at `old.Epoch+1`, keeping §4.1's per-key epoch
  comparison monotonic across the lineage — a stale pre-split claim must
  reject against either half, which `Epoch = 1` would break as soon as the
  claim exceeded 1.
- `Orchestrator.Merge(left, right, into)`: the inverse, intent on the left
  descriptor; requires adjacency and a COMMON replica set (reconfigure
  first — merge moves no data either).
- cmd: `driveRanges` resumes recorded split/merge intents before considering
  placement moves; `/admin/split?range&at` and `/admin/merge?left&right`
  are the operator triggers, driver-gated like joins (split-point policy is
  the roadmap's deferred sub-decision). Crash between cutover and tombstone
  leaves an un-tombstoned old descriptor that no routing names — snapshot
  distribution makes the tombstone belt-and-braces for direct register
  readers only.

Tested additionally: split→write-both-halves→merge round trip with all data
readable throughout; interrupted-split resume (intent recorded, driver
crashed) and conflicting-parameter refusal; merge refusal across differing
replica sets. §4.3 is complete; remaining §4.3-adjacent polish: tombstone GC
(TTL, deferred sub-decision) and split-point selection heuristics.
- A degraded read (guard refusal) lands on the owner's 1-RTT identity round,
  not the 2-RTT full path — reads never get slower than pre-W4.
- `quint/owner_reads.qnt` proves the guard under adversarial skew
  (`NoStaleRead`, both clients' offsets universally quantified), with
  `OwnerReadsBug.cfg` as the negative control (naive lapsed-checks: TLC
  finds the fast-clock successor / slow-clock reader overlap). Both verified
  by TLC this session.
- Fence recovery must poison per-key caches IN PLACE (`Disown`), never by
  replacing the keyOwner objects — a caller mid-retry holds a reference,
  and a replacement would later self-conflict with what the retained object
  commits post-bump.

---

## W5 — Validation: contention profile, new invariants, CI

**Why.** Cloudflare's Meerkat bar is DST + formal verification; cask's ladder already
matches it. What's missing is coverage of exactly the regime QuePaxa was built for —
contention + asymmetric latency — plus a structural invariant that catches the W0
class (lost updates) everywhere, not just where a fault asserts it.

**What.**

1. **Invariant S13 — append-only committed history** (`invariants.go`): the gate keeps
   the previous snapshot per key; at each quiescent step, the prior version list must
   be a prefix of the current one (modulo compaction: drop entries below
   `CompactedBelow` before comparing; tombstones included). This converts any lost
   update — W0's class, future regressions — into a gate violation regardless of
   which fault produced it. Checkable from durable register state alone, like
   S1–S3/S11.
2. **Liveness check L1** (first liveness invariant, `gate.go`): during healed dwells
   (network healed, no fault active), workload rounds must complete without
   `ErrPreempted`/`ErrContended`. Fault-aware via the existing `expectedUnderFault`
   filtering; never evaluated mid-fault (liveness under partition is not promised).
3. **Profile `contention`** (`profile.go`): faults `dueling_proposers`,
   `owner_vs_full_proposer`, `slow_link`, `asymmetric_reachability`,
   `duplicate_delivery`, `epoch_old_owner_write`; site overrides
   `acceptor_spurious_preempted: 0.05`, `proposer_drop_vote: 0.03`,
   `owned_proposer_force_full_round: 0.05`. This is the QuePaxa regime as a standing
   release gate.
4. **CI** (`.github/`, `scripts/`, `gate_ci_test.go`): add `contention` to the PR
   seed budget; nightly run gets the larger budget. Regression corpus
   (`regression.go`) grows with any seed that ever fails — unchanged discipline.
5. **Watch Meerkat follow-ups** (doc note): Cloudflare promised posts on QuePaxa
   internals, DST methodology, Rust formal verification, bootstrapping/cluster
   management, and replica placement (`blog.cloudflare.com/tag/meerkat/`). The
   bootstrapping and placement posts bear directly on the roster and HRW placement;
   revisit this plan when they land.

**Files.** `testutil/sim/invariants.go`, `gate.go`, `profile.go`,
`faults/protocol.go`, `gate_ci_test.go`, `docs/sim-gate.md` (document S13/L1/profile
and the buggify concurrency rule from W2), `.github/workflows/`, `scripts/`.

**Done when.** S13 catches the un-fixed W0 bug when run against a pre-W0 tree
(validation that the invariant is sharp); `contention` profile green across the PR
budget on main; docs updated.

**Implementation notes (landed 2026-07-15; S13 + FAULT-ASSERT landed early with
W0, where they were needed).** The healed-dwell liveness check shipped as
**L3** (L1/L2 remain the roadmap's reserved names for rmap/lock fairness):
inline in the gate loop rather than as a snapshot predicate, since it needs
the per-round fault schedule. The `contention` profile raises
`acceptor_spurious_preempted` to 0.05, `proposer_drop_vote` to 0.03, and
`owned_proposer_force_full_round` to 0.05 over the duel faults + gray links.
The gate now runs every profile × {mvcc, owned} workload — locally, in
`scripts/sim-gate.sh` (sharded per pair), and in CI (per-pair budget halved
to keep the total scenario count at the old tier). Meerkat follow-up posts
remain a standing watch item (blog.cloudflare.com/tag/meerkat/).

---

## W6 — Paper, blog post, Kubernetes multi-cluster story

Follow-up direction (added 2026-07-14): publish cask's design in the same register as
QuePaxa/Meerkat, and demonstrate a headline application.

**Naming.** The paper/blog needs a distinctive, collision-checked name ("cask"
collides with CaskDB / Bitcask-adjacent projects and is weak for search). Run a naming
pass before the blog draft; repo rename can follow or lag. Candidate criteria:
evokes coordination/fleet, googleable, unclaimed on pkg.go.dev/GitHub/crates.

**Paper (target: arXiv preprint → workshop/industrial track, e.g. NSDI/SOSP
industrial or PaPoC).** Framing: *"Coordination without a log"* — the deliberate
inverse of Meerkat. Meerkat's answer to leader fragility is randomized consensus on a
log; cask's is to delete the log. Claimed contributions, each with an artifact:

1. **Per-key leaderless CAS registers at fleet scale** — no election, no failover
   pause; ownership as a pure optimization (the QuePaxa-leader analogy made precise),
   1-RTT writes (W1), 0-RTT lease-guarded reads (W4).
2. **Epoch-fenced ownership**: fencing tokens, not time, as the primary safety
   mechanism; the W0 ballot-space discipline as the subtle bit worth writing down.
3. **The reflexive roster**: a consensus register that manages its own membership via
   joint-consensus carry-forward — no meta-range, no external coordinator.
4. **Confidence discipline**: TLA+ + seed-deterministic simulation with BUGGIFY +
   invariants-as-code (S1–S13, L1); every bug becomes a fault.

Evaluation plan (feeds directly from W1/W2/W4 benches + W5 gate): latency/throughput
vs etcd on identical hardware (single-key and fleet-sharded), liveness under the
`contention` profile with/without backoff (the QuePaxa comparison, honestly scoped:
we match its *goal* under our workload, not its asynchrony guarantees), WAN profile
with widened backoff/MaxOffset.

**Blog post** (Meerkat-style narrative, publish with/after the preprint): working
title *"The tyranny nobody talks about: the log itself."* Structure mirroring
Cloudflare's post: incident-shaped motivation (leader failover pauses, timeout tuning)
→ "what if no key ever waited for an election" → registers-not-log → ownership =
Meerkat's leader, fencing = its randomness → honest limitations section (no multi-key
transactions — by design; uncertainty window; capacity bounds).

**Kubernetes aggregated API server for multi-cluster CRDs** (the demo application;
extends roadmap §5.5): a Kubernetes **API extension (aggregated) server** — not CRDs
stored in each cluster's etcd — backed by cask, exposing *fleet-scoped* resources
visible and strongly consistent across every member cluster.

- Implement `k8s.io/apiserver`'s `storage.Interface` on cask: objects as mvcc
  registers; `resourceVersion` = per-key Seq (per-key semantics match k8s's
  per-object optimistic concurrency — no global revision needed for single-object
  ops); `Watch` from `mvcc.History`/`KeyWatcher` cursors (Plumtree push, §3.5, when it
  lands); k8s optimistic concurrency maps 1:1 onto `KV.CAS`.
- List/watch-across-prefix uses range-ordered keys (prefix locality is already the
  ranges design, `ranges.go:1-8`); cross-key list consistency is the documented
  design-A uncertainty window — acceptable for k8s semantics (lists are already not
  point-in-time across objects in practice).
- Controllers coordinating across clusters get cask sessions/locks with fencing
  tokens — a materially stronger story than k8s leader-election Leases (which are
  wall-clock leases without fencing).
- Demo: 3 clusters (the existing multi-cloud demo infra), one aggregated APIService,
  a fleet-scoped CRD (e.g. `ClusterClaim`) created in cluster A, read/watched in B/C,
  with a fenced controller surviving a partition + failover without split-brain.
- Deliverables: design doc (`docs/k8s-aggregation.md`), `cmd/cask-apiserver`
  prototype, demo script. Depends on: W1/W4 (perf story), §5.4 wire hardening for
  anything beyond demo.

**Done when.** Naming decided; paper outline + evaluation matrix drafted; blog draft
review-ready; apiserver prototype serves get/list/watch/update for one CRD across 3
demo clusters.

**Status (2026-07-16).** All W6 deliverables landed except the vs-etcd benchmark
harness (paper TBD rows remain). Naming: **cask stays** (decided 2026-07-15). Docs:
`docs/paper/coordination-without-a-log.md`, `docs/blog/the-log-itself.md`,
`docs/k8s-aggregation.md`. Prototype: `cmd/cask-apiserver` — a hand-rolled
aggregated-API-style server (same Go module; `k8s.io/apiserver` dependency judged
too heavy for a prototype, deviating from the plan above) serving
`fleet.cask.dev/v1alpha1` `Device`/`DeviceClaim`; `resourceVersion` = per-key mvcc
Seq, claim binding = fenced lock `Acquire`, watch = poll-diff over an index
register. Demo: `demo/kind/` — **three kind clusters** (user directives: kind, and
cask **embedded** in the extension servers) with NO external cask processes: each
apiserver carries its own acceptor (`--listen-consensus`/`--advertise-consensus`,
Pebble `--data-dir` on a hostPath so a restarted acceptor keeps its promises;
hostNetwork because pod networks aren't routable across kind clusters), and the
three apiservers form the consensus group among themselves. 4-act walkthrough:
cross-cluster visibility, global single lease under a claim race, higher-fence
handover, zombie holder fenced out while the surviving 2/3 keep committing.
Verified in-environment by a 3-process localhost smoke (create via east/read via
north, race → one Bound, write with a node down, durable restart) plus
`cmd/cask-apiserver/server_test.go` under `-race`; the kind flow itself needs
docker (run from the macOS host — this guest has none).
