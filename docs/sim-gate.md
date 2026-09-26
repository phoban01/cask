# The simulator release gate

The gate (cask roadmap §6.6 / §6.7) runs the cluster against a **seeded fault
adversary** and checks the §6.5 safety invariants at every quiescent step. Any
violation is a release-blocker; the seed and event trace are kept so the failure
can be replayed and added to the regression set.

```
scripts/sim-gate.sh                 # local quick run (1000 seeds/profile)
SEEDS=10000 scripts/sim-gate.sh     # CI tier
go test ./testutil/sim/             # the gate's own unit + negative-control tests
```

## What "deterministic" means here (read this)

PR #0 builds a **seed-deterministic adversary**, not a single-goroutine
logical-time simulator. One `*rand.Rand`, seeded from the scenario seed, drives
*every* nondeterministic decision: which fault fires, when, against which node;
every `buggify.Maybe` outcome; every clock advance. Time is controlled through
`hlc.PhysicalFunc` (the `SimClock`).

What is **not** controlled: goroutine interleaving. Scenarios run multiple
proposers on the real Go scheduler. So a replayed seed re-runs the same
*adversary*, but not necessarily the same OS-level interleaving. Reproducing a
specific failure is therefore best-effort — we persist the seed **and** the full
event trace (`Violation.Trace`) so a failure can be understood even if its exact
interleaving differs run-to-run.

Full single-goroutine determinism is deferred. It is a multi-week rewrite
(continuation-based transport, a cooperative scheduler) and should land only if
a real bug proves the concurrent gate can't reproduce it. The RNG seam is built
so that scheduler can slot in behind it later without touching call sites.

## Current coverage (honest accounting)

Invariants (`testutil/sim/invariants.go`, `sim.All()`):

| Live now | Deferred (registered, `Implemented:false`) |
|---|---|
| S1 per-register (per-ballot) agreement | S4/S5 reconfig safety — needs roster-observation plumbing |
| S2 MVCC Seq monotonic + exactly-once op | S6/S7 lease single-holder / fence monotone — needs lease observation |
| S3 per-key HLC monotonic | S8/S9 split safety / descriptor epoch — needs §4.3 ranges |
| S11 owner-epoch dominance (chosen ballot ≥) | S10 cross-range skew — needs `hlc.MaxOffset` (§4.4) |
| S13 committed MVCC history append-only (no lost update) | S12 watcher non-starvation — needs watch-cursor observation |
| | L1/L2 liveness — needs a fairness model |

The live five are checkable from durable acceptor register state alone — exactly
what survives a crash. The deferred ones are enumerated (not omitted) so the
table is complete and the gate is honest about what it does and does not yet
prove.

S13 compares only **quorum-committed** chains (a majority holding the same
accepted ballot) across snapshot pairs — partial accepts from abandoned rounds
are legitimately droppable and comparing them would false-positive. Its blind
spot is a lost update provoked *and* healed within a single round (snapshots
are quiescent endpoints); that class is covered by **fault assertions**: a
fault that observes its own expectation broken records a `WARNING` trace
event, and the gate turns any new WARNING into a `FAULT-ASSERT` violation at
the end of the round (`gate.go`). WARNINGs are therefore gate-failing, not
log noise — a fault must only WARN on a genuine safety break.

Faults (`testutil/sim/faults/`, profiles in `testutil/sim/profile.go`):

| Live now | Deferred |
|---|---|
| `partition`, `crash_restart`, `mass_failure` | `mid_cutover_partition`, `concurrent_split_same_range` (§4.3) |
| `asymmetric_reachability` (partial-reachability) | `clock_skew_within/exceeds_bound` (§4.4) |
| `slow_fsync` (SlowStore decorator) | `mid_grv_partition` (§4.6) |
| `epoch_old_owner_write` | `lighthouse_loss`, `core_stale_rejoin` |
| `owner_vs_full_proposer` (W0 ballot-space duel) | |
| `dueling_proposers` (W3 liveness: K symmetric writers, no driver convention) | |
| `keepalive_blackhole` (buggify-driven) | `message_reorder` (needs logical-time scheduler) |
| `duplicate_delivery` (at-least-once link) | membership-layer dup/reorder (HyParView/Plumtree not gate-driven) |
| `slow_link` (link-latency injection) | |

Profiles: `smoke`, `consensus`, `lease`, `cluster`, and `contention` — the
QuePaxa regime as a standing gate (dueling proposers, owner/full duels, gray
links, raised buggify cruelty), paired with the **L3 healed-dwell liveness
check**: with no fault active in a round, exhausting a retry budget
(`ErrPreempted`/`ErrContended`) is a violation — contention must cost latency,
never liveness. The `ranges` and `snapshot` profiles land with their features
(§4.3 / §4.6).

Every profile runs under two workloads: `mvcc` (full two-phase rounds) and
`owned` (the W1 ownership fast-path topology, including lease-guarded local
reads and the fast/slow interleave the `owned_proposer_force_full_round` site
forces). `scripts/sim-gate.sh` shards profile × workload × seeds across
processes.

### Gray-failure link faults and what's still not modelled

The gate's network is a **consensus-level** link (Prepare/Accept), not a raw
packet bus. Two transport gray-failure modes a clean up/down link omits are now
modelled at that level:

- **`duplicate_delivery`** — at-least-once delivery: the acceptor processes a
  Prepare/Accept twice (a TCP retransmit or proposer retry, neither of which the
  real wire dedups), while the caller correlates to the first reply. This
  stresses consensus idempotency: a re-delivered same-ballot accept must be a
  no-op, not a second commit (checked by S1/S2). Note the asymmetry the fault
  surfaces — re-delivering a *Prepare* at the same ballot is rejected the second
  time by the strict-greater promise check (`acceptor.go`), but leaves register
  state unchanged, so safety holds.
- **`slow_link`** — per-acceptor link latency, distinct from `slow_fsync`'s slow
  disk. Widens the in-flight window and stresses proposer timeout/backpressure.

Three failure classes remain **out of model**, and the gate cannot catch bugs in
them — stated here so "the simulator passed" is not over-read:

1. **Message reordering.** The proposer drives acceptors synchronously and
   serially, so there is no in-flight queue to reorder. Real reordering needs the
   deferred single-goroutine logical-time scheduler (see "What 'deterministic'
   means" above). Tracked as `message_reorder`.
2. **Membership-layer duplication/reordering.** HyParView and Plumtree are
   exercised by their own package simulators, not driven through this gate, so
   the gate says nothing about duplicate/reordered Shuffle/Graft/IHave messages.
   These protocols assume exactly-once in-order delivery and have no dedup — a
   real gap (the transport gives that only per-connection).
3. **Sub-message faults** (partial delivery, byte corruption, fragmentation).
   By design; would need a byte-level fake transport. Jepsen is the layer that
   catches these (`docs/confidence.md`).

## Adding a fault

1. Create `testutil/sim/faults/<name>.go` with a value implementing
   `sim.Fault` (`Name() string`, `Inject(*sim.Sim)`).
2. Add the name constant to `testutil/sim/profile.go` and list it in the
   relevant profile(s).
3. Add it to the catalog in `faults.All()`.

That's it — one file plus one profile line. `Inject` mutates the network, the
stores, or the clock; the gate heals between dwells.

## Adding an invariant

1. Add an `Invariant{ID, Desc, Implemented:true, Check}` entry to `sim.All()`.
2. `Check(prev, cur Snapshot) error` returns non-nil on violation. The snapshot
   gives per-acceptor `caspaxos.Register` state for every observed key; decode
   the value if you need MVCC/lease structure.
3. Add a negative-control test in `gate_test.go` that hand-crafts a violating
   snapshot and asserts the check flags it — otherwise a vacuous predicate
   passes silently.

## Adding a buggify site

In protocol code, at a spot you think "this should be fine":

```go
if buggify.Maybe("my_site_name", 0.02) {
    // the cruel path
}
```

Register it in that package's `init()` (`buggify.Register(name, desc, prob)`) so
it shows in the catalog. **Site names are public API** — the regression set
references them by string; renaming one breaks regressions. In production
`buggify.Maybe` is a single nil-check and the cruel path is never taken.

Placed sites today: `owned_proposer_force_full_round`, `proposer_drop_vote`,
`acceptor_spurious_preempted`, `roster_abort_joint`, `session_drop_keepalive`,
`hlc_skew_forward`, `watch_spurious_compacted`. Declared-but-deferred:
`mvcc_skip_owner_cache` (§3.1), `store_slow_fsync` (§3.0),
`router_force_refetch` (§4.1/§4.3), `split_pause_pre_cutover` (§4.3).

## Reading a failure

A violation prints (and, with `SIM_RECORD_DIR`/`RECORD_DIR`, writes a JSON
record):

```
seed=42 profile=consensus round=17 invariant=S2: key "k1" op {Node:1 Seq:9} appears twice
```

Re-run that single seed to inspect it:

```
SIM_PROFILE=consensus SIM_SEED_START=42 SIM_SEEDS=1 go test -run TestGate -v ./testutil/sim/
```

The JSON record carries the full event trace (faults injected, buggify sites
fired) for the scenario. When you fix the bug, add the seed+profile as a
`RegressionScenario` in `testutil/sim/regression.go` so it can never silently
regress — that discipline is what compounds the gate's strength over time
(`docs/confidence.md`).
