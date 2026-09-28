# Plan: "cask" — a two-tier, leaderless, MVCC coordination store on CASPaxos

## Context

A distributed key-value store for (1) **leases / locks on global resources** (with fencing)
and (2) **small amounts of metadata**, with a **modern, no-config** UX (auto-discovery, minimal
flags), targeting a fleet of **10,000+ participating nodes**. Consensus substrate is **CASPaxos**
(arXiv:1802.07000): each key is a *mutable register* updated by a Paxos round whose proposer
applies `f(value)->newValue`; **leaderless, log-less, crash-fault-tolerant, multi-writer**.

This plan is the product of a structured design interrogation ("grill-me"). The key realization:
the 10k figure is about **how many participants coordinate**, not data volume (the dataset is
tiny). That splits the system into **two tiers** and removes the need to run consensus across 10k
nodes. Decisions captured below are load-bearing and were each chosen deliberately.

**Why state-replication (CASPaxos), not a log:** a log gives a global order + free Watch/MVCC but
needs compaction and pushes you to a **leader** (a shared array of log slots needs one sequencer to
avoid dueling-proposer livelock and apply-order gaps). Per-key registers have **no global order to
sequence → no leader needed**, which is exactly what lets us scale the participant fleet and get
native multi-writer. The price — cross-key snapshots, Watch, MVCC history — we pay back explicitly
below (HLC, version chains). References consulted: **PaxosStore** (per-object Paxos over a shared
LSM, at WeChat scale), **Partisan** (overlay/transport lessons), **HyParView/Plumtree**, **Rapid**,
**Consul/Serf** (two-tier gossip + sessions).

### Decisions captured (each chosen during the grill)
- **Node roles:** **Agents + smaller storage core.** Up to 10k+ lightweight agents coordinate
  through a much smaller, range-sharded storage tier (tens–hundreds of active replicas).
- **Storage topology:** **Range-sharded** storage tier (ordered keyspace, meta-index, split/merge).
- **Storage membership:** **Fully elastic** — any node may host ranges; placement self-organizes.
- **Churn safety:** **Failure-domain-aware placement + hysteresis + catch-up-before-release**, RF=5–7.
- **Agent↔storage write path:** **Agent forwards to the range owner**, which proposes.
- **Read/Watch path:** **Linearizable via owner + agent-side Watch fan-out**, opt-in stale local reads.
- **Watch:** **Full Watch** (keys + prefixes + historical resume).
- **Watch history:** **Bounded window + `compacted` signal**; GC never reclaims below a live watcher.
- **Lease keepalives:** **Agent-managed sessions, batched renewal** (Consul model).
- **MVCC:** per-key version chains **+ cross-key snapshot isolation via HLC**.
- **Membership plane:** in-house **HyParView + Plumtree** (built right from the start, not memberlist).
- **Placement authority:** **Hybrid** — HRW default seeding + consensus-managed range descriptors.
- **API:** **ConnectRPC** (gRPC + HTTP/JSON). **Language:** **Go**.

---

## Language: Go (recap)

Critical path is network RTT + fsync, equal in both languages; Zig's no-GC/raw-CPU wins optimize a
cost this workload never pays. Go's ecosystem *is* the work (ConnectRPC, pebble, the partial-view
references, porcupine), and `devbox.json` already targets Go. Zig only wins for microsecond-tail /
embedded targets that aren't required.

---

## Architecture — two tiers, one binary

```
cmd/cask                       single binary; role (agent | storage | both) chosen dynamically
internal/caspaxos              PURE consensus core (Clock/Storage/Transport injected)
internal/hlc                   hybrid logical clock
internal/mvcc                  version chains, snapshot resolution, bounded-history GC
internal/store                 storage-tier durable state: pebble (registers + mvcc history + descriptors)
internal/range                 range = ordered key-interval; per-range CASPaxos group + owner
internal/placement             elastic placement driver: HRW seed + descriptors + reconfig + split/merge
internal/membership            PURE HyParView+Plumtree liveness/broadcast (+ static, sim bindings)
internal/roster                consensus-managed cluster roster + meta-range routing index
internal/transport             per-peer multi-channel RPC (consensus | gossip | bulk)
internal/agent                 proposer + router + session holder + watch fan-out + read cache
internal/lease                 sessions, Grant/KeepAlive/Revoke/TTL, reaper, fencing
internal/watch                 change feed, prefix watch, resume/compaction, fan-out
internal/api                   ConnectRPC services (KV/Snapshot/Watch/Lease/Lock/Session/Admin)
internal/cluster               discovery (mDNS + seeds/DNS-SRV), bootstrap, planes wiring
internal/server                composition root + lifecycle
testutil/sim                   deterministic seeded whole-cluster simulation
test/linearizability           porcupine models + recorded histories
quint/                         Quint specs checked with TLC and Apalache (safety-critical protocols)
jepsen/                        Clojure Jepsen test (Elle/Knossos), nemeses incl. churn + clock-skew
```

`internal/caspaxos`, `internal/membership`, and `internal/mvcc` stay **pure/deterministic** (inject
Clock/Transport/Storage; no `time.Now()`/`net`/goroutines) so the whole two-tier cluster runs under
the seeded simulator — the only credible way to validate consensus + a partial-view protocol +
elastic churn under partitions.

### Agent tier (up to 10k+)

- **Roles:** CASPaxos *proposer* + client-facing ConnectRPC endpoint. No durable consensus state.
- **Membership:** full participant in the **HyParView (active ~5 / passive ~30) + Plumtree** plane —
  O(log N) state per node, no O(N) membership table (flat SWIM caps ~5k/pool per Consul; partial
  views are non-optional for 10k).
- **Routing:** look up key → range descriptor (cached, Plumtree-disseminated) → **forward the op to
  the range's owner** on the storage tier. Keeps acceptor fan-in bounded to the storage tier's small
  internal mesh; each agent holds only a few connections to nearby storage nodes.
- **Sessions:** holds a small number of **session-leases** with storage and heartbeats those; client
  locks/leases bind to a session. Storage sees **O(agents)** keepalives, not O(total-leases). Agent
  death expires its session → its locks release (fencing tokens stay monotonic). Blast radius of an
  agent failure = that agent's clients.
- **Watch fan-out:** subscribes **once per range** to the owner and multiplexes events to all local
  client watchers → storage sees O(agents-with-watchers), not O(clients).
- **Reads:** linearizable by default (via owner → quorum). Opt-in **bounded-stale** reads served from
  a local cache kept fresh by the agent's watch subscriptions.

### Storage tier (elastic, tens–hundreds of active replicas)

- **Roles:** CASPaxos *acceptor*; durable range state in pebble with **fsync persist-before-reply**
  (a reply implies durability — non-negotiable for safety, and doubly so under elastic churn).
- **Range-sharded ordered keyspace:** keys partitioned into contiguous **ranges** so a prefix lives
  in one/few adjacent ranges → **prefix Watch/Range are cheap and ordered** (the reason we picked
  range over hash sharding). Each range is its own **CASPaxos group**, **RF=5–7**.
- **Range owner:** per range, a **stable owner** holds a long-lived promise → **1-RTT** proposes, and
  is the **Watch hub** for that range. Owner is implicit (HRW over the range's replicas) and
  **preemptible** — losing it is plain Paxos preemption (safe, instant failover, no election
  protocol). Leaderless for *correctness*; owner is only a latency optimization.
- **Elastic placement (`internal/placement`):** any node may be promoted to host ranges. The driver
  spreads each range's RF replicas across **distinct failure domains/zones**; relocates a replica
  **only on a durable roster change** (the Rapid-stable roster, never gossip flaps) with
  **hysteresis** to avoid thrashing; a new replica must finish **state catch-up and be committed into
  the joint config BEFORE any old replica is released** (no quorum gap). Reconfiguration is safe
  per-range joint consensus (overlapping quorums).
- **MVCC + HLC:** the register holds the latest `{hlc, version, valueRef}`; history rows
  `mvcc|range|key|hlc -> value` are written by each acceptor on Accept (replicated across the quorum).
  Writes are HLC-stamped; `SnapshotRead(keys, T)` returns the consistent cross-key snapshot as of T
  (no global log). **Bounded history**: retain a configurable window; GC reclaims below the watermark
  but **never below the oldest live watcher's cursor**; resume past the window → `compacted` signal.
- **Split/merge:** size/hotness-triggered; rare given small data → **start with static initial ranges**,
  split as an occasional operation. Split/merge are atomic consensus ops that update the meta-index.

### Meta-index / roster (`internal/roster`)

- **Cluster roster** (node IDs + zone/domain, slow-changing) and the **range descriptor table**
  (key-range → replica set + epoch) live in a small **consensus-managed meta-range** (the
  TiKV-PD / CockroachDB meta-range pattern). The dataset is small, so the descriptor table is also
  **disseminated to agents via Plumtree and cached**, with the meta-range as authority on a cache miss.
- **HRW seeds default placement**, the descriptor is the authority — and crucially, **placement and
  per-range quorums depend only on the stable consensus roster**, while gossip liveness only nudges
  *routing preference* (which replica/owner to try first). This is the clean split that prevents a
  gossip false-positive during a partition from destabilizing the consensus tier.
- **Failure detection:** phi-accrual on HyParView active-view edges + **Rapid-style multi-observer
  cut detection** — a node is committed up/down in the roster only when multiple observers agree.

### Transport (`internal/transport`) — Partisan lesson

Per-peer links carry **multiple named channels** by traffic class — `consensus` (prepare/accept,
latency-critical), `gossip` (Plumtree/roster deltas), `bulk` (range catch-up/snapshot transfer) —
so an elastic-rebalance transfer can never head-of-line-block a consensus round. Monotonic channels
drop superseded messages (epoch/roster/HLC deltas). Sparse, on-demand connection pools (no mesh).

### API (`internal/api`) — ConnectRPC (gRPC + HTTP/JSON)

One schema, gRPC and curl/browser-friendly. Verbs: **KV** (Get/Put/Delete/Cas/GetAt), **Snapshot**
(SnapshotRead/BeginSnapshot), **Watch** (key/prefix, resume-from-revision, `compacted` signal),
**Lease/Session** (OpenSession/Heartbeat/Grant/KeepAlive/Revoke/TimeToLive), **Lock** (Acquire/Release,
returns fencing token), **Admin** (roster, ranges, placement, status). `caskctl` CLI over the same API.

---

## Critical files (where correctness lives)
- `internal/caspaxos/{proposer,acceptor}.go` — round state machine + persist-before-reply durability.
- `internal/membership/*` — pure HyParView+Plumtree (validated under simulated partitions).
- `internal/placement/reconfig.go` — failure-domain placement, hysteresis, catch-up-before-release.
- `internal/mvcc/{snapshot,gc}.go` — HLC snapshot resolution + watcher-aware bounded-history GC.
- `internal/lease/session.go` — agent sessions, fencing-token monotonicity across owner/range moves.

---

## Milestones
- **M0 Spec the core:** `quint/caspaxos.qnt` + `quint/lease.qnt` model-checked in TLC (agreement,
  monotonicity, single-holder, fencing) — the safety contract the code must refine.
- **M1 Range core + MVCC:** pure `caspaxos` + pebble; per-key version chains; HLC; Put/Get/Cas/GetAt
  on one range. Property tests (`rapid`) mirroring the M0 invariants. No network.
- **M2 Range group (RF=5):** quorum fan-out, linearizable + snapshot reads; static membership;
  crash/restart durability under the simulator + porcupine. Owner is **routing affinity** (HRW)
  only here — the 1-RTT phase-1 *skip* is moved to **M7**, because a blind accept-only skip is
  unsafe without a lease (it admits a lost update: another proposer can prepare+accept between two
  owner ops and the owner's higher ballot silently overwrites it with no NACK to detect it).
  *Wire transport (ConnectRPC) is deferred until `buf`/protoc codegen is added to devbox; an
  interim HTTP/JSON acceptor transport unblocks multi-process runs in the meantime.*
- **M3 Two tiers + routing:** agent role (proposer/router), forward-to-owner, range descriptors +
  meta-range routing (static ranges), agent connection model. Multi-range snapshot reads.
- **M4 Membership plane:** in-house **HyParView+Plumtree** (pure, simulated partitions/asymmetric/mass
  failure), phi-accrual + multi-observer cut detection, consensus **roster** driving placement; mDNS
  + seeds/DNS-SRV discovery; genesis/bootstrap.
- **M5 Elastic placement (spec-first):** write & model-check `quint/reconfig.qnt` (no-lost-value under
  churn) **before coding**; then the placement driver (failure-domain spread, hysteresis,
  catch-up-before-release), per-range joint-config reconfig, split/merge. Churn + partition tests
  (no lock loss) in the simulator.
- **M6 Watch:** full key/prefix Watch, resume-from-revision, bounded-history retention + `compacted`
  signal + watcher-aware MVCC GC, agent-side fan-out.
- **M7 Leases/locks:** agent-managed sessions, batched keepalive, Grant/KeepAlive/Revoke/TTL, lazy
  expiry + reaper, fencing tokens (monotone across owner/range moves). Clock-skew tests.
- **M8 Scale + Jepsen + hardening / v1:** drive agents to 10k + storage tier to hundreds in
  simulation; zone/WAN-aware views + Vivaldi bias; **Jepsen** lock-fencing + register + set workloads
  under partition/clock-skew/churn nemeses (partition+churn lock-fencing run = release gate); metrics,
  logging, backpressure, docs, single-binary release.

---

## Correctness & verification — four layers

The protocols here (CASPaxos + MVCC, **per-range joint-consensus reconfiguration under elastic
churn**, leases/fencing) are exactly the class where Paxos-family bugs hide. We verify in four
escalating layers, **spec-first for the risky parts** — model-check a protocol before trusting code
built on it.

### Layer 1 — Formal specification: Quint, checked with TLC and Apalache (in `quint/`)
Write machine-checked specs and model-check invariants with **TLC** (and **Apalache**, the symbolic
checker, for larger state spaces) *before* implementing the corresponding code:
- **`CasPaxosMvcc.tla`** — our CASPaxos register variant with version chains + HLC stamping.
  Invariants: **agreement** (no two different values chosen at/above a quorum-chosen ballot),
  per-key version monotonicity, HLC monotonicity.
- **`Reconfig.tla`** (the highest-value spec) — per-range **joint-consensus reconfiguration with
  catch-up-before-release** under nodes joining/leaving. Invariant: **no committed value is ever lost
  across any sequence of reconfigurations** (i.e. elastic churn cannot drop a lock/lease) and quorum
  overlap is preserved at every step. This is where the M5 churn risk is retired on paper.
- **`Lease.tla`** — lease/session expiry + fencing. Invariants: **at most one valid holder**, and
  **fencing tokens strictly monotonic** across owner preemption and range relocation.
- Linearizability is argued via a **refinement mapping** from `CasPaxosMvcc` to a sequential register
  spec. (Gossip/HyParView is probabilistic → validated by simulation in Layer 3, not TLA+, except the
  roster-commit / multi-observer cut-detection consistency, which is specced.)
- These specs are living artifacts: CI runs TLC on bounded models; spec changes gate protocol changes.

### Layer 2 — Property-based testing (Go, `pgregory.net/rapid`)
Translate the TLA+ invariants into executable properties over the real Go code:
- **Invariant properties:** consensus agreement, version-chain + HLC + fencing monotonicity,
  range split/merge keyspace-coverage invariants, serialization round-trips.
- **Stateful / model-based PBT:** generate random op sequences against the implementation and a
  simple sequential reference model; assert observable equivalence (the executable echo of the TLA+
  refinement). Shrinking gives minimal failing histories.

### Layer 3 — Deterministic simulation (`testutil/sim`) + linearizability checking
Whole two-tier cluster in one goroutine, seeded PRNG driving reorder/drop/dup/delay, partitions,
asymmetric links, crash/restart, **per-node clock skew**, and **elastic churn** (storage nodes join/
leave mid-flight); fully replayable from a seed. Recorded client histories are checked with
**`anishathalye/porcupine`**: a KV register model (linearizable ops) and a snapshot model (cross-key
reads-as-of-T). Key asserted property under fault+churn: **no lock/lease lost across reconfiguration**.
Optionally drive the same core via **`jepsen-io/maelstrom`** for a second, independent checker
(knossos/elle) cheaply.

### Layer 4 — Jepsen (real clusters, `jepsen/` Clojure project)
A real Jepsen test driving **running `cask` clusters** via the ConnectRPC API / `caskctl`:
- **Workloads:** `lin-kv` register (Knossos/Elle linearizability), a **lock workload with a fencing
  checker** (assert no two holders + monotonic tokens — the headline use case), and a set/append
  workload (Elle) for snapshot-isolation claims.
- **Nemeses (matched to our actual risks):** network partitions, **clock skew** (stresses HLC +
  lease expiry), process kill/pause, and **membership churn** (add/remove storage nodes mid-test to
  exercise elastic placement + catch-up-before-release).
- Runs in the hardening phase but is wired early; a partition+churn lock-fencing run is the v1 release gate.

**Manual smoke:** `devbox run build`; launch a few storage nodes + several agents on a LAN with **no
flags** (mDNS forms the cluster); `caskctl put/get/watch/lock` + a session keepalive/expiry cycle;
then a larger seeded fleet to watch elastic placement + failure-domain spread + churn recovery.

---

## Key risks & open questions
1. **Elastic storage churn is the sharpest risk** (your choice, eyes open): correlated loss of a
   range's quorum = lost locks. Mitigation is designed in (failure-domain spread, hysteresis,
   catch-up-before-release, RF=5–7) **and retired in two places**: model-checked in `quint/reconfig.qnt`
   (no committed value lost across reconfiguration) before coding, then exercised by simulator churn
   tests and a Jepsen partition+churn lock-fencing run (the release gate). Consider a configurable
   **minimum-stability** gate so brand-new/flaky nodes can't hold a quorum alone.
2. **In-house HyParView+Plumtree** is the biggest *build* risk — partition/asymmetric-link correctness
   is subtle; build pure + test-first (M4), mine `hashicorp/hyparview`/Partisan/papers as references.
3. **HLC uncertainty window** for snapshot reads needs bounded clock skew (NTP); choose read-restart
   vs commit-wait; surface skew metrics. Same assumption etcd/Consul make.
4. **Owner ≈ soft-leader honesty:** funneling a range's writes through its owner narrows the
   leaderless differentiator to *no election protocol + instant preemptive failover + no log* — still
   real, but document it so expectations are right.
5. **Cross-range atomic writes are out of scope for v1** (single-key CAS is atomic; HLC gives
   consistent cross-key *reads*, not multi-key atomic *writes*). Multi-key txns would need 2PC across
   ranges — revisit only if a use case demands it.
6. **Bootstrap chicken-and-egg:** the meta-range/roster must exist before placement can run; genesis
   seeds it (lowest-ID seeder commits the initial roster + one range), single node runs RF=1, joiners
   via reconfig; guard independent geneses from silent merge (opt-in `--bootstrap-expect N`).
7. **Security:** mTLS optional (off for zero-config LAN, on by flag). Auth/ACLs out of scope for v1 —
   call out before any internet-facing use.
