# Coordination Without a Log

*Working draft / outline — target: arXiv preprint, then an industrial-track or
workshop venue (PaPoC, HotOS, or NSDI/SOSP industrial). Name decided
2026-07-15: the project stays **cask** — the searchability cost of the
CaskDB/Bitcask/Homebrew collisions was judged worth avoiding a rename; the
paper title carries the differentiation instead.*

## Abstract (draft)

Coordination services — locks, leases, sessions, placement — are the
control-plane backbone of modern infrastructure, and nearly all of them are
built the same way: a totally-ordered replicated log under a elected leader
(etcd/Raft, ZooKeeper/ZAB, Chubby/Multi-Paxos). The log buys generality that
coordination workloads rarely use, and the leader sells it back as fragility:
election pauses, timeout tuning, and a single node through which every write
must flow. Recent work attacks the leader (EPaxos, QuePaxa/Meerkat's
randomized consensus) while keeping the log. We take the other branch: delete
the log. Cask is a coordination store built from independent per-key CASPaxos
registers — leaderless by construction, with no election and no failover
pause — extended with an epoch-fenced ownership fast path (1-RTT writes,
0-RTT lease-guarded reads that match a log-based leader's best case), a
membership register that reconfigures itself, and consensus-managed range
descriptors that make data migration and range splitting safe without any
global ordering. We show that the subtle costs of composing a skip-phase-1
fast path with plain Paxos are real (a ballot-space collision our own model
checker caught as a silent lost update) and give the discipline that makes
the composition sound. The system is validated by machine-checked TLA+
specifications with negative controls, a deterministic fault-injecting
simulation gate run on every change, and wire-level tests of every latency
claim.

## 1. Introduction

- The framing: **the log is a tax coordination workloads pay for generality
  they don't use.** Locks, leases, sessions, and placement records are
  independent registers with per-key linearizability requirements; nothing in
  the workload demands a total order across keys.
- The industry's response to leader fragility keeps the log and fixes the
  leader: Meerkat (Cloudflare, 2026) adopts QuePaxa's randomized consensus to
  escape election timeouts, EPaxos exploits commutativity. Both still
  replicate a log.
- Our position: for the coordination workload specifically, deleting the log
  is simpler AND faster. Contributions:
  1. **A leaderless coordination store from per-key CAS registers** with an
     MVCC version chain *inside* each register value (exactly-once mutation
     via operation ids; per-key HLC ordering).
  2. **Epoch-fenced ownership**: the fast path (1-RTT writes, 0-RTT reads)
     is an *optimization exactly the way QuePaxa's leader is* — never
     required for liveness; a dead or partitioned owner is fenced by the
     next fencing token, not waited out by an election. Fencing tokens, not
     wall-clock leadership, are the primary safety mechanism; clocks appear
     only in the read lease, behind a documented MaxOffset contract.
  3. **The ballot-space discipline** (§4): composing skip-phase-1 owned
     writes with plain CASPaxos proposers on one register is unsound as
     naively done — a NodeID tiebreak can hand a stale owner's write a
     higher ballot than a committed value (silent lost update). The fix
     (epoch-boundary jumps + lineage-monotonic epochs across range splits)
     is small, and its necessity was demonstrated by TLC on a model our
     abstract agreement spec structurally could not check.
  4. **A reflexive control plane**: the membership register stores its own
     acceptor set and reconfigures itself via joint consensus; range
     descriptors ride the same core and drive lossless data migration
     (publish-joint → routing-lease settle → majority-union carry-forward →
     release) and four-commit range splits with no data movement.
  5. **A correctness methodology for a system with no log to replay**:
     TLA+ specs with *negative controls* (configs that must fail — a spec
     that cannot find the seeded bug has lost its teeth), a
     seed-deterministic fault-injecting simulation gate with
     invariants-as-code, and the FDB-style rule that every bug becomes a
     permanent fault.

## 2. Background and related work

- CASPaxos (Rystsov): per-register Paxos with a change function; our base.
- QuePaxa (SOSP'23) and Meerkat (Cloudflare '26): randomized, hedged
  consensus on a log; the "escape the leader" branch. Key contrast table:
  their leader == our owner (both optimizations); their randomness for
  liveness == our randomized backoff (defense-in-depth; per-key contention
  is rare by construction); their log == deleted.
- EPaxos, Raft leases/read-index, Chubby, ZooKeeper.
- FoundationDB's simulation discipline; Jepsen.
- Physalia (per-cell consensus for EBS), Delos: control-plane stores with
  non-monolithic designs.

## 3. System design

### 3.1 Registers, not a log
Per-key CASPaxos with an MVCC chain inside the value. Exactly-once mutation
via OpIDs (retries and carry-forwards are idempotent). HLC per key. Explicit
non-goals: multi-key transactions, global event ordering, large values.
Cross-key coordination is delivered by fencing tokens at write time.

### 3.2 The ownership fast path
Per-range grants; fence token = ballot epoch (high bits). TakeOwnership =
one phase-1 round; steady-state write = one accept round (measured: exactly
3 accepts, 0 prepares on a 3-replica group). Reads served from the owner's
cache under a double-sided MaxOffset lease (owner stops MaxOffset early;
takeover waits MaxOffset past expiry) — 0 network rounds, degraded mode is
the owner's 1-RTT identity round, never slower than the 2-RTT baseline.
Writes-via-owner discipline: request forwarding (one hop, loop-guarded) +
"you cannot commit around a live read lease" (a full-path write is refused,
retryably, while another node's ownership session covers reads).

### 3.3 The ballot-space discipline (the subtle part — full section)
The lost-update construction; why the abstract voting spec (SafeAt-by-
construction) cannot see it; the epoch-jump rule; idempotent takes;
retry-after-bump (the self-collision livelock of a fallback that epoch-jumps
into the fence you just raised); lineage-monotonic epochs across splits.
TLC evidence: the negative-control model finds the exact 5-step trace; the
guarded model passes (~1M states).

### 3.4 The reflexive control plane
Roster value carries its own Core; joint-consensus self-reconfiguration
(proved in RosterReconfig.tla). Range descriptors as registers on the Core;
the roster as index (RangeIDs). Replica migration: publish-joint (routers
see Joint and propose through both quorums) → settle the routing lease
(bounded by the snapshot-poll interval) → carry forward every key
(enumerated as the union over any MAJORITY of old replicas — every
quorum-committed key intersects any majority) → release. Splits/merges as
four register commits with recorded intents (resumable by any successor
driver); data moves nowhere at split time.

### 3.5 Liveness without a leader
Ballot bumping + full-jitter randomized backoff (invoked only between
preempted rounds); concurrent fan-out with early-quorum return and
straggler cancellation (phase latency = max over fastest quorum, not sum of
replica RTTs; a dead peer behind an un-timed-out transport costs nothing);
the healed-dwell liveness rule enforced by the gate (retry-budget exhaustion
with no active fault is a release-blocking violation).

## 4. Correctness methodology

- Eight TLA+ specs, all TLC-verified, two with negative controls
  (OwnedRegisterBug: the pre-fix ballot rule must violate NoLostUpdate;
  OwnerReadsBug: naive lease checks must violate NoStaleRead under
  adversarial per-client skew). The first-ever full-suite run found four
  latent spec defects and one genuine design rule (snapshot reads require
  t ≤ the range's applied HLC — the GetReadVersion rule).
- The simulation gate: seed-deterministic adversary; 11 faults; snapshot
  invariants incl. S13 (committed MVCC history is append-only — the
  structural detector for the lost-update class) and FAULT-ASSERT (a
  fault's own violated expectation fails the gate); 5 profiles × 2
  topologies (full-round and ownership fast-path) on every PR.
- Wire-shape tests: every latency claim in §3 is asserted by counting
  prepare/accept RPCs on the wire, made straggler-immune against the
  fan-out's early-quorum returns.

## 5. Evaluation (matrix — measured vs. planned)

| Claim | Status | Evidence |
|---|---|---|
| Owned write = 1 accept round (3 accepts, 0 prepares) | **Measured** | owner wire-shape tests |
| Owned read = 0 rounds; degraded = 1 round | **Measured** | owner read tests |
| Slow/dead replica off the critical path | **Measured** | 300ms-replica and blocked-peer tests |
| Lost-update discipline necessary & sufficient | **Machine-checked** | OwnedRegister.tla + negative control; unit + fault + gate reproduce pre-fix |
| Read lease sound under skew ≤ MaxOffset | **Machine-checked** | OwnerReads.tla + negative control |
| Migration loses nothing (disjoint sets, mid-migration writes) | **Tested** | orchestrator + cmd driver tests |
| Group-commit durability | **Measured** (with honest fsync-cost caveat on dev VM) | Pebble bench |
| Throughput/latency vs etcd, same hardware | **Measured** | `bench/` harness — see §5.1 |
| Cold start (100 nodes, 50 ranges) < 1s | **Measured** | `bench/` cold-start — see §5.2 |
| WAN profile | **TBD** | widened backoff/MaxOffset |

### 5.1 Throughput/latency vs etcd

`bench/run.sh` stands up a real 3-node cask cluster and a real 3-node etcd
cluster on the same host and drives both through one load generator with
identical workloads, key/value shapes, and percentile code (`cmd/cask-bench`).
Both run as clustered processes, so both pay real client RPC, inter-node
consensus RPC, and fsync. Writes use a fresh key per op (cask's MVCC register
appends an unbounded per-key version chain, so a hot keyspace would measure
history growth, not the commit path); reads are linearizable on both sides
(etcd default `Get`, not `WithSerializable`). Representative run — same-host
aarch64 VM, 10 vCPU, 64 clients, 10s, 256-byte values:

| workload | cask ops/s | etcd ops/s | cask p50/p99 | etcd p50/p99 |
|---|---:|---:|---|---|
| put  | 10,626 | 13,168 | 5.3ms/19ms | 4.9ms/10ms |
| get  |  9,586 | 65,282 | 5.8ms/22ms | 0.9ms/3.4ms |
| cas (create) | 10,582 | 12,173 | 5.3ms/19ms | 5.2ms/13ms |
| lock | 3,850 | 6,271 | 15.5ms/38ms | 9.0ms/39ms |

The reading is honest in both directions. On **writes and CAS** cask is within
the same order of magnitude as etcd (heavier p99 tail from full-jitter backoff
between the three leaderless proposers) — dropping the log does not cost the
write path. On **reads** etcd wins decisively, and this is cask's *worst case by
construction*: the flat `--peers` topology establishes no range ownership, so a
linearizable `Get` runs a full CASPaxos round while etcd's read-index is
near-leader-local. cask's architectural answer — the owned read served in **zero
rounds** — is the separately-measured row above (owner read tests); this harness
does not set up the ownership topology, so the `get` row should be read as "an
un-owned cask read is a full consensus round," not as cask's steady-state read
latency. Absolute numbers are disk/VM-bound (fsync dominates); the relative
comparison is the artifact. Full rationale and caveats: `bench/README.md`.

### 5.2 Cold start

A new node joining a formed 100-node/50-range cluster reads the roster register
from the Core, reads all 50 range descriptors, builds its range map, and serves
its first read (`cmd/cask-bench coldstart`, in-process over `testutil/sim`). We
form the cluster once and time only the join, sweeping an injected per-hop
latency because the cost is the `1 + N` register reads to the Core, not CPU. At
the CPU floor the whole join is sub-millisecond. With the N independent
descriptor reads **fanned out** — the natural implementation — the join is ~2
Core round-trips regardless of range count: p99 22ms at 1ms/hop, 61ms at
5ms/hop, ~234ms even on a 20ms/hop WAN, all well under the 1s bar. Fetching the
descriptors *serially* is `1 + N` round-trips and crosses 1s around ~8ms/hop, so
fan-out — not raw consensus speed — is what keeps a 50-range cold start
sub-second on a WAN. (The register reads themselves ride the same leaderless
CASPaxos path §3 measures; the reflexive control plane §3.4 is what makes "read
the roster, read the descriptors" the entire join protocol — no meta-range, no
gossip-wait.)

## 6. Limitations (honest)

No multi-key transactions or global order (by design; fencing is the
cross-key story). The read lease inherits a clock-skew contract (violating
MaxOffset can yield stale reads, never lost writes). Non-owner writes pay
forwarding or the lease-wait (as Raft leases do). Epoch space is finite
(2^24 handoffs/key; contention burns epochs — documented budgets). Bare
in-process proposers can bypass the API-layer discipline (out of contract,
like writing to etcd's bbolt behind its back). No auth layer yet.

## 7. Naming note

Decided 2026-07-15: **cask** stays. The collision landscape (CaskDB
tutorials, Bitcask associations, Homebrew casks) was weighed against rename
churn (repo, module path, binary, docs) and the rename lost. Mitigation:
public materials always lead with the qualified form — "cask, a
coordination store without a log" — so search anchors on the phrase, not
the word.
