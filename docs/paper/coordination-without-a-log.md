# Coordination Without a Log

*Working draft / outline — target: arXiv preprint, then an industrial-track or
workshop venue (PaPoC, HotOS, or NSDI/SOSP industrial). Project name pending
(see naming note at the end); "cask" is the working name throughout.*

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
| Throughput/latency vs etcd, same hardware | **TBD** | needs the benchmark harness (§6 plan) |
| Cold start (100 nodes, 50 ranges) < 1s | **TBD** | §4.3 bench |
| WAN profile | **TBD** | widened backoff/MaxOffset |

## 6. Limitations (honest)

No multi-key transactions or global order (by design; fencing is the
cross-key story). The read lease inherits a clock-skew contract (violating
MaxOffset can yield stale reads, never lost writes). Non-owner writes pay
forwarding or the lease-wait (as Raft leases do). Epoch space is finite
(2^24 handoffs/key; contention burns epochs — documented budgets). Bare
in-process proposers can bypass the API-layer discipline (out of contract,
like writing to etcd's bbolt behind its back). No auth layer yet.

## 7. Naming note

"cask" collides badly (CaskDB tutorials, Bitcask, Homebrew casks).
Shortlist with collision status: **capstan** (nautical winch a small crew
uses to move heavy loads — mechanism-not-vessel; only a dormant OSv
packaging tool collides), **coxswain** (steers without rowing; a rowing
Android app exists; awkward to type), keep **cask** (searchability cost).
Decision pending.
