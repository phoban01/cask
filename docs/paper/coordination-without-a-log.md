# Coordination Without a Log

*Working draft — target: arXiv preprint, then an industrial-track or
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
the composition sound. The system is validated by machine-checked Quint
specifications, checked with TLC and Apalache, with negative controls, a
deterministic fault-injecting simulation gate run on every change, and
wire-level tests of every latency claim.

## 1. Introduction

Coordination services sit underneath nearly everything: Kubernetes stores its
entire cluster state in etcd, service meshes elect leaders through it, and
databases lease their shards from ZooKeeper or Chubby. These systems are the
part of the stack that must never be wrong, and they are almost all built the
same way — a totally-ordered, replicated log maintained by an elected leader
(etcd over Raft, ZooKeeper over ZAB, Chubby over Multi-Paxos). The log is the
source of the guarantee: every replica applies the same operations in the same
order, so every replica agrees.

Our starting observation is that the coordination workload never asks for that
guarantee. A lock, a lease, a session, a placement record — each is an
independent key with a per-key linearizability requirement. Acquiring lock *A*
has no ordering relationship to acquiring lock *B*; a client that reads the
lease on shard 7 does not care what order shard 12's writes committed in.
The total order across keys that the log works so hard to provide is generality
the workload does not consume. **It is a tax paid in the coin of the leader**:
the single node every write must pass through, whose failure stalls the whole
service for an election timeout, whose throughput ceilings the cluster, and
whose liveness demands the careful timeout tuning every operator of these
systems knows.

A vigorous line of recent work attacks the leader while keeping the log.
EPaxos exploits command commutativity to commit non-conflicting operations
without a stable leader; QuePaxa (SOSP'23) and Cloudflare's Meerkat replace the
election with randomized, hedged consensus so there is no timeout to wait out.
These are real improvements, and they are the right move *if you need the log*.
We take the other branch. For the coordination workload specifically, we argue
the log itself is the thing to remove — and that removing it is not only
simpler but, on the write path, faster.

Cask is a coordination store with no log. Its state is a collection of
independent per-key CASPaxos registers: leaderless by construction, with no
election, no failover pause, and no single write path. On top of this base it
layers the machinery a real coordination service needs — an ownership fast path
that recovers a log-leader's best-case latency without a log-leader's failure
mode, a membership register that reconfigures itself, and consensus-managed
range descriptors that make sharding and migration safe without any cross-key
order. The central technical risk in this design is that these layers interact:
a skip-phase-1 fast path composed with plain Paxos on the same register is
*unsound* as naively built, and the way it fails — a silently lost update — is
invisible to the abstract agreement argument. Much of this paper is about that
interaction and the small discipline that makes it sound.

We make five contributions:

1. **A leaderless coordination store built from per-key CAS registers**, each
   carrying an MVCC version chain *inside* its value. Mutations are
   exactly-once under retries and carry-forwards (operation ids), and each key
   carries its own hybrid-logical clock. There is no shared log and no global
   sequence number.

2. **Epoch-fenced ownership as a pure optimization.** An owner serves 1-RTT
   writes and 0-RTT lease-guarded reads — matching a log leader's best case —
   but ownership is *never required for liveness*, exactly as QuePaxa's leader
   is never required for liveness. A dead or partitioned owner is not waited
   out by an election; it is fenced by the next fencing token. Fencing tokens,
   not wall-clock leadership, are the primary safety mechanism; clocks enter
   only through the read lease, behind an explicit MaxOffset contract.

3. **The ballot-space discipline (§3.3).** Composing skip-phase-1 owned writes
   with ordinary CASPaxos proposers on one register is unsound if done the
   obvious way: a proposer's NodeID tiebreak can lift a stale owner's write to
   a higher ballot than an already-committed value, silently losing it. The fix
   — epoch-boundary ballot jumps with lineage-monotonic epochs across range
   splits — is small, but its *necessity* is the interesting part: our abstract
   agreement spec is safe-by-construction and structurally cannot see the bug,
   while a model that represents the ballot encoding finds it as a short
   concrete trace.

4. **A reflexive control plane.** The membership register stores its own
   acceptor set and reconfigures itself through joint consensus; range
   descriptors are ordinary registers hosted on that same core, indexed by the
   roster. Data migration is lossless by protocol construction (publish-joint →
   routing-lease settle → majority-union carry-forward → release), and range
   splits are four register commits that move no data.

5. **A correctness methodology for a system with no log to replay.** Quint
   specifications checked with TLC and Apalache, with *negative controls* —
   steps that must fail, so that a spec which can no longer find its seeded
   bug is known to have lost its teeth — a seed-deterministic, fault-injecting simulation gate with
   invariants checked as code, and the FoundationDB-style rule that every bug
   found becomes a permanent fault in the catalog.

## 2. Background and related work

**CASPaxos.** Cask's base primitive is CASPaxos (Rystsov): single-decree Paxos
generalized so that a proposer submits a *change function* rather than a fixed
value, and the register's committed value evolves by applying that function to
the previously chosen value. Each key is its own CASPaxos register with its own
ballot space. Cask keeps an MVCC version chain and a hybrid-logical clock
*inside* the agreed value, so a single register commit both advances the value
and stamps its order; agreement over that value is what `caspaxos.qnt`
establishes (invariant `Consistency`: once a value is chosen, no different value
is ever chosen).

**Escaping the leader while keeping the log.** The most direct related work is
the recent effort to remove leader *fragility* without removing the log.
QuePaxa (SOSP 2023) replaces leader election and view change with randomized,
hedged consensus: each replica runs a proposer and a passive recorder, random
per-round priorities give termination probability ≥ ½ per round under full
asynchrony, and a designated leader may still take a 1-RTT fast path while a
non-leader drive costs three RTTs. Cloudflare's Meerkat (2026) is QuePaxa's
first industrial deployment attempt. Both still replicate a totally-ordered
log. The contrast with cask is clean and, we think, clarifying:

| QuePaxa / Meerkat | cask |
|---|---|
| A global SMR **log** of slots | Independent per-key **registers**, no log |
| A **leader** takes the 1-RTT fast path (never required for safety) | An **owner** takes the 1-RTT fast path (never required for safety) |
| **Randomized priorities** guarantee termination under contention | **Randomized backoff** + ballot bumping give termination under contention |
| Natural multi-key **transactions** from the shared log | No cross-key order; **fencing tokens** are the cross-key story |
| Scales by pushing one log harder | Scales by **sharding keys across ranges** |

The rows on the left and right of the fast-path line are the same idea: an
optimization whose loss costs latency, never liveness. The difference is that
QuePaxa needs the machinery because it maintains a log; cask, having no log,
never contracted the leader-election disease QuePaxa cures. We evaluated
migrating cask's consensus to QuePaxa and declined: QuePaxa's 1-RTT fast path is
already matched by cask's owned path, its non-leader 3-RTT path is *worse* than
cask's 2-RTT slow path, and its decisive wins appear under adversarial WAN
asymmetry at hundreds-of-datacenter scale — not cask's design center of small
values and RF-3 ranges at fleet/regional latencies. We instead adopted four of
its engineering ideas (parallel fan-out with early-quorum return, randomized
backoff, owner-cached reads, and wiring the owned fast path onto the production
path), and would revisit the protocol only if cask's scope grew to demand a
totally-ordered log — at which point QuePaxa should be compared against Raft,
not against CASPaxos.

**Other consensus and coordination systems.** EPaxos removes the stable leader
by exploiting command commutativity, but still orders a log of interfering
commands. Raft-based stores recover leader-local read latency with leader leases
and read-index; cask's ownership lease is the analogous mechanism at per-range
granularity, differing in that a stale owner is fenced by an epoch rather than
protected by a lease it might wrongly believe it still holds. Chubby and
ZooKeeper are the canonical log-based coordination services whose interface —
locks, sessions, ephemeral state — cask targets while discarding the log
underneath. Cask deliberately occupies a narrower niche than any of these: it
provides no total order and no multi-key transactions, and leans on fencing
tokens for every cross-key interaction.

**Non-monolithic control planes.** Cask's shape — many small consensus groups
rather than one large one — follows Physalia (per-cell Paxos for EBS lease
management) and Delos (a control plane over a virtualized, swappable log).
Physalia in particular shares the thesis that a coordination store should be
decomposed into blast-radius-limited units; cask decomposes to the individual
key, and treats the membership and placement of those units as more registers
of the same kind.

**Correctness methodology.** Our validation approach is modeled on
FoundationDB's deterministic simulation discipline — running the real code under
a seeded adversary with virtualized I/O and fault injection — and on Jepsen's
black-box history checking. §4 describes how we adapt these to a system with no
log to replay.

## 3. System design

### 3.1 Registers, not a log

Every piece of cask state — a lock, a lease, a session, a range descriptor, the
membership roster itself — is a single CASPaxos register keyed independently.
There is no shared log, no global sequence number, and no ordering relationship
between distinct keys. What a log-based store gets from replaying an ordered
history, cask gets from the register value itself: the committed value of a key
is an **MVCC version chain**, and each mutation appends one version through the
register's change function. Because the change is a function of the prior value,
a write and a compare-and-set are the same operation with a different function,
and a linearizable read is the identity function run to a quorum.

Two properties make this safe under the retries and hand-offs a real system
performs. First, every mutation carries an **operation id**, and the change
function is a no-op if that id already appears in the chain; this makes writes
exactly-once even when a proposal is retried by another proposer or carried
forward during reconfiguration (`caspaxos.qnt` proves per-register
agreement; the simulation gate's invariants **S2** and **S3** check that the MVCC
sequence is monotone with exactly-once operations and that each key's HLC is
monotone). Second, each key carries its own hybrid-logical clock, stamped inside
the version, so per-key order is well-defined without any cross-key coordinate.

The non-goals are deliberate and load-bearing for the rest of the design:
**no multi-key transactions, no global event order, no large values.** A store
with no log has nothing to anchor a multi-key commit to, and we do not pretend
otherwise. Cross-key coordination is instead delivered by **fencing tokens**
handed out at write time (§3.2): a client that must act on two keys carries a
token from the first into its interaction with downstream systems, so a stale
actor's late write is rejectable rather than ordered-around.

### 3.2 The ownership fast path

A register reached through plain CASPaxos costs two round-trips: a prepare phase
to establish a ballot and an accept phase to commit. Cask recovers a log
leader's best-case latency for hot keys with a per-range **ownership grant**
that is a pure optimization. Taking ownership of a range is a single phase-1
round at ballot `(e, 0, owner)` for a fresh epoch `e`, carrying the highest
accepted value into a local cache. Thereafter the owner writes with **no prepare
phase**: it derives the next value from its cache, bumps the ballot's sequence
within epoch `e`, and issues a single accept round — the steady-state write is
one Accept RPC per key, which on a three-replica group is exactly three accepts
and zero prepares on the wire (asserted by a counting decorator in the wire-shape
tests, §4).

The fence token *is* the ballot epoch, encoded in the ballot's high bits, so an
owner at epoch `e` dominates every ballot below `(e+1, 0, ·)` and no proposer at
a lower epoch can write behind it (the simulation gate checks this as invariant
**S11**, owner-epoch dominance). Losing an owner therefore blocks nothing: the
next writer takes the range at a strictly higher epoch and fences the old owner,
with no election and no wait.

Reads are the subtler half. An owner serves reads from its cache in **zero
network rounds**, but only under a *double-sided* MaxOffset lease (default
MaxOffset 500 ms). The owner stops serving local reads once `now + MaxOffset ≥
Expiry` — it gives up the lease early by a full clock-uncertainty margin — and a
would-be successor treats a lapsed owner as takeable only once `now ≥ Expiry +
MaxOffset`, waiting a full margin past expiry. Under any real clock skew bounded
by MaxOffset, the old owner has provably stopped serving before the new owner
can start, so no stale value is ever served; `owner_reads.qnt` proves this
(`NoStaleRead`), and — crucially — its negative-control configuration, which
uses the naive single-sided lapsed check, is *required to fail*, exhibiting the
fast-clocked-successor / slow-clocked-incumbent stale read. Beyond the MaxOffset
bound the contract degrades to a documented uncertainty window: stale reads
become possible, but writes are never lost, because writes never depend on the
clock. Writes obey a matching discipline: they go only through the current
owner (one loop-guarded forwarding hop for a misrouted request), and a node
cannot commit a full-path write *around* a live read lease — it must first wait
past the margin and fence the lease's epoch, or the successor's reads could
observe an order the incumbent never saw.

### 3.3 The ballot-space discipline

The fast path and the plain path share one register, and composing them is
where the design is genuinely subtle — subtle enough that the obvious
composition is wrong in a way the standard Paxos safety argument cannot see.

Ballots in cask are triples `(epoch, seq, node)`, packed as a single counter
`epoch << 40 | seq` with the node id as tiebreak and compared lexicographically.
An owner prepares once at `(e, 0, o)` and then mints `(e, s, o)` for increasing
`s` without re-preparing — which is safe *only if nothing else mints a ballot
inside epoch `e`*. Now suppose a full proposer runs against the same register
while an owner holds it. The construction is five steps:

1. The owner has written up to sequence `s`; acceptor promises stand at
   `e << 40 | s`.
2. The full proposer's prepare is rejected, and the conflict it learns is the
   promise `e << 40 | s`.
3. Its fallback bumps to `e << 40 | (s+1)` — *inside the owner's epoch space* —
   and commits a new value there.
4. The owner's next write, derived from its now-stale cache, also mints
   `e << 40 | (s+1)`: the very same counter.
5. The node-id tiebreak decides the collision. If the owner's node id is the
   larger, its accept satisfies the acceptors' promise and **overwrites the
   full proposer's already-committed value with a value derived from stale
   cache** — a silent, quorum-accepted lost update.

The reason this matters methodologically is that an abstract CASPaxos agreement
spec written in the Lamport voting style is *safe by construction* — it never
represents the ballot encoding, so it cannot represent two proposers minting the
same counter, and it certifies the buggy system as correct. We therefore model
the ballot encoding explicitly (`owned_register.qnt`). Its invariant
`NoLostUpdate` states that a committed value must contain its predecessor;
under the pre-fix `+1` bump rule the model reports a violation as a short
concrete trace (owner and full proposer tie at counter `(epoch, seq) = (2, 1)`,
owner wins the tiebreak, the committed value vanishes), and this negative
control is *required to fail* — a passing run would mean the model had lost its
teeth.

The fix is a small change to how a proposer jumps past a conflict: when the
conflicting ballot's epoch bits are nonzero — i.e. it belongs to an owner's
epoch — round the next ballot **up to the next epoch boundary**, `(e+1, 0, ·)`,
rather than incrementing the sequence. The interloper's ballot now strictly
dominates the owner's entire epoch, so the owner's next write NACKs cleanly and
converts to a re-acquire instead of a silent overwrite. Under this rule
(`JumpRule = TRUE`) the model passes at roughly one million distinct states.
Three further rules round out the discipline: **epoch 0 is reserved** for
plain-proposer space so an owner can never mint in the non-owned region from the
other side; **takes are idempotent per epoch**, so writers racing the first take
do not re-prepare at the same epoch and depose themselves; and a **retry after a
fence bump** breaks a self-collision livelock in which a node's own full-path
fallback epoch-jumps into exactly the epoch its manager just bumped to and, being
minted by the same node id, collides with its own next take forever. Finally,
epochs are **lineage-monotonic across range splits**: a child descriptor starts
at `parent.epoch + 1`, so a stale pre-split claim always loses the per-key epoch
comparison. The epoch budget is 2^24 per key; sustained adversarial contention
burns at most the retry budget of epochs per failed proposal, a documented cost.

### 3.4 The reflexive control plane

Membership and placement are not a separate subsystem in cask — they are more
registers of the same kind, hosted on the same consensus, and this reflexivity
is what keeps the control plane from needing a control plane of its own.

The **roster** is a single register whose value is
`{Epoch, Members, Core, Joint, ConfigGen, RangeIDs}`. Its `Core` field names the
bounded set of acceptors that store the roster register itself — by default the
three highest-id members (`RegisterRF = 3`). The striking property is that the
roster reconfigures *itself*: to move the Core from an old set to a new one, cask
runs joint consensus **on the roster register**, advancing the membership value
through the very transition that hands the register to a new quorum.
`roster_reconfig.qnt` proves the two invariants that make this sound
(`NoLostMembership`: once handed to the new Core, the latest committed
membership is present there; `AlwaysAvailable`: the latest membership is always
present on some active configuration). Three orthogonal counters keep the
concerns separate: `Epoch` advances on membership and range changes, `ConfigGen`
advances only on Core reconfiguration, and each range carries its own epoch.
(The Nebula lighthouses that assist NAT traversal are explicitly *not* the Core;
they carry no consensus role.)

**Range descriptors** are ordinary registers hosted on that same Core, one per
range at key `\x00rd/<id>`, each holding `{ID, Start, End, Replicas, Epoch}`. A
descriptor's `Replicas` are placed by rendezvous (HRW) hashing over the full
member set — that is where the range's *data* lives — but the descriptor
registers themselves live uniformly on the Core, so they inherit the roster's
reconfiguration proof (`reconfig.qnt`: `NoLostValue` and `CatchUpHeld`, that a
joint-consensus carry-forward preserves everything chosen in the old
configuration at release time). The roster's `RangeIDs` is the authoritative
index of live ranges; a joining node reads the roster, reads each descriptor,
and is serving — no meta-range, no gossip round, no recursion (§5.2 measures
exactly this cold start).

Two protocols run over these registers. **Replica migration** moves a range to a
new replica set without losing a write: publish a joint descriptor (routers that
see the `Joint` field propose through both old and new quorums) → let the routing
lease settle (bounded by the snapshot-poll interval) → carry forward every key,
enumerated as the union over any *majority* of the old replicas (every
quorum-committed key intersects any majority, so the union is complete) → release
the old set. **Range splits** are four register commits with recorded, resumable
intents: write the left child descriptor, write the right child descriptor,
atomically swap the parent's id for the two children in the roster's `RangeIDs`
(the single observable cutover point), then tombstone the parent with a
`ReplacedBy` marker. A `Splitting` CAS flag on the parent serializes concurrent
splits of the same range without a cross-register transaction, and **no data
moves at split time** — the children's replicas are the parent's until the
placement driver later rebalances, and correctness never depends on that being
prompt. `range_descriptors.qnt` proves the split safe (`NoSplitBrain`: every
accepted write targets the authoritative range for its phase; `TombstoneIs
Terminal`: a tombstoned range never accepts again) and live
(`EventuallyCaughtUp`: every client eventually believes the authoritative range).

### 3.5 Liveness without a leader

A leaderless system has no election to stall on, but it can livelock: two
proposers can preempt each other's ballots indefinitely. Cask's answer is
ballot bumping plus **full-jitter randomized backoff**, invoked only *between*
preempted rounds — never on a first attempt, so the fast path pays nothing. A
preempted proposer sleeps a uniform random duration in `[0, min(Cap, Base <<
attempt)]` (defaults Base 5 ms, Cap 500 ms) before retrying, within a retry
budget (`maxRounds = 12`). This replaces an earlier liveness-by-convention
scheme (a single designated driver) that was correct but did not compose;
randomized backoff is the composable replacement, and we treat it as
defense-in-depth rather than a load-bearing guarantee, since per-key contention
is rare by construction in a coordination workload.

The other half of liveness is not being held hostage by a slow replica. Every
phase fans out one goroutine per acceptor under a per-phase cancelable context
and returns the instant a quorum is *satisfiable* — so phase latency is the time
to the fastest quorum, not the sum or the max over all replicas, and a dead peer
behind an un-timed-out transport costs nothing (§5.3 measures this straggler
immunity directly). The same logic fails a phase early when a quorum becomes
provably *impossible*. Late or cancelled accepts may still land on stragglers;
Paxos tolerates this, and the corresponding at-least-once delivery is exercised
as a standing simulation fault so the acceptor's idempotency is not merely
assumed. Finally, liveness itself is asserted as an invariant: in the gate's
contention profile, exhausting the retry budget in a round where *no fault is
active* is a violation — contention must cost latency, never liveness.

## 4. Correctness methodology

A store with no log has nothing to replay: there is no ordered history to diff a
suspect run against, and the hardest bug in the system — the ballot-space lost
update of §3.3 — is invisible to the abstract agreement argument and can be
provoked and *healed* within a single consensus round, leaving no durable trace.
This shapes how we validate cask. We use four pillars of escalating cost and
coverage, each of which we hold catches a strict superset of the previous one's
bugs: **Quint** model checking (TLC and Apalache) of the protocols,
**deterministic simulation
testing** of the real code under a seeded adversary, **Jepsen** against real
clusters, and long-running **production burn-in**. Confidence is tracked as an
explicit trust ladder from demo-grade (reached) to external-critical (12–18
months), with each rung gated on named work rather than elapsed time.

**Quint specifications with negative controls.** Eight Quint specifications,
checked with TLC and Apalache, cover the protocol designs (`quint/`):
per-register agreement (`caspaxos.qnt`), the ballot-space discipline
(`owned_register.qnt`), lease-guarded reads (`owner_reads.qnt`), locks and
fencing (`lease.qnt`: `SingleHolder`, `FenceMonotone`), per-range
reconfiguration (`reconfig.qnt`), the reflexive roster (`roster_reconfig.qnt`),
the four-step split (`range_descriptors.qnt`), and the cross-range snapshot
contract (`cross_range.qnt`). They began as TLA+ specifications; each Quint
port reproduces its original's TLC state count (`quint/PARITY.md`). The
methodological commitment is the **negative control**: every module ships a
step that the checkers are *required to fail*. In `owned_register.qnt` the
pre-fix `+1` bump must report a `ChosenChain` (lost update) violation, and in
`owner_reads.qnt` the naive single-sided lease check must report a
`NoStaleRead` violation. A spec that can no longer find its own seeded bug has
silently lost its teeth, and the negative control is what detects that
regression. The first full-suite TLC run was itself informative: it surfaced four
latent defects in previously-unchecked specs and one genuine design rule —
snapshot reads must use a timestamp at or below the range's applied HLC (the
`SnapshotRead` guard in `cross_range.qnt`, generalized in the implementation as
the GetReadVersion rule).

**Deterministic simulation as a release gate.** The second pillar runs the real
cask code — the same proposer, acceptor, and store used in production — under a
seed-deterministic adversary that drives which fault fires, when, and where,
every `buggify.Maybe` decision, and every clock advance from a single seeded
source. (We are honest that this is a seeded adversary, not a single-goroutine
logical-time simulator: goroutine interleaving is not controlled, so replay is
best-effort and each violation persists its seed and full trace.) The catalog
holds eleven live faults — partitions, crash-restarts, mass failure, asymmetric
reachability, slow fsync, at-least-once duplicate delivery, link latency, and
four consensus-specific faults including the §3.3 owner-versus-full-proposer
duel and a driverless dueling-proposers liveness fault. Invariants are checked as
code against durable acceptor state: per-register agreement (**S1**), MVCC
monotonicity and exactly-once ops (**S2**), per-key HLC monotonicity (**S3**),
owner-epoch dominance (**S11**), and — the structural detector for the
lost-update class — **S13**, that committed MVCC history is append-only. S13 has
one blind spot by construction: a lost update that is both provoked and healed
inside a single round leaves append-only history intact. That gap is closed by
**fault assertions**: a fault that observes its own expectation violated emits a
warning trace event, and the gate promotes any new warning to a hard failure at
end of round. Five profiles (`smoke`, `consensus`, `lease`, `cluster`,
`contention`) each run under two topologies — full two-phase rounds and the
ownership fast path — on every pull request, so the fast-path composition is
exercised on the same footing as the base protocol, and the `contention` profile
keeps the QuePaxa-style adversarial regime as a standing gate.

**Wire-shape tests.** Every latency claim in §3 is an assertion about RPC counts,
not a benchmark, so we test it as one: a counting acceptor decorator confirms
that an owned write issues exactly one accept round and zero prepares, and that a
lease-guarded read issues none. These assertions are made straggler-immune
against the early-quorum fan-out, so they pin the protocol's shape rather than a
particular scheduling.

**Every bug becomes a fault.** The rule that ties the pillars together, borrowed
from FoundationDB, is that every bug found anywhere — in production, under
Jepsen, or reported by a user — is reproduced as a named fault at the code
location that would have surfaced it, and that fault then runs on every
subsequent pull request. The catalog only grows; a class of bug, once seen, is
caught forever. This is why the simulation gate, not any single spec, is the
center of gravity: the Quint specs prove the designs, but the gate is where the
running code is made to keep earning the proofs' assumptions.

## 5. Evaluation

| Claim | Status | Evidence |
|---|---|---|
| Owned write = 1 accept round (3 accepts, 0 prepares) | **Measured** | owner wire-shape tests |
| Owned read = 0 rounds; degraded = 1 round | **Measured** | owner read tests |
| Slow/dead replica off the critical path | **Measured** | 300ms-replica and blocked-peer tests |
| Lost-update discipline necessary & sufficient | **Machine-checked** | owned_register.qnt + negative control; unit + fault + gate reproduce pre-fix |
| Read lease sound under skew ≤ MaxOffset | **Machine-checked** | owner_reads.qnt + negative control |
| Migration loses nothing (disjoint sets, mid-migration writes) | **Tested** | orchestrator + cmd driver tests |
| Group-commit durability | **Measured** (with honest fsync-cost caveat on dev VM) | Pebble bench |
| Throughput/latency vs etcd, same hardware | **Measured** | `bench/` harness — see §5.1 |
| Cold start (100 nodes, 50 ranges) < 1s | **Measured** | `bench/` cold-start — see §5.2 |
| WAN profile | **Measured** | `bench/` wan — see §5.3 |

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

### 5.3 WAN profile

Steady-state latency/throughput of one RF=5 range as per-hop RTT grows
(`cmd/cask-bench wan`, in-process over `testutil/sim`, same latency seam as
§5.2). Two results, both matching §3. First, latency tracks **~2×RTT** — a
prepare round plus an accept round to a quorum — at every hop latency (p50
4/12/44/106ms for 1/5/20/50ms hops); the owned fast path §3.2 collapses this to
one accept round ≈1×RTT, so this flat-path number is the ceiling. Second,
**straggler immunity §3.5**: with one of five replicas at 400ms (20× the other
four), write p50 is 44ms — identical to the 20ms-uniform baseline — because a
quorum is the *fastest* majority and the slow replica never touches the critical
path. That is "phase latency = max over the fastest quorum, not the sum of
replica RTTs," measured. Full data and method: `bench/README.md`.

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
