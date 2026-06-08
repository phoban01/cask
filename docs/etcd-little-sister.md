# Cask — etcd's Little Sister

A working roadmap to make cask the **faster, lighter, way-more-scalable**
sibling to etcd, for the workload cask is actually well-shaped for:
**leases, locks, sessions, and small metadata** at fleet scale.

This document is a **handoff for the next agent**. Sections are independent
work items; each lists the problem, the proposed design, the files to touch,
and an acceptance test. Pick items roughly in priority order — the early ones
gate the scale claim from being a benchmark-on-localhost story.

> Repo baseline used for every file/line reference in this doc: as of the
> commit `171c8a2 fix(demo): OCI AMD-micro fallback, host firewall, deploy +
> teardown robustness` on `main`. Verify lines before editing — drift is the
> default.

> **Before relying on anything in this doc**, read
> [`docs/confidence.md`](confidence.md) — what TLA+, FDB-style
> deterministic simulation, and Jepsen actually buy you, what they
> don't, and the cask trust ladder. The roadmap below is the *what*;
> confidence.md is the *how-do-we-know-it-works*.

## 0. Source plans and what is canonical here

Three plan documents exist in `~/.claude/plans/`. This roadmap is the
canonical reference for performance + scale work, but it builds on top
of decisions already locked in by those plans:

| Plan | Date | Role | Canonical for |
|---|---|---|---|
| `i-would-like-to-logical-goose.md` | 2026-06-01 | The master architecture plan ("logical goose"). | Two-tier topology, milestones M0–M8, verification layers, language choice, the existence of TLA+ as a release gate. |
| `warm-plotting-biscuit.md` Phase 1 | 2026-06-06 | Multi-cloud demo (implemented + live). | Cross-cloud deployment scripts under `demo/`, Nebula listen-vs-advertise split, client-only mode. |
| `warm-plotting-biscuit.md` Phase 2 | superseded | Registry + relay control plane. | **Superseded** by nifty-globe. Do not implement. |
| `i-was-thinking-more-nifty-globe.md` | 2026-06-06 | Stateless mint + DNS-SRV + **reflexive roster** on a dynamic Core. | **The control-plane shape**: bootstrap (founder gate), NodeID lifecycle (cert is the persistent identity), roster value shape (`{Epoch, Members, Core, Joint, ConfigGen}`), DNS-SRV discovery. |
| `docs/etcd-little-sister.md` | 2026-06-06 | This document. | Performance + scale roadmap, range-descriptor placement (C'), cross-range consistency (A), invariants list, TLA+ specs added for §4.3 and §4.4. |

**Where this doc supersedes the master plan:** the master plan said
range descriptors live in a "small consensus-managed meta-range" with
Plumtree-disseminated caching. We have instead chosen **C'** (§4.3):
descriptors are individual CASPaxos registers, all hosted on the **Core**
— the same dynamic acceptor set that already holds the roster per
nifty-globe. This unifies the control-plane consensus group into one
Core rather than two (roster + meta-range), and reuses the joint-
consensus reconfiguration machinery `internal/reconfig` and
`tla/Reconfig.tla` already prove correct.

**Where this doc inherits from nifty-globe:** the `Core` concept,
the reflexive roster register shape, the `--bootstrap` founder gate,
DNS-SRV as the canonical discovery backend, and the rule that
**lighthouses are pure Nebula NAT-traversal infrastructure** with no
consensus role.

If a future plan supersedes any of these, update §0 first.

---

## 1. Positioning: what cask is for, what it's not for

**Cask is etcd's little sister.** Smaller surface area, but:

- **Faster** in the common path: 1-RTT writes (already shipped via
  `caspaxos.OwnedProposer`), and — once item §3.1 lands — zero-RTT reads
  during a lease epoch.
- **Lighter** to deploy: single binary, embedded encrypted overlay (Nebula
  in-process, no TUN, no root), no operator-curated peer URLs.
- **Way more scalable** in fleet size: O(log N) per-node membership state via
  HyParView, consensus-confirmed roster decoupled from gossip flap, range-
  sharded keyspace so write throughput is not bottlenecked by a single Raft
  leader.

**Workloads cask targets:**

- Distributed locks with fencing tokens (the headline primitive).
- Sessions and TTL-keepalive for fleets of 10⁴ agents.
- Small metadata under per-key linearizability.
- A change feed (watch) over leases and small metadata.

**Workloads cask deliberately does not chase:**

- Multi-key ACID transactions over arbitrary key sets. CASPaxos has no global
  log to anchor multi-key commit to. We will not pretend otherwise.
- Strongly-ordered global event streams ("everything after revision X across
  the whole keyspace"). HLC ordering within a range only.
- Large values. Coordination store, not a document store.

The positioning matters: every design choice below is justified by the
target workload, not by a "be etcd" instinct.

### Layers — primitives + bundled patterns + external extensions

Cask is shaped as a **small primitive** with a thin **bundled layer
ring** and an **open invitation for external layers**, in the style of
FoundationDB's record / document / queue layers.

| Tier | Components | Stability contract |
|---|---|---|
| **Primitive** | `internal/caspaxos` (per-register CASPaxos) + `internal/mvcc` (per-key version chains + HLC) + `internal/agent.Router` (range routing) | Stable, versioned API. Breaking changes require a major version bump. |
| **Bundled layers** | `internal/lease` (sessions + locks + fencing), `internal/watch` (key/prefix change feed), `internal/roster` (membership + descriptors per §4.3) | Stable, versioned. Can be replaced if a workload demands a different shape. |
| **External layers** | User code on top of the primitive: rate limiter, feature-flag store, leader election, work queue, distributed cron, config bundle. | User's responsibility. We document patterns, not implementations. |

The framing matters because it lets cask be a **small, sharp core**
that solves the hard distributed-systems problems once, and lets users
compose the patterns they actually need. The bundled layers are
**proofs the primitive is rich enough**, not the whole product surface.

Encouraged external-layer patterns to document (not implement):

- **Rate limiter** — lease + fencing token = a single-holder semaphore.
- **Feature flag** — versioned KV + watch = an etcd-FF-style store.
- **Leader election** — the lease primitive itself, no extra layer needed.
- **Work queue** — lease + watch + prefix scan = a poor-man's queue.
- **Config bundle** — `SnapshotRead` across a prefix at a single HLC
  (composes with §4.6 `GetReadVersion`).

Items in this roadmap apply to the **primitive** and the **bundled
layers**. External layers are user code and out of roadmap scope.

> README still leads with "KV store with leases" — update to the layers
> framing as part of the §4.6 / §6 work. Cross-reference when that
> lands.

---

## 2. Where the code is today (skip if you know it)

Directories that matter most for this roadmap:

| Path                              | Role                                                          |
|-----------------------------------|---------------------------------------------------------------|
| `internal/caspaxos/`              | Per-register CASPaxos: proposer, acceptor, ballots, owner fast path. |
| `internal/store/mem.go`           | Acceptor storage. **In-memory only today.**                   |
| `internal/transport/`             | ConnectRPC consensus transport + Network seam (TCP / Nebula). |
| `internal/transport/nebula/`      | Embedded Nebula userspace overlay + lighthouse-seeded discovery. |
| `internal/mvcc/`                  | Per-key version chain, snapshot reads, compact watermark.     |
| `internal/watch/`                 | Watch streams, prefix merge by HLC, FanOut, ErrCompacted.     |
| `internal/lease/`                 | Sessions, locks, fencing tokens, reaper soft-leader.          |
| `internal/ranges/`                | Range descriptors, ordered map, split/merge primitives.       |
| `internal/placement/`             | HRW + failure-domain replica selection, hysteresis.           |
| `internal/agent/`                 | `Router` — routes a key to its range's proposer.              |
| `internal/membership/`            | HyParView + Plumtree.                                         |
| `internal/failure/`               | Phi-accrual + Rapid cut detector.                             |
| `internal/roster/`                | Consensus membership register + Controller.                   |
| `internal/reconfig/`              | Joint-consensus carry-forward for range reconfig.             |
| `cmd/cask/`                       | Runnable single binary; static + overlay wiring.              |
| `tla/`                            | TLA+ safety specs (per-register, lease, reconfig).            |

Two facts that drive most of the optimizations below:

- **Reads are full Paxos rounds.** `mvcc.go:214` (`Get`) calls
  `kv.read(ctx, key)` which calls `kv.prop.Propose(ctx, key, caspaxos.Identity)`
  (`mvcc.go:331`). Two phases, two network round trips, every read.
- **No batching anywhere on the transport.** `grep -rn batch internal/transport/`
  returns nothing. One Accept = one RPC.

Both are huge perf headroom.

---

## 3. Performance roadmap — prioritized

Items are ordered: **scale-blocking → biggest-perf-wins → polish**. Each
item is independent unless `Depends on:` says otherwise. The agent picking
this up should be able to land them one at a time.

### 3.0 — Durable acceptor storage (Pebble) **[gates everything]**

**Why.** The acceptor store today is `internal/store/mem.go` — pure RAM.
Any process restart loses all CASPaxos register state, which means every
"safety under partition+crash" claim is currently bypassed by the fact
that the only `Storage` implementation can't crash-and-recover. README
already names Pebble as the planned backing store; this is now the gating
work item.

**What.**

1. Implement a `Storage` (see `internal/caspaxos/acceptor.go:9-19`) on top
   of Pebble:
   - Key encoding: `acceptor-state/<range-id>/<key>` → CBOR/proto-encoded
     `Register{Promise, Accepted, Value}`.
   - Single WAL per node, group-commit batching:
     - All `Store` calls within a configurable window (e.g. 1 ms) coalesce
       into one Pebble batch + one fsync.
     - The waiter pattern: each caller blocks on a per-batch
       `chan struct{}` that closes when fsync returns.
   - Crash recovery: on startup, replay nothing — Pebble's WAL handles
     durability; we just open the DB and serve.

2. Add a `WithFsyncEvery(d time.Duration)` knob so tests can run at 0
   (no batching) and production at e.g. 500 µs.

3. Wire Pebble in `cmd/cask/cluster.go` behind a `--data-dir` flag.
   Without `--data-dir`, keep the in-memory path for the deterministic
   simulator and unit tests.

**Files to touch.**

- `internal/store/pebble.go` (new)
- `internal/store/groupcommit.go` (new)
- `internal/caspaxos/acceptor.go` — no API change expected; verify
  `Storage` interface still suffices.
- `cmd/cask/cluster.go` — `--data-dir`, lifecycle, close on shutdown.
- `go.mod` — `github.com/cockroachdb/pebble`.

**Done when.**

- Unit tests: durability roundtrip (`Store`, kill DB handle, reopen,
  observe `Load` returns the same `Register`).
- A bench: `bench_acceptor_pebble_test.go` showing ≥ 50 k ops/s sustained
  Promise+Accept on a single SSD with group commit on; ≤ 1 k ops/s with
  it off, demonstrating the win.
- Existing `test/linearizability` and `test/jepsen` suites pass against
  the Pebble store under partition+crash nemesis.

---

### 3.1 — Lease-cached owner reads (zero-RTT reads in the common case) **[biggest read-path win]**

**Why.** Every `Get` runs a full two-phase Paxos round
(`mvcc.go:331`). For a coordination store where reads are typically
≥ 10× writes, this is the single biggest perf gap vs etcd. etcd serves
"linearizable" reads in one round trip via the leader's read-index;
follower reads can be zero round trips with a stale-read flag.

Cask already has the safety primitive in `OwnedProposer`: while an
owner holds the epoch lease for a key, **its accepted value is the
latest committed value**, because (a) the epoch encoding in the ballot's
high bits guarantees no other proposer can write at a lower epoch, and
(b) `OwnedProposer.TakeOwnership` carries forward whatever was last
accepted. That is exactly the invariant we need for a local read.

**What.**

1. Add `OwnedProposer.ReadLocal(key) ([]byte, bool, error)` that:
   - Confirms `time.Now() < epochExpiry` (lease still valid).
   - Confirms the local replica is in the current replica set for the
     range (from the roster epoch).
   - Returns the locally-stored `Accepted.Value` for the key.
   - Returns `ErrLostOwnership` if the lease is stale.

2. Plumb a `LocalReader` boundary in `mvcc.KV` so `Get` and `GetAt` try
   `ReadLocal` first, falling back to `Propose(Identity)` on miss.

3. Add an explicit `StaleOK bool` opt that allows zero-RTT reads even
   without lease (best-effort, may be stale by clock skew + replication
   lag).

**Safety argument.** A lease-bounded local read is linearizable because
the epoch-fenced owner is the unique writer for the key for the lease
duration. Any future writer must increment the epoch (via
`TakeOwnership`), which invalidates this owner's lease before any new
value can be accepted. Cross-check with `tla/Lease.tla`: extend it with
a `LocalRead(owner, key)` action that returns `Accepted.Value` when
the owner's lease is live, and verify the existing invariants still
hold.

**Files to touch.**

- `internal/caspaxos/owned.go` — add `ReadLocal`; expose the lease
  expiry from `OwnedProposer`.
- `internal/mvcc/mvcc.go` — `Get`, `GetAt`, `read` to consult a
  `LocalReader` interface.
- `internal/agent/router.go` — let the agent's per-range proposer
  expose its `OwnedProposer` for read-side access.
- `tla/Lease.tla` — add `LocalRead` action, re-run TLC.

**Done when.**

- New bench `bench_get_owned` shows ≥ 200 k Get/s/node (vs ~1–2 k for
  current full-Paxos reads on the same hardware).
- Porcupine linearizability test in `test/linearizability` extended to
  include reads served via `ReadLocal`, still passes under partition
  nemesis.
- Jepsen `test/jepsen/fencing_test.go` extended with read-after-write
  across an epoch flip — must observe the new owner's value, never the
  old.

**Depends on:** §3.0 (otherwise local read returns stale RAM on crash).

---

### 3.2 — Accept-phase batching across keys per owner **[biggest write-path win]**

**Why.** With `OwnedProposer`, the steady-state write is a single Accept
RPC per key. But there's no batching: 1000 lock acquires from a single
agent against the same range-replica-set become 1000 separate Accept
RPCs. The acceptor's per-key stripe lock (`acceptor.go:84-95`) means
they can run concurrent, but the network is being wasted.

**What.**

1. Introduce a `BatchAccept(epoch, []{key, seq, value})` RPC in the
   proto schema:
   ```proto
   message BatchAcceptRequest {
     uint64 epoch = 1;
     repeated AcceptEntry entries = 2;
   }
   message AcceptEntry {
     bytes key = 1;
     uint64 seq = 2;     // per-epoch sequence
     bytes value = 3;
   }
   message BatchAcceptResponse {
     repeated AcceptResult results = 1;  // index-aligned with request entries
   }
   ```
2. Acceptor handler iterates entries, taking the stripe lock per key,
   applying the same `b.Less(reg.Promise)` check (`accept.go:69`). Each
   result is independent: one entry can NACK while others succeed.
3. Proposer side: an `OwnedProposer` write coalescer with a 200 µs (or
   tunable) window that batches all pending writes for the same
   {range, replica-set, epoch} into one RPC per replica.
4. Backpressure: if a batch is rejected (epoch invalidated), all in-batch
   writes return `ErrLostOwnership`.

**Files to touch.**

- `proto/cask/v1/consensus.proto` — new RPC.
- `internal/transport/consensus.go` — server + client.
- `internal/caspaxos/owned.go` — `BatchedWriter` coalescer.
- `internal/store/mem.go` and `internal/store/pebble.go` —
  `BatchStore([]Entry)` so the group-commit fsync window absorbs the
  batch.

**Done when.**

- Bench shows ≥ 5× throughput improvement on a 3-node cluster for a
  workload of 10 concurrent writers, mixed-key, against one range.
- Wire trace confirms one RPC per ~50 writes under load (depending on
  batch window).
- Linearizability tests still pass; porcupine model extended to inject
  partial-batch failures (entry 7 of 10 NACKs, others succeed).

**Depends on:** §3.0 (pebble batch).

---

### 3.3 — Piggyback session keepalive on consensus traffic **[lighter wire]**

**Why.** O(agents) keepalive is shipped, but it's still a dedicated
heartbeat RPC per session per TTL/3 (`internal/lease/session.go`). At
10 k agents with a 30 s TTL, that's ~1 k RPCs/s of pure heartbeat.

**What.** Any successful CASPaxos proposal from an agent already proves
the agent is alive. Add an optional `SessionID` field on the
`ProposeRequest` envelope; when present, the receiving acceptor's range
leader (or the session's owning range's owner) treats it as a keepalive
and refreshes the session expiry.

Edge cases:
- An agent that's reading-only still needs an explicit `KeepAlive` RPC.
  Fine — that's the rare case.
- The session may live in a different range from the write. Solution:
  the receiving proposer enqueues a lightweight "touched" notification
  on the session's owning range; eventual consistency for liveness is
  acceptable (the TTL has slack built in).

**Files to touch.**

- `proto/cask/v1/consensus.proto` — `optional bytes session_id = N` on
  proposal request.
- `internal/lease/session.go` — `Touch(sessionID)` API.
- `internal/agent/router.go` — propagate session-touched notifications
  to the session's range.

**Done when.**

- A workload of "100 agents, each issuing 10 writes/s, sessions with
  30 s TTL" sustains zero dropped sessions with **zero** explicit
  KeepAlive RPCs on the wire.
- Existing `lease_test.go` keepalive tests unchanged.

---

### 3.4 — Pipeline Accept behind in-flight Prepare for the same owner

**Why.** Once `OwnedProposer` has taken ownership, every subsequent
write is just a sequence bump in the ballot. There's no need to wait for
the previous Accept response before issuing the next — they're ordered
by sequence on the acceptor side via the stripe lock.

**What.** A bounded pipeline depth (default 16) of in-flight Accepts per
{owner, key}. On `ErrPreempted` from any one, drain the pipeline and
return errors to all in-flight callers behind it (epoch was lost). On
quorum, ack each in order.

**Files to touch.**

- `internal/caspaxos/owned.go` — pipeline state machine.
- `internal/transport/consensus.go` — concurrent Accept handling per
  connection (already concurrent at the gRPC/HTTP2 layer; verify).

**Done when.**

- Single-writer latency-bound benchmark: p99 write latency drops by
  ≥ 30 % on a 1 ms-RTT link vs current serialized model.
- Linearizability tests pass with pipeline depth set to 1, 8, 32.

---

### 3.5 — Plumtree-pushed watches (kill the poll)

**Why.** `KeyWatcher.Poll` (`internal/watch/watch.go`) is a pull model:
client asks "anything after cursor X?" on a timer. For 1000 watchers on
1000 keys, that's a lot of needless storage reads even with `FanOut`
already optimizing the storage side.

We already have **Plumtree** (`internal/membership/plumtree.go`) — an
epidemic broadcast tree over the HyParView overlay, delivered with O(N)
messages and O(log N) latency. Use it.

**What.** When a CASPaxos commit lands, the owning proposer publishes a
`WatchDelta{range, key, seq, hlc, value}` envelope on the Plumtree
broadcast layer with a topic of the range ID. Watchers subscribe to
range topics and receive deltas directly. Polling becomes the fallback
(cold start, after a missed delta).

Ordering: deltas within a single key are ordered by their per-key Seq
(monotonic). Across keys, HLC. Plumtree may reorder; the watcher
re-sorts by Seq per key and merges by HLC across keys.

**Files to touch.**

- `internal/watch/push.go` (new) — Plumtree-backed delta producer
  hooked into `mvcc.KV.commit`.
- `internal/membership/plumtree.go` — confirm message-topic
  multiplexing; add if not present.
- `internal/watch/watch.go` — receive-side merge, fallback to Poll on
  detected gaps.

**Done when.**

- Watch latency p99 drops from O(poll interval) to O(broadcast latency)
  (target: < 50 ms on the deterministic simulator's network model).
- Storage-read load on the acceptor under "1000 watchers, 10 writes/s
  per key" drops to near-zero (only FanOut pump reads; deltas pushed).

**Depends on:** Plumtree topic multiplex — verify in `membership/`.

---

### 3.6 — Hedged proposals for tail-latency cuts

**Why.** Under tail-latency events (GC pause on one replica, transient
packet loss), a Prepare or Accept waits for the full quorum from a
fixed set. Hedging — send to all replicas in parallel, take first
quorum — is the standard fix.

**What.** `Proposer.Propose` already gathers responses from all
acceptors; today it waits for `quorumInAllGroups` (`proposer.go:83-98`).
The hedge is implicit because we already send to all. The real
opportunity is **early-quorum return**: as soon as enough OKs arrive,
cancel the remaining requests' contexts.

Add a `hedge.Selector` that cancels in-flight outliers once quorum is
reached. Measure: this should mostly help p99 on networks with one
flaky replica.

**Files to touch.**

- `internal/caspaxos/proposer.go` — context cancel after quorum.
- `internal/transport/consensus.go` — ensure server-side cancellation
  closes the per-RPC context (no half-applied state — Paxos already
  tolerates this).

**Done when.**

- Bench under a deterministic-sim nemesis that delays 1-of-3 replicas
  by 50 ms shows p99 latency unchanged from no-nemesis baseline.

---

### 3.7 — Connection multiplexing & cold-start pruning

**Why.** Today `internal/transport/consensus.go` likely opens one HTTP/2
or HTTP/1 connection per peer for ConnectRPC. For an N=10 k overlay,
that's a lot of file descriptors per node. HyParView already keeps
active view ~5 — but the *proposer* dials all replicas for its ranges,
which can be more.

**What.** A `connpool` keyed by overlay address with idle eviction
(15 s) and an LRU cap of 128 connections per node. Backed by HTTP/2
multiplexing so all RPCs to one peer share one connection.

**Files to touch.**

- `internal/transport/connpool.go` (new)
- `internal/transport/consensus.go` — wire pool.
- `internal/transport/nebula/nebula.go` — verify
  `Network.HTTPClient()` is reused, not constructed per-call.

**Done when.**

- File descriptor count under a 1 k-node simulated cluster stays
  bounded (≤ 256 per node).

---

## 4. Correctness gaps that gate scale

These aren't perf items, but they're release-blocking for any "more
scalable than etcd" claim that involves more than one range.

### 4.1 — `ErrRangeChanged` in the proposer error space

**Problem.** `internal/agent/router.go:67-107` does a single rmap
lookup, dials the cached proposer, and proposes. If the client's
rmap is stale (range R1 just split into R1a/R1b), the proposal goes
to acceptors that may no longer manage that key, returning a generic
`ErrPreempted` with no signal to refresh.

**Fix.**

1. Add `ErrRangeChanged` to `internal/caspaxos/errors.go`.
2. The acceptor returns it when its range descriptor epoch is newer
   than the proposer's claimed epoch (must thread current epoch on the
   request).
3. `Router.Propose` catches it, refetches the range descriptor (initially
   from the roster; once §4.3 lands, from a meta-range), retries once
   against the new replica set.

**Files.** `proto/cask/v1/consensus.proto`, `internal/caspaxos/errors.go`,
`internal/transport/consensus.go`, `internal/agent/router.go`.

**Test.** New `test/linearizability` scenario: a partition that triggers
a range reconfig mid-workload; the client must observe linearizable
semantics across the rmap refresh.

---

### 4.2 — Persistent NodeID (via cert persistence)

**Problem.** `nebula/discovery.go:115-138` derives `NodeID` from the
overlay IP. If the node's overlay IP changes on restart, it shows up
as a new roster member; the old member must be cut-detected and
removed; in-flight leases bound to the old NodeID are reaped.

**Fix (per nifty-globe Work Item 2).** Persistence is the **minted
cert**: a node ships with `--mint <url> --token --zone --role` (or
loads a pre-minted cert from disk). After the first `cask mint` call
the node writes the cert+key to disk; on every subsequent start it
loads them, getting the **same overlay IP and therefore the same
NodeID**. The cert is the persistent identity.

Trade-off (accept, document): a node that **loses its cert** (data-dir
wiped) and re-mints becomes a **new** node. The stale NodeID ages out
via the failure detector and is removed from the roster. There is no
"recover my old NodeID" path — and nor should there be, because the
cert key is the security boundary.

**Files.**

- `cmd/cask/main.go` — `enroll()` writes cert+key to `<data-dir>/cert/`
  on first mint; loads from disk on every subsequent start.
- `internal/transport/nebula/discovery.go` — no change; identity
  derivation from overlay IP is correct, just stable now.
- `internal/roster/controller.go` — already idempotent on
  `Add(self)` (nifty-globe §c); no change.

**Depends on:** §3.0 (need a data dir to write the cert to), and
nifty-globe Work Item 2 (`cask mint`).

**Out of scope:** the UUID-in-data-dir approach previously considered
here is dropped — cert persistence is simpler and aligns with the
nifty-globe identity model.

---

### 4.3 — Range descriptors: roster-as-index, descriptors-on-the-Core ("design C'")

**Status.** Decided 2026-06-06. No code yet. This section is the
authoritative design.

> The "Core" referenced below is the reflexive-roster Core specified
> by `i-was-thinking-more-nifty-globe.md` — a bounded, dynamic
> acceptor set (size `registerRF`, default 3, the `registerRF` highest-
> NodeID members), reconfigured via joint consensus on the roster key
> itself. Lighthouses are **not** the Core; they are pure Nebula
> NAT-traversal infrastructure with no consensus role.

#### Model

- The **roster** (`internal/roster/`) becomes the consensus-tier index.
  Per nifty-globe its `Value` already carries
  `{Epoch, Members, Core, Joint, ConfigGen}`; we add one more field:
  `RangeIDs []uint64` — the authoritative list of live range IDs.
  Membership changes bump `Epoch` (data-placement input); `RangeIDs`
  bumps `Epoch` too (a new range is observable like any membership
  change); `Core`/`Joint` changes bump `ConfigGen`.
- Each **range descriptor** is its own CASPaxos register at key
  `\x00rd/<id>` (big-endian uint64 id). Carries
  `{ID, Start, End, Replicas, Epoch}`. `Replicas` is HRW-placed over
  the **full Members set** — i.e. where the data lives. The
  descriptor's own epoch bumps only on replica-set reconfig of that
  one range.
- **The Core hosts every descriptor register.** All Core members are
  acceptors for every `\x00rd/<id>`; quorum is `len(Core)/2 + 1`. No
  HRW placement for descriptors — uniform replication across the Core,
  same as the roster.
- The Core may itself change (growing 1→3→5 as `Members` grows, or
  replacing a condemned core member). That is the reflexive-roster
  reconfiguration nifty-globe specifies, which `tla/Reconfig.tla`
  already proves preserves any committed value. **Descriptors ride on
  the same Core**, so they inherit the same proof — no separate Core
  for descriptors.

**Two-epoch design.** The roster's `Epoch` and per-descriptor `Epoch`
move independently. A roster-Epoch bump (new member joined, new range
created) does not invalidate any descriptor. A descriptor-Epoch bump
(range reconfig) does not invalidate the roster or any other
descriptor. `ConfigGen` is a third, orthogonal counter for Core
membership; it only matters to the consensus-tier dialer.

#### Cold-start bootstrap

1. New node forms its Nebula overlay (lighthouse-assisted hole-punch),
   then resolves the discovery name (DNS-SRV per §5.1) ∪ static seeds
   to find any current Core member.
2. Reads `roster` register from any Core member → gets `Members`,
   `Core`, `RangeIDs`, `ConfigGen`.
3. For each id in `RangeIDs`, reads `\x00rd/<id>` from the Core → builds
   local rmap (`OrderedMap[Start → Descriptor]`).
4. Serves traffic. No recursion, no meta-range, no gossip-wait.

A node with a **stale believed Core** (was reconfigured out while it
was unreachable) fails its first read, falls back to discovery to find
a live Core member, then re-fetches. This is the chicken-and-egg
recovery anchor nifty-globe §d specifies; descriptors reuse it.

Target: < 1 s to first served read on a 100-node cluster with 50
ranges.

#### Steady-state routing

Client uses local rmap. On `ErrRangeChanged{newDescriptorEpoch}` from
the proposer (§4.1):

- Refetch the named descriptor from the cohort.
- If the key falls outside the refetched descriptor's `[Start, End)`,
  the range was split; refetch full `RangeIDs` and any missing
  descriptors.

This is the etcd `revision → re-list` pattern, scoped per-range.

#### Split protocol — Variant 1 (descriptors-first, roster-cutover)

A split of `R_old → R_left + R_right` is four CASPaxos commits in
order. Steps 1–2 are safe to run concurrently:

1. **Write `\x00rd/<L>`** — new left descriptor,
   `[R_old.Start, midpoint)`, `Replicas` copied from `R_old.Replicas`
   initially, `Epoch = 1`. Acceptors = the Core.
2. **Write `\x00rd/<R>`** — new right descriptor,
   `[midpoint, R_old.End)`, same replicas, `Epoch = 1`. After this
   commit, both new descriptors exist on the Core but **no client
   routes to them yet** — the roster still names `R_old`.
3. **Update roster** atomically: `RangeIDs` removes `R_old.ID`, adds
   `L.ID` and `R.ID`; bump roster epoch. **This commit is the
   cutover.** Clients refreshing after this point route to L/R.
4. **Tombstone `\x00rd/<R_old.ID>`** by writing a marker value
   `{Tombstoned: true, ReplacedBy: [L.ID, R.ID]}`. Any client that
   catches `ErrRangeChanged` on the old descriptor reads this marker
   and deterministically refetches the new descriptors.

Data physically lives on `R_old`'s replicas until the placement driver
rebalances L and R to different replica sets — but correctness does
not depend on that happening promptly. The split is observable
atomically at step 3; data movement is async bookkeeping.

Concurrent splits of the same range are serialized via a `Splitting`
flag on the descriptor — set via CAS in step 1, cleared in step 4.
Avoids needing a cross-register joint commit.

#### Merge protocol

Inverse of split. Write the new merged descriptor first
(`[L.Start, R.End)`), update the roster's `RangeIDs` (remove L.ID,
R.ID; add new), tombstone the two old descriptors.

#### Replica-set reconfig (separate from split/merge)

A range that needs new replicas (heal under-replication, drain a node,
improve zone spread) writes a new version of its descriptor with new
`Replicas` and bumped `Epoch`. **No roster change.** Joint-quorum
carry-forward (existing `internal/reconfig/`) drives the data
migration. `internal/placement/driver.go`'s `NeedsReconfig` already
makes the trigger decision; we wire it to the orchestrator.

#### Files to touch

- `internal/roster/roster.go` — add `RangeIDs []uint64` to `Value`
  (the rest of the Value shape — `Core`, `Joint`, `ConfigGen` — comes
  from nifty-globe Work Item 4a, which is a prerequisite). Update
  `Genesis`, `Add`, `Remove`, normalization, serialization.
- `internal/ranges/descriptor.go` (new) — `RangeDescriptor` type, key
  encoding `\x00rd/<id>`, codec (proto, matching the wire format).
- `internal/ranges/coreproposer.go` (new) — helper to construct a
  CASPaxos proposer over the **current Core**, using the same proposer-
  factory closure pattern nifty-globe Work Item 4e establishes for
  the roster register. Resolves Core member IDs through the existing
  `dialer` at call time so reconfigs are picked up automatically.
- `internal/ranges/orchestrator.go` (new) — `Split`, `Merge`,
  `Reconfig` primitives driving the protocols above; the `Splitting`
  flag CAS.
- `internal/agent/router.go` — bootstrap reads from roster + Core
  descriptors; rmap refresh on `ErrRangeChanged`; stale-Core recovery
  via discovery (reuses nifty-globe §d's follow logic).
- `internal/caspaxos/errors.go` — `ErrRangeChanged{NewEpoch uint64}`
  (this is §4.1; §4.3 depends on it).
- `proto/cask/v1/` — `RangeDescriptor` message; wire descriptor epoch
  on the `ProposeRequest`.
- `cmd/cask/cluster.go` — drop the static-single-range path; let
  bootstrap come from the orchestrator creating an initial range
  spanning the full keyspace immediately after roster founding.

#### Done when

- **Cold-start bench:** 100-node cluster, 50 ranges; new node joins,
  builds rmap, serves first read in < 1 s.
- **Split-during-write** (`test/linearizability/split_test.go` new):
  porcupine workload runs while a range splits; linearizable
  semantics preserved across the cutover, zero lost or duplicated
  writes.
- **Merge-during-watch:** `PrefixWatcher` spans two ranges that merge;
  contiguous event stream, no gaps, HLC order preserved.
- **Core-failure:** kill `floor(|Core|/2)` Core members — descriptor
  reads still succeed (quorum). Kill `floor(|Core|/2)+1` — descriptor
  reads fail safe with `ErrNoQuorum`; data on the full Members set
  keeps serving reads against cached descriptors (degraded mode).
  When the Core recovers, the cut detector triggers a Core reconfig
  via nifty-globe §c.
- **TLA+:** extend `tla/Reconfig.tla` (or add `tla/SplitMerge.tla`)
  to model the four-step split with a concurrent client; verify no
  reachable state lets two clients write the same key to disjoint
  replica sets.

#### Depends on

- §4.1 (`ErrRangeChanged` plumbed through the proposer error space).
- §3.0 (descriptors are CASPaxos registers; Core members need durable
  storage for them — same Pebble store the roster uses).
- nifty-globe Work Item 4 (reflexive roster on a dynamic Core). The
  Core concept must exist in `internal/roster` before §4.3 can wire
  descriptors onto it.

#### Sub-decisions deferred to implementation

- **Tombstone GC.** When can `\x00rd/<old-id>` be removed entirely?
  Probably a TTL ≥ max-client-refresh-window; default 1 h.
- **Split-point selection.** Start with 50-50 by key range. Add
  hot-key isolation later (split on the key with highest contention
  observed).
- **Reverse migration to a sharded descriptor cohort.** If we ever
  exceed the Core's capacity (~10 k ranges, since every descriptor
  read traverses all Core members), we can shard descriptors across
  multiple Cores indexed by range-id hash. Not anticipated; flagged
  for future. Distinct from "meta-range" — there's no recursive
  bootstrap, just a deterministic mapping `id → owning-Core`.

---

### 4.4 — Cross-range consistency: documented uncertainty window (design A)

**Status.** Decided 2026-06-06. No code yet. This section is the
authoritative design.

#### The contract

`SnapshotRead(keys, t)` returns a consistent view for keys that have
**not been modified in the window `[t - MaxOffset, t]`**. `MaxOffset`
is the cluster's max clock skew bound; default **500 ms**, configurable
via `--max-clock-offset`.

Cross-key consistency in cask is delivered **at write time** by
fencing tokens (the lease/lock primitive), not at read time by
snapshot semantics. Snapshots are for backup, diagnostic, and audit
reads — not for the lock-state-machine path.

This is honest about the limit and reserves the right to add strict
cross-range linearizability later behind a `SnapshotReadStrict` API
without breaking the default contract.

#### Why not the HLC barrier (option B)

The barrier's only structural win is linearizability with respect to
concurrent cross-range mutations. For cask's target workload (sessions
+ locks + small metadata), cross-key consistency is already enforced
at write time by fencing tokens. The barrier would add an RTT to every
snapshot read for a guarantee the lock state machine doesn't need.

Costs of B that we're declining to pay now:

- One extra RTT per snapshot read.
- HLC-wait state machine in every proposer (needs TLA+ to be trusted).
- New failure mode: an unreachable range stalls every snapshot until
  timeout.

B remains *additive* — when a workload shows up needing it, expose it
as `SnapshotReadStrict` behind a `--strict-snapshot` flag. No
architectural change required.

#### Files to touch

- `internal/hlc/hlc.go` — add `MaxOffset time.Duration` on the clock;
  default `500 * time.Millisecond`.
- `internal/mvcc/mvcc.go` — `SnapshotRead` doc comment states the
  uncertainty contract explicitly (text below).
- `cmd/cask/cluster.go` — `--max-clock-offset` flag wiring.
- `README.md` — replace any "consistent point in time across keys in
  different ranges" wording with the uncertainty-window contract; lead
  with fencing tokens as the cross-key consistency story.
- `tla/CrossRange.tla` (new) — model two ranges with clock skew
  bounded by `MaxOffset`; verify that for any key K and snapshot time
  t such that K was last modified at time t' < t - MaxOffset, the
  snapshot observes K at t'.

The `mvcc.SnapshotRead` doc comment should read approximately:

> SnapshotRead returns a consistent view of `keys` at HLC timestamp t,
> subject to the cluster's `MaxOffset` skew bound. For any key in
> `keys` that has not been modified during `[t - MaxOffset, t]`, the
> returned value is the unique linearizable value at t. Keys modified
> within that window may return any value committed in the window.
>
> SnapshotRead is intended for diagnostic, backup, and audit reads.
> Cask's cross-key consistency story for coordination workloads is
> the fencing-token discipline at write time, not snapshot semantics.
> For strict cross-range linearizability, see `SnapshotReadStrict`
> (gated behind `--strict-snapshot`, not yet implemented).

#### Done when

- `MaxOffset` config knob lands with the 500 ms default and a flag.
- Existing `SnapshotRead` callers tolerate the documented contract; no
  test in the repo currently depends on cross-range linearizability
  beyond what HLC ordering provides within a range (verify this).
- `tla/CrossRange.tla` passes TLC with `MaxOffset = 500ms`,
  `NumRanges = 2`, `NumKeys = 4`, depth ≥ 20.
- README and `mvcc.SnapshotRead` godoc both carry the new wording, and
  any conflicting prose is removed.

#### Depends on

Nothing structural. Can land independently of §4.3.

---

### 4.5 — TLA+ for joint reconfig + range carry-forward together

**Status.** `tla/CasPaxosMvcc.tla` proves per-register agreement.
`tla/Lease.tla` proves fencing monotonicity. `tla/Reconfig.tla` proves
joint-consensus reconfig is non-lossy. They don't compose.

**Fix.** A `tla/CrossRange.tla` that has two ranges, one range reconfig
in flight, snapshot reads spanning both, and checks the combined
invariant. This is what gates "we believe the cross-range story".

**Files.** `tla/CrossRange.tla` (new), `tla/README.md`.

---

### 4.6 — `GetReadVersion()` API (FDB-lesson #2)

**Why.** §4.4 fixes the cross-range snapshot contract — an `HLC` `t` is
the consistent point clients read at, subject to `MaxOffset`. But there
is no API today to *get* the right `t` cheaply. Clients are left to
guess (their local clock, ±skew) or to construct a `t` from a recent
write's reply, which couples reads to writes.

FoundationDB's `GetReadVersion()` ("GRV") solves exactly this: one
round trip to the cluster returns a snapshot timestamp safe for use as
a read version. The client then issues many reads at that version
without coordination per read. This is what makes consistent multi-
key reads usable at the API level.

**What.**

1. Add `KV.GetReadVersion(ctx) (hlc.Timestamp, error)` to
   `internal/mvcc/mvcc.go`. Implementation:
   - Query a quorum of Core members for their current HLC ceiling.
     The Core already holds the roster + descriptors and is the
     deterministic cluster-wide synchronization point.
   - Return `max(core.hlc) - MaxOffset` — the most recent timestamp
     guaranteed to be outside every range's uncertainty window. Clients
     reading at this `t` get the §4.4 contract with the uncertainty
     window already subtracted; they don't have to reason about
     `MaxOffset` themselves.
   - Single round trip in the steady state (one parallel scatter-gather
     to the `|Core|` members, max ≈ |Core| = 3).

2. Add `SnapshotRead(keys, t)` as the natural consumer. Document that
   `t` should come from `GetReadVersion`; using a client-local time
   weakens the contract because the client's clock may be ahead of any
   range's HLC.

3. Wire a `GET /grv` HTTP endpoint in `cmd/cask` so the demo and
   `caskctl` can use it (`caskctl snapshot --keys k1,k2,k3` becomes
   `caskctl grv | xargs caskctl snapshot --at`).

4. Expose `GetReadVersion` to bundled layers — the `lease` reaper, the
   `watch` cold-start replay, and the `roster` `Reconcile` loop all
   benefit from a known-consistent snapshot point.

**Composes with.**

- §4.4 — the GRV-returned `t` is by construction outside the
  uncertainty window, so the `SnapshotRead(t)` it feeds is consistent
  for every key.
- §3.1 — owner-cached reads bypass GRV entirely (they read at "now"
  within their epoch lease); GRV is for *snapshot* reads, not
  point reads.
- §4.3 — GRV queries the Core, which §4.3 already plumbs. No new
  cohort needed.

**Files.**

- `internal/mvcc/grv.go` (new) — `GetReadVersion(ctx)`, the quorum
  scatter-gather, and the `MaxOffset` subtraction.
- `internal/mvcc/mvcc.go` — `KV.GetReadVersion`; update
  `SnapshotRead`'s godoc to recommend GRV-sourced `t`.
- `proto/cask/v1/` — `GetReadVersionRequest`/`Response` if exposing
  over ConnectRPC; otherwise just the HTTP endpoint.
- `cmd/cask/cluster.go` — `GET /grv` HTTP route.
- `cmd/cask/caskctl` or equivalent — `caskctl grv`.

**Done when.**

- Single-RTT bench: `bench_grv` shows p99 < 5 ms on a 3-Core cluster
  with healthy NTP.
- `bench_snapshot_consistent` does `t := GRV(); read = SnapshotRead(t)`
  on N keys across M ranges; verifies the §4.4 contract holds for
  every read (no key returns `NoValue` if it was written before
  `t - MaxOffset`, no key returns a value with `hlc > t`).
- Jepsen `multi_key_snapshot_test` (new) uses GRV under the partition
  + clock-skew nemesis; observes no consistency violations across N
  iterations.
- README updated: snapshot reads are demoed via GRV, not via a magic
  timestamp.

**Depends on:**

- §3.0 (Core members need durable storage for HLC).
- §4.4 (the uncertainty contract that GRV honors).
- nifty-globe Work Item 4 (the Core must exist).

**Out of scope:**

- A *strict* GRV (FDB's actual semantics, requiring barrier-style
  HLC synchronization across ranges). That is the §4.4 option B path,
  reserved for a future flag-gated `GetReadVersionStrict()`.

---

## 5. Discovery: close the "zero-config" gap

The README's "self-forming over Nebula" story is real and good. Three
small items make it complete.

### 5.1 — DNS-SRV discovery backend

**Spec is canonical in nifty-globe Work Item 3** — the design,
including the `Unmap()` requirement, identity derivation from overlay
IPs, error propagation, and union-with-seeds dedup, is already
specified there. This roadmap item is a pointer to that spec, not a
duplicate. Implement as written.

**Files.** `internal/discovery/srv.go` (new, per nifty-globe);
`cmd/cask/cluster.go` behind `--discovery-srv <domain>`; export
`nebula.NodeIDFromIP`.

**Depends on:** nothing structural — can land independently.

---

### 5.2 — mDNS for LAN bootstrap

Useful for demos and devbox setups. Multicast `_cask._udp.local` so
nodes find each other without any config at all.

**Files.** `internal/discovery/mdns.go` (new), behind `--discovery-mdns`.

---

### 5.3 — Federation across lighthouses

Today every cask node uses the same Nebula lighthouse set. To support
multi-cloud-without-a-single-overlay, design a lighthouse-federation
story: a node's lighthouse list is per-region; cross-region members
are reachable through gateway nodes that bridge two overlays.

**This is exploratory** — write a design doc in `docs/federation.md`
before code.

---

## 6. Benchmarks the next agent should add

Without numbers, "faster than etcd" is marketing. Add a `bench/`
directory with:

| Bench                       | Scenario                                                         | Target          |
|-----------------------------|------------------------------------------------------------------|-----------------|
| `bench_lock_acquire`        | N agents, single hot key, contended acquire/release loop         | ≥ 50 k ops/s    |
| `bench_lock_acquire_uncontended` | N agents, distinct keys, no contention                      | ≥ 200 k ops/s   |
| `bench_kv_get_owned`        | Read after warm OwnedProposer epoch                              | ≥ 200 k ops/s   |
| `bench_kv_put_owned`        | Single-writer steady-state writes via owner fast path            | ≥ 50 k ops/s    |
| `bench_kv_put_batched`      | After §3.2, batched writes from one writer to one range          | ≥ 250 k ops/s   |
| `bench_watch_fanout`        | One key, 10 k subscribers, measure CPU per delivered event       | ≤ 5 µs/event    |
| `bench_membership_scale`    | Grow overlay 100 → 3 k nodes; per-node state & convergence time  | already passing |
| `bench_cold_start_join`     | New node joins existing overlay; time-to-first-write             | < 1 s           |

Compare each against an etcd 3.5 cluster on the same hardware. Publish
the deltas in `docs/benchmarks.md`. If we ship numbers that aren't
better than etcd's on this workload mix, we don't get to use the
"little sister" framing.

Also add the `GetReadVersion` bench (§4.6): `bench_grv` (≥ 100 k
ops/s/node from a warm client), `bench_snapshot_consistent` (the
multi-key snapshot via GRV, end-to-end p99 < 20 ms across 10 ranges).

## 6.6 — Simulator as the release gate (FDB-lesson #1)

**Why.** FoundationDB's reputation for rock-solid behaviour comes from
one thing more than any other: a deterministic simulator that runs the
whole database in one goroutine, with seeded RNG, time control, and a
fault catalog, and the discipline that **nothing ships unless the
simulator runs clean on enough seeds**. Cask already has the bones —
`testutil/sim`, pure protocol cores in `internal/caspaxos`,
`internal/mvcc`, `internal/membership` — but no formal release gate.
This section formalises it.

### The gate

Any change to a **protocol-bearing package** must run the simulator
with the **fault catalog** (§6.7) for the configured seed budget and
emit zero violations of any §6.5 invariant. A violation is a
release-blocker; the seed and trace are kept and added to the
regression set.

Protocol-bearing packages: `internal/caspaxos`, `internal/mvcc`,
`internal/membership`, `internal/failure`, `internal/lease`,
`internal/reconfig`, `internal/roster`, `internal/ranges`,
`internal/agent` (`Router`), `internal/watch` (`FanOut`,
`SafeCompactPoint`). Other code (logging, HTTP wiring, demo scripts)
does not gate.

### Two seed budgets

| Tier | Seeds | Wall time | When |
|---|---|---|---|
| **CI** | 10⁴ per fault profile | ≤ 15 min | every PR; gates merge |
| **Pre-release** | 10⁶+ per fault profile | hours | gates a tagged release |

CI uses a fixed seed prefix (deterministic, fast to debug). Pre-release
uses a random seed prefix per run (broad coverage). Both fail-fast on
the first invariant violation.

### Invariant coverage

The §6.5 invariants list **must be enumerable in code** — see
`testutil/sim/invariants.go` (new). Each step of the simulator
evaluates all enumerated invariants against the current global state;
any violation halts the run with a complete event trace. This is what
makes the gate real — without an automated invariant check at every
step, "the simulator passed" is meaningless.

Maps to §6.5:

- **S1** (per-register agreement): proposer reads + acceptor state
  agree on the chosen value across all replicas.
- **S2** (per-key version monotonicity): consecutive committed versions
  have strictly increasing `Seq`.
- **S3** (HLC monotonicity per range): committed HLCs strictly
  increase.
- **S4–S5** (reconfig safety, catch-up): every value chosen in the old
  config is choosable in the new config before old is released.
- **S6–S7** (lease single-holder, fence monotone): no overlapping live
  leases; fencing tokens never go backwards.
- **S8–S9** (split protocol safety, descriptor-epoch carry-forward):
  no client write lands in a range whose epoch is not current.
- **S10** (cross-range skew bound): HLC drift never exceeds
  `MaxOffset`.
- **S11** (owner-epoch dominance): no stale owner ever commits.
- **S12** (watcher non-starvation): compact watermark never advances
  past a live watcher's cursor.

### Files to touch

- `testutil/sim/gate.go` (new) — driver: takes a seed range, a fault
  profile, and an invariant set; runs N seeded scenarios; reports per-
  seed results; emits a JSON regression record per violation.
- `testutil/sim/invariants.go` (new) — enumerable invariant predicates
  matching §6.5; each takes a snapshot of `sim.Cluster` state.
- `testutil/sim/regression.go` (new) — replayable scenarios for any
  prior violation; CI runs these unconditionally on every PR before
  exploring new seeds.
- `scripts/sim-gate.sh` (new) — invoke from CI and from a release
  build; configurable seed budget; pretty-prints failures.
- `.github/workflows/sim-gate.yml` (new) — CI workflow that runs the
  CI-tier seed budget per PR.
- `docs/sim-gate.md` (new) — onboarding doc: how to add a new fault,
  how to add a new invariant, how to inspect a failure trace.

### Done when

- Every §6.5 safety invariant has a corresponding predicate in
  `testutil/sim/invariants.go` and is evaluated on every simulator
  step.
- The CI tier (10⁴ seeds × every fault profile in §6.7) runs in
  ≤ 15 min on a standard CI runner.
- Three known historical bugs (the M2 non-atomic per-key RMW; the
  M2 non-linearizable CAS abort; the M2 non-exactly-once MVCC append
  under proposer retries — README §M2) are captured as named regression
  scenarios in `testutil/sim/regression.go`.
- A documented `release-gate` make target runs the pre-release tier
  and produces a signed report.

### Depends on

- §6.5 — the invariants list is what the gate checks against.
- §6.7 — the fault catalog is what the gate runs.

### Cost / honesty

This is engineering discipline, not new code mass: ~1500 LOC for the
driver + invariants + regression set, plus a CI workflow. The expensive
part is the discipline of holding the line when a release nears and
the simulator finds one more thing. The FDB lesson is: hold the line.

### 6.6.1 — `buggify`: Go-idiomatic BUGGIFY sites (FDB's secret sauce)

**Why.** FoundationDB's `BUGGIFY` macro is the **single highest-impact
testing primitive** in the field. It lets engineers annotate any line
of code with "here's a place where reality could be cruel" — and the
simulator, during a run, sometimes takes that cruel path. Slow this
fsync. Drop this RPC. Force the full Paxos round instead of the fast
path. Reorder this batch. The discipline that engineers add a
`BUGGIFY` site at every spot where they wrote *"this should be fine"*
is what surfaces the bugs you would otherwise discover at 3am two
years post-launch.

Cask must have an equivalent. Go has no preprocessor macros, but a
clean Go-idiomatic shape works just as well.

#### Design

`internal/buggify/buggify.go` (new):

```go
// Package buggify provides FDB-style fault-injection annotation sites.
//
// Call sites in protocol code use Maybe to declare "here is a point
// where reality could be cruel." In production builds the function is
// always false (single nil check, eliminated by the compiler). In
// simulation builds the simulator's registered Hook decides with
// per-site probability.
package buggify

// Hook is set by the simulator at init time. Production never sets it.
// Returns true to make the call site take the "cruel" path.
//
// The 'name' identifies the site for the simulator's report and for the
// fault catalog (§6.7) — every site is implicitly a named fault.
//
// The 'prob' is a hint to the simulator; the simulator may override it
// per profile (e.g. cluster profile turns up the probability of
// network-related sites).
var Hook func(name string, prob float64) bool

// Maybe returns true with the configured probability when the simulator
// is active, false otherwise. Use at any point in protocol code where
// you suspect reality could deviate.
//
// Examples:
//
//	if buggify.Maybe("owned_proposer_force_full_round", 0.02) {
//	    return p.fullPropose(ctx, key, change) // skip the 1-RTT path
//	}
//
//	if buggify.Maybe("acceptor_reject_promise", 0.01) {
//	    return ErrPreempted // pretend a higher ballot arrived first
//	}
//
//	if buggify.Maybe("batch_split", 0.05) {
//	    half := len(batch) / 2
//	    return append(p.send(batch[:half]), p.send(batch[half:])...)
//	}
func Maybe(name string, prob float64) bool {
	if Hook == nil {
		return false
	}
	return Hook(name, prob)
}

// Register declares a buggify site for the simulator's site registry.
// Optional; sites that haven't called Register are still callable but
// the simulator can't report on them or override their probabilities.
//
// Convention: register at package init() time so the simulator knows
// the full site set on startup.
func Register(name, description string, defaultProb float64) {
	// In production this is a no-op too (the registry only lives in the
	// simulator).
	if registerHook == nil {
		return
	}
	registerHook(name, description, defaultProb)
}

var registerHook func(name, description string, defaultProb float64)
```

`testutil/sim/buggify.go` (new) installs the hooks:

```go
// In the simulator's Init():
buggify.Hook = func(name string, prob float64) bool {
    site := siteOrDefault(name, prob)         // registry lookup
    if profile.Disabled(name) {
        return false
    }
    return sim.RNG.Float64() < profile.AdjustedProb(name, prob)
}
buggify.RegisterHook = func(name, desc string, defaultProb float64) {
    sim.SiteRegistry.Add(name, desc, defaultProb)
}
```

#### Where to place sites

Three categories. Sprinkle in roughly equal measure:

1. **Slow-path forcing.** Anywhere there's a fast path and a slow
   path, BUGGIFY to take the slow path. This ensures the slow path
   stays exercised under all faults — the most common shape of
   "we never noticed this code rotted" bugs.
   - `OwnedProposer.Propose`: force full phase-1 round
   - `agent.Router`: invalidate cached descriptor and refetch
   - `mvcc.KV.Get`: skip the owner cache, go via full Paxos round
2. **Adverse environmental conditions.** Anywhere code assumes "this
   call succeeds quickly," BUGGIFY to make it fail or take longer.
   - `Storage.Store`: sleep before fsync (slow disk)
   - `Storage.Load`: occasional `ErrTransient` (lying disk)
   - `Transport.Send`: drop the packet (network nemesis already does
     partition; this models gray failure)
   - `hlc.Now`: jump time forward by some skew within MaxOffset
3. **Concurrency stressors.** Anywhere there's a critical section,
   BUGGIFY to widen the window.
   - Mid-Promise/Accept: sleep to widen the race window
   - Mid-roster reconfig: sleep between phase 2 and phase 3
   - Mid-split: sleep between CreateR and Cutover

#### The discipline

This is what makes `BUGGIFY` valuable, not the primitive itself:

- **Every protocol PR adds ≥ one `buggify.Maybe` site** at a place
  where the PR author thought "this should be fine but I'm not sure."
  This is a PR-review checklist item, not a suggestion.
- **A bug found in prod, in Jepsen, or by an external user adds a
  buggify site** at the exact code location that would have surfaced
  it. The fault catalog (§6.7) grows by one named entry.
- **The simulator runs with `--buggify-rate` configurable per profile.**
  Default 0.05 (5% per site) for CI; up to 0.2 for pre-release. High
  rates exercise unrealistic combinations but find race windows
  systematic testing can't.
- **Site names live in code, not config.** Names are stable strings
  used by the regression set; renaming one breaks regressions. Treat
  the name like a public API.

#### Files

- `internal/buggify/buggify.go` (new) — the Hook + Maybe + Register
  package.
- `testutil/sim/buggify.go` (new) — the simulator-side Hook
  implementation and site registry.
- `testutil/sim/profile.go` (extend §6.7) — `AdjustedProb(name, prob)`
  per profile.
- A `BUGGIFY.md` style doc enumerating the placed sites, updated as
  the catalog grows. (Or just `grep buggify.Maybe -r`.)

#### Initial sites to seed (places I'm already confident need them)

- `caspaxos.OwnedProposer.Propose` — force full phase-1 (`owned_proposer_force_full_round`).
- `caspaxos.Proposer.Propose` — drop a vote response from one acceptor (`proposer_drop_vote`).
- `caspaxos.Acceptor.Promise` — return `ErrPreempted` spuriously (`acceptor_spurious_preempted`).
- `mvcc.KV.read` — bypass the owner cache (`mvcc_skip_owner_cache`).
- `store.Pebble.Store` — sleep up to 5s before fsync (`store_slow_fsync`).
- `agent.Router.Propose` — return `ErrRangeChanged` spuriously (`router_force_refetch`).
- `roster.Roster.Add` — abort mid-reconfig phase 2 (`roster_abort_joint`).
- `ranges.Orchestrator.Split` — pause between CreateR and Cutover
  (`split_pause_pre_cutover`).
- `lease.Session.KeepAlive` — drop the heartbeat (`session_drop_keepalive`).
- `hlc.Clock.Now` — jump forward by `rand.Intn(MaxOffset)`
  (`hlc_skew_forward`).
- `watch.KeyWatcher.Poll` — return ErrCompacted spuriously
  (`watch_spurious_compacted`).

These eleven cover most of the protocol surface. The catalog should be
into the **hundreds of sites** before v1 — that's the FDB benchmark.

#### Done when

- `buggify.Maybe` is callable from every protocol-bearing package with
  zero cost when the simulator is not active.
- The eleven seed sites above are placed and tested.
- The §6.6 simulator gate respects per-site probability overrides per
  §6.7 profile.
- A "BUGGIFY catalog" view (CLI or markdown) lists every placed site,
  its description, and its default probability — so engineers can see
  the cruelty surface at a glance.
- A PR-template checkbox: "I have added a `buggify.Maybe` site for any
  new code path that could be slow, fail, or race."

#### Depends on

- §6.6 (the gate); without it, sites don't get exercised.
- §6.7 (the catalog); buggify sites *are* the catalog at the
  finest grain.

## 6.7 — Fault catalog (FDB-lesson #4)

**Why.** Faults injected by the simulator must be a **named, growable
catalog in code**, not folklore in a nemesis package. Every bug found
in prod, in CI, or in pre-release becomes a new named fault that the
simulator can re-inject deterministically. This is how the simulator
keeps getting stronger over time.

### The catalog (initial)

Each fault lives in `testutil/sim/faults/<name>.go` and exports a
`Fault` value (an injector wrapping `sim.Network`, `sim.Clock`,
`sim.Storage`, or `sim.Process`).

| Name | Models | Used to find |
|---|---|---|
| `partition` | network split into two halves | quorum loss, dueling proposers (already present) |
| `crash_restart` | process kill + restart | non-durable Promise/Accept (already present) |
| `clock_skew_within_bound` | each process's clock drifts within `±MaxOffset` | §4.4 uncertainty contract correctness |
| `clock_skew_exceeds_bound` | clock drifts past `MaxOffset` | shows the system *fails safe* — operations refuse or retry, no silent data corruption |
| `slow_fsync` | acceptor `Storage.Store` takes seconds | propose backpressure, watcher starvation, lease expiry under storage stress |
| `asymmetric_reachability` | A→B reachable, B→A not | partial-failure handling in HyParView + roster cut detector |
| `lighthouse_loss` | Nebula lighthouse becomes unreachable | new-node bootstrap resilience; in-flight cluster impact |
| `core_stale_rejoin` | a Core member with believed-Core != current rejoins | nifty-globe §d stale-config recovery |
| `mid_cutover_partition` | partition triggered between §4.3 steps 2 and 3 | §4.3 split-protocol atomicity around the cutover commit |
| `slow_watch_consumer` | a watcher reads at 1 ev/s while writes are 1 k ev/s | `SafeCompactPoint` watcher-aware compaction |
| `mass_failure` | kill 30% of nodes simultaneously | HyParView healing (already present, scale-gate) |
| `keepalive_blackhole` | session keepalive RPCs dropped silently | lease expiry + reaper cascade-free |
| `epoch_old_owner_write` | a previously-deposed `OwnedProposer` attempts an Accept | §S11 owner-epoch dominance |
| `concurrent_split_same_range` | two orchestrators try to split the same range simultaneously | §4.3 `Splitting` flag CAS serialization |
| `mid_grv_partition` | partition during a `GetReadVersion` quorum scatter | §4.6 GRV resilience |

### Fault profiles

A **profile** is a named subset of faults plus a probability
distribution. CI runs each profile to the CI seed budget; pre-release
runs each to the pre-release budget. Initial profiles:

- `smoke` — `partition` + `crash_restart`. Fastest. Runs on every PR.
- `consensus` — all consensus-facing faults: `partition`,
  `crash_restart`, `slow_fsync`, `asymmetric_reachability`,
  `epoch_old_owner_write`. Runs on every PR.
- `cluster` — adds `mass_failure`, `lighthouse_loss`,
  `core_stale_rejoin`. Runs on every PR.
- `lease` — `partition` + `crash_restart` + `clock_skew_within_bound`
  + `clock_skew_exceeds_bound` + `keepalive_blackhole`. Runs on every
  PR.
- `ranges` — `partition` + `mid_cutover_partition` +
  `concurrent_split_same_range`. Runs once §4.3 lands.
- `snapshot` — `partition` + `clock_skew_within_bound` +
  `mid_grv_partition`. Runs once §4.6 lands.

### Files to touch

- `testutil/sim/faults/` (new package) — one file per fault, all
  exporting `Fault` values implementing a small `Inject(*sim.Cluster)`
  interface.
- `testutil/sim/profile.go` (new) — `Profile` type, the named subsets
  above.
- `docs/sim-gate.md` (extend) — fault catalog quickstart: "to add a
  new fault, create `faults/<name>.go` with a Fault value and add it
  to the relevant profile."

### Done when

- All faults in the table above exist and are exercised under at least
  one profile.
- Adding a new fault is **one new file** plus one line in the relevant
  profile. No friction.
- Every known prod / Jepsen bug has a corresponding fault.
- The `mid_cutover_partition` fault catches a deliberately-introduced
  bug in §4.3 (a smoke test that the gate works).

### Growing the catalog

When a bug is found in prod, in `test/jepsen`, or by chance, the
first PR fixing it **must include a new fault** that would have caught
it in the simulator. This is the engineering discipline that
compounds — every found bug strengthens the gate.

---

## 6.5 — Invariants (the safety contract)

Every implementation in this roadmap must preserve the invariants
below. Each is paired with the TLA+ spec that model-checks it (existing
or to-be-written) and the Go test layer (rapid PBT or porcupine /
Jepsen) that exercises it against the running code. **A regression to
any of these is a release-blocker.**

| # | Invariant | Stated as | TLA+ spec | Go test |
|---|---|---|---|---|
| **S1** | **Per-register agreement.** Once a value is chosen for a key at any ballot, no different value is ever chosen for that key. | `Consistency` | `tla/CasPaxosMvcc.tla` | `internal/caspaxos/proposer_test.go` (rapid), `test/linearizability` (porcupine) |
| **S2** | **Per-key version monotonicity.** A key's MVCC `Seq` is strictly increasing in commit order. | implicit in HLC chain stamping | proven structurally from S1 + stamping | `internal/mvcc/mvcc_test.go` |
| **S3** | **HLC monotonicity per range.** Within a range, HLC timestamps are strictly increasing in commit order. | per-range `HLC' > HLC` | (covered by `internal/hlc` invariants) | `internal/hlc/hlc_test.go` |
| **S4** | **No committed value lost across reconfig.** For any sequence of joint-quorum reconfigs of a register's acceptor set, every previously chosen value remains chosen. | `NoLostValue` | `tla/Reconfig.tla` | `internal/reconfig/reconfig_test.go` |
| **S5** | **Catch-up before release.** A reconfig may not finalize the new-only phase until every value chosen in the old config is chosen in the new config. | `CatchUpHeld` | `tla/Reconfig.tla` | same |
| **S6** | **Single lock holder.** At any instant, at most one client holds a live lease on a given lock register. | `SingleHolder` | `tla/Lease.tla` | `internal/lease/lock_test.go`, `test/jepsen/fencing_test.go` |
| **S7** | **Fence monotonicity.** The fencing token issued on every successful `Acquire` is strictly greater than every previously issued token for that lock — across owner preemption, range relocation, and core reconfig. | `FenceLatest`, `FenceMonotone` | `tla/Lease.tla` (extend to include carry-forward across reconfig — see TLA work below) | `test/jepsen/fencing_test.go` |
| **S8** | **No two replica sets for one key (post-cutover).** After a split's roster cutover (§4.3 step 3), no two clients can successfully write the same key to disjoint replica sets at the same descriptor epoch. | `NoSplitBrain` | **`tla/RangeDescriptors.tla` (new — §4.3)** | `test/linearizability/split_test.go` (new) |
| **S9** | **Descriptor-epoch carry-forward.** A descriptor's `Epoch` strictly increases on every reconfig of that range; an `ErrRangeChanged` is returned for any proposal carrying an older epoch. | per-descriptor epoch monotonicity | `tla/RangeDescriptors.tla` (new) | `internal/agent/router_test.go` (extend) |
| **S10** | **Cross-range HLC skew bound.** For any two ranges r1, r2, the HLC values they have observed differ by at most `MaxOffset` plus in-flight network delay; a `SnapshotRead(t)` returns the value at `t'` ≤ `t - MaxOffset` for any key not modified in `[t - MaxOffset, t]`. | `UncertaintyContract`, `NoSkewOverflow` | **`tla/CrossRange.tla` (new — §4.4)** | `internal/mvcc/snapshot_test.go` (extend with skew injection) |
| **S11** | **Owner-epoch dominance (1-RTT safety).** A stale owner's ballot is dominated by every ballot a current owner can mint at a higher epoch; a stale owner's `Accept` always NACKs. | proven by ballot encoding (high bits = epoch) | covered by `CasPaxosMvcc.tla` via ballot comparison; document the encoding | `internal/caspaxos/owned_test.go` |
| **S12** | **Watcher non-starvation.** Compaction never advances past the cursor of a live watcher registered before the compaction call. | `SafeCompactPoint(cursors)` contract | (not model-checked; structural argument) | `internal/watch/watch_test.go` |
| **L1** | **Liveness — eventual rmap consistency.** Under fair scheduling, every client eventually refreshes its rmap after an `ErrRangeChanged`. | weak fairness on `ClientRefreshRmap` | `tla/RangeDescriptors.tla` (under WF assumption) | exercised by `split_test.go` |
| **L2** | **Liveness — eventual lock takeover.** Under fair scheduling, an expired lock is eventually re-acquirable by any waiting client. | weak fairness on `Acquire` | `tla/Lease.tla` (extend, optional) | `internal/lease/lock_test.go` |

Safety items (S*) are non-negotiable. Liveness items (L*) require
fairness assumptions; document them where they bite.

**TLA+ work this section calls out:**

- New `tla/RangeDescriptors.tla` — proves S8, S9, L1.
- New `tla/CrossRange.tla` — proves S10.
- Extension to `tla/Lease.tla` — add carry-forward across a range
  reconfig action so S7 is proved across the boundary, not just within
  one register. (S7 is already implemented and tested; the spec gap is
  closing the proof loop.)

## 7. What the next agent should do first

The four FDB lessons (§1 layers, §4.6 GRV, §6.6 simulator gate, §6.7
fault catalog) reshape priority. §6.6 + §6.7 are now **PR #0 — the
discipline that makes every subsequent PR safer**.

**PR #0 (~one week): build the gate before adding more protocol code.**

1. **§6.6 + §6.7** together. The current `testutil/sim` nemesis becomes
   the seed catalog; the existing invariants become enumerable
   predicates. The CI runs `smoke` + `consensus` profiles on every PR.
   Capture the three historical M2 bugs as named regression scenarios.

   Without this, the next protocol PRs are flying blind. With it, every
   subsequent change gets stronger as new faults accrete.

**PR #1 (~two weeks): the foundation.**

1. **§3.0 Pebble store** — unblocks §3.1, §3.2, §4.2, durable Core
   storage for §4.3.
2. **§3.1 lease-cached owner reads** — single biggest perf delta.
3. **§4.1 ErrRangeChanged** — small, gates §4.3.

PR #0's gate runs against every commit of PR #1. Storage durability
under `slow_fsync` + `crash_restart` is verified before merge.

**PR #2 (~three weeks): C' + GRV together.**

1. **§4.3 range descriptors C'** — the cluster's biggest structural
   landmark.
2. **§4.6 `GetReadVersion`** — small additive API; lands here because
   the Core (which §4.3 depends on) is what GRV queries.
3. Add the `ranges` and `snapshot` fault profiles to §6.7 in the same
   PR.

**PR #3:** §3.2 batched accepts + §3.5 Plumtree-pushed watches.

**PR #4:** §1 layers framing in README + `docs/layers.md` with the
external-layer recipes. No code; positioning + docs.

---

## 8. Open questions

All architecture-level decisions are closed. The two items below are
**implementation-time defaults** — the next agent should pick one and
move; neither blocks the roadmap.

1. **Acceptor storage engine** (§3.0). Default: **Pebble** (LSM, fast,
   good group-commit fsync, battle-tested in CockroachDB). Alternatives
   (Badger, Bolt) are acceptable if Pebble proves awkward to embed; not
   worth re-deciding without a concrete blocker.
2. **Persistent-NodeID format** (§4.2). Default: **hash of the
   overlay cert fingerprint** (reproducible after data-dir loss, so a
   reinstalled node with the same cert keeps its identity). Fall back
   to UUIDv4 in `<data-dir>/node-id` only if the cert-derived form
   conflicts with how the cohort wants to handle re-keying.

**Decided (2026-06-06):**

- **§4.3 range descriptor storage: design C'** — roster-as-index,
  descriptors-on-the-**Core** (the dynamic acceptor set of the
  reflexive roster per nifty-globe), split via Variant 1 (descriptors-
  first, roster-cutover). See §4.3 for the full spec. *Corrected
  2026-06-06: the initial framing said "lighthouses" but lighthouses
  have no consensus role; the Core is the correct cohort.*
- **Lighthouses are pure Nebula NAT-traversal infrastructure** plus
  the stateless `cask mint` endpoint. They are **not** consensus
  participants. (Inherited from nifty-globe.)
- **§4.4 cross-range consistency: design A** — documented uncertainty
  window with `MaxOffset` default 500 ms. Cross-key consistency for
  coordination workloads is fencing tokens at write time, not snapshot
  semantics. Strict mode (`SnapshotReadStrict`) is reserved as an
  additive feature for a future need.
- **NodeID is the minted cert** — cert persistence to disk gives stable
  identity; re-mint = new identity, documented trade-off. (Inherited
  from nifty-globe Work Item 2.) Supersedes the earlier
  UUID-in-data-dir idea.
- **FDB lessons (four, all four committed):**
  - **§1 Layers framing** — cask is a primitive + bundled layers +
    invitation for external layers. README to be updated.
  - **§4.6 `GetReadVersion`** — single-RTT snapshot timestamp via the
    Core; composes with §4.4.
  - **§6.6 Simulator as the release gate** — every protocol change
    runs N seeds × the fault catalog with zero §6.5 violations. CI
    tier 10⁴ seeds; pre-release tier 10⁶+ seeds.
  - **§6.7 Fault catalog as code** — named, growable list under
    `testutil/sim/faults/`. Every found bug becomes a new named
    fault. The gate gets stronger over time.

---

## 9. Out of scope (don't get distracted)

The next agent should **not** chase these without explicit ask:

- **Multi-key transactions.** Not what cask is for. If a use case forces
  it, that's a signal to use a different store, not to bolt Txn on.
- **Large values.** Hard cap at e.g. 1 MiB per value; document it.
- **Replacing CASPaxos with Raft.** The per-register model is the entire
  scalability story. Don't trade it away to look more etcd-like.
- **A "v3 API" surface for etcd-tool compatibility.** Tempting, but the
  semantic gap (global revision vs per-key) means it will always be a
  half-fit that mis-sells.

---

## 10. Glossary of cask-specific terms

- **Range** — a contiguous key interval, owned by a CASPaxos replica set.
- **Roster** — the consensus-confirmed cluster membership (separate from
  HyParView's flapping gossip view).
- **OwnedProposer** — a proposer that has acquired the epoch lease for a
  key and writes with one Accept round.
- **Epoch** — the lease fence, encoded in the ballot's high 24 bits.
- **Cut detector** — Rapid-style multi-observer agreement before a node
  is declared down (in the roster).
- **FanOut** — one upstream poll, many subscribers; the O(agents) watch
  multiplexer.

---

*This document is intended to be read once, then used as a checklist.
Update sections as work lands; delete items once they ship. Cross-link
new design docs from §3 / §4 entries as they're written.*
