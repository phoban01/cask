# Integration — etcd compatibility and Kubernetes

How cask interoperates with the etcd ecosystem **without** pretending to be
etcd. Two layers, build-orderable independently:

1. an **etcd-compatible coordination profile** — a gRPC surface speaking
   `etcdserverpb` for the subset of etcd semantics cask maps cleanly, so an
   existing `clientv3` program using locks/leases/leader-election can point at
   cask unchanged;
2. a **Kubernetes aggregated API server** — an extension server serving cask's
   own coordination resources to the kube control plane, layered over the same
   mapping.

Both are **external layers** in the §1 sense (`docs/etcd-little-sister.md`):
user-facing surface over the stable primitive + bundled layers, not changes to
the core. Neither is implemented yet; this note is the design and the contract.

> **The one sentence that governs everything here:** cask has **no global
> revision**. etcd's whole API hangs off a single cluster-wide monotonically
> increasing `revision`; cask has per-key `Seq` and per-range HLC, with
> cross-range consistency only outside the `MaxOffset` uncertainty window
> (§4.4). Every "yes this maps" below is a per-key/per-object claim, and every
> "no" traces back to this sentence.

---

## 1. What maps and what doesn't

The load-bearing distinction: etcd's `revision` is **global and totally
ordered**; cask's ordering is **per-key** (`mvcc.Version.Seq`) and **per-range**
(HLC). Anything an etcd client does that needs only per-key ordering maps
cleanly. Anything that needs the global order does not.

| etcd / `clientv3` surface | cask mapping | Fidelity |
|---|---|---|
| `Put` / `Get` (point) | `mvcc.KV` (`/kv/` today) | **clean** |
| `Delete` (point) | `mvcc.KV` delete | **clean** |
| `Txn` with a single-key compare (the CAS idiom) | per-key CAS (`/cas/` today) | **clean** — covers the overwhelming majority of real `Txn` use |
| `Lease` Grant / KeepAlive / Revoke / TTL | `lease.Sessions` (`/session/`) | **clean**, and cask adds a fencing token etcd has no equivalent for |
| `concurrency.Mutex` / `concurrency.Election` | `lease.Locks` (`/lock/`) — single-holder + fence | **clean**, strictly stronger (fenced) |
| `Watch` on one key, resume from a revision | `watch.KeyWatcher` (Seq cursor) | **clean** — Seq is the per-key revision |
| watch compaction → client must re-list | `watch.ErrCompacted` → relist | **clean** — cask copied this contract directly (`mvcc.Compact` + `SafeCompactPoint`) |
| `Watch` on a prefix | `watch.PrefixWatcher` (HLC-merged) | **range-local clean**; across ranges, HLC order not global commit order |
| range / prefix `Get` (`WithPrefix`) | prefix scan / `SnapshotRead` + GRV (§4.6) | **consistent only outside the uncertainty window** — see §3 |
| `Txn` spanning multiple keys | — | **unsupported.** No global log to anchor multi-key commit (roadmap §9). Return `Unimplemented`. |
| global `header.revision` semantics | — | **the unbridgeable gap.** There is no global counter to return. |
| `MoveLeader`, `Defragment`, member list / add / remove | — | different cluster model (CASPaxos per-register, roster). Map the read-only `MemberList` to the roster; stub the rest. |
| Auth / RBAC (per-key roles) | — | cask has no auth layer yet (§5.4); the authz story is fencing tokens, not key-prefix roles |

**`header.revision`:** every etcd response carries one. The honest options for
a shim are (a) return the key's per-key `Seq` in `ModRevision`/`Version` (correct
per-key — and what watch resumption actually needs), and (b) for the *response
header's* cluster-wide `revision`, return a per-range HLC-derived value and
**document that it is not globally comparable across keys**. A client that
compares header revisions across unrelated keys to infer global order is relying
on a guarantee cask does not make; that is exactly the class of usage the
coordination profile must reject loudly rather than fake.

---

## 2. Layer 1 — the etcd coordination profile (gRPC `etcdserverpb`)

A flag-gated gRPC listener implementing the subset above and returning
`codes.Unimplemented` (with a pointer to this doc) for everything else.

- **KV service:** `Put`, `Range` (point + prefix), `DeleteRange` (point +
  single-key), `Txn` **restricted to single-key compares** — reject a `Txn`
  whose compares/ops span more than one key with `Unimplemented`.
- **Lease service:** `LeaseGrant`, `LeaseKeepAlive` (streaming), `LeaseRevoke`,
  `LeaseTimeToLive` → `lease.Sessions`. A lease's attached keys are the
  session's locks.
- **Watch service:** `Watch` (streaming) → `KeyWatcher` for a single key,
  `PrefixWatcher` for a prefix. Map `ErrCompacted` to etcd's
  `ErrCompacted`/`WatchResponse{CompactRevision}` so a `clientv3` watcher's
  existing relist path fires unchanged.
- **Maintenance/Cluster:** `MemberList` → roster (read-only); `Status` →
  health; the rest stubbed.

**Why this is worth it:** the long tail of teams running a 3-node etcd *purely*
for locks, leader election, and a little config — cask's exact target user —
can repoint `clientv3` and get fencing tokens for free. The migration is the
product pitch, and the shim is the best possible proof the §1 primitive is rich
enough.

**Why it is not "drop-in etcd":** the moment a workload needs multi-key `Txn`
or global-revision watch ordering, it falls off the clean-mapping table. The
profile must advertise itself as **"etcd-compatible coordination profile,"**
never "drop-in etcd."

**Gating.** Run the simulator gate against the shim's handlers and a Jepsen
`lin-kv` workload over the gRPC surface (the same register-linearizability
check etcd itself is tested with), plus the lock+fencing workload. Sequence
after PR #2 — the profile wants `GetReadVersion` (§4.6) for consistent prefix
`Range` and `ErrRangeChanged` (§4.1) settled.

---

## 3. Layer 2 — Kubernetes aggregated API server

**Do this; do not back the kube-apiserver with cask as its etcd.** The
distinction is the whole design:

### Why not the etcd-backend path (the kine shape)

kube-apiserver-over-etcd demands the **global-revision** semantics cask
deliberately lacks: `resourceVersion` must be a single cluster-wide monotonic
counter, and the apiserver's watch cache breaks subtly without gap-free global
resumption. Tools like kine work precisely because a SQL backend hands them an
`AUTO_INCREMENT` sequencer — i.e. they *reintroduce* the single sequencer cask
exists to avoid. Backing the apiserver with cask would mean funneling all writes
through one sequencing range: the single-Raft-leader bottleneck, rebuilt. Off
the table.

### Why the aggregation layer fits cask's grain almost exactly

An **extension API server** (an `APIService` registered with the aggregation
layer, `apiserver-builder`/`sample-apiserver` shape) serves cask's **own** API
group — e.g. `coord.cask.io/v1` with `Lock`, `Session`, `Lease` resources. The
main apiserver routes `/apis/coord.cask.io/...` to cask's server and keeps its
own etcd for core objects. Crucially, the aggregation contract is **per-resource
watch/list**, not a global keyspace order — so *you* define what
`resourceVersion` means for *your* resources, and a per-object counter is all
the watch cache needs for that object.

| Extension-server requirement | cask mapping | Fit |
|---|---|---|
| `Get`/`Create`/`Update`/`Delete` per object | `mvcc.KV` point ops | clean |
| `resourceVersion` per object, monotonic | per-key `Seq` | **clean — per-object, not global** |
| `Watch` from a `resourceVersion`, gap-free per object | `KeyWatcher` (Seq cursor) | clean |
| `410 Gone` when the resume point is compacted → client relists | `ErrCompacted` → relist | clean — same contract |
| optimistic concurrency (update if RV matches) | per-key CAS | clean — *stronger*: also fenced |
| LIST a namespace/prefix | `PrefixWatcher` / prefix scan | range-local clean; cross-range: §3 caveat |
| cross-object atomic write | — | unsupported — design each resource so ops are single-object |

### The one real constraint

A LIST that must be a **single consistent snapshot across many objects in
different ranges** holds only **outside `MaxOffset`**. So:

- Feed multi-object LISTs from `GetReadVersion` (§4.6) once it lands — the GRV
  timestamp is by construction outside every range's uncertainty window, so the
  snapshot is consistent for every listed object.
- Design the CRDs so the hot path is **single-object** read/write/watch — which
  is the natural shape for locks, leases, sessions, and leader election anyway.
  Those use cases — the exact reason anyone runs a coordination store behind
  Kubernetes — map with **no compromise**, and cask hands them fencing tokens
  etcd-backed leader election cannot.

### Build order

Layer 2 sits over Layer 1's mapping (the resourceVersion/watch/CAS translation
is shared), so build the gRPC profile first to validate the mapping against
`clientv3` directly, then the extension server as a thin registry/storage
adapter over the same translation. Both gated by the simulator + a Jepsen run
against the surface itself.

---

## 4. What we will not do

- **Back the kube-apiserver via cask-as-etcd.** §3 above. The semantic gap is
  structural, not a missing feature.
- **A "v3 API" that claims global-revision fidelity.** Half-fit that mis-sells
  (roadmap §9).
- **Multi-key transactions to satisfy a client that wants them.** That is a
  signal to use a different store for that workload, not to bolt `Txn` onto
  cask (roadmap §9).
- **Per-key RBAC to match etcd auth.** Cask's authz is fencing tokens; auth
  hardening is §5.4, not an etcd-role clone.

---

## 5. Status

Design only. Depends on PR #2 (`GetReadVersion` §4.6 for consistent LISTs;
`ErrRangeChanged` §4.1) and benefits from §5.4 wire hardening before either
surface is exposed off localhost. Tracked as roadmap §5.5.
