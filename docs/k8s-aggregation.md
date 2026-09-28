# Multi-cluster CRDs via a cask-backed aggregated API server

*Design doc (W6). Extends roadmap §5.5 (etcd-compatible API + Kubernetes
integration) with the specific shape we will build first: a Kubernetes
**API extension (aggregated) server** — not CRDs stored per-cluster in etcd —
whose storage backend is cask, so one fleet-scoped resource is visible and
strongly consistent from every member cluster.*

## Why an aggregated API server (and not CRDs)

A CRD's objects live in the host cluster's etcd: three clusters means three
disjoint copies with no consistency story. An aggregated APIService instead
delegates a whole API group to our own server, which can back objects with
whatever storage it likes — here, a cask fleet spanning the clusters. Every
cluster registers the same APIService; `kubectl get clusterclaims` anywhere
in the fleet reads the same linearizable state.

## The storage mapping

The k8s apiserver machinery talks to storage through
`k8s.io/apiserver/pkg/storage.Interface`. The mapping onto cask is almost
embarrassingly direct, because k8s's data model is itself per-object
registers with optimistic concurrency — there is no cross-object
transaction in the storage interface at all:

| storage.Interface | cask | Notes |
|---|---|---|
| `Create(key, obj)` | `mvcc.CAS(key, expected=absent)` | absent-guard = create-only semantics |
| `GuaranteedUpdate(key, precond, tryUpdate)` | read → `mvcc.CAS(key, expected=current)` retry loop | k8s optimistic concurrency **is** CAS; conflicts map to `Conflict` errors the machinery already retries |
| `Delete(key, precond)` | `mvcc.CAS` to tombstone / `Delete` | preconditions on UID/RV check the head version |
| `Get(key)` | `mvcc.Get` | 0-RTT on the owner (W4), 1-RTT degraded, 2-RTT anywhere |
| `GetList(prefix)` | range scan over ordered keyspace | prefix locality is why ranges are ordered, not hashed |
| `Watch(key/prefix, fromRV)` | `mvcc.History` cursors / `watch.KeyWatcher` | Plumtree push (§3.5) upgrades poll → push later |
| `Count(prefix)` | scan count | cheap at coordination-store scale |
| `Versioner` (resourceVersion) | **index sequence of the object's index entry** | see below (issue #150) |

**Decided (issue #150): resourceVersion = index sequence.** Every
resourceVersion a client sees is a sequence of the resource type's index
register. Each index entry records the object sequence and the index
sequence at which the index recorded it. An update checks the client's
resourceVersion against the entry, then compares and sets on the object
sequence. A reflector can then resume a watch from any event. The rules
are in `docs/spec/fleet.md` sections 3 and 4. The paragraph below is the
earlier design sketch.

**resourceVersion = per-key Seq.** k8s semantics require: RV monotonic per
object; watch-from-RV per object; list RVs usable for "not older than"
reads. Per-object optimistic concurrency needs no global revision — cask's
per-key Seq satisfies conflict detection exactly. For LIST+WATCH-from-list
across a prefix, we return a composite cursor (per-key seq map compressed,
or an HLC watermark with the documented §4.4 uncertainty contract — lists
across objects are already not point-in-time in practice, and the apiserver
machinery tolerates `TooLargeResourceVersion`-style renegotiation).
Snapshot-timestamp reads obey the TLC-derived rule: t must not exceed the
range's applied HLC (the GetReadVersion rule).

**Key layout.** `/<group>/<resource>/<namespace>/<name>` under the client
keyspace — ordered, so a resource's objects live in one or a few adjacent
ranges and lists/watches are range-local.

## What the fleet gets that etcd can't give it

1. **One control plane across clusters** without running stretched etcd
   (whose quorum latency and operational blast radius across WAN links is
   exactly what §5.5 warns about) — cask's ranges place replicas
   zone/cluster-aware, reads serve from range owners at 0-RTT, and the
   overlay (Nebula) already crosses NATs and clouds.
2. **Fencing-token controllers.** k8s leader election (`coordination.k8s.io`
   Leases) is wall-clock leasing WITHOUT fencing — a paused controller can
   wake up believing it leads and act on stale authority. Controllers
   coordinating fleet-scoped resources instead take cask locks: every
   acquisition mints a monotonic fencing token that downstream effects
   carry, so a zombie's writes are rejectable at the resource. This is the
   headline demo: kill/pause the leading controller mid-reconcile, show the
   successor's token fencing the zombie's late writes.

## Prototype plan (cmd/cask-apiserver)

1. `storage.Interface` implementation over `mvcc.KV` + `lease` (~the table
   above), with the composite-RV cursor.
2. One demo resource type (`ClusterClaim`) served through the standard
   `genericapiserver` scaffolding; APIService registration manifests for
   each member cluster.
3. Demo (rides the existing multi-cloud demo infra): 3 clusters, create in
   A, get/watch in B and C; partition A; show the fenced-controller
   failover.
4. Out of scope for the prototype: admission webhooks, OpenAPI aggregation
   polish, CRD-style structural schemas (we serve one concrete type),
   auth (cask's §5.4 wire hardening gates any real deployment).

Dependency note: `k8s.io/apiserver` is a heavy module tree; the prototype
lives in its own Go module under `cmd/cask-apiserver/` so the core stays
lean.

## Risks / open questions

- Watch semantics under range splits: cursors are per-key, so split is
  transparent to a key watcher; prefix watchers must merge two ranges'
  streams (the watch.FanOut/HLC-merge machinery §3.5 formalizes).
- List-RV semantics: validate against the apiserver machinery's actual
  tolerance (`ResourceVersionMatch` handling) early — this is the only
  place cask's no-global-revision stance meets a k8s assumption head-on.
- Object size: k8s objects run 1–100 KB — within the coordination-store
  envelope, but watch fan-out bandwidth deserves a measurement before the
  demo claims scale.
