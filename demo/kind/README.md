# Multi-cluster device fleet on kind

Three kind clusters. One global inventory of devices. **At most one lease per
device across the whole fleet** — enforced by cask's fenced locks
(model-checked: `SingleHolder`, `FenceMonotone` in `quint/lease.qnt`), not by
anything running in Kubernetes.

cask is **embedded**: each cluster's extension apiserver carries its own cask
acceptor, and the three apiservers form the consensus group among themselves.
There is no external coordination infrastructure anywhere in this demo — no
etcd for the fleet state, no cask cluster to operate. The API extension
servers *are* the fleet.

```
┌─ kind: east ─────────────┐ ┌─ kind: west ─────────────┐ ┌─ kind: north ────────────┐
│ kube-apiserver           │ │ kube-apiserver           │ │ kube-apiserver           │
│   │ APIService           │ │   │ APIService           │ │   │ APIService           │
│   ▼ fleet.cask.dev       │ │   ▼ fleet.cask.dev       │ │   ▼ fleet.cask.dev       │
│ cask-apiserver           │ │ cask-apiserver           │ │ cask-apiserver           │
│   [embedded cask node]◄──┼─┼──►[embedded cask node]◄──┼─┼──►[embedded cask node]   │
└──────────────────────────┘ └──────────────────────────┘ └──────────────────────────┘
                 consensus over the shared kind docker network
```

## Run it

```sh
demo/kind/demo.sh       # needs docker, kind, kubectl
demo/kind/teardown.sh
```

The script walks four acts:

1. **One fleet, one truth.** `kubectl --context kind-east create device gpu-7`
   → `kubectl --context kind-west get devices gpu-7` returns the same object,
   same `resourceVersion` (which is cask's per-key MVCC sequence — k8s
   optimistic concurrency *is* cask CAS).
2. **The race.** Two clusters create a `DeviceClaim` for `gpu-7`. Exactly one
   binds; the other reports `Pending / device leased elsewhere`. Binding IS
   acquiring the device's cask lock — the apiservers contain no
   mutual-exclusion logic of their own.
3. **Handover.** Deleting the winning claim releases the lock; the other
   cluster's claim binds with a **strictly higher fencing token**. The token
   is in `status.fence` on both the claim and the device — it is what a
   workload presents downstream so late writes from a previous holder are
   rejectable.
4. **The zombie.** Scale the holder's apiserver to zero (a paused process, a
   partitioned cluster). That kills one of the three consensus nodes too —
   the remaining two are a quorum, so the fleet keeps committing. The
   holder's lease session lapses; another cluster takes over at a higher
   fence. Wake the zombie: it rejoins consensus from its durable acceptor
   state, its claim reconciles to `Lost`, and its stale device-status writes
   cannot regress the advertised lease — the status writer refuses fence
   regressions. Compare with `coordination.k8s.io` Leases, which have no
   fencing at all: a paused holder that wakes believing it leads can act on
   stale authority.

## Notes

- **Embedded topology.** `--listen-consensus` makes the apiserver serve its
  own acceptor; `--cask-peers` lists all three apiservers' consensus
  addresses, and the entry matching `--advertise-consensus` stays in-process.
  The same binary also runs proposer-only against an external cask fleet
  (`--cask-peers` without `--listen-consensus`) or as a process-local
  single node (no flags) for dev.
- **hostNetwork.** Pod networks aren't routable between kind clusters, but
  the kind nodes share the `kind` docker network — so consensus rides the
  node IPs, which the script wires into `__CASK_PEERS__` and each pod learns
  its own via the downward API.
- **Mutual TLS on consensus.** The script runs
  `cask-apiserver gen-consensus-certs` in the demo image. It makes a fleet
  CA and one certificate per cluster in `demo/kind/.consensus-certs`. Each
  apiserver gets its certificate in the `cask-consensus-tls` Secret and
  starts with `--consensus-cert`, `--consensus-key`, and `--consensus-ca`.
  The consensus port then accepts only peers with a certificate that the
  fleet CA signed. `teardown.sh` deletes the files.
- **Durability.** Each embedded acceptor persists to Pebble on the
  StatefulSet's PersistentVolumeClaim (`--data-dir`). This is not optional
  hygiene: an acceptor that restarts empty forgets its promises, which is
  unsafe for consensus — and the zombie act restarts one on purpose.
- **Disruption budget.** A PodDisruptionBudget with `maxUnavailable: 1`
  lets a drain take at most one voter per cluster. It does not coordinate
  across clusters. Upgrade one cluster at a time, as
  [docs/runbooks/rolling-upgrade.md](../../docs/runbooks/rolling-upgrade.md)
  describes.
- **Delegated auth.** The apiserver is a generic server from
  `k8s.io/apiserver`. The kube-apiserver of each cluster authenticates and
  authorizes every request for the group: the apiserver asks it with
  TokenReview and SubjectAccessReview. The `cask-apiserver`
  ServiceAccount is bound to `system:auth-delegator` and may read the
  `extension-apiserver-authentication` ConfigMap.
- The APIService is registered with `insecureSkipTLSVerify: true`. With
  no `--tls-cert-file`, the apiserver makes a self-signed certificate in
  memory at each start. Issue #41 replaces it with a certificate that the
  kube-apiserver can verify.
- Claim TTLs in the demo are 15s so the zombie act completes quickly; the
  lease is renewed by the claim's managing apiserver every 2s reconcile tick.
- Everything the demo shows also runs as in-process tests
  (`go -C cmd/cask-apiserver test ./...`): two apiservers sharing one consensus
  group, with a fake clock for deterministic expiry.
