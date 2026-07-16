# Multi-cluster device fleet on kind

Two kind clusters. One global inventory of devices. **At most one lease per
device across the whole fleet** — enforced by cask's fenced locks
(model-checked: `SingleHolder`, `FenceMonotone` in `tla/Lease.tla`), not by
anything running in Kubernetes.

```
┌─ kind: east ──────────────┐        ┌─ kind: west ──────────────┐
│ kube-apiserver            │        │ kube-apiserver            │
│   └─ APIService ──────┐   │        │   ┌────────── APIService  │
│      fleet.cask.dev   │   │        │   │   fleet.cask.dev      │
│ cask-apiserver ◄──────┘   │        │   └──────► cask-apiserver │
└───────┬───────────────────┘        └───────────────┬───────────┘
        │          shared consensus (docker net)     │
        └────────► cask-1 ── cask-2 ── cask-3 ◄──────┘
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
2. **The race.** Both clusters create a `DeviceClaim` for `gpu-7`. Exactly one
   binds; the other reports `Pending / device leased elsewhere`. Binding IS
   acquiring the device's cask lock — the apiservers contain no
   mutual-exclusion logic of their own.
3. **Handover.** Deleting the winning claim releases the lock; the other
   cluster's claim binds with a **strictly higher fencing token**. The token
   is in `status.fence` on both the claim and the device — it is what a
   workload presents downstream so late writes from a previous holder are
   rejectable.
4. **The zombie.** Scale the holder's apiserver to zero (a paused process, a
   partitioned cluster). Its lease session lapses; the other cluster takes
   over at a higher fence. Wake the zombie: its claim reconciles to `Lost`,
   and its stale device-status writes cannot regress the advertised lease —
   the status writer refuses fence regressions. Compare with
   `coordination.k8s.io` Leases, which have no fencing at all: a paused
   holder that wakes believing it leads can act on stale authority.

## Notes

- The cask fleet runs in static `--peers` mode as three containers on the
  kind docker network; each cluster's apiserver reaches them by container IP
  (pods use cluster DNS, not docker DNS — the script wires the IPs in).
- The APIService is registered with `insecureSkipTLSVerify: true` against
  the apiserver's per-boot self-signed cert (`--self-signed-tls`); real
  deployments want real serving certs and cask's §5.4 wire hardening.
- Claim TTLs in the demo are 15s so the zombie act completes quickly; the
  lease is renewed by the claim's managing apiserver every 2s reconcile tick.
- Each cluster's apiserver is a **single pod** (`replicas: 1`). It is
  stateless — every object and lease lives in cask — so a crash loses
  nothing, and act 4 exploits this: scaling it to 0 is the cleanest way to
  simulate a paused/partitioned holder. Running multiple replicas per
  cluster needs two things the demo doesn't wire up: a unique cask proposer
  `--id` per replica (a StatefulSet ordinal, not a Deployment's fixed
  `__ID__`), and reconcile idempotence across replicas renewing the same
  claim sessions.
- Everything the demo shows also runs as in-process tests
  (`go test ./cmd/cask-apiserver/`): two apiservers sharing one consensus
  group, with a fake clock for deterministic expiry.
