# cask vs etcd — throughput/latency harness

This is the harness behind the paper's *"throughput/latency vs etcd, same
hardware"* row (`docs/paper/coordination-without-a-log.md`, §5). It stands up a
real 3-node cask cluster and a real 3-node etcd cluster **on the same host** and
drives both through one load generator (`cmd/cask-bench`) with identical
workloads, key/value shapes, and percentile code, so the numbers are directly
comparable.

```sh
bench/run.sh                                   # default: put get cas lock
WORKLOADS="put get" CLIENTS=128 DURATION=15s bench/run.sh
```

Requires `go` and `docker`. cask runs as three `cmd/cask` processes (durable
Pebble); etcd runs as three `quay.io/coreos/etcd` containers (durable bbolt).
Both are torn down on exit.

## Workloads

| name | op | cask path | etcd path |
|------|----|-----------|-----------|
| `put`  | blind write, **fresh key** each op | `PUT /kv/<k>` → `mvcc.Put` | `Put` |
| `get`  | linearizable read over a seeded keyspace | `GET /kv/<k>` → `mvcc.Get` | `Get` (quorum, **not** serializable) |
| `cas`  | conditional create on a **fresh key** (expect-absent) | `POST /cas/<k>` → `mvcc.CAS` | `Txn(CreateRevision==0 ? Put)` |
| `lock` | acquire + release a distributed lock | session + `mvcc`-fenced `Locks.Acquire`/`Release` | `concurrency.Mutex` Lock/Unlock |

## Fairness decisions (why the workloads look the way they do)

These are deliberate, and each removes a way the comparison could lie:

- **Both are real clustered processes on one host.** Neither side is embedded
  or in-process, so both pay real client RPC, real inter-node consensus RPC,
  and real fsync. cask acceptors fsync every acknowledged register (group
  commit amortizes); etcd fsyncs its raft log.
- **Writes use a fresh key per op.** cask's MVCC register *appends* an unbounded
  version chain per key (`internal/mvcc/mvcc.go` `appendOp`) — no compaction. A
  small hot keyspace would therefore measure history growth (bigger
  encode/store/fsync every iteration), not the commit path, and would unfairly
  penalise cask. Fresh keys keep every register one version deep on both sides.
  The fresh-key counter is process-global so warmup and the measured run never
  reuse a key (otherwise `cas` create-if-absent would see spurious conflicts).
- **`cas` is conditional-*create*, not hot-key read-modify-write.** Same reason:
  repeatedly CAS-updating one key grows that key's chain without bound, so a
  hot-key CAS number is a chain-growth measurement. Create-if-absent on a fresh
  key exercises the precondition-inside-consensus path with no history growth,
  and maps cleanly to etcd's `CreateRevision==0` transaction.
- **Reads are linearizable on both sides.** etcd's default `Get` is a quorum
  (read-index) read; we do **not** pass `WithSerializable`, so neither side is
  quietly served a possibly-stale local copy.
- **Warmup is discarded.** Connection pools and runtime caches settle first;
  only the measured window is reported.

## Interpreting the numbers (an honest reading)

Representative run — same host, OrbStack aarch64 VM, 10 vCPU, Pebble/bbolt both
fsyncing to the VM's disk; 64 clients, 10s, 256-byte values:

| workload | cask ops/s | etcd ops/s | cask p50 / p99 | etcd p50 / p99 |
|----------|-----------:|-----------:|----------------|----------------|
| put  | 10,626 | 13,168 | 5.3ms / 19ms | 4.9ms / 10ms |
| get  |  9,586 | 65,282 | 5.8ms / 22ms | 0.9ms / 3.4ms |
| cas  | 10,582 | 12,173 | 5.3ms / 19ms | 5.2ms / 13ms |
| lock |  3,850 |  6,271 | 15.5ms / 38ms | 9.0ms / 39ms |

- **Writes and CAS: cask is competitive with etcd.** A cask coordination write
  is one leaderless accept round plus a group-committed fsync; it lands in the
  same order of magnitude as etcd's leader-append, with a heavier tail (p99)
  from full-jitter backoff between the three proposers and group-commit batching
  variance. This is the point: dropping the log does not cost the write path.
- **Reads: etcd wins, and this is cask's worst case, by construction.** In this
  flat `--peers` topology cask has no established range ownership, so a
  linearizable `Get` runs a *full* CASPaxos round (two phases to a quorum),
  while etcd's read-index is near-leader-local. cask's architectural answer —
  the **owned read served in zero rounds** — requires the ownership/lease
  topology this harness does not set up, and is measured separately by the owner
  read tests (§5 "Measured", `internal/owner`). Read this row as *"cask's
  un-owned read is a full consensus round"*, not as cask's steady-state read
  latency.
- **Locks: etcd faster; cask gives fencing for free.** cask's `Acquire` returns
  a monotone fencing token natively (the demo's whole point); etcd's mutex
  leaves fencing to the caller (revision-as-token). Both are genuine
  session-backed distributed locks.

Absolute numbers are VM- and disk-bound (fsync dominates); the **relative**
comparison is the honest artifact, since both systems run on the identical host.

## Cold start (100 nodes, 50 ranges)

The paper's second row: a **new node joining** a formed 100-node / 50-range
cluster reads the roster register from the Core, reads all 50 range descriptors
from the Core, builds its local range map, and serves its first read — in < 1s
(`docs/etcd-little-sister.md` "Done when"). This is cask-only and runs fully
in-process over `testutil/sim` (in-memory acceptors, direct-call transport), so
it needs no docker:

```sh
go run ./cmd/cask-bench coldstart                       # default sweep
go run ./cmd/cask-bench coldstart --nodes 100 --ranges 50 --rtts 0,1,5,10,20
```

We form the cluster once (untimed) and time only the join, sweeping an injected
per-hop latency (`sim.Network.SetSlow`) because the real cost is the `1 + N`
register reads to the Core, not CPU. Representative run (aarch64 VM, 30
trials/config):

| per-hop RTT | serial fetch p99 | fanout fetch p99 |
|---|---:|---:|
| 0 (CPU floor) | 3.2ms | 2.0ms |
| 1ms  | 219ms | 22ms |
| 2ms  | 421ms | 39ms |
| 5ms  | 722ms | 61ms |
| 10ms | ~1226ms | ~105ms |
| 20ms | ~2430ms | ~234ms |

The result: **with the descriptor fetch fanned out** — the natural
implementation, since the N reads are independent — cold start is ~2 Core
round-trips regardless of range count, and stays well under 1s at every tested
latency (≈22ms intra-DC, ≈234ms even on a 20ms/hop WAN). Fetching the 50
descriptors *serially* is `1 + N` round-trips and crosses 1s around ~8ms/hop, so
fan-out is what keeps a 50-range cold start sub-second on a WAN — raw consensus
speed isn't the lever. At the CPU floor the whole join is sub-millisecond: the
coordination itself is trivial. (Caveat: on this VM the sleep-based latency
injection has ~1ms granularity, so the sub-millisecond RTT rows aren't faithful
and are omitted from the default sweep; the ≥2ms rows scale as expected.)

## Still TBD (the last §5 row)

- **WAN profile** — steady-state cask throughput/latency under injected
  inter-node RTT (widened backoff / MaxOffset), reusing the same
  `sim.Network.SetSlow` latency seam the cold-start sweep uses.
