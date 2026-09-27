# cask

A two-tier, leaderless, MVCC coordination store on **CASPaxos** — for leases/locks
on global resources and small metadata, with a modern no-config UX, designed to
scale to a fleet of 10,000+ participating nodes.

The design and rationale live in-repo:

- [`docs/etcd-little-sister.md`](docs/etcd-little-sister.md) — the canonical
  performance + scale roadmap, the decided designs (range descriptors C',
  cross-range consistency A), and the §6.5 safety-invariant contract.
- [`docs/confidence.md`](docs/confidence.md) — how cask earns trust (TLA+,
  deterministic simulation, Jepsen, burn-in) and where it sits on its own
  trust ladder today (**demo**).
- [`docs/sim-gate.md`](docs/sim-gate.md) — the simulator release gate: what it
  proves, what it defers, how to add faults/invariants.
- [`docs/integration.md`](docs/integration.md) — etcd-compatible coordination
  profile + Kubernetes aggregated API server: what maps, what doesn't, and why.
- [`docs/plans/`](docs/plans/) — the vendored source plans (master
  architecture, multi-cloud demo, control-plane shape) that the roadmap's §0
  builds on.

## Status

Early implementation. One caveat applies to every durability claim below:
the only `Storage` backend today is **in-memory** (`internal/store/mem.go`),
so "crash/restart" coverage is against the simulated crash model — durable
acceptor storage (Pebble) is the gating roadmap item
(`docs/etcd-little-sister.md` §3.0). Built so far:

- **M0 — safety specs** (`tla/`): `CasPaxosMvcc.tla` (consensus agreement) and
  `Lease.tla` (single-holder + fencing monotonicity). Spec-first; require TLC to
  model-check (see `tla/README.md`).
- **M1 — range core + MVCC** (`internal/`): the pure, deterministic CASPaxos core
  (`caspaxos`), a hybrid logical clock (`hlc`), per-key MVCC version chains with
  snapshot/time-travel reads (`mvcc`), and an in-memory acceptor store (`store`).
  Verified with unit tests and a model-based property test (`pgregory.net/rapid`),
  race-clean.
- **M2 — multi-node range group**: a deterministic fault-injecting network
  (`testutil/sim`), HRW owner/replica selection (`placement`), an interim
  HTTP/JSON acceptor transport (`transport`, real multi-process; ConnectRPC once
  `buf` is available), and a **porcupine linearizability** test plus crash/restart
  durability under a partition+crash nemesis (`test/linearizability`). This gate
  caught three real bugs — non-atomic per-key read-modify-write, a non-linearizable
  CAS abort, and non-exactly-once MVCC appends under proposer retries — all fixed.
- **M3 — two tiers + range routing**: a range-sharded ordered keyspace
  (`ranges`: descriptors, ordered map, static builder, HRW owner) and an agent
  `Router` (`agent`) that routes each key to its range's replica group — so one
  `mvcc.KV` over a Router spans the whole keyspace. Adds multi-range
  `SnapshotRead` at an HLC timestamp. The proposer is now concurrency-safe.
  Verified: routing correctness, cross-range point-in-time snapshots, and
  concurrent multi-key linearizability under a nemesis (per-key porcupine).

- **M4.1 — HyParView overlay** (`membership`): an in-house, pure/deterministic
  partial-view membership protocol (active view ~5, passive ~30 → O(log N) state
  per node) with join/forward-join, a symmetric NEIGHBOR handshake,
  disconnect, shuffle, and failure-driven healing. A deterministic simulator
  proves the overlay forms a single connected component (sizes 5→120), shuffle
  populates passive views, and the overlay **self-heals to one component after a
  30% mass failure**.

- **M4.2 — Plumtree broadcast** (`membership`): epidemic broadcast trees over the
  overlay — eager-push payloads along tree edges, lazy `IHave` advertisements,
  `Graft`/`Prune` to heal and thin the tree. Tests prove reliable broadcast to all
  nodes, pruning down to ~a spanning tree, and **graft-repair** restoring delivery
  after interior nodes fail.

- **M4.3 — failure detection** (`failure`): a phi-accrual detector (heartbeat
  timing → a continuous suspicion level) and a Rapid-style multi-observer cut
  detector that declares a node down only once several independent observers
  agree — suppressing the false positives a single flaky monitor would cause, so
  the roster stays stable. Both are deterministic (time passed in, no wall clock).

- **M4.4 — consensus roster** (`roster`): the authoritative membership set held
  in a CASPaxos register (genesis bootstrap, idempotent add/remove, epoch bumps),
  which is what HRW placement is computed over — deliberately separate from the
  flapping gossip view. A controller reconciles the roster from discovery
  (seed/DNS-SRV/mDNS behind one interface) and from the cut detector's
  threshold-crossed decisions, so membership changes only on stable, committed
  evidence. Tested over the sim network, including the stable-until-cut-decides
  property.

- **M5 — elastic placement** (spec-first): `tla/Reconfig.tla` model-checks that
  joint-consensus reconfiguration with catch-up-before-release loses no committed
  value. The proposer now supports **joint quorums** (a majority required in every
  config group); `reconfig` carries a range old→joint→new with no value lost,
  verified in simulation including under node churn. The `placement` driver adds
  **failure-domain-aware** replica selection and **hysteresis** (heal on dead
  replicas / under-replication / better zone spread, but don't thrash on cosmetic
  churn); `ranges` gains **split/merge**.

- **M6 — Watch** (`watch` + `mvcc`): the version chain is exposed as a resumable
  change feed. `KeyWatcher` streams a key's versions after a cursor and resumes
  from a past revision; `PrefixWatcher` merges a prefix's keys into one
  HLC-ordered stream. History is **bounded** (`mvcc.Compact` + `CompactedBelow`),
  resuming below the watermark yields **`ErrCompacted`** (re-list, etcd-style),
  and `SafeCompactPoint` makes GC **watcher-aware** (never strands a live
  watcher). `FanOut` multiplexes one upstream subscription to many client
  watchers — verified O(agents), not O(clients) (one storage read per pump).

- **M7 — leases & locks** (`lease`): the headline coordination primitive,
  session-gated (Consul-style). A client holds one **session** (TTL, heartbeated)
  and its **locks** bind to it, so renewing one session keeps many locks alive —
  **O(agents) keepalive, not O(locks)** (verified). Every acquire mints a strictly
  monotonic **fencing token**; a **reaper** (soft-leader elected via a lock)
  cascade-frees a dead session's locks, while lazy expiry guarantees an expired
  lock is never observed held. Tests cover single-holder, fence monotonicity,
  **clock skew** (fencing is the source of truth when wall clocks disagree),
  batched keepalive, reaper cascade, and **fence monotonicity across a range
  reconfiguration** — matching the model-checked `tla/Lease.tla` invariants.
- **M7.2 — 1-RTT owner fast path** (`caspaxos.OwnedProposer`): the phase-1 skip
  deferred from M2, now safe. The ownership **epoch (a lease fence) is encoded in
  the ballot's high bits**, so a newer owner's ballots dominate every ballot an
  old owner can mint — a stale owner's accepts are always NACKed (returns
  `ErrLostOwnership`, never a lost update). After one phase-1 round to take
  ownership, writes commit in a single accept round.

- **M8 — scale, Jepsen gate, runnable binary**: a simulation **scale gate**
  drives the overlay to 1000 and 3000 nodes and confirms per-node state stays
  bounded (active view avg ~4.9 / max 5, passive max 30 — **constant regardless of
  N**, the O(log N) property behind 10k) while staying fully connected. An
  in-process **Jepsen-style lock-fencing gate** (`test/jepsen`) verifies fencing
  monotonicity + token uniqueness under a partition nemesis (the release gate;
  a Clojure out-of-process harness is scaffolded under `jepsen/`). And **`cmd/cask`**
  is a runnable single binary — a real 3-node cluster does cross-node KV, CAS, and
  fenced locks over HTTP.

## Running

```sh
devbox run build                      # or: go build -o bin/cask ./cmd/cask

# a 3-node cluster
bin/cask --id 1 --listen :8001 --peers :8001,:8002,:8003 &
bin/cask --id 2 --listen :8002 --peers :8001,:8002,:8003 &
bin/cask --id 3 --listen :8003 --peers :8001,:8002,:8003 &

curl -XPUT  localhost:8001/kv/greeting -d 'hello'     # write via node 1
curl        localhost:8002/kv/greeting                # read via node 2 -> hello
curl -XPOST 'localhost:8003/cas/greeting?expect=hello' -d 'world'
curl -XPOST 'localhost:8001/session/me?ttl=30'
curl -XPOST 'localhost:8001/lock/widget?session=me'   # -> {"token":1}
```

This is the **static-membership** path. Inter-node consensus rides
**ConnectRPC** (protobuf) by default; `--transport http` selects the interim
JSON path. The client KV/lock API stays HTTP/JSON.

> **Security posture (read before exposing a port).** On this static-TCP path
> consensus is **plaintext and unauthenticated** — anyone who can inject traffic
> between nodes can forge Prepare/Accept replies and break consensus safety. It
> is for local/dev/CI and trusted-network use only. For any real deployment use
> the Nebula overlay below, where consensus rides an encrypted, mutually
> cert-authenticated mesh. The **client API** (`/kv`, `/lock`, `/session`) and
> the control endpoints (`/health`, `/roster`) are unauthenticated everywhere
> today — cask has no auth layer yet (a deliberate current limitation, see
> `docs/confidence.md`), so bind them to localhost or the overlay, never a
> public interface. Wire hardening (TLS-or-remove on the TCP path; client-API
> auth) is tracked as a roadmap item.

### Self-forming over a Nebula overlay

Instead of `--peers`, a node can join over an encrypted [Nebula](https://github.com/slackhq/nebula)
mesh — embeddable in userspace (no TUN, no root), so the same single binary forms
a cluster across clouds/NAT:

```sh
# each node points at a standard Nebula config (CA + node cert + lighthouse hosts)
bin/cask --nebula-config nebula.yml --overlay-port 8001 --listen 127.0.0.1:8080
```

The node derives its identity from its overlay IP and zone from its cert's
`zone:<name>` group, seeds the **roster** from its lighthouse hosts, installs
membership by consensus, and routes a placement-driven range over the overlay. A
background loop reconciles the roster with discovery and re-places the range when
membership changes. Consensus rides the overlay (`--overlay-port`); the client API
is served locally (`--listen`) so you can still `curl localhost`.

Regenerate the wire stubs after editing `proto/`:

```sh
buf generate
```

## Layout

```
internal/caspaxos   pure CASPaxos: ballots, register, acceptor, proposer, change funcs
internal/hlc        hybrid logical clock
internal/mvcc       version chains + snapshot/time-travel over a register
internal/store      acceptor storage (in-memory now; pebble later)
internal/transport  ConnectRPC consensus transport + the Network seam (TCP / overlay)
internal/transport/nebula  optional Nebula-overlay Network backend + lighthouse discovery
internal/roster     consensus membership register; internal/placement zone-aware HRW
proto/, gen/        protobuf schema and generated ConnectRPC stubs (buf)
tla/                TLA+ safety specifications
```

The consensus, clock, and MVCC cores are deliberately **pure** (no I/O, net, or
wall-clock) so the whole cluster can later run under a deterministic simulator.

## Develop

```sh
devbox run build    # go build ./... in both modules
devbox run test     # go test -race ./... in both modules
go test -race ./...
go -C cmd/cask-apiserver test -race ./...
```

`cmd/cask-apiserver` is its own Go module. It holds the Kubernetes
libraries, so the core module does not depend on them. Its `go.mod` has a
`replace` to the repo root, so it always builds against the core in this
tree. A root `go test ./...` does not reach it. Use `go -C` to test it.
