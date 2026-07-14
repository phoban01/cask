# QuePaxa / Cloudflare Meerkat review — comparison and improvement plan

Status: reviewed 2026-07-13. Verdict: **do not migrate to QuePaxa; adopt four of its ideas.**
Detailed implementation plan: `quepaxa-learnings-implementation.md` (adds W0, a
ballot-space safety fix discovered while planning W1, and W6, the paper/blog/k8s story).

## Sources

- Cloudflare, "Introducing Meerkat: an experiment in global consensus" (2026-07-08),
  https://blog.cloudflare.com/meerkat-introduction/ — Larisch, Halley, Leite (Cloudflare Research).
- Tennage et al., "QuePaxa: Escaping the tyranny of timeouts in consensus", SOSP 2023,
  https://bford.info/pub/os/quepaxa/quepaxa.pdf — reference prototype at https://github.com/dedis/quepaxa (Go, BSD-3).

The blog deliberately defers protocol internals, storage, reconfiguration, and performance
numbers to future posts; mechanism-level details below come from the paper. Meerkat is
explicitly experimental, internal-only, and not in production ("first industrial deployment
attempt", up to 50 replicas worldwide in proof-of-concept runs).

## What Meerkat/QuePaxa is

- **Log-based SMR**: a log of slots; each slot decided by one QuePaxa instance. A
  transactional KV store and a leasing system are planned as layers on top.
- **Roles**: every replica runs an active *proposer* and a passive *recorder* (an Interval
  Summary Register — a threshold logical clock that max-aggregates ⟨priority, proposer,
  value⟩ triples; constant space, one `record(s,v)` RPC).
- **Rounds of 4 phases**, each phase one RTT to a majority of recorders. Random priorities
  mean each round decides with probability ≥ ½ under full asynchrony — expected < 2 rounds,
  no timeouts, no view change, no backoff.
- **Fast path**: a designated leader proposes at reserved max priority in round 1 and decides
  in **1 RTT**. Non-leader drive costs **3 RTTs**. The leader is a performance optimization
  only — never required for liveness.
- **Hedging instead of timeouts**: proposers activate on a delay schedule (leader at 0, then
  δ, 2δ, …), skipping activation if the step already completed. A misconfigured δ costs
  performance, never liveness. A multi-armed bandit re-ranks the hedging order per epoch.
- **Motivation at Cloudflare**: leader-unavailability incidents in Raft-style systems, and
  WAN timeout tuning ("when the timeout is shorter than the network delay… replicas will
  constantly be timing out and thus blocking writes").
- **Gaps**: the paper's prototype does not implement reconfiguration; the blog covers neither
  storage nor membership; nothing is open-sourced by Cloudflare. Validation is deterministic
  simulation + formal verification of parts of the Rust code (details promised in future posts).

## Comparison with cask

| Dimension | cask (CASPaxos) | QuePaxa/Meerkat |
|---|---|---|
| Replication unit | Independent per-key registers, no log | Global SMR log of slots |
| Leader | None; any proposer drives any key | None *required*; designated leader is the fast path |
| Fast-path write | 1 RTT via epoch-fenced `OwnedProposer` (primitive exists; not yet on prod path) | 1 RTT via leader + decision broadcast |
| Slow-path write | 2 RTTs (prepare + accept) | 3 RTTs (phases 0–2) + broadcast |
| Linearizable read | 2 RTTs today (full round); owner-cached 0-RTT planned | Same as a write; stale local reads offered |
| Liveness under contention | Ballot bumping + retry budget; livelock managed by convention (single roster driver) | Randomized priorities: expected < 2 rounds, guaranteed probabilistic termination |
| Failover | No election; new owner fences via higher epoch, immediately | No election; hedging schedule activates next proposer |
| Multi-key transactions | Explicitly out of scope (no log to anchor them) | Natural (log order); Meerkat plans "general transactions" |
| Reconfiguration | Joint consensus, model-checked (TLA+), reflexive roster | Not implemented in paper; not described by Cloudflare |
| Core size | ~615 LOC, pure, sim-gated | Paper prototype 4,368 LOC; ISR + 4-phase rounds + bandit scheduler |

## Migration verdict: no

"Leaderless" is the attraction, but **cask is already leaderless in the sense that matters**:
there is no election, no failover pause, and any proposer can drive any key. The tyranny of
timeouts QuePaxa escapes is a *leader-election-timeout* tyranny — a Raft/Multi-Paxos disease
cask never contracted. Ownership in cask is an optimization exactly the way QuePaxa's leader
is: losing the owner never blocks a key; the next writer fences it with a higher epoch.

Concretely, against the guiding principles:

- **Simple**: QuePaxa would replace a 615-LOC pure core (plus TLA+ specs, sim invariants
  S1–S11, and a CASPaxos-shaped joint-consensus reconfig story that already handles the
  reflexive roster) with a log, an ISR, 4-phase rounds, a hedging scheduler, and a
  reconfiguration design the paper itself defers. The mvcc exactly-once OpID scheme, the
  roster register, and range-descriptor design C' would all need rework.
- **Fast**: for cask's workload the numbers favor CASPaxos. QuePaxa's 1-RTT fast path is
  matched by `OwnedProposer`; its non-leader path (3 RTTs) is *worse* than cask's slow path
  (2 RTTs). QuePaxa's throughput wins appear under WAN asymmetry, DoS, and adversarial delay
  at 330-datacenter scale — not cask's design center (small values, RF=3 ranges, fleet LAN/
  regional latencies).
- **Scalable**: cask scales writes by sharding keys across ranges, not by pushing one log
  harder. A single global log is the thing cask deliberately does not have; adopting one to
  get leaderlessness we already have would be backwards.
- A log would also reopen the door to multi-key transactions — explicitly out of scope
  ("CASPaxos has no global log to anchor multi-key commit to. We will not pretend otherwise").
  Fencing tokens remain the cross-key story.

Revisit trigger: only if cask's scope ever changes to require a totally-ordered log or
general transactions — at which point compare QuePaxa against Raft directly, not against
CASPaxos.

## What we should steal

The real lesson of QuePaxa is a design stance: **mechanisms whose misconfiguration costs
performance, never liveness**. Cask violates that stance in three places today. Workstreams
in priority order:

### W1 — Wire `OwnedProposer` into the production KV/lease path

The 1-RTT fast path exists (`internal/caspaxos/owned.go`) but is only used by a simulator
fault. `mvcc.KV` and `lease` go through full 2-RTT `Propose`. Meerkat's headline latency
story is exactly this fast path; ours is built and idle.

- Plumb an ownership-aware proposer behind the existing `Proposer` interface seam
  (`mvcc.go:100-106`) so mvcc/lease/roster are untouched.
- Epoch source: the ownership lease fencing token, as designed.
- Extend sim: raise `owned_proposer_force_full_round` coverage; add an invariant that a
  fenced owner's write is never visible (S11 already close).

### W2 — Parallel fan-out, early-quorum return, and hedged RPCs

Proposer RPCs are sequential (`proposer.go:151-198`); a dead peer costs the full 4 s
overlay RPC timeout **per phase**. This is cask's own small tyranny of timeouts.

- Fan out prepare/accept concurrently; return on quorum (roadmap §3.6, promoted).
- Hedge the tail QuePaxa-style: send to a quorum immediately, hedge the remaining replicas
  after δ (δ = observed p99 per-peer latency). A bad δ costs duplicate messages, not
  correctness — acceptors are idempotent and the sim already injects `duplicate_delivery`.
- Keep the pure core synchronous-per-call; concurrency lives in the `AcceptorClient`
  fan-out layer.

### W3 — Randomized backoff on preemption (kill livelock-by-convention)

The core handles dueling proposers by ballot bumping with a retry budget; the bootstrap
livelock was fixed by *convention* (single driver = highest core ID, adopt-don't-read).
Conventions don't compose; QuePaxa's randomization guarantees termination instead.

- Add jittered randomized backoff between `Propose` rounds in the impure command layer
  (core stays pure — same placement as the existing reconfigure retry jitter).
- Add a sim fault `dueling_proposers` (N writers, one key, no driver convention) and a
  liveness check: expected rounds-to-decide stays bounded. This empirically proves the
  property QuePaxa gets by construction, on the conditions where it shines.
- Keep the single-driver rule for the roster (it also reduces message load), but it stops
  being load-bearing for liveness.

### W4 — Owner-cached / local reads (roadmap §3.1, reprioritized)

Meerkat offers stale-but-consistent local reads; cask's linearizable read is a full
2-RTT write-imposing round — which is also what made startup reads duel the bootstrap
seeder. Owner-cached reads give 0-RTT linearizable reads under ownership; a documented
stale-read mode from any replica's accepted register value covers the rest.

### W5 — Validation posture (confirm, and extend where Meerkat is ahead)

Cloudflare's bar for Meerkat is deterministic simulation + formal verification — cask's
confidence ladder (TLA+ → sim gate → Jepsen → burn-in) already matches or exceeds it.
Two additions prompted by this review:

- An **asymmetric-latency / contention profile** in the sim (the regime QuePaxa targets):
  slow links + concurrent proposers + no healing dwell, gating W2/W3.
- Watch the promised Meerkat follow-up posts (QuePaxa internals, DST, Rust formal
  verification, bootstrapping, replica placement) — the bootstrapping and placement posts
  bear directly on cask's roster and HRW placement.

### Deferred / rejected

- **Bandit-based owner placement** (QuePaxa's multi-armed bandit hedging order): latency-aware
  choice of which node takes ownership per range. Real but premature — revisit after W1 ships
  and we have ownership telemetry.
- **Migrating any register to QuePaxa rounds**: a mutable register under QuePaxa is a per-key
  slot log — strictly more machinery for a worse non-owner path. Rejected.
