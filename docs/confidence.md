# Confidence — how cask earns the right to be trusted

A document about *how* you should trust this thing in production, and
the techniques cask uses to earn that trust. Read this once when
deciding whether to put cask in front of your workload; re-read it
when something breaks.

Cask is small in surface area but distributed systems are merciless,
and confidence is not a feeling you can purchase with intuition. The
field has converged on a small set of techniques that *together* close
most of the gap between "looks right" and "is right." None of them is
sufficient on its own. Cask commits to using all of them.

## The picture, briefly

| Technique | What it does | What it catches | What it does NOT catch |
|---|---|---|---|
| **TLA+** | Model-check protocol designs | Logic bugs, bad state interleavings, composition errors between protocols | Implementation bugs in code, performance, dependency bugs, anything the spec abstracted away |
| **Deterministic simulation testing (DST)**, FDB-style | Run the actual code under a single-threaded simulator with seeded RNG, virtualized I/O, and fault injection | The implementation-vs-spec gap, state interleavings the spec didn't model, regressions (captured seeds) | Bugs in code paths the simulator doesn't exercise, real OS/disk/network quirks, dependency bugs |
| **Jepsen** | Drive *real* clusters under partition, kill, clock-skew nemeses; check histories for linearizability / fencing / SI | Reality-gap bugs: real network reordering, real fsync lies, real GC pauses, real BGP flaps | Bugs that only manifest at scale, operational mistakes, customer-specific config issues |
| **Production burn-in** | Long-running clusters under synthetic + real load | Operational sharp edges, dependency surprises, scale-only behaviors | Future regressions (handled by re-running everything above) |

Each catches a strict superset of bugs the one before it does, **at
strictly higher cost.** The discipline is to use them in order: cheap
techniques find the easy bugs first so the expensive techniques find
only the hard ones.

## The four pillars in detail

### 1. TLA+ — proving the design

TLA+ is a logic for describing concurrent systems and a model checker
(TLC) for asking "across every reachable state of this system, does
property P hold?" Cask uses it for the few protocols where a logic bug
would be catastrophic and hard to spot otherwise.

What's already written (see `tla/README.md`):

- `tla/CasPaxosMvcc.tla` — per-register Paxos agreement (S1).
- `tla/Lease.tla` — single-holder + fencing monotonicity (S6, S7).
- `tla/Reconfig.tla` — joint-consensus carry-forward (S4, S5).
- `tla/RangeDescriptors.tla` — the four-step split protocol (S8, S9).
- `tla/CrossRange.tla` — HLC uncertainty contract (S10).

Each invariant has a number in `docs/etcd-little-sister.md` §6.5.

**What TLA+ catches that nothing else does:** state-interleaving bugs.
Paxos has decades of literature about "I thought this was safe but
when these three messages reorder…" — TLC finds those mechanically.
The S8 invariant in `RangeDescriptors.tla` is precisely the kind of
thing humans miss: "what if a client with a stale rmap writes after
cutover but before tombstone?" TLC enumerates the state space and
either finds a violation or proves there isn't one.

**What TLA+ does NOT catch:**

- **Spec-vs-code drift.** The spec says "ballots are totally ordered."
  The Go code can still have an integer overflow that breaks the order
  in practice. TLA+ doesn't know your Go code exists.
- **What you didn't model.** A spec models "a network." It usually
  doesn't model TCP half-closed connections, DNS resolution races,
  IPv4-mapped-IPv6 mismatches, or hostname re-resolution intervals.
  When production breaks on something you didn't model, the spec is
  silent.
- **Performance.** TLC tells you no reachable state violates the
  invariant; it says nothing about tail latency, GC pauses, or
  saturated network cards.

The honest rule: TLA+ proves your *design* is right, conditioned on
the abstractions you chose. The simulator's job is to close the gap
between the design and the code; Jepsen's job is to close the gap
between the model and reality.

### 2. Deterministic simulation testing — proving the code

This is FoundationDB's invention, and the largest single source of
confidence in the modern distributed-systems toolbox. Will Wilson's
2014 Strange Loop talk is the canonical text; TigerBeetle's `vopr/` is
the cleanest open-source example.

The shape:

- **Single process, single thread.** The entire cluster — N nodes,
  network, disks, clocks — runs in one Go goroutine with cooperative
  scheduling. No real I/O, no real network, no kernel scheduling.
- **Seeded RNG.** Every random decision (which message delivers first,
  which disk write is slow, which clock ticks) is from one seeded
  `*rand.Rand`. Same seed = bit-identical replay, forever.
- **Virtualized I/O.** Clock, Transport, Storage are interfaces. The
  simulator provides implementations that obey the seed; production
  provides real ones.
- **`BUGGIFY` sites (`internal/buggify`, planned §6.6.1).** Every
  spot in protocol code where "this should be fine" is annotated with
  a `buggify.Maybe("name", prob)` call. In production it's a single
  nil-check no-op; in simulation it's a probabilistic branch into a
  cruel alternative. **This is the FDB secret sauce.** Engineers write
  the optimistic path *and* the pessimistic path and force the
  simulator to take the pessimistic one a fraction of the time.

What cask already has:

- Pure protocol cores (`internal/caspaxos`, `internal/mvcc`,
  `internal/membership`, `internal/failure`, `internal/lease`,
  `internal/reconfig`) — Clock/Storage/Transport injected.
- `testutil/sim` — the simulator scaffold.
- `pgregory.net/rapid` for property-based testing inside it.

What cask must add (PR #0 in the roadmap):

- `internal/buggify` — the `Maybe` primitive (§6.6.1).
- `testutil/sim/gate.go` — the seed-budget runner.
- `testutil/sim/invariants.go` — the enumerable §6.5 invariant
  predicates evaluated at every step.
- `testutil/sim/regression.go` — the captured-seed regression set.
- `testutil/sim/faults/` — the named fault catalog (§6.7).
- CI workflow that runs the `smoke` + `consensus` profiles on every PR.

**What DST catches that TLA+ doesn't:**

- **Implementation bugs.** A Promise that doesn't actually persist
  before returning. A retry that double-applies. A channel that drops
  on a full select. The simulator runs the real code; if the code is
  wrong, the simulator can catch it.
- **State interleavings even the spec didn't think of.** A simulator
  with `BUGGIFY` sites at 200 places explores combinations no human
  enumerates.
- **Regressions.** Once a failing seed is captured, it runs forever.
  Bug found in 2026 stays caught in 2030.

**What DST does NOT catch:**

- **Real-world I/O.** A simulator's `Storage.Store` is in-memory or
  a fake-Pebble with injected delays. Real Pebble has its own quirks
  (LSM compaction stalls, WAL truncation races) the simulator doesn't
  model unless you make it.
- **Bugs in dependencies.** Pebble's fsync semantics, Nebula's relay
  behavior, the Go runtime's GC. The simulator can't catch what it
  doesn't run.
- **Operational mistakes.** A wrong `--max-clock-offset` flag, a
  half-rolled-out config, an operator's `rm -rf`.

The honest rule: DST proves the *code matches the design under the
abstractions you simulated*. It's much stronger than TLA+ alone
because it runs the real code, but it shares one weakness — the
abstraction gap. That's where Jepsen comes in.

#### The discipline that makes DST work (not the code, the discipline)

The code is ~1500 LOC. The discipline is what pays off:

1. **Every protocol PR adds ≥ one `buggify.Maybe` site** at a spot
   where the author thought "this should be fine but I'm not sure."
   PR-review checklist item.
2. **Every bug found anywhere — prod, Jepsen, by a user — adds a
   named fault** in `testutil/sim/faults/` at the code location that
   would have surfaced it. The catalog grows.
3. **No protocol-bearing PR merges without the simulator clean** on
   the configured seed budget against the configured profile.
4. **A pre-release ritual:** run the full fault catalog at 10⁶+ seeds
   for several CPU-days. Any violation blocks the tag.

The FDB benchmark for "we're confident": **millions of CPU-hours** of
simulation before each release. Cask should aim for the same shape if
not the same wall-clock budget — the runs are embarrassingly parallel.

### 3. Jepsen — proving the system

Jepsen is Kyle Kingsbury's framework for testing distributed systems
under real partition, kill, and clock-skew nemeses. It runs against
**real clusters on real OSes with real networks** and checks recorded
histories against linearizability, snapshot isolation, or custom
properties (Kleppmann fencing for our case).

What Jepsen catches that the simulator can't:

- **Real network reordering.** Linux's net stack does things no
  fake-network models exactly: SACKs, RTO backoffs, MTU black-holes,
  asymmetric NAT keepalive expiry.
- **Real disk lies.** Some disks report fsync success and lose data
  on power-cut. Some Linux kernel versions have ext4 quirks. Real
  Pebble built on real ext4 on real disks has surprises a fake
  in-memory Pebble cannot.
- **Real GC pauses.** A 200ms Go GC pause during a 500ms lease window
  is the kind of thing only real running clusters expose.
- **Real DNS, real time.** Lighthouse hostname re-resolution; NTP
  step events; leap seconds.
- **Real cross-cloud weirdness.** GCP and AWS have different default
  TCP keepalive intervals. Their NAT tables age out at different
  rates. Real packets between them have real latency distributions.

The cask scaffold lives at `jepsen/` (currently just a README); the
in-process Go simulator-style fencing test lives at
`test/jepsen/fencing_test.go`. The real Clojure Jepsen suite needs
~1 week of work to wire up, and the master plan names the
**partition+churn lock-fencing run as the v1 release gate.**

Workloads cask should run:

- **lin-kv** — `Put`/`Get`/`CAS` over the `/kv` HTTP API, checked
  with Knossos (linearizable register).
- **lock + fencing** — many clients `POST /lock/<name>`, custom
  checker for "no two hold simultaneously" + "tokens monotonic with
  grant order." **The release gate.**
- **set / append** — Elle, for the cross-range snapshot story (§4.4
  + §4.6 GRV).

Nemeses to run against each:

- `partition` (split the storage tier; majority must remain).
- `clock-skew` (stresses HLC and lease expiry).
- `kill/pause` (durability under crash).
- `membership churn` (add/remove nodes mid-test — exercises §4.3 and
  the master plan's M5 reconfig).

Each combination produces a `results.edn` + `timeline.html`. Failing
runs ship a minimal counterexample subhistory. Read every counter-
example carefully: those are the bugs no other technique would have
caught.

**The classic Jepsen finding shape:** "Under partition + clock-skew,
client A read a value with version V, client B wrote V+1, client A
later read V again — non-monotonic." That kind of thing only emerges
when real clocks lie to each other across real partitions; no
simulator finds it because no simulator models exactly *that* clock
behavior.

### 4. Production burn-in — proving the operation

Even after TLA+ + DST + Jepsen, there are bugs that only show up in
operation:

- A config knob nobody set in tests has a wrong default.
- A log line format breaks a downstream tool a quarter after launch.
- Disk fills up during a 3am migration nobody scheduled.
- A dependency's behavior changes in a minor version bump.
- An operator runs the wrong command at the wrong time.

The only countermeasure is **time**. Specifically:

- **Dogfood.** Run cask to coordinate your own services before
  anyone else's. Find the operational sharp edges with low blast
  radius.
- **Soft launch.** First external customer should be a friendly
  design partner who knows they're early.
- **Graduated rollout.** Demo → internal non-critical → internal
  critical → external non-critical → external critical. Don't skip.
- **An incident review culture.** When the first real incident
  happens, the post-mortem either adds a TLA+ invariant, a `BUGGIFY`
  site, a Jepsen nemesis, or all three. **The catalog grows with each
  failure.**

The honest rule: nothing accelerates the burn-in clock. Cockroach took
years. FoundationDB took years. etcd has had multiple correctness
incidents even with massive deployment scale. Cask will too.

## The cask trust ladder

Where each level lives, in time:

| Level | Description | What it takes | Roadmap state |
|---|---|---|---|
| **Demo / showcase** | Plan + working binary + multi-cloud demo | Already there | **Reached.** |
| **Internal non-critical** | Use cask for ops/internal coordination | PR #0 (gate) + PR #1 (storage + ErrRangeChanged) + 2 weeks burn-in | After ~6 weeks. |
| **Internal critical** | Cask coordinates production services owned by the team | PR #2 (C' + GRV) + Jepsen partition+churn fencing run passes (master-plan v1 gate) + 1 month dogfood | After ~3 months. |
| **External non-critical** | A friendly design partner, side-by-side with their existing solution | Independent expert review + a mirror of their read-only workload + cask running ≥ 3 months internally | After ~6 months. |
| **External critical** | Load-bearing for someone's business | ≥ 1 year of operation at scale + at least one resolved Jepsen-class incident + a real ops playbook + observability story | 12–18 months. |

**Today (M7.2/M8 in code, post-this-roadmap):** demo. With PR #0–#2
landed, internal non-critical to internal critical.

## The honest limits of all four techniques combined

Even with all four pillars in place, none of these is caught:

- **Dependency bugs.** Pebble has its own changelog. Nebula has its
  own changelog. Go's runtime has its own changelog. When a minor
  version bump breaks something subtle, no test in cask catches it
  until it shows up in burn-in. Pin versions; read changelogs.
- **Operator error.** Cask's API can be used wrong. A config flag set
  wrong. A backup not actually backed up. Documentation and tooling
  mitigate this; tests don't.
- **Scale-only behaviors.** A protocol that's correct at N=10 might
  deadlock at N=10,000. The simulator can test N=10,000 only if you
  asked it to; production exposes the cases you didn't think of.
- **Adversarial users.** Cask has no auth layer. Anyone who can reach
  the API can do anything. That's a *policy* decision (per the master
  plan's risk #7), not a bug, but it's load-bearing for trust.
- **Hardware failure modes you didn't anticipate.** A NIC that
  corrupts the 47th bit of every TCP segment is a real failure mode;
  the simulator doesn't model byte-level corruption unless you tell it
  to. ECC memory faults under cosmic rays do occur.

The countermeasures for these are operational, not algorithmic:
versioning discipline, runbooks, scale-load testing, blast-radius
design, and humility.

## The compounding curve

Here's why the discipline matters more than the techniques: **every
bug found makes the next bug harder to escape.**

- A bug found in prod becomes a new `BUGGIFY` site (§6.6.1).
- That site joins a profile in the fault catalog (§6.7).
- The simulator-as-gate (§6.6) runs that fault on every subsequent PR.
- The class of bug is caught forever after.

If you find 200 bugs in the first year and convert each to a
`BUGGIFY` site, year two has 200 fewer ways to ship a regression. The
catalog **strictly grows**; correctness **strictly improves**. This is
the property that distinguishes the FDB-style approach from one-shot
testing: it compounds.

By contrast, a project that fixes bugs without converting them to
faults is doing the same correctness work over and over. The bug
might be fixed; the *class* of bug remains liable to recur with a
different surface.

The single most important sentence in this document: **make every
found bug add a fault.** That discipline, more than any technique, is
what bought FoundationDB its reputation.

## Order of operations

For cask specifically, in time order:

1. **TLA+ for the protocols where it matters** — done for the
   existing five specs; extension to `Lease.tla` to compose with
   `Reconfig.tla` (close S7's proof loop) is the next gap.
2. **PR #0: simulator gate + fault catalog + BUGGIFY** —
   `internal/buggify`, the gate driver, the seed eleven `buggify.Maybe`
   sites, CI integration. **This is the multiplier on everything
   else.**
3. **PR #1, #2, #3** continue with the simulator gate enforcing
   every change.
4. **Jepsen** — wire up the Clojure suite once §3.0 (durable
   storage) and §4.1 (`ErrRangeChanged`) are in. The lock+fencing
   workload under partition+churn is the v1 release gate.
5. **Dogfood** — use cask for our own coordination before anyone
   else's.
6. **Independent expert review** — pay a known practitioner
   (Kingsbury's Jepsen service, or someone like Marc Brooker, or a
   Cockroach-veteran consultant) for a week of design + code review
   before PR #2 lands.
7. **Graduated production rollout** — internal non-critical → internal
   critical → external non-critical → external critical, with each
   step taking months not weeks.

There are no shortcuts.

## Reading list (in priority order)

These are the resources to read before trusting any verdict about
distributed systems you didn't operate yourself:

1. **Will Wilson, "Testing Distributed Systems w/ Deterministic
   Simulation"** — Strange Loop 2014. 40 min. Watch first.
2. **Kyle Kingsbury, "Jepsen Analyses"** — jepsen.io/analyses. Read
   the Cassandra, etcd, MongoDB, FoundationDB writeups. The best
   teaching material in the field for what to test for.
3. **FoundationDB SIGMOD 2021 paper** — sections 5 (testing) and
   6 (Flow runtime).
4. **TigerBeetle's `vopr/` and design docs** — the cleanest
   open-source FDB-style simulator in Zig. Joran Dirk Greef has
   written extensively about the discipline.
5. **Antithesis Engineering blog** — Will Wilson's new company
   sells FDB-style simulation as a service. Their posts are recent
   and clear.
6. **Phil Eaton, "What is deterministic simulation testing"** —
   short, well-written overview.
7. **The Specification of Paxos by Lamport** + **Diego Ongaro's
   Raft thesis** — both are required reading even though cask
   doesn't use either; the literacy buys you the right to ask hard
   questions.
8. **Aphyr's "Distributed Systems Safety" course** — the most
   compressed delivery of "things distributed-systems engineers
   should know" available. Paid; worth it.

## The bottom line

Cask uses every technique the field knows for building distributed-
systems confidence. The plan is structurally sound: pure protocol
cores, TLA+ specs first, FDB-style DST with the planned `BUGGIFY`
discipline (§6.6.1), Jepsen as the release gate, graduated rollout.

**None of that makes cask production-ready today.** It makes cask
*on the right path* to becoming production-ready over months and
years. The path is faster than most distributed-systems projects'
because the techniques are stronger; it is not zero.

When deciding whether to trust cask for your workload, ask:

1. What level on the trust ladder is cask at right now? (Today:
   demo.)
2. What level does my workload require? (Be honest.)
3. If they're not equal, what's the gap and how long does it take to
   close?

If you can answer those three with specifics, you can make an honest
decision. If you can't, neither can anyone else.
