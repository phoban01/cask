# The tyranny nobody talks about: the log itself

*Review-ready draft. Register and structure deliberately mirror Cloudflare's
Meerkat introduction — incident-shaped motivation, honest limitations —
because it is written to sit beside it in the same conversation.*

---

Two weeks ago, Cloudflare introduced [Meerkat](https://blog.cloudflare.com/meerkat-introduction/),
an experimental global consensus service built on QuePaxa. Their diagnosis
will be familiar to anyone who has operated etcd, ZooKeeper, or anything
Raft-shaped: leaders fail, elections pause the world, and timeout tuning on
real networks is a game you lose slowly. Their cure is elegant — randomized
consensus that needs no timeouts and no leader to stay live.

We think the diagnosis stops one layer too early. Elections, terms, quorum
heartbeats, log compaction, snapshot shipping, read indexes — every one of
those mechanisms exists to protect a single data structure: **the totally
ordered replicated log**. And here is the uncomfortable question we started
from: *what in a coordination workload actually needs a total order?*

Locks don't. Leases don't. Sessions, service registrations, leader records,
placement entries — each is an independent little register whose reads and
writes must be linearizable *per key*. The log is a tax these workloads pay
for generality they never use. Meerkat makes the tax collector friendlier.
We stopped paying.

## Registers all the way down

Cask is a coordination store — think "etcd's little sister":
leases, locks, sessions, and small config at fleet scale — in which every
key is its own CASPaxos register. There is no log anywhere in the system.
A write is one consensus decision on one register; two writes to two keys
never contend, never queue behind each other, and never wait for a leader,
because there isn't one.

"No leader" here is worth being precise about, because it is the same
property Meerkat advertises with different machinery. In QuePaxa, the leader
is a performance optimization: it commits in one round trip when healthy,
and the protocol survives without it. In cask, the same role is played by
**ownership**: a node holding a fencing token for a range of keys skips the
first Paxos phase and commits writes in **one round trip**, and serves reads
from its own memory in **zero** — and if it dies, vanishes, or gets
partitioned away, nobody elects anything. The next writer's fencing token
simply supersedes it, mid-flight writes from the stale owner bounce off the
epoch check, and life continues. Ownership failover is a CAS, not a
campaign.

The numbers are the boring kind of true: our test suite counts RPCs on the
wire. A warm owned write is exactly one accept round — three messages out,
zero prepares. A warm owned read is zero network operations. When the read
lease is too close to expiry to trust, the read degrades to the owner's
one-round-trip path — never slower than the two-round-trip baseline every
key supports with no owner at all.

## The part that almost bit us

Composing a skip-phase-1 fast path with plain Paxos proposers on the same
register turns out to have a trap in it, and we want to describe it because
we nearly shipped it.

An owner writes at ballots that encode its fencing epoch in the high bits
and a sequence in the low bits. A non-owner that gets rejected retries at
"conflict + 1" — which lands *inside the owner's epoch space*, at exactly
the counter the owner's next write will use. Two proposals, same number,
tie broken by node id. Half the time, the owner's write — derived from its
now-stale cache — wins the tiebreak and silently overwrites a committed
value. No error, no torn quorum, just a lost update.

What makes this worth a blog paragraph is *where* it was caught. Our
abstract Paxos agreement spec could never see it: in that spec, every vote
is safe by construction, and the fast path's entire point is discharging
that obligation by an external argument. So we wrote a second model in
which the argument itself is the thing being checked — and kept the broken
variant as a permanent **negative control**. TLC finds the five-step
lost-update trace in the buggy configuration and proves the fixed rule
(reject-and-jump past the whole epoch space) clean. If that negative
control ever passes, the model has lost its teeth and the build should be
red. We now do this for every subtle mechanism: our clock-skew read-lease
model ships with a config where the naive lease check *must* fail.

## A control plane that manages itself

Without a log, "cluster metadata" cannot be log entries. So it's registers
too, all the way up: the membership roster is a register that stores *its
own acceptor set* and reconfigures itself with joint consensus. Range
descriptors — which keys live where — are registers on the same small core.
Moving a range's data to new replicas is the same joint-quorum dance,
driven through the descriptor so every router in the fleet sees the
migration and writes through both replica sets while it runs; keys are
enumerated as the union over any *majority* of the old replicas, which
provably contains every committed key. Splitting a range is four register
commits and moves no data at all.

And when a driver crashes mid-protocol? The intent lives in the register.
Whoever picks it up next finishes the job. There is no log to replay —
there is only state, and state is what registers are for.

## What we deliberately don't do

Honesty section, in the Meerkat spirit:

- **No multi-key transactions, no global ordering.** There is no log to
  anchor them to, and we will not pretend otherwise. The cross-key story is
  fencing tokens, which is what coordination clients should be using
  anyway.
- **The zero-round-trip read has a clock contract.** The owner stops
  serving reads a MaxOffset before its lease expires; a successor waits a
  MaxOffset after. Within the assumed skew bound the reads are
  linearizable; beyond it you can read stale — never lose a write, because
  writes never depend on clocks.
- **Writes from non-owner nodes pay a hop** (forwarding to the owner) or,
  if the owner is unreachable, wait out its read lease — the same bill Raft
  leader-leases present, just itemized.
- **Quorum latency is still quorum latency.** Like Meerkat says of their
  own system: there's no getting around that.

## What's next

A Kubernetes aggregated API server backed by cask, exposing multi-cluster
resources with fencing-token controllers — the design is in the repo. And
benchmarks against etcd on identical hardware, which we will publish with
the harness, not as a bar chart.

The code, the TLA+ models (negative controls included), and the
deterministic fault-injection gate that runs on every pull request are all
in the repository.
