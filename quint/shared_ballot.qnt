// shared_ballot.qnt — two proposers that choose the same ballot (#204).
//
// A cask ballot is (counter, node id). Each proposer keeps its own
// counter, and one process builds many proposers with one node id: a
// roster operation, a core carry, and a configuration change each build
// a fresh one. So two proposers can run at the same ballot on one key.
//
// caspaxos.qnt cannot show what that does. It models Lamport's abstract
// voting, where SafeAt reads every acceptor's votes at once. With the
// one-value-per-ballot guard removed there, Consistency still holds;
// only OneValuePerBallot fails. The real proposer does not see every
// vote. It adopts the value of the highest accepted ballot in its own
// prepare quorum. So this module models the two phases as the code runs
// them, and lets any proposer pick any ballot, shared or not.
//
// The acceptor promises a ballot only if it is strictly higher than its
// promise (internal/caspaxos/acceptor.go). So it promises each ballot at
// most once, and at most one proposer can gather a prepare quorum at a
// ballot. The negative control `stepLoosePromise` promises a ballot equal
// to the promise, as a >= check would. Then two proposers at one ballot
// both gather a quorum, both send accepts, and two values are chosen.
//
//   OneValuePerBallot  acceptors vote for at most one value per ballot
//   Consistency        at most one value is ever chosen
//
// Gate checks, read by scripts/quint-check.sh. The shortest control trace
// that breaks Consistency has 12 steps: two starts, four prepares, two
// proposes, four accepts. Apalache finds it at 12 steps in 8 s. The good
// step holds at 12 steps in about 5 minutes on an aarch64 VM.
// quint-check: invariants=Consistency,OneValuePerBallot
// quint-check: control=stepLoosePromise:Consistency
// quint-check: control=stepLoosePromise:OneValuePerBallot
// quint-check: verify-steps=12
module shared_ballot {
  pure val ACCEPTORS = Set("a1", "a2", "a3")
  pure val PROPOSERS = Set("p1", "p2")
  pure val BALLOTS = 1.to(2)
  pure val VALUES = Set("v1", "v2")
  pure val NONE = ""

  // Each proposer writes its own value unless it must carry one forward.
  pure def own(p: str): str = if (p == "p1") "v1" else "v2"

  pure val QUORUMS = ACCEPTORS.powerset().filter(Q => 2 * Q.size() > ACCEPTORS.size())

  // Acceptor state, as in caspaxos.Register. Ballot 0 is the zero ballot.
  var promise: str -> int
  var accBal: str -> int
  var accVal: str -> str

  // Proposer state. phase is "idle", "prepare", or "accept".
  var bal: str -> int
  var phase: str -> str
  var promised: str -> Set[str]       // acceptors that promised bal[p]
  var reports: str -> Set[(int, str)] // (accepted ballot, value) they reported
  var pval: str -> str                // the value p sends in phase 2

  // History: every (acceptor, ballot, value) vote ever cast.
  var votes: Set[(str, int, str)]

  action init = all {
    promise' = ACCEPTORS.mapBy(a => 0),
    accBal' = ACCEPTORS.mapBy(a => 0),
    accVal' = ACCEPTORS.mapBy(a => NONE),
    bal' = PROPOSERS.mapBy(p => 0),
    phase' = PROPOSERS.mapBy(p => "idle"),
    promised' = PROPOSERS.mapBy(p => Set()),
    reports' = PROPOSERS.mapBy(p => Set()),
    pval' = PROPOSERS.mapBy(p => NONE),
    votes' = Set(),
  }

  // p starts a round at ballot b. Any b is allowed, so both proposers
  // can pick the same one. A proposer may also abandon a round.
  action start(p: str, b: int): bool = all {
    bal' = bal.set(p, b),
    phase' = phase.set(p, "prepare"),
    promised' = promised.set(p, Set()),
    reports' = reports.set(p, Set()),
    pval' = pval.set(p, NONE),
    promise' = promise, accBal' = accBal, accVal' = accVal, votes' = votes,
  }

  // Acceptor a answers p's prepare. strict is the real rule: promise only
  // a ballot strictly above the current promise. The control sets it false.
  action prepareWith(p: str, a: str, strict: bool): bool = all {
    phase.get(p) == "prepare",
    not(promised.get(p).contains(a)),
    if (strict) promise.get(a) < bal.get(p) else promise.get(a) <= bal.get(p),
    promise' = promise.set(a, bal.get(p)),
    promised' = promised.set(p, promised.get(p).union(Set(a))),
    reports' = reports.set(p, reports.get(p).union(Set((accBal.get(a), accVal.get(a))))),
    accBal' = accBal, accVal' = accVal,
    bal' = bal, phase' = phase, pval' = pval, votes' = votes,
  }

  // With a prepare quorum, p picks the value of the highest accepted
  // ballot it saw, or its own value if none was accepted. If two values
  // share that highest ballot, it may pick either: Proposer.prepare keeps
  // the first reply at the highest ballot.
  action propose(p: str): bool = {
    val top = reports.get(p).fold(0, (m, r) => if (r._1 > m) r._1 else m)
    val carried = reports.get(p).filter(r => r._1 == top and top > 0).map(r => r._2)
    val choices = if (top == 0) Set(own(p)) else carried
    all {
      phase.get(p) == "prepare",
      QUORUMS.exists(Q => Q.subseteq(promised.get(p))),
      nondet v = choices.oneOf()
      all {
        pval' = pval.set(p, v),
        phase' = phase.set(p, "accept"),
      },
      promise' = promise, accBal' = accBal, accVal' = accVal,
      bal' = bal, promised' = promised, reports' = reports, votes' = votes,
    }
  }

  // Acceptor a accepts p's value if p's ballot is at least its promise.
  action accept(p: str, a: str): bool = all {
    phase.get(p) == "accept",
    bal.get(p) >= promise.get(a),
    promise' = promise.set(a, bal.get(p)),
    accBal' = accBal.set(a, bal.get(p)),
    accVal' = accVal.set(a, pval.get(p)),
    votes' = votes.union(Set((a, bal.get(p), pval.get(p)))),
    bal' = bal, phase' = phase, promised' = promised, reports' = reports, pval' = pval,
  }

  action stepWith(strict: bool): bool = {
    nondet p = PROPOSERS.oneOf()
    nondet a = ACCEPTORS.oneOf()
    nondet b = BALLOTS.oneOf()
    any { start(p, b), prepareWith(p, a, strict), propose(p), accept(p, a) }
  }

  //= docs/spec/fleet.md#3-storage-model
  //= type=implication
  //# An acceptor MUST promise a ballot only if the ballot is higher than every ballot that the acceptor has promised.
  action step = stepWith(true)

  // ---- invariants -------------------------------------------------------------

  def ChosenAt(b: int, v: str): bool = QUORUMS.exists(Q => Q.forall(a => votes.contains((a, b, v))))
  def Chosen(v: str): bool = BALLOTS.exists(b => ChosenAt(b, v))

  // Agreement: once a value is chosen, no different value is ever chosen.
  val Consistency =
    tuples(VALUES, VALUES).forall(((v, w)) => (Chosen(v) and Chosen(w)) implies v == w)

  // Acceptors vote for at most one value at each ballot.
  val OneValuePerBallot =
    votes.forall(x => votes.forall(y => x._2 == y._2 implies x._3 == y._3))

  // ---- negative control: promise a ballot equal to the promise ------------------

  action stepLoosePromise = stepWith(false)

  // ---- witness runs ------------------------------------------------------------

  //= docs/spec/fleet.md#3-storage-model
  //= type=implication
  //# Two proposers MAY choose the same ballot.
  // Both proposers start at ballot 1. p1 gathers a1 and a2. p2 is refused
  // by both, so it cannot reach a quorum, and v1 is chosen alone.
  run sharedBallotOneWinnerTest =
    init
      .then(start("p1", 1))
      .then(start("p2", 1))
      .then(prepareWith("p1", "a1", true))
      .then(prepareWith("p1", "a2", true))
      .then(prepareWith("p2", "a3", true))
      .then(propose("p1"))
      .then(accept("p1", "a1"))
      .then(accept("p1", "a2"))
      .expect(Chosen("v1") and Consistency and OneValuePerBallot)
      .then(prepareWith("p2", "a1", true))
      .fail()

  // The same schedule with a loose promise: p2 also gathers a1 and a2 at
  // ballot 1, reads nothing accepted yet, and chooses v2 over v1.
  run loosePromiseTwoValuesTest =
    init
      .then(start("p1", 1))
      .then(start("p2", 1))
      .then(prepareWith("p1", "a1", false))
      .then(prepareWith("p1", "a2", false))
      .then(prepareWith("p2", "a1", false))
      .then(prepareWith("p2", "a2", false))
      .then(propose("p1"))
      .then(propose("p2"))
      .then(accept("p1", "a1"))
      .then(accept("p1", "a2"))
      .then(accept("p2", "a1"))
      .then(accept("p2", "a2"))
      .expect(Chosen("v1") and Chosen("v2") and not(Consistency) and not(OneValuePerBallot))
}
