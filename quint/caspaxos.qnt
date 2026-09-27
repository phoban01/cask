// caspaxos.qnt — Quint port of tla/CasPaxosMvcc.tla.
//
// Every cask key is an independent CASPaxos register. A write runs one
// Paxos round that agrees on the register's next value. This module models
// one register in Lamport's abstract voting style (no messages) and checks
// agreement:
//
//   OneVotePerBallot   no acceptor votes for two values at one ballot
//   OneValuePerBallot  all acceptors vote for at most one value per ballot
//   VotesSafe          every vote is safe at its ballot (the inductive core)
//   Consistency        at most one value is ever chosen
//   Inv                TypeOK and all four above, as in the TLA+ source
//
// Constants are the values from tla/CasPaxosMvcc.cfg: three acceptors, two
// values, ballots 0 to 2.
//
// Differences from the TLA+ source:
//   - Constants are pure vals, not CONSTANTS, so the model is fixed.
//   - The ASSUME QuorumsIntersect becomes a pure val checked by a run test.
//   - A tuple field is vt._1 in Quint, vt[1] in TLA+.
//   - SafeAt ranges over the constant BALLOTS set with c < b, not over
//     0..(b - 1), so that Apalache accepts it.
//   - The negative control `stepNoPromise` is new. It drops the promise
//     check (maxBal[a] =< b) from VoteFor and MUST violate Consistency and
//     VotesSafe.
//
// Gate checks, read by scripts/quint-check.sh. The shortest stepNoPromise
// trace that breaks Consistency has 6 steps. At 10 steps Apalache ran for
// more than 40 minutes on the good step (quint/PARITY.md).
// quint-check: invariants=Consistency,OneValuePerBallot,VotesSafe
// quint-check: control=stepNoPromise:Consistency
// quint-check: verify-steps=6
module caspaxos {
  pure val ACCEPTORS = Set("a1", "a2", "a3")
  pure val VALUES = Set("v1", "v2")
  pure val MAX_BAL = 2
  pure val BALLOTS = 0.to(MAX_BAL)
  pure val NONE = -1

  // Majority quorums: any two majorities of a finite acceptor set intersect.
  pure val QUORUMS = ACCEPTORS.powerset().filter(Q => 2 * Q.size() > ACCEPTORS.size())
  pure val QuorumsIntersect =
    QUORUMS.forall(Q1 => QUORUMS.forall(Q2 => Q1.intersect(Q2) != Set()))

  var votes: str -> Set[(int, str)]  // votes[a] = the (ballot, value) pairs a voted for
  var maxBal: str -> int             // highest ballot a still takes part in; NONE = none

  val TypeOK = and {
    votes.keys() == ACCEPTORS,
    maxBal.keys() == ACCEPTORS,
    ACCEPTORS.forall(a => votes.get(a).subseteq(tuples(BALLOTS, VALUES))),
    ACCEPTORS.forall(a => BALLOTS.union(Set(NONE)).contains(maxBal.get(a))),
  }

  action init = all {
    votes' = ACCEPTORS.mapBy(a => Set()),
    maxBal' = ACCEPTORS.mapBy(a => NONE),
  }

  def VotedFor(a, b, v) = votes.get(a).contains((b, v))

  // A value is chosen at ballot b once a whole quorum voted for it there.
  def ChosenAt(b, v) = QUORUMS.exists(Q => Q.forall(a => VotedFor(a, b, v)))
  def Chosen(v) = BALLOTS.exists(b => ChosenAt(b, v))

  def DidNotVoteAt(a, b) = VALUES.forall(v => not(VotedFor(a, b, v)))

  // a can never vote at b in the future: it moved past b without voting.
  def CannotVoteAt(a, b) = maxBal.get(a) > b and DidNotVoteAt(a, b)

  // No value other than v can still become chosen at ballot b.
  def NoneOtherChoosableAt(b, v) =
    QUORUMS.exists(Q => Q.forall(a => VotedFor(a, b, v) or CannotVoteAt(a, b)))

  // v is safe at b if no other value is still choosable at any lower ballot.
  // TLA+ ranges over 0..(b - 1). Apalache needs a constant range, so the
  // port filters the constant BALLOTS set. The meaning is the same.
  def SafeAt(b, v) = BALLOTS.forall(c => c < b implies NoneOtherChoosableAt(c, v))

  // ---- actions --------------------------------------------------------------

  action IncreaseMaxBal(a, b) = all {
    b > maxBal.get(a),
    maxBal' = maxBal.set(a, b),
    votes' = votes,
  }

  // The vote without the promise check. VoteFor adds the check. The
  // negative control uses this action alone.
  action voteUnpromised(a, b, v) = all {
    votes.get(a).forall(vt => vt._1 != b),                    // a has not voted at b
    ACCEPTORS.exclude(Set(a)).forall(c =>
      VALUES.forall(w => VotedFor(c, b, w) implies w == v)),  // one value per ballot
    SafeAt(b, v),                                             // carry-forward safety
    votes' = votes.set(a, votes.get(a).union(Set((b, v)))),
    maxBal' = maxBal.set(a, b),
  }

  action VoteFor(a, b, v) = all {
    maxBal.get(a) <= b,                                       // the promise check
    voteUnpromised(a, b, v),
  }

  action step = {
    nondet a = ACCEPTORS.oneOf()
    nondet b = BALLOTS.oneOf()
    nondet v = VALUES.oneOf()
    any { IncreaseMaxBal(a, b), VoteFor(a, b, v) }
  }

  // ---- invariants -------------------------------------------------------------

  // No acceptor votes for two different values at the same ballot.
  val OneVotePerBallot =
    tuples(ACCEPTORS, BALLOTS, VALUES, VALUES).forall(((a, b, v, w)) =>
      (VotedFor(a, b, v) and VotedFor(a, b, w)) implies v == w)

  // The whole system agrees on at most one value per ballot.
  val OneValuePerBallot =
    tuples(ACCEPTORS, ACCEPTORS, BALLOTS).forall(((a1, a2, b)) =>
      tuples(VALUES, VALUES).forall(((v1, v2)) =>
        (VotedFor(a1, b, v1) and VotedFor(a2, b, v2)) implies v1 == v2))

  // Every vote is safe (the inductive heart of the agreement proof).
  val VotesSafe =
    tuples(ACCEPTORS, BALLOTS, VALUES).forall(((a, b, v)) =>
      VotedFor(a, b, v) implies SafeAt(b, v))

  // Agreement: once a value is chosen, no different value is ever chosen.
  val Consistency =
    tuples(VALUES, VALUES).forall(((v, w)) => (Chosen(v) and Chosen(w)) implies v == w)

  val Inv = and { TypeOK, OneVotePerBallot, OneValuePerBallot, VotesSafe, Consistency }

  // ---- negative control: vote without the promise check ------------------------

  // An acceptor that promised a higher ballot still votes at a lower one.
  // The vote breaks the CannotVoteAt fact that made a higher vote safe.
  action stepNoPromise = {
    nondet a = ACCEPTORS.oneOf()
    nondet b = BALLOTS.oneOf()
    nondet v = VALUES.oneOf()
    any { IncreaseMaxBal(a, b), voteUnpromised(a, b, v) }
  }

  // ---- witness runs ------------------------------------------------------------

  run quorumsIntersectTest = init.expect(QuorumsIntersect and QUORUMS.size() == 4)

  // v2 is chosen at ballot 1. A vote at ballot 2 must carry v2 forward:
  // v1 is not safe there, v2 is.
  run carryForwardTest =
    init
      .then(IncreaseMaxBal("a1", 1))
      .then(IncreaseMaxBal("a2", 1))
      .then(VoteFor("a1", 1, "v2"))
      .then(VoteFor("a2", 1, "v2"))
      .expect(Chosen("v2") and not(SafeAt(2, "v1")) and SafeAt(2, "v2"))
      .then(VoteFor("a3", 2, "v2"))
      .expect(Inv)

  // With the promise check dropped, a2 votes at ballot 0 after it voted at
  // ballot 1, and a3 joins it. Two values are chosen.
  run noPromiseBreaksAgreementTest =
    init
      .then(IncreaseMaxBal("a1", 1))
      .then(IncreaseMaxBal("a2", 1))
      .then(VoteFor("a1", 1, "v2"))
      .then(VoteFor("a2", 1, "v2"))
      .then(voteUnpromised("a2", 0, "v1"))
      .then(voteUnpromised("a3", 0, "v1"))
      .expect(Chosen("v1") and Chosen("v2") and not(Consistency) and not(VotesSafe))

  // The promise check blocks the same low vote.
  run promiseBlocksLowVoteTest =
    init
      .then(IncreaseMaxBal("a2", 1))
      .then(VoteFor("a2", 0, "v1"))
      .fail()
}
