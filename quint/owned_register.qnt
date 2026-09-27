// quint-check: invariants=ChosenChain,OneValuePerBallot
// quint-check: control=stepBumpRule:ChosenChain
// quint-check: verify-steps=6
//
// NoLostUpdate and Inv are the same checks under their TLA+ names, so the
// gate lists only ChosenChain and OneValuePerBallot. The shortest lost
// update under stepBumpRule has 9 steps. `quint verify` at 6 steps finishes
// in about 3 minutes but cannot reach it; at 9 steps it does not finish in
// 15 minutes. `quint run` and TLC find the violation.
//
// owned_register.qnt — Quint port of tla/OwnedRegister.tla.
//
// The owner fast path (OwnedProposer) and a full CASPaxos proposer write
// one register. Ballots are (epoch, seq, node) and compare in that order.
// The owner prepares once at (e, 0, owner). It then accepts at (e, s, owner),
// s >= 1, with no new prepare. This is safe only while no other proposer
// mints a ballot inside epoch e. The rule under test is how the full
// proposer moves past a conflict ballot c:
//
//   jump rule (the fix):  next epoch boundary (c.e + 1, 0, full)
//   bump rule (the bug):  seq plus one        (c.e, c.s + 1, full)
//
// Invariants:
//
//   ChosenChain        each newly committed value contains the last one
//                      (the name in tla/README.md and issue #10)
//   NoLostUpdate       the TLA+ name for ChosenChain
//   OneValuePerBallot  two acceptors at one ballot hold one value
//   Inv                NoLostUpdate and OneValuePerBallot, as in the TLA+
//
// Constants are the values from tla/OwnedRegister.cfg: three acceptors,
// owner node 2, full node 1, owner ops x and y, full op z, epochs and seqs
// up to 2.
//
// Differences from the TLA+ source:
//   - Constants are pure vals, not CONSTANTS, so the model is fixed.
//   - JumpRule is not a constant. The actions that depend on it take a
//     `jump` flag. `step` passes true. The negative control `stepBumpRule`
//     passes false. It stands for tla/OwnedRegisterBug.cfg and MUST
//     violate ChosenChain.
//   - The TLA+ uses CHOOSE in ReadFrom and RecordCommit. Quint has no
//     CHOOSE. ReadFrom takes the union of the candidate values. The
//     candidates are one value while OneValuePerBallot holds, so the
//     result is the same. RecordCommit picks a commit witness with
//     nondet. All witnesses hold one value, because two committing
//     quorums intersect, so the next state is the same.
//   - CommittedIn becomes CommitWitness, a predicate on one acceptor.
//     It avoids a set of sets, which is slow in Apalache.
//   - RecordCommit reads accBal' and accVal' in the TLA+. Quint cannot read
//     a primed variable, so the deliver actions pass the new maps in.
//   - Acceptors are strings, so TLC cannot use SYMMETRY on the compiled
//     module. quint/PARITY.md compares both without symmetry.
module owned_register {
  pure val ACCEPTORS = Set("a1", "a2", "a3")
  pure val OWNER_NODE = 2
  pure val FULL_NODE = 1
  pure val OWNER_OPS = Set("x", "y")
  pure val FULL_OPS = Set("z")
  pure val MAX_EPOCH = 2
  pure val MAX_SEQ = 2
  pure val EPOCHS = 1.to(MAX_EPOCH)

  pure val QUORUMS = ACCEPTORS.powerset().filter(Q => 2 * Q.size() > ACCEPTORS.size())
  pure val QuorumsIntersect =
    QUORUMS.forall(Q1 => QUORUMS.forall(Q2 => Q1.intersect(Q2) != Set()))

  type Ballot = { e: int, s: int, n: int }
  type Proposal = { b: Ballot, v: Set[str] }

  pure def Bal(e: int, s: int, n: int): Ballot = { e: e, s: s, n: n }
  pure val Bot = Bal(-1, -1, -1)

  pure def Less(x: Ballot, y: Ballot): bool = or {
    x.e < y.e,
    x.e == y.e and (x.s < y.s or (x.s == y.s and x.n < y.n)),
  }
  pure def Leq(x: Ballot, y: Ballot): bool = Less(x, y) or x == y

  pure val NoProp: Proposal = { b: Bot, v: Set() }  // no accept in flight

  var promise: str -> Ballot   // highest ballot a promised or accepted at
  var accBal: str -> Ballot    // ballot of a's accepted value (Bot if none)
  var accVal: str -> Set[str]  // a's accepted value, a set of ops
  var lastC: Set[str]          // the last quorum-committed value
  var bad: bool                // a committed value lost its predecessor
  // owner state
  var oEpoch: int
  var oSeq: int
  var oCache: Set[str]
  var oLive: bool
  var oProp: Proposal
  // full-proposer state
  var fFloor: Ballot
  var fBal: Ballot
  var fRead: Set[str]
  var fPrep: bool
  var fProp: Proposal

  // The value at the highest accepted ballot in Q: what phase 1 carries
  // forward. The TLA+ CHOOSEs one value; the union is the same value.
  def ReadFrom(Q: Set[str]): Set[str] =
    Q.filter(a => accBal.get(a) != Bot and Q.forall(a2 => Leq(accBal.get(a2), accBal.get(a))))
     .map(a => accVal.get(a))
     .flatten()

  // a0 shows a commit in acceptor state (bal, vals): a quorum that holds
  // a0 accepted a0's ballot and value. The TLA+ CommittedIn builds the set
  // of committed values instead. The port avoids a set of sets, which is
  // slow in Apalache. All witnesses in one state hold one value.
  pure def CommitWitness(bal: str -> Ballot, vals: str -> Set[str], a0: str): bool = and {
    bal.get(a0) != Bot,
    QUORUMS.exists(Q =>
      Q.contains(a0) and Q.forall(a => bal.get(a) == bal.get(a0) and vals.get(a) == vals.get(a0))),
  }

  // Record the newly committed value, if any, and flag a lost update when
  // it does not contain its predecessor.
  action RecordCommit(newBal: str -> Ballot, newVal: str -> Set[str]): bool = {
    val ws = ACCEPTORS.filter(a0 => CommitWitness(newBal, newVal, a0))
    if (ws == Set())
      all { lastC' = lastC, bad' = bad }
    else {
      nondet a0 = ws.oneOf()
      all {
        lastC' = newVal.get(a0),
        // Parenthesized, as in the TLA+: without them the assignment binds
        // first and bad' is unassigned on the violating step.
        bad' = (bad or not(lastC.subseteq(newVal.get(a0)))),
      }
    }
  }

  action init = all {
    promise' = ACCEPTORS.mapBy(a => Bot),
    accBal' = ACCEPTORS.mapBy(a => Bot),
    accVal' = ACCEPTORS.mapBy(a => Set()),
    lastC' = Set(),
    bad' = false,
    oEpoch' = 0,
    oSeq' = 0,
    oCache' = Set(),
    oLive' = false,
    oProp' = NoProp,
    fFloor' = Bot,
    fBal' = Bot,
    fRead' = Set(),
    fPrep' = false,
    fProp' = NoProp,
  }

  // ---- owner actions: OwnedProposer ---------------------------------------------

  // TakeOwnership: one quorum-atomic phase 1 at (e, 0, owner). The highest
  // accepted value goes into the cache.
  action OwnerTake(e: int, Q: Set[str]): bool = all {
    e >= 1,
    e > oEpoch,
    Q.forall(a => Less(promise.get(a), Bal(e, 0, OWNER_NODE))),
    promise' = ACCEPTORS.mapBy(a => if (Q.contains(a)) Bal(e, 0, OWNER_NODE) else promise.get(a)),
    oEpoch' = e,
    oSeq' = 0,
    oCache' = ReadFrom(Q),
    oLive' = true,
    oProp' = NoProp,
    accBal' = accBal, accVal' = accVal, lastC' = lastC, bad' = bad,
    fFloor' = fFloor, fBal' = fBal, fRead' = fRead, fPrep' = fPrep, fProp' = fProp,
  }

  // Write with NO phase 1: derive the next value from the cache, bump seq,
  // put the accept in flight. The ballot rule must protect this action.
  action OwnerWrite(op: str): bool = all {
    oLive,
    oSeq < MAX_SEQ,
    oProp == NoProp,
    oSeq' = oSeq + 1,
    oCache' = oCache.union(Set(op)),
    oProp' = { b: Bal(oEpoch, oSeq + 1, OWNER_NODE), v: oCache.union(Set(op)) },
    promise' = promise, accBal' = accBal, accVal' = accVal, lastC' = lastC, bad' = bad,
    oEpoch' = oEpoch, oLive' = oLive,
    fFloor' = fFloor, fBal' = fBal, fRead' = fRead, fPrep' = fPrep, fProp' = fProp,
  }

  // Deliver the owner's accept to one acceptor. The acceptor takes it iff
  // the ballot is at or above its promise.
  action OwnerDeliver(a: str): bool = all {
    oProp != NoProp,
    Leq(promise.get(a), oProp.b),
    promise' = promise.set(a, oProp.b),
    accBal' = accBal.set(a, oProp.b),
    accVal' = accVal.set(a, oProp.v),
    RecordCommit(accBal.set(a, oProp.b), accVal.set(a, oProp.v)),
    oEpoch' = oEpoch, oSeq' = oSeq, oCache' = oCache, oLive' = oLive, oProp' = oProp,
    fFloor' = fFloor, fBal' = fBal, fRead' = fRead, fPrep' = fPrep, fProp' = fProp,
  }

  // The owner moves on from an in-flight write, delivered or not.
  action OwnerFinish: bool = all {
    oProp != NoProp,
    oProp' = NoProp,
    promise' = promise, accBal' = accBal, accVal' = accVal, lastC' = lastC, bad' = bad,
    oEpoch' = oEpoch, oSeq' = oSeq, oCache' = oCache, oLive' = oLive,
    fFloor' = fFloor, fBal' = fBal, fRead' = fRead, fPrep' = fPrep, fProp' = fProp,
  }

  // ---- full-proposer actions: Proposer.Propose -----------------------------------

  // The rule under test: how the next ballot is minted past floor c.
  pure def NextBalWith(jump: bool, c: Ballot): Ballot =
    if (c == Bot) Bal(0, 1, FULL_NODE)
    else if (jump and c.e >= 1) Bal(c.e + 1, 0, FULL_NODE)
    else Bal(c.e, c.s + 1, FULL_NODE)

  pure def NextBal(c: Ballot): Ballot = NextBalWith(true, c)

  // Quorum-atomic phase 1 at the minted ballot. It reads the carry-forward
  // value.
  action FullPrepareWith(jump: bool, Q: Set[str]): bool = {
    val b = NextBalWith(jump, fFloor)
    all {
      not(fPrep),
      b.e <= MAX_EPOCH,
      b.s <= MAX_SEQ,
      Q.forall(a => Less(promise.get(a), b)),
      promise' = ACCEPTORS.mapBy(a => if (Q.contains(a)) b else promise.get(a)),
      fBal' = b,
      fRead' = ReadFrom(Q),
      fPrep' = true,
      accBal' = accBal, accVal' = accVal, lastC' = lastC, bad' = bad,
      oEpoch' = oEpoch, oSeq' = oSeq, oCache' = oCache, oLive' = oLive, oProp' = oProp,
      fFloor' = fFloor, fProp' = fProp,
    }
  }

  action FullPrepare(Q: Set[str]): bool = FullPrepareWith(true, Q)

  // A NACK: a promise dominates the current attempt. Adopt it as the floor
  // and restart the round.
  action FullConflict(a: str): bool = all {
    Less(fFloor, promise.get(a)),
    fFloor' = promise.get(a),
    fPrep' = false,
    fProp' = NoProp,
    promise' = promise, accBal' = accBal, accVal' = accVal, lastC' = lastC, bad' = bad,
    oEpoch' = oEpoch, oSeq' = oSeq, oCache' = oCache, oLive' = oLive, oProp' = oProp,
    fBal' = fBal, fRead' = fRead,
  }

  // Phase 2 starts: the read value plus one op, at the prepared ballot.
  action FullAcceptStart(op: str): bool = all {
    fPrep,
    fProp == NoProp,
    fProp' = { b: fBal, v: fRead.union(Set(op)) },
    promise' = promise, accBal' = accBal, accVal' = accVal, lastC' = lastC, bad' = bad,
    oEpoch' = oEpoch, oSeq' = oSeq, oCache' = oCache, oLive' = oLive, oProp' = oProp,
    fFloor' = fFloor, fBal' = fBal, fRead' = fRead, fPrep' = fPrep,
  }

  action FullDeliver(a: str): bool = all {
    fProp != NoProp,
    Leq(promise.get(a), fProp.b),
    promise' = promise.set(a, fProp.b),
    accBal' = accBal.set(a, fProp.b),
    accVal' = accVal.set(a, fProp.v),
    RecordCommit(accBal.set(a, fProp.b), accVal.set(a, fProp.v)),
    oEpoch' = oEpoch, oSeq' = oSeq, oCache' = oCache, oLive' = oLive, oProp' = oProp,
    fFloor' = fFloor, fBal' = fBal, fRead' = fRead, fPrep' = fPrep, fProp' = fProp,
  }

  // The round ends. The floor adopts the ballot just used, so the
  // proposer's counter stays monotonic.
  action FullFinish: bool = all {
    fProp != NoProp,
    fProp' = NoProp,
    fPrep' = false,
    fFloor' = if (Less(fFloor, fBal)) fBal else fFloor,
    promise' = promise, accBal' = accBal, accVal' = accVal, lastC' = lastC, bad' = bad,
    oEpoch' = oEpoch, oSeq' = oSeq, oCache' = oCache, oLive' = oLive, oProp' = oProp,
    fBal' = fBal, fRead' = fRead,
  }

  // Each disjunct picks only its own parameters, as the TLA+ Next does.
  // This keeps the TLC generated-state count close to the original.
  action stepWith(jump: bool): bool = any {
    { nondet e = EPOCHS.oneOf()
      nondet Q = QUORUMS.oneOf()
      OwnerTake(e, Q) },
    { nondet op = OWNER_OPS.oneOf()
      OwnerWrite(op) },
    { nondet a = ACCEPTORS.oneOf()
      OwnerDeliver(a) },
    OwnerFinish,
    { nondet Q = QUORUMS.oneOf()
      FullPrepareWith(jump, Q) },
    { nondet a = ACCEPTORS.oneOf()
      FullConflict(a) },
    { nondet op = FULL_OPS.oneOf()
      FullAcceptStart(op) },
    { nondet a = ACCEPTORS.oneOf()
      FullDeliver(a) },
    FullFinish,
  }

  action step = stepWith(true)

  // ---- invariants -----------------------------------------------------------------

  // Every write adds one op to the value it read, so each newly committed
  // value contains the one before. bad is a lost update.
  val NoLostUpdate = not(bad)
  val ChosenChain = NoLostUpdate

  // Two acceptors that accepted at one ballot hold one value.
  val OneValuePerBallot =
    tuples(ACCEPTORS, ACCEPTORS).forall(((a1, a2)) =>
      (accBal.get(a1) == accBal.get(a2) and accBal.get(a1) != Bot)
        implies accVal.get(a1) == accVal.get(a2))

  val Inv = NoLostUpdate and OneValuePerBallot

  // ---- negative control: the pre-W0 bump rule --------------------------------------

  // tla/OwnedRegisterBug.cfg. The full proposer bumps seq inside the
  // owner's epoch and commits there. The owner's next write, built from a
  // stale cache, wins the node tiebreak and overwrites it.
  action stepBumpRule = stepWith(false)

  // ---- witness runs ------------------------------------------------------------------

  run quorumsIntersectTest = init.expect(QuorumsIntersect and QUORUMS.size() == 4)

  // The owner holds epoch 1. The full proposer hits the owner's promise and
  // takes its floor from it.
  run conflictSetsFloorTest =
    init
      .then(OwnerTake(1, Set("a1", "a2")))
      .then(FullConflict("a1"))
      .expect(fFloor == Bal(1, 0, OWNER_NODE)
        and NextBalWith(true, fFloor) == Bal(2, 0, FULL_NODE)
        and NextBalWith(false, fFloor) == Bal(1, 1, FULL_NODE))

  // Bump rule: the full proposer commits {z} at (1, 1, full). The owner
  // then commits {x} at (1, 1, owner) from its empty cache. {z} is lost.
  run bumpLosesUpdateTest =
    init
      .then(OwnerTake(1, Set("a1", "a2")))
      .then(FullConflict("a1"))
      .then(FullPrepareWith(false, Set("a1", "a2")))
      .then(FullAcceptStart("z"))
      .then(FullDeliver("a1"))
      .then(FullDeliver("a2"))
      .expect(lastC == Set("z") and Inv)
      .then(OwnerWrite("x"))
      .then(OwnerDeliver("a1"))
      .then(OwnerDeliver("a2"))
      .expect(lastC == Set("x") and not(ChosenChain) and not(Inv))

  // Jump rule: the full proposer commits {z} at (2, 0, full), above the
  // owner's whole epoch.
  run jumpCommitsAboveEpochTest =
    init
      .then(OwnerTake(1, Set("a1", "a2")))
      .then(FullConflict("a1"))
      .then(FullPrepare(Set("a1", "a2")))
      .expect(fBal == Bal(2, 0, FULL_NODE))
      .then(FullAcceptStart("z"))
      .then(FullDeliver("a1"))
      .then(FullDeliver("a2"))
      .expect(lastC == Set("z") and Inv)

  // Jump rule: the owner's stale write at (1, 1, owner) is below the
  // promise at (2, 0, full). The acceptor rejects it.
  run jumpBlocksStaleWriteTest =
    init
      .then(OwnerTake(1, Set("a1", "a2")))
      .then(FullConflict("a1"))
      .then(FullPrepare(Set("a1", "a2")))
      .then(FullAcceptStart("z"))
      .then(FullDeliver("a1"))
      .then(FullDeliver("a2"))
      .then(OwnerWrite("x"))
      .then(OwnerDeliver("a1"))
      .fail()

  // A later owner epoch carries the committed value forward.
  run takeCarriesForwardTest =
    init
      .then(OwnerTake(1, Set("a1", "a2")))
      .then(OwnerWrite("x"))
      .then(OwnerDeliver("a1"))
      .then(OwnerDeliver("a2"))
      .then(OwnerFinish)
      .then(OwnerTake(2, Set("a2", "a3")))
      .expect(oCache == Set("x") and lastC == Set("x") and Inv)
}
