// quint-check: invariants=NoLostValue,CatchUpHeld,TypeOK
// quint-check: control=stepNoCatchUp:CatchUpHeld
// quint-check: verify-steps=7
//
// Inv is TypeOK, NoLostValue and CatchUpHeld under the TLA+ name, so the
// gate lists the three parts. The shortest violation under stepNoCatchUp
// has 4 steps. TLC reports a search depth of 8, so every state is at most
// 7 steps from init. `quint verify` at 7 steps reaches every state and
// finishes in about 15 seconds (quint/PARITY.md). The control found its
// violation in 20 of 20 runs with random seeds at 30 steps and 2000
// samples.
//
// reconfig.qnt — Quint port of tla/Reconfig.tla.
//
// One register moves from the replica set Cold to Cnew through a joint
// configuration. While joint, a value is chosen only with a quorum in both
// Cold and Cnew. A joint quorum overlaps every Cold quorum and every Cnew
// quorum. Before Cold is released, every value chosen in Cold is carried
// into Cnew (catch-up). So no chosen value is lost or contradicted across
// the change.
//
// Ballots are abstracted, as in the TLA+. caspaxos.qnt has the per-ballot
// agreement. Here a vote is allowed only when every active acceptor that
// voted has voted for the same value.
//
// How this relates to core_change.qnt: core_change.qnt models the change
// that the extension server runs. There the roster core is the acceptor
// set of every data register, so one core change moves all of them, with
// per-voter fences, a key listing, a carry round per key, and stale
// writers in flight. This module is the abstract argument under it, for
// one register: joint quorums overlap, and catch-up comes before release.
// The control here drops catch-up. It matches the stepNoCarry control in
// core_change.qnt, which releases the old core without the carry.
//
// Invariants:
//
//   NoLostValue  never two different chosen values
//   CatchUpHeld  once in Cnew, every value chosen in Cold at release is
//                chosen in Cnew
//   TypeOK       the variables stay in their ranges
//   Inv          TypeOK, NoLostValue and CatchUpHeld, as in the TLA+
//
// Constants are the values from tla/Reconfig.cfg: five acceptors, two
// values, Cold = {a1, a2, a3}, Cnew = {a3, a4, a5}.
//
// Differences from the TLA+ source:
//   - Constants are pure vals, not CONSTANTS, so the model is fixed.
//   - The TLA+ has no Bug config. The negative control is new, as issue
//     #13 asks: stepNoCatchUp releases Cold before catch-up. LeaveJoint
//     calls leaveJointIf with the catch-up check. The control passes
//     true. It MUST violate CatchUpHeld.
//   - Quorums(C) is quorums(c). The catch-up check in LeaveJoint gets
//     the name CaughtUp. ChosenNow is unused, as in the TLA+.
module reconfig {
  pure val ACCEPTORS = Set("a1", "a2", "a3", "a4", "a5")
  pure val VALUES = Set("v1", "v2")
  pure val COLD = Set("a1", "a2", "a3")
  pure val CNEW = Set("a3", "a4", "a5")
  pure val PHASES = Set("old", "joint", "new")

  // Majority quorums of a configuration.
  pure def quorums(c: Set[str]): Set[Set[str]] =
    c.powerset().filter(q => 2 * q.size() > c.size())

  var phase: str                // "old" -> "joint" -> "new"
  var voted: str -> Set[str]    // voted[a] = the values a voted for
  var oldChosen: Set[str]       // the values chosen in Cold when LeaveJoint ran

  val TypeOK = and {
    PHASES.contains(phase),
    voted.keys() == ACCEPTORS,
    ACCEPTORS.forall(a => voted.get(a).subseteq(VALUES)),
    oldChosen.subseteq(VALUES),
  }

  action init = all {
    phase' = "old",
    voted' = ACCEPTORS.mapBy(a => Set()),
    oldChosen' = Set(),
  }

  // The configurations whose quorums are required to choose in this phase.
  val RequiredConfigs: Set[Set[str]] =
    if (phase == "old") Set(COLD)
    else if (phase == "joint") Set(COLD, CNEW)
    else Set(CNEW)

  // v has a quorum of c that all voted for v.
  def ChosenIn(v: str, c: Set[str]): bool =
    quorums(c).exists(q => q.forall(a => voted.get(a).contains(v)))

  // v is choosable now: every required configuration has such a quorum.
  def ChosenNow(v: str): bool = RequiredConfigs.forall(c => ChosenIn(v, c))

  // v is chosen under Cold or under Cnew.
  def Chosen(v: str): bool = ChosenIn(v, COLD) or ChosenIn(v, CNEW)

  val ActiveAcceptor: Set[str] = RequiredConfigs.flatten()

  // ---- actions --------------------------------------------------------------

  // An active acceptor votes for v, only if every active acceptor that
  // voted has voted for v. This stands for the per-ballot agreement in
  // caspaxos.qnt.
  action Vote(a: str, v: str): bool = all {
    ActiveAcceptor.contains(a),
    ActiveAcceptor.forall(b => voted.get(b).forall(w => w == v)),
    voted' = voted.setBy(a, s => s.union(Set(v))),
    phase' = phase,
    oldChosen' = oldChosen,
  }

  //= docs/spec/fleet.md#6-membership
  //= type=implication
  //# The voter set MUST change only by joint-consensus reconfiguration of the roster.
  // Enter the joint configuration. Only LeaveJoint then reaches Cnew.
  action EnterJoint: bool = all {
    phase == "old",
    phase' = "joint",
    voted' = voted,
    oldChosen' = oldChosen,
  }

  // Every value chosen in Cold is also chosen in Cnew.
  val CaughtUp: bool =
    VALUES.forall(v => ChosenIn(v, COLD) implies ChosenIn(v, CNEW))

  // Leave the joint configuration for Cnew once ready holds. The values
  // chosen in Cold are snapshotted now. So CatchUpHeld does not count a
  // Cold quorum that a later vote by a shared acceptor completes.
  action leaveJointIf(ready: bool): bool = all {
    phase == "joint",
    ready,
    oldChosen' = VALUES.filter(v => ChosenIn(v, COLD)),
    phase' = "new",
    voted' = voted,
  }

  //= docs/spec/fleet.md#6-membership
  //= type=implication
  //# A core change MUST carry every data register forward to the new core before it releases the old core.
  // Leave only after catch-up. This is the load-bearing precondition.
  action LeaveJoint: bool = leaveJointIf(CaughtUp)

  action step = any {
    { nondet a = ACCEPTORS.oneOf()
      nondet v = VALUES.oneOf()
      Vote(a, v) },
    EnterJoint,
    LeaveJoint,
  }

  // ---- invariants -------------------------------------------------------------

  // Agreement across the change: never two different chosen values.
  val NoLostValue =
    tuples(VALUES, VALUES).forall(((v, w)) =>
      (Chosen(v) and Chosen(w)) implies v == w)

  // Once in Cnew, every value chosen in Cold at release is chosen in Cnew.
  val CatchUpHeld =
    phase == "new" implies oldChosen.forall(v => ChosenIn(v, CNEW))

  val Inv = and { TypeOK, NoLostValue, CatchUpHeld }

  // ---- negative control: release before catch-up --------------------------------

  // LeaveJoint without the catch-up check. Cold is released while a value
  // chosen there has no Cnew quorum.
  action stepNoCatchUp = any {
    { nondet a = ACCEPTORS.oneOf()
      nondet v = VALUES.oneOf()
      Vote(a, v) },
    EnterJoint,
    leaveJointIf(true),
  }

  // ---- witness runs ------------------------------------------------------------

  // v1 is chosen in Cold only. LeaveJoint must wait for catch-up.
  run releaseWaitsForCatchUpTest =
    init
      .then(Vote("a1", "v1"))
      .then(Vote("a2", "v1"))
      .expect(ChosenIn("v1", COLD) and not(ChosenIn("v1", CNEW)))
      .then(EnterJoint)
      .then(LeaveJoint)
      .fail()

  // v1 is chosen in Cold, carried into Cnew while joint, and survives the
  // release.
  run carryForwardTest =
    init
      .then(Vote("a1", "v1"))
      .then(Vote("a2", "v1"))
      .then(EnterJoint)
      .then(Vote("a4", "v1"))
      .then(Vote("a5", "v1"))
      .expect(ChosenNow("v1") and CaughtUp)
      .then(LeaveJoint)
      .expect(phase == "new" and oldChosen == Set("v1") and Inv)

  // While joint, an acceptor of either configuration cannot vote for a
  // second value.
  run jointBlocksSecondValueTest =
    init
      .then(Vote("a1", "v1"))
      .then(EnterJoint)
      .then(Vote("a5", "v2"))
      .fail()

  // In the old phase an acceptor only in Cnew cannot vote.
  run oldPhaseExcludesNewTest =
    init
      .then(Vote("a4", "v1"))
      .fail()

  // With nothing chosen, the change goes straight through.
  run emptyReleaseTest =
    init
      .then(EnterJoint)
      .then(LeaveJoint)
      .expect(phase == "new" and oldChosen == Set() and Inv)

  // Control: Cold is released before catch-up. CatchUpHeld fails at once.
  // Cnew then chooses v2, so NoLostValue fails too.
  run releaseBeforeCatchUpTest =
    init
      .then(Vote("a1", "v1"))
      .then(Vote("a2", "v1"))
      .then(EnterJoint)
      .then(leaveJointIf(true))
      .expect(not(CatchUpHeld) and NoLostValue)
      .then(Vote("a3", "v2"))
      .then(Vote("a4", "v2"))
      .expect(not(NoLostValue))
}
