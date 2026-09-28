// quint-check: invariants=NoLostMembership,AlwaysAvailable,TypeOK
// quint-check: control=stepNoCatchUp:NoLostMembership
// quint-check: control=stepNoCatchUp:AlwaysAvailable
// quint-check: verify-steps=4
//
// Inv is TypeOK, NoLostMembership and AlwaysAvailable under the TLA+ name,
// so the gate lists the three parts. The shortest violation under
// stepNoCatchUp has 2 steps. TLC reports a search depth of 5, so every
// state is at most 4 steps from init. `quint verify` at 4 steps reaches
// every state (quint/PARITY.md). The control found its violation of each
// invariant in 20 of 20 runs with different seeds at 30 steps and 2000
// samples.
//
// roster_reconfig.qnt — Quint port of tla/RosterReconfig.tla.
//
// The membership register stores its own acceptor set, the core. The core
// moves from Cold to Cnew through a joint phase that runs on the register
// itself. Membership writes (joins and departures) keep committing during
// the move. So the value moves on while the register changes hands. This
// is the reflexive case of reconfig.qnt.
//
// The membership value is a version that only rises. A version is chosen
// in a configuration when a quorum of it holds that version or a higher
// one. A write commits the next version on a quorum of every required
// configuration: Cold while old, both cores while joint, Cnew while new.
// The old core is released only when Cnew holds the latest chosen
// version. So no membership committed under the old core is lost.
//
// Ballots are abstracted, as in the TLA+. caspaxos.qnt has the per-ballot
// agreement.
//
// How this relates to core_change.qnt: core_change.qnt models the change
// that the extension server runs. There the roster core is the acceptor
// set of every data register. It models the data registers that one core
// change moves, with per-voter fences, a key listing, a carry round per
// key, and stale writers. It has the roster value only as the phase and
// the generation each voter has accepted. Membership writes during the
// change are not in it. This module proves
// that step for the roster register: the roster value moves on during its
// own handoff, and the handoff still loses no committed membership. The
// control here drops the catch-up check. It matches the stepNoCarry
// control in core_change.qnt, which releases the old core without the
// carry.
//
// Invariants:
//
//   NoLostMembership  once the old core is released, Cnew holds the
//                     latest chosen membership version
//   AlwaysAvailable   some required configuration holds the latest chosen
//                     membership version
//   TypeOK            the variables stay in their ranges
//   Inv               TypeOK, NoLostMembership and AlwaysAvailable, as in
//                     the TLA+
//
// Constants are the values from tla/RosterReconfig.cfg: five nodes,
// Cold = {n1, n2, n3}, Cnew = {n3, n4, n5}, MaxVer 2.
//
// Differences from the TLA+ source:
//   - Constants are pure vals, not CONSTANTS, so the model is fixed. The
//     quorums of Cold and Cnew are pure vals too, so Apalache does not
//     expand a powerset in each step.
//   - MaxChosen is a fold over the chosen versions, not CHOOSE. Version 0
//     is always chosen, as in the TLA+, so both give the same value.
//   - Write takes the two quorums as parameters. `step` picks them with
//     nondet, as the TLA+ Write does with \E.
//   - The TLA+ has no Bug config. The negative control is new:
//     stepNoCatchUp releases the old core without the catch-up check.
//     LeaveJoint calls leaveJointIf with the check. The control passes
//     true. It MUST violate NoLostMembership and AlwaysAvailable.
//   - Issue #14 names a different control: a membership write that goes
//     only to the old core while joint. In this model that write cannot
//     break NoLostMembership. The catch-up check in LeaveJoint is the
//     same predicate, so it blocks the release until Cnew holds the
//     version. The write goes only to Cold, so Cnew never gets it, and
//     the change can never finish. That is a liveness loss, not a safety
//     loss. The run oldOnlyWriteBlocksReleaseTest shows it. TLC on the
//     TLA+ with that change also finds no violation.
module roster_reconfig {
  pure val NODES = Set("n1", "n2", "n3", "n4", "n5")
  pure val COLD = Set("n1", "n2", "n3")
  pure val CNEW = Set("n3", "n4", "n5")
  pure val MAX_VER = 2
  pure val NO_VER = -1          // a node that holds no membership value yet
  pure val VERS = 0.to(MAX_VER)
  pure val PHASES = Set("old", "joint", "new")

  // Majority quorums of a configuration.
  pure def quorums(c: Set[str]): Set[Set[str]] =
    c.powerset().filter(q => 2 * q.size() > c.size())

  pure val Q_COLD = quorums(COLD)
  pure val Q_CNEW = quorums(CNEW)

  var phase: str          // "old" -> "joint" -> "new": the core change stage
  var ver: str -> int     // ver[a]: the highest membership version a holds, or NO_VER

  val TypeOK = and {
    PHASES.contains(phase),
    ver.keys() == NODES,
    NODES.forall(a => ver.get(a) == NO_VER or VERS.contains(ver.get(a))),
  }

  // Genesis: the old core holds version 0. No other node holds a value.
  action init = all {
    phase' = "old",
    ver' = NODES.mapBy(a => if (COLD.contains(a)) 0 else NO_VER),
  }

  // The configurations whose quorums are required to choose in this phase.
  val RequiredConfigs: Set[Set[str]] =
    if (phase == "old") Set(COLD)
    else if (phase == "joint") Set(COLD, CNEW)
    else Set(CNEW)

  def quorumsOf(c: Set[str]): Set[Set[str]] = if (c == COLD) Q_COLD else Q_CNEW

  // Version k is chosen in c when some quorum of c all hold k or higher.
  def ChosenIn(k: int, c: Set[str]): bool =
    quorumsOf(c).exists(q => q.forall(a => ver.get(a) >= k))

  def Chosen(k: int): bool = ChosenIn(k, COLD) or ChosenIn(k, CNEW)

  // The highest chosen version. Version 0 is always chosen.
  val MaxChosen: int =
    VERS.fold(0, (m, k) => if (Chosen(k) and k > m) k else m)

  // ---- actions --------------------------------------------------------------

  // The nodes in s now hold version k. A node only raises its version.
  def Raise(s: Set[str], k: int): str -> int =
    NODES.mapBy(a => if (s.contains(a)) k else ver.get(a))

  //= docs/spec/fleet.md#6-membership
  //= type=implication
  //# The voter set MUST change only by joint-consensus reconfiguration of the roster.
  // A membership write commits the next version on a quorum of every
  // required configuration: one core while old or new, both while joint.
  action Write(qo: Set[str], qn: Set[str]): bool = all {
    MaxChosen < MAX_VER,
    val k = MaxChosen + 1
    ver' =
      if (phase == "old") Raise(qo, k)
      else if (phase == "joint") Raise(qo.union(qn), k)
      else Raise(qn, k),
    phase' = phase,
  }

  //= docs/spec/fleet.md#6-membership
  //= type=implication
  //# The voter set MUST change only by joint-consensus reconfiguration of the roster.
  // Start the core change. From now on every write needs a joint quorum.
  action EnterJoint: bool = all {
    phase == "old",
    phase' = "joint",
    ver' = ver,
  }

  // Release the old core once ready holds.
  action leaveJointIf(ready: bool): bool = all {
    phase == "joint",
    ready,
    phase' = "new",
    ver' = ver,
  }

  //= docs/spec/fleet.md#6-membership
  //= type=implication
  //# A core change MUST carry every data register forward to the new core before it releases the old core.
  // Release only after catch-up: Cnew already holds the latest chosen
  // version. This is the load-bearing precondition.
  action LeaveJoint: bool = leaveJointIf(ChosenIn(MaxChosen, CNEW))

  action step = any {
    { nondet qo = Q_COLD.oneOf()
      nondet qn = Q_CNEW.oneOf()
      Write(qo, qn) },
    EnterJoint,
    LeaveJoint,
  }

  // ---- invariants -------------------------------------------------------------

  // Once the register is handed to Cnew, Cnew holds the latest chosen
  // version. Versions only rise, so no committed membership is lost.
  val NoLostMembership = phase == "new" implies ChosenIn(MaxChosen, CNEW)

  // The latest chosen version is on some required configuration. The
  // register is never left on a quorum the change has left behind.
  val AlwaysAvailable = RequiredConfigs.exists(c => ChosenIn(MaxChosen, c))

  val Inv = and { TypeOK, NoLostMembership, AlwaysAvailable }

  // ---- negative control: release before catch-up --------------------------------

  // LeaveJoint without the catch-up check. The old core is released while
  // Cnew does not hold the latest chosen version. It MUST violate
  // NoLostMembership and AlwaysAvailable.
  action stepNoCatchUp = any {
    { nondet qo = Q_COLD.oneOf()
      nondet qn = Q_CNEW.oneOf()
      Write(qo, qn) },
    EnterJoint,
    leaveJointIf(true),
  }

  // ---- witness runs ------------------------------------------------------------

  pure val QO = Set("n1", "n2")
  pure val QN = Set("n4", "n5")

  // At the start only n3 of Cnew holds a value. The change cannot release
  // the old core until a joint write reaches a quorum of Cnew.
  run releaseWaitsForJointWriteTest =
    init
      .then(EnterJoint)
      .then(LeaveJoint)
      .fail()

  // The membership moves on during the handoff and survives it.
  run advanceDuringHandoffTest =
    init
      .then(Write(QO, QN))
      .expect(MaxChosen == 1 and ChosenIn(1, COLD) and not(ChosenIn(1, CNEW)))
      .then(EnterJoint)
      .then(Write(QO, QN))
      .expect(MaxChosen == 2 and ChosenIn(2, CNEW) and ver.get("n3") == 0)
      .then(LeaveJoint)
      .expect(phase == "new" and Inv)

  // While new, a write needs only a Cnew quorum.
  run writeAfterReleaseTest =
    init
      .then(EnterJoint)
      .then(Write(QO, QN))
      .then(LeaveJoint)
      .then(Write(QO, Set("n3", "n4")))
      .expect(MaxChosen == 2 and ver.get("n1") == 1 and Inv)

  // The write the issue names: while joint, a write reaches only the old
  // core. Cnew does not hold the latest version, so LeaveJoint stays
  // blocked. Safety holds; the change cannot finish.
  run oldOnlyWriteBlocksReleaseTest =
    init
      .then(EnterJoint)
      .then(Write(QO, QN))
      .then(all { phase == "joint", ver' = Raise(QO, MaxChosen + 1), phase' = phase })
      .expect(MaxChosen == 2 and not(ChosenIn(2, CNEW)) and Inv)
      .then(LeaveJoint)
      .fail()

  // No write beyond MaxVer.
  run versionBoundTest =
    init
      .then(Write(QO, QN))
      .then(Write(QO, QN))
      .then(Write(QO, QN))
      .fail()

  // Control: the old core is released before any joint write. Cnew holds
  // no chosen version, so both invariants fail at once.
  run releaseBeforeCatchUpTest =
    init
      .then(EnterJoint)
      .then(leaveJointIf(true))
      .expect(not(NoLostMembership) and not(AlwaysAvailable) and TypeOK)
}
