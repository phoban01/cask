// A core change must not lose a committed data write (docs/spec/fleet.md §6).
//
// In the extension server every data register uses the roster core as its
// acceptor set. So a core change moves every data register, not only the
// roster key. The change runs in three phases: old, joint, and new.
//
// The model has two keys and three acceptors. The core grows from {1} to
// {1, 2, 3}, the first growth step a fleet takes. It abstracts ballots: the
// per-ballot agreement is in caspaxos.qnt. A write commits a fresh version on
// a write quorum of the current phase. A read takes the highest version on a
// read quorum. `committed` records the last committed version of each key.
//
// The invariant is that every read quorum of the current phase still holds
// the last committed version of every key. Two rules keep it:
//   - release waits until every key is carried forward to the new core;
//   - a write in the joint phase reaches a quorum of both cores.
// Each rule has a negative control that drops it and must break the
// invariant.
//
// Gate checks, read by scripts/quint-check.sh:
// quint-check: invariants=NoCommittedLoss
// quint-check: control=stepNoCarry:NoCommittedLoss
// quint-check: control=stepOldQuorumWrite:NoCommittedLoss
// quint-check: verify-steps=8
module core_change {
  type Phase = Old | Joint | New

  pure val KEYS: Set[str] = Set("a", "b")
  pure val ACCS: Set[int] = Set(1, 2, 3)
  pure val COLD: Set[int] = Set(1)
  pure val CNEW: Set[int] = Set(1, 2, 3)
  pure val MAXV: int = 4

  // Majority quorums of one core.
  pure def quorums(c: Set[int]): Set[Set[int]] =
    c.powerset().filter(q => 2 * q.size() > c.size())

  // A joint quorum is a majority of the old core plus a majority of the new.
  pure val JOINT_QUORUMS: Set[Set[int]] =
    tuples(quorums(COLD), quorums(CNEW)).map(p => p._1.union(p._2))

  // The highest version a set of acceptors holds for one key.
  pure def maxOn(row: int -> int, q: Set[int]): int =
    q.fold(0, (m, a) => if (row.get(a) > m) row.get(a) else m)

  var phase: Phase
  var stored: str -> (int -> int)   // key -> acceptor -> version it holds
  var committed: str -> int         // key -> last committed version
  var carried: Set[str]             // keys carried forward in this joint phase
  var nextV: int                    // the next fresh version

  // The quorums the current phase uses for reads and for writes.
  def phaseQuorums: Set[Set[int]] = match phase {
    | Old => quorums(COLD)
    | Joint => JOINT_QUORUMS
    | New => quorums(CNEW)
  }

  action init = all {
    phase' = Old,
    stored' = KEYS.mapBy(k => ACCS.mapBy(a => 0)),
    committed' = KEYS.mapBy(k => 0),
    carried' = Set(),
    nextV' = 1,
  }

  // Commit version v of key k on every acceptor in q.
  action commitOn(k: str, q: Set[int]): bool = all {
    nextV <= MAXV,
    stored' = stored.setBy(k, row => ACCS.mapBy(a => if (q.contains(a)) nextV else row.get(a))),
    committed' = committed.set(k, nextV),
    nextV' = nextV + 1,
    phase' = phase, carried' = carried,
  }

  //= docs/spec/fleet.md#6-membership
  //= type=implication
  //# During a core change, a data write MUST reach a quorum of both the old and the new core.
  // A data write reaches a write quorum of the current phase. In the joint
  // phase that is a quorum of both cores.
  action write = {
    nondet k = oneOf(KEYS)
    nondet q = oneOf(phaseQuorums)
    commitOn(k, q)
  }

  action enterJoint = all {
    phase == Old,
    phase' = Joint, carried' = Set(),
    stored' = stored, committed' = committed, nextV' = nextV,
  }

  // Carry key k forward: read it on a joint quorum and write the value back
  // on a joint quorum. This is one CASPaxos Identity round with both cores.
  action carryWith(k: str, qr: Set[int], qw: Set[int]): bool = {
    val v = maxOn(stored.get(k), qr)
    all {
      phase == Joint,
      stored' = stored.setBy(k, row => ACCS.mapBy(a => if (qw.contains(a)) v else row.get(a))),
      carried' = carried.union(Set(k)),
      phase' = phase, committed' = committed, nextV' = nextV,
    }
  }
  action carry = {
    nondet k = oneOf(KEYS)
    nondet qr = oneOf(JOINT_QUORUMS)
    nondet qw = oneOf(JOINT_QUORUMS)
    carryWith(k, qr, qw)
  }

  //= docs/spec/fleet.md#6-membership
  //= type=implication
  //# A core change MUST carry every data register forward to the new core before it releases the old core.
  action release = all {
    phase == Joint,
    carried == KEYS,
    phase' = New,
    stored' = stored, committed' = committed, carried' = carried, nextV' = nextV,
  }

  action step = any { write, enterJoint, carry, release }

  // Every read quorum of the current phase sees the last committed version
  // of every key. A lost write shows up as a read quorum that misses it.
  val NoCommittedLoss =
    KEYS.forall(k => phaseQuorums.forall(q => maxOn(stored.get(k), q) >= committed.get(k)))

  // ---- negative control 1: release without the carry-forward ---------------

  // This is the bug in issue #110: the roster moves its own key, then
  // releases the old core with the data registers still on it alone.
  action releaseNoCarry = all {
    phase == Joint,
    phase' = New,
    stored' = stored, committed' = committed, carried' = carried, nextV' = nextV,
  }
  action stepNoCarry = any { write, enterJoint, carry, releaseNoCarry }

  // ---- negative control 2: a stale writer uses the old core alone ----------

  //= docs/spec/fleet.md#6-membership
  //= type=implication
  //# A voter MUST reject a data write that names an older core configuration than the one it knows.
  // A writer that has not seen the joint phase commits on an old quorum
  // after its key was carried forward. The fence keeps this action out of
  // `step`; this control shows the invariant needs it.
  action oldQuorumWrite = {
    nondet k = oneOf(KEYS)
    nondet q = oneOf(quorums(COLD))
    all { phase == Joint, commitOn(k, q) }
  }
  action stepOldQuorumWrite = any { write, oldQuorumWrite, enterJoint, carry, release }

  // ---- witness runs --------------------------------------------------------

  // Write both keys on the founder, grow to three, and read both keys from
  // the two new voters alone, as if the founder had stopped.
  run growKeepsWritesTest =
    init
      .then(commitOn("a", Set(1)))
      .then(commitOn("b", Set(1)))
      .then(enterJoint)
      .then(carryWith("a", Set(1, 2), Set(1, 2, 3)))
      .then(carryWith("b", Set(1, 3), Set(1, 2)))
      .then(release)
      .expect(NoCommittedLoss)
      .expect(maxOn(stored.get("a"), Set(2, 3)) == 1)
      .expect(maxOn(stored.get("b"), Set(2, 3)) == 2)

  // Without the carry-forward, the two new voters do not hold the writes.
  run releaseWithoutCarryLosesTest =
    init
      .then(commitOn("a", Set(1)))
      .then(enterJoint)
      .then(releaseNoCarry)
      .expect(not(NoCommittedLoss))
}
