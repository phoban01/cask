// A core change must not lose a committed data write (docs/spec/fleet.md §6).
//
// In the extension server every data register uses the roster core as its
// acceptor set. So a core change moves every data register, not only the
// roster key. The roster moves through three configurations: old (gen 0),
// joint (gen 1), and new (gen 2). Here the core grows from {1, 2, 3} to
// {1, 2, 3, 4, 5}.
//
// What the model keeps from the implementation:
//   - The fence is per voter and durable: `rgen` is the ConfigGen of the
//     roster value each acceptor has accepted. The joint value lands on a
//     majority of the old core, and spreads to other acceptors later, or
//     never.
//   - A writer uses any view of the core up to the current one, so a
//     writer can be stale before, during, and after the change.
//   - A write reaches its acceptors one at a time. Each acceptor checks the
//     fence as it accepts. A write commits when its whole quorum accepted.
//     A stale write can start before the joint and finish after the
//     listing or the release.
//   - The carry runs a joint Identity round per listed key. Its prepare
//     makes the acceptors it touches reject a lower ballot, so an older
//     write in flight on that key can no longer land there. A round can
//     fail part way. The driver records a key as carried only when its
//     round committed.
//   - A resumed change lists the keys again and keeps its record, so it
//     carries only the keys it has not recorded.
//   - Keys can be written at any time, also after the listing.
//
// Ballots are abstracted: caspaxos.qnt has the per-ballot agreement. A write
// commits a fresh version. A read takes the highest version on a read quorum.
//
// The invariant is that every read quorum of the current configuration
// holds the last committed version of every key. Four negative controls
// break it: a release without the carry, a fence that reads a lagging view
// instead of the accepted roster value, a key listing that counts old
// voters that do not hold the joint roster value, and a resumed carry that
// skips a key whose round did not commit.
//
// Gate checks, read by scripts/quint-check.sh:
// quint-check: invariants=NoCommittedLoss
// quint-check: control=stepNoCarry:NoCommittedLoss
// quint-check: control=stepLaggingFence:NoCommittedLoss
// quint-check: control=stepListAnyVoter:NoCommittedLoss
// quint-check: control=stepCarryCountsAnyAck:NoCommittedLoss
// quint-check: verify-steps=6
module core_change {
  type Phase = Old | Joint | New

  // A write in flight: key, the writer's view (gen), its quorum, its
  // version, the acceptors that took it, and the acceptors a carry has
  // since promised a higher ballot on this key.
  type Pend = {
    active: bool, k: str, vg: int, q: Set[int], v: int,
    acked: Set[int], blocked: Set[int],
  }

  pure val KEYS: Set[str] = Set("a", "b")
  pure val ACCS: Set[int] = Set(1, 2, 3, 4, 5)
  pure val COLD: Set[int] = Set(1, 2, 3)
  pure val CNEW: Set[int] = Set(1, 2, 3, 4, 5)
  pure val MAXV: int = 4

  pure val NO_PEND: Pend = {
    active: false, k: "a", vg: 0, q: Set(), v: 0, acked: Set(), blocked: Set(),
  }

  // Majority quorums of one core.
  pure def quorums(c: Set[int]): Set[Set[int]] =
    c.powerset().filter(q => 2 * q.size() > c.size())

  // A joint quorum is a majority of the old core plus a majority of the new.
  pure val JOINT_QUORUMS: Set[Set[int]] =
    tuples(quorums(COLD), quorums(CNEW)).map(p => p._1.union(p._2))

  // The quorums a writer uses with a view of configuration gen.
  pure def quorumsFor(gen: int): Set[Set[int]] =
    if (gen == 0) quorums(COLD) else if (gen == 1) JOINT_QUORUMS else quorums(CNEW)

  // The highest version a set of acceptors holds for one key.
  pure def maxOn(row: int -> int, q: Set[int]): int =
    q.fold(0, (m, a) => if (row.get(a) > m) row.get(a) else m)

  var phase: Phase                  // the chosen roster configuration
  var rgen: int -> int              // acceptor -> ConfigGen of its accepted roster value
  var view: int -> int              // acceptor -> ConfigGen its member has learned (lags)
  var stored: str -> (int -> int)   // key -> acceptor -> version it holds
  var committed: str -> int         // key -> last committed version
  var pend: Pend                    // the write in flight, if any
  var didList: bool                 // the carry has listed the keys
  var listed: Set[str]              // keys the listing found
  var carried: Set[str]             // keys carried forward
  var nextV: int                    // the next fresh version

  def curGen: int = match phase { | Old => 0 | Joint => 1 | New => 2 }
  def phaseQuorums: Set[Set[int]] = quorumsFor(curGen)

  action init = all {
    phase' = Old,
    rgen' = ACCS.mapBy(a => 0),
    view' = ACCS.mapBy(a => 0),
    stored' = KEYS.mapBy(k => ACCS.mapBy(a => 0)),
    committed' = KEYS.mapBy(k => 0),
    pend' = NO_PEND,
    didList' = false, listed' = Set(), carried' = Set(),
    nextV' = 1,
  }

  // ---- writes ------------------------------------------------------------

  //= docs/spec/fleet.md#6-membership
  //= type=implication
  //# During a core change, a data write MUST reach a quorum of both the old and the new core.
  // A writer picks a view no newer than the chosen configuration and the
  // quorum that view gives. A view of the joint gives a joint quorum.
  action writeStartWith(k: str, vg: int, q: Set[int]): bool = all {
    not(pend.active), nextV <= MAXV, vg <= curGen, quorumsFor(vg).contains(q),
    pend' = { active: true, k: k, vg: vg, q: q, v: nextV, acked: Set(), blocked: Set() },
    nextV' = nextV + 1,
    phase' = phase, rgen' = rgen, view' = view, stored' = stored, committed' = committed,
    didList' = didList, listed' = listed, carried' = carried,
  }
  action writeStart = {
    nondet k = oneOf(KEYS)
    nondet vg = oneOf(0.to(2))
    nondet q = oneOf(quorumsFor(vg))
    writeStartWith(k, vg, q)
  }

  // Acceptor a takes the write if its fence allows it. fenceGen is the
  // ConfigGen the fence compares against.
  action acceptWith(a: int, fenceGen: int): bool = {
    val acked = pend.acked.union(Set(a))
    val done = acked == pend.q
    all {
      pend.active,
      pend.q.contains(a), not(pend.acked.contains(a)), not(pend.blocked.contains(a)),
      fenceGen <= pend.vg,
      stored' = stored.setBy(pend.k, row => row.set(a, pend.v)),
      committed' = if (done) committed.set(pend.k, pend.v) else committed,
      pend' = if (done) NO_PEND else pend.with("acked", acked),
      phase' = phase, rgen' = rgen, view' = view, nextV' = nextV,
      didList' = didList, listed' = listed, carried' = carried,
    }
  }

  //= docs/spec/fleet.md#6-membership
  //= type=implication
  //# A voter MUST reject a data write that names an older core configuration than the roster value it has accepted.
  action writeAccept = { nondet a = oneOf(ACCS) acceptWith(a, rgen.get(a)) }

  // The writer gives up. Acceptors keep what they took.
  action writeAbort = all {
    pend.active, pend' = NO_PEND,
    phase' = phase, rgen' = rgen, view' = view, stored' = stored, committed' = committed,
    didList' = didList, listed' = listed, carried' = carried, nextV' = nextV,
  }

  // ---- the roster --------------------------------------------------------

  // The joint roster value is chosen on a majority of the old core.
  action publishJointOn(q: Set[int]): bool = all {
    phase == Old, quorums(COLD).contains(q),
    phase' = Joint,
    rgen' = ACCS.mapBy(a => if (q.contains(a) and rgen.get(a) < 1) 1 else rgen.get(a)),
    view' = view, stored' = stored, committed' = committed, pend' = pend,
    didList' = didList, listed' = listed, carried' = carried, nextV' = nextV,
  }
  action publishJoint = { nondet q = oneOf(quorums(COLD)) publishJointOn(q) }

  // A later roster round gives acceptor a the chosen roster value.
  action spread = {
    nondet a = oneOf(ACCS)
    all {
      rgen.get(a) < curGen,
      rgen' = rgen.set(a, curGen),
      phase' = phase, view' = view, stored' = stored, committed' = committed, pend' = pend,
      didList' = didList, listed' = listed, carried' = carried, nextV' = nextV,
    }
  }

  // The member of acceptor a learns the chosen configuration. The real
  // fence does not read this; the lagging-fence control does.
  action learnView = {
    nondet a = oneOf(ACCS)
    all {
      view' = view.set(a, curGen),
      phase' = phase, rgen' = rgen, stored' = stored, committed' = committed, pend' = pend,
      didList' = didList, listed' = listed, carried' = carried, nextV' = nextV,
    }
  }

  // ---- the carry ---------------------------------------------------------

  // List the keys on the old voters in L, as one step per voter set. ok
  // says whether L qualifies. A resumed change lists again. The new listing
  // replaces the old one, and the keys carried so far stay carried.
  action listWith(L: Set[int], ok: bool): bool = all {
    phase == Joint, ok,
    didList' = true,
    listed' = KEYS.filter(k => L.exists(a => stored.get(k).get(a) > 0)),
    phase' = phase, rgen' = rgen, view' = view, stored' = stored, committed' = committed,
    pend' = pend, carried' = carried, nextV' = nextV,
  }

  //= docs/spec/fleet.md#6-membership
  //= type=implication
  //# A core change MUST list data keys only on old voters that have accepted the joint roster value.
  action list = {
    nondet L = oneOf(quorums(COLD))
    listWith(L, L.forall(a => rgen.get(a) >= 1))
  }

  // Carry key k: read it on the joint quorum qr and write the value back on
  // the acceptors qw. The round's prepare blocks an older write in flight on
  // k at every acceptor it touches. A round that fails part way writes back
  // on fewer acceptors. counts says whether the driver records k as carried.
  action carryMark(k: str, qr: Set[int], qw: Set[int], counts: bool): bool = {
    val v = maxOn(stored.get(k), qr)
    all {
      phase == Joint, didList, listed.contains(k), JOINT_QUORUMS.contains(qr),
      stored' = stored.setBy(k, row => ACCS.mapBy(a => if (qw.contains(a)) v else row.get(a))),
      pend' = if (pend.active and pend.k == k) pend.with("blocked", pend.blocked.union(qr).union(qw)) else pend,
      carried' = if (counts) carried.union(Set(k)) else carried,
      phase' = phase, rgen' = rgen, view' = view, committed' = committed,
      didList' = didList, listed' = listed, nextV' = nextV,
    }
  }

  //= docs/spec/fleet.md#6-membership
  //= type=implication
  //# A resumed core change MUST carry every data register that it did not carry under the same joint configuration.
  // The driver records k as carried only when the write-back reached a
  // joint quorum, that is, when the round committed. A resumed change
  // lists again (listWith) and carries only the keys not recorded.
  action carryWith(k: str, qr: Set[int], qw: Set[int]): bool =
    carryMark(k, qr, qw, JOINT_QUORUMS.contains(qw))
  action carry = {
    nondet k = oneOf(KEYS)
    nondet qr = oneOf(JOINT_QUORUMS)
    nondet qw = oneOf(ACCS.powerset())
    carryWith(k, qr, qw)
  }

  // The released roster value is chosen on a joint quorum.
  action releaseOn(q: Set[int], ready: bool): bool = all {
    phase == Joint, ready, JOINT_QUORUMS.contains(q),
    phase' = New,
    rgen' = ACCS.mapBy(a => if (q.contains(a)) 2 else rgen.get(a)),
    view' = view, stored' = stored, committed' = committed, pend' = pend,
    didList' = didList, listed' = listed, carried' = carried, nextV' = nextV,
  }

  //= docs/spec/fleet.md#6-membership
  //= type=implication
  //# A core change MUST carry every data register forward to the new core before it releases the old core.
  action release = {
    nondet q = oneOf(JOINT_QUORUMS)
    releaseOn(q, didList and listed.subseteq(carried))
  }

  action step = any {
    writeStart, writeAccept, writeAbort, publishJoint, spread, list, carry, release,
  }

  // Every read quorum of the current configuration sees the last committed
  // version of every key. A lost write shows up as a read quorum that
  // misses it.
  val NoCommittedLoss =
    KEYS.forall(k => phaseQuorums.forall(q => maxOn(stored.get(k), q) >= committed.get(k)))

  // ---- negative control 1: release without the carry ---------------------

  // The bug in issue #110: release the old core with the data registers
  // still on it alone.
  action releaseNoCarry = { nondet q = oneOf(JOINT_QUORUMS) releaseOn(q, true) }
  action stepNoCarry = any {
    writeStart, writeAccept, writeAbort, publishJoint, spread, list, carry, releaseNoCarry,
  }

  // ---- negative control 2: the fence reads a lagging view ----------------

  // The first fix on PR #117: the fence read the member's in-memory view,
  // which a stalled run loop or a restart leaves behind.
  action writeAcceptLagging = { nondet a = oneOf(ACCS) acceptWith(a, view.get(a)) }
  action stepLaggingFence = any {
    writeStart, writeAcceptLagging, writeAbort, publishJoint, spread, learnView, list, carry, release,
  }

  // ---- negative control 3: list on voters without the joint value --------

  // A voter without the joint value still takes a stale write after the
  // listing. So its listing can miss a key that later commits. The step
  // leaves out writeAbort and spread: they only hide the bug, and without
  // them the simulator finds it in every run.
  action listAnyVoter = { nondet L = oneOf(quorums(COLD)) listWith(L, true) }
  action stepListAnyVoter = any {
    writeStart, writeAccept, publishJoint, listAnyVoter, carry, release,
  }

  // ---- negative control 4: count a key as carried before it commits ------

  // The driver records a key as carried after a round that wrote it back on
  // only some acceptors. A resumed change then skips the key, and the
  // release leaves its value off a quorum of the new core.
  action carryCountsAnyAck = {
    nondet k = oneOf(KEYS)
    nondet qr = oneOf(JOINT_QUORUMS)
    nondet qw = oneOf(ACCS.powerset())
    carryMark(k, qr, qw, true)
  }
  action stepCarryCountsAnyAck = any {
    writeStart, writeAccept, writeAbort, publishJoint, spread, list, carryCountsAnyAck, release,
  }

  // ---- witness runs --------------------------------------------------------

  // Write key a on the old core, grow to five, and read it from 3, 4 and 5
  // alone, as if 1 and 2 had stopped.
  run growKeepsWritesTest =
    init
      .then(writeStartWith("a", 0, Set(1, 2)))
      .then(acceptWith(1, 0))
      .then(acceptWith(2, 0))
      .then(publishJointOn(Set(2, 3)))
      .then(listWith(Set(2, 3), true))
      .then(carryWith("a", Set(2, 3, 4, 5), Set(3, 4, 5)))
      .then(releaseOn(Set(2, 3, 4, 5), true))
      .expect(NoCommittedLoss)
      .expect(maxOn(stored.get("a"), Set(3, 4, 5)) == 1)

  // A carry of key a commits, then the change stops. The resumed change
  // lists again, skips a, and releases. The new core alone still reads a.
  run resumeSkipsCarriedKeyTest =
    init
      .then(writeStartWith("a", 0, Set(1, 2)))
      .then(acceptWith(1, 0))
      .then(acceptWith(2, 0))
      .then(publishJointOn(Set(2, 3)))
      .then(listWith(Set(2, 3), true))
      .then(carryWith("a", Set(2, 3, 4, 5), Set(2, 3, 4, 5)))
      .then(listWith(Set(2, 3), true))
      .then(releaseOn(Set(2, 3, 4, 5), didList and listed.subseteq(carried)))
      .expect(NoCommittedLoss)
      .expect(maxOn(stored.get("a"), Set(3, 4, 5)) == 1)

  // A stale writer (view gen 0) after the joint: voter 2 holds the joint
  // roster value and rejects it, so the write cannot commit on {1, 2}.
  run staleWriteIsFencedTest =
    init
      .then(publishJointOn(Set(2, 3)))
      .then(writeStartWith("a", 0, Set(1, 2)))
      .then(acceptWith(1, 0))
      .then(acceptWith(2, 1))
      .fail()
}
