// fleet.qnt — Quint model of docs/spec/fleet.md sections 3 (storage model),
// 4 (list and watch), and 5 (claims and fencing).
//
// The model has two clusters and two objects. It is small on purpose so
// `quint verify` finishes quickly. What it checks:
//
//   IndexNeverAhead   the index never records a sequence the object lacks,
//                     and never drops a name whose object is not tombstoned
//   IndexRepaired     an interrupted index write is caught up by the next
//                     write or by the sweep, never left behind silently
//   SingleHolder      at most one claim is Bound at the current fence
//   FenceBounded      no claim carries a fence the lock has not minted
//   NoStaleEffect     a receiver never accepts a fence lower than one it
//                     already accepted (a zombie cannot overwrite a successor)
//   WatchGapFree      a watch delivers index sequences 1..cursor, in order,
//                     with no gaps
//
// Negative controls (`stepIndexFirst`, `stepIncrementIndex`,
// `stepNoFenceCheck`, `stepDeleteIndexFirst`, `stepWatchJump`) each drop one
// rule and MUST violate the matching invariant. scripts/quint-check.sh runs
// both directions.
module fleet {
  pure val NAMES = Set("a", "b")
  pure val CLUSTERS = Set("east", "west")
  pure val MAX_SEQ = 3
  pure val MAX_FENCE = 3
  // Tombstone marker. In objSeq it marks a deleted object register. In
  // idxEntries it marks a name removed from the index register.
  pure val TOMB = -1

  type Phase = Pending | Bound | Lost
  type Holder = Nobody | Held(str)

  // ---- storage model (spec §3) ------------------------------------------
  var objSeq: str -> int          // object register sequence; 0 = absent, TOMB = deleted
  var idxEntries: str -> int      // index register: name -> recorded sequence; TOMB = removed
  var idxSeq: int                 // index register sequence (the list RV)
  var pending: Set[str]           // object written, index write outstanding
  var dirty: Set[str]             // index write was lost to a crash

  // ---- list and watch (spec §4) -----------------------------------------
  // One watch that starts at resourceVersion 0. The cursor never passes
  // idxSeq, so the MAX_SEQ and MAX_FENCE bounds also bound the cursor.
  var watchCursor: int            // last index sequence the watch delivered
  var delivered: List[int]        // index sequences delivered, in order

  // ---- claims and fencing (spec §5) -------------------------------------
  var holder: str -> Holder       // lock holder per object
  var fence: str -> int           // last fence minted per object
  var phase: (str, str) -> Phase  // (cluster, name) -> claim phase
  var claimFence: (str, str) -> int
  var accepted: str -> int        // highest fence the receiver accepted
  var regressions: int            // accepted effects with a lower fence

  action init = all {
    objSeq' = NAMES.mapBy(n => 0),
    idxEntries' = NAMES.mapBy(n => 0),
    idxSeq' = 0,
    pending' = Set(),
    dirty' = Set(),
    holder' = NAMES.mapBy(n => Nobody),
    fence' = NAMES.mapBy(n => 0),
    phase' = tuples(CLUSTERS, NAMES).mapBy(p => Pending),
    claimFence' = tuples(CLUSTERS, NAMES).mapBy(p => 0),
    accepted' = NAMES.mapBy(n => 0),
    regressions' = 0,
    watchCursor' = 0,
    delivered' = List(),
  }

  // ---- §3 actions ---------------------------------------------------------

  //= docs/spec/fleet.md#3-storage-model
  //# A mutation MUST write the object register before the index register.
  action writeObject(n) = all {
    pending == Set(),
    objSeq.get(n) != TOMB,
    objSeq.get(n) < MAX_SEQ,
    objSeq' = objSeq.set(n, objSeq.get(n) + 1),
    pending' = Set(n),
    idxEntries' = idxEntries, idxSeq' = idxSeq, dirty' = dirty,
    holder' = holder, fence' = fence, phase' = phase,
    claimFence' = claimFence, accepted' = accepted, regressions' = regressions,
    watchCursor' = watchCursor, delivered' = delivered,
  }

  //= docs/spec/fleet.md#3-storage-model
  //# When the index write of a mutation did not complete, the next index
  //# write for that object MUST record the object register's current
  //# sequence.
  // For a tombstoned object the current sequence is TOMB, so this same
  // write is the follow-up index removal of a delete.
  action writeIndex = {
    nondet n = oneOf(NAMES)
    all {
      pending.contains(n),
      idxEntries' = idxEntries.set(n, objSeq.get(n)),
      idxSeq' = idxSeq + 1,
      pending' = Set(),
      dirty' = dirty.exclude(Set(n)),
      objSeq' = objSeq,
      holder' = holder, fence' = fence, phase' = phase,
      claimFence' = claimFence, accepted' = accepted, regressions' = regressions,
      watchCursor' = watchCursor, delivered' = delivered,
    }
  }

  //= docs/spec/fleet.md#3-storage-model
  //# A delete MUST tombstone the object register before it removes the name
  //# from the index register.
  // The tombstone is the first write. writeIndex removes the name next,
  // and crash may fall between the two.
  action deleteObject(n) = all {
    pending == Set(),
    objSeq.get(n) > 0,
    objSeq' = objSeq.set(n, TOMB),
    pending' = Set(n),
    idxEntries' = idxEntries, idxSeq' = idxSeq, dirty' = dirty,
    holder' = holder, fence' = fence, phase' = phase,
    claimFence' = claimFence, accepted' = accepted, regressions' = regressions,
    watchCursor' = watchCursor, delivered' = delivered,
  }

  // The process dies between the two writes.
  action crash = all {
    pending != Set(),
    dirty' = dirty.union(pending),
    pending' = Set(),
    objSeq' = objSeq, idxEntries' = idxEntries, idxSeq' = idxSeq,
    holder' = holder, fence' = fence, phase' = phase,
    claimFence' = claimFence, accepted' = accepted, regressions' = regressions,
    watchCursor' = watchCursor, delivered' = delivered,
  }

  //= docs/spec/fleet.md#3-storage-model
  //# The extension server MUST reconcile the index register against the
  //# object registers at startup.
  action sweep = all {
    dirty != Set(),
    pending == Set(),
    idxEntries' = NAMES.mapBy(n => if (dirty.contains(n)) objSeq.get(n) else idxEntries.get(n)),
    idxSeq' = idxSeq + 1,
    dirty' = Set(),
    pending' = pending, objSeq' = objSeq,
    holder' = holder, fence' = fence, phase' = phase,
    claimFence' = claimFence, accepted' = accepted, regressions' = regressions,
    watchCursor' = watchCursor, delivered' = delivered,
  }

  // ---- §4 actions ---------------------------------------------------------

  //= docs/spec/fleet.md#4-list-and-watch
  //# A watch from a resourceVersion MUST deliver every index change after
  //# that version, in order, with no gaps.
  // The watch delivers the next index sequence, one at a time.
  action deliver = all {
    watchCursor < idxSeq,
    watchCursor' = watchCursor + 1,
    delivered' = delivered.append(watchCursor + 1),
    objSeq' = objSeq, idxEntries' = idxEntries, idxSeq' = idxSeq,
    pending' = pending, dirty' = dirty,
    holder' = holder, fence' = fence, phase' = phase,
    claimFence' = claimFence, accepted' = accepted, regressions' = regressions,
  }

  // ---- §5 actions ---------------------------------------------------------

  //= docs/spec/fleet.md#5-claims-and-fencing
  //# Every successful acquisition MUST mint a fence strictly greater than
  //# every fence previously minted for that object.
  action acquire(c, n) = all {
    objSeq.get(n) > 0,
    holder.get(n) == Nobody,
    phase.get((c, n)) == Pending,
    fence.get(n) < MAX_FENCE,
    holder' = holder.set(n, Held(c)),
    fence' = fence.set(n, fence.get(n) + 1),
    phase' = phase.set((c, n), Bound),
    claimFence' = claimFence.set((c, n), fence.get(n) + 1),
    objSeq' = objSeq, idxEntries' = idxEntries, idxSeq' = idxSeq,
    pending' = pending, dirty' = dirty,
    accepted' = accepted, regressions' = regressions,
    watchCursor' = watchCursor, delivered' = delivered,
  }

  // The holder's session lapses. The holder does not know yet: its claim
  // stays Bound until it observes the loss. That is the zombie.
  action expire(n) = all {
    holder.get(n) != Nobody,
    holder' = holder.set(n, Nobody),
    objSeq' = objSeq, idxEntries' = idxEntries, idxSeq' = idxSeq,
    pending' = pending, dirty' = dirty,
    fence' = fence, phase' = phase, claimFence' = claimFence,
    accepted' = accepted, regressions' = regressions,
    watchCursor' = watchCursor, delivered' = delivered,
  }

  //= docs/spec/fleet.md#5-claims-and-fencing
  //# When a claim's session lapses, the controller MUST set the claim to
  //# Lost.
  action observeLost(c, n) = all {
    phase.get((c, n)) == Bound,
    holder.get(n) != Held(c),
    phase' = phase.set((c, n), Lost),
    objSeq' = objSeq, idxEntries' = idxEntries, idxSeq' = idxSeq,
    pending' = pending, dirty' = dirty,
    holder' = holder, fence' = fence, claimFence' = claimFence,
    accepted' = accepted, regressions' = regressions,
    watchCursor' = watchCursor, delivered' = delivered,
  }

  //= docs/spec/fleet.md#5-claims-and-fencing
  //# Deleting a Bound claim MUST release the object's lock.
  action release(c, n) = all {
    phase.get((c, n)) == Bound,
    holder.get(n) == Held(c),
    holder' = holder.set(n, Nobody),
    phase' = phase.set((c, n), Pending),
    objSeq' = objSeq, idxEntries' = idxEntries, idxSeq' = idxSeq,
    pending' = pending, dirty' = dirty,
    fence' = fence, claimFence' = claimFence,
    accepted' = accepted, regressions' = regressions,
    watchCursor' = watchCursor, delivered' = delivered,
  }

  // A Bound claim (possibly a zombie) applies an effect carrying its fence.
  //= docs/spec/fleet.md#5-claims-and-fencing
  //# A receiver MUST reject an effect whose fence is lower than the highest
  //# fence it has accepted for that object.
  action applyEffect(c, n) = all {
    phase.get((c, n)) == Bound,
    accepted' = if (claimFence.get((c, n)) >= accepted.get(n))
                  accepted.set(n, claimFence.get((c, n)))
                else accepted,
    regressions' = regressions,
    watchCursor' = watchCursor, delivered' = delivered,
    objSeq' = objSeq, idxEntries' = idxEntries, idxSeq' = idxSeq,
    pending' = pending, dirty' = dirty,
    holder' = holder, fence' = fence, phase' = phase, claimFence' = claimFence,
  }

  // ---- the good step --------------------------------------------------------
  action anyWriteObject = { nondet n = oneOf(NAMES) writeObject(n) }
  action anyDeleteObject = { nondet n = oneOf(NAMES) deleteObject(n) }
  action anyAcquire = { nondet c = oneOf(CLUSTERS) nondet n = oneOf(NAMES) acquire(c, n) }
  action anyExpire = { nondet n = oneOf(NAMES) expire(n) }
  action anyObserveLost = { nondet c = oneOf(CLUSTERS) nondet n = oneOf(NAMES) observeLost(c, n) }
  action anyRelease = { nondet c = oneOf(CLUSTERS) nondet n = oneOf(NAMES) release(c, n) }
  action anyApplyEffect = { nondet c = oneOf(CLUSTERS) nondet n = oneOf(NAMES) applyEffect(c, n) }

  action step = any {
    anyWriteObject, anyDeleteObject, writeIndex, crash, sweep, deliver,
    anyAcquire, anyExpire, anyObserveLost, anyRelease, anyApplyEffect,
  }

  // ---- invariants -------------------------------------------------------------

  //= docs/spec/fleet.md#3-storage-model
  //# The index register MUST NOT record a sequence higher than the object
  //# register holds.
  //= docs/spec/fleet.md#3-storage-model
  //# A delete MUST tombstone the object register before it removes the name
  //# from the index register.
  // A tombstoned object may still be in the index. The reverse, a name
  // removed from the index while its object register is live, is a
  // violation.
  val IndexNeverAhead = NAMES.forall(n =>
    if (objSeq.get(n) == TOMB) true
    else idxEntries.get(n) != TOMB and idxEntries.get(n) <= objSeq.get(n))

  // An object may lag in the index only while its write is in flight or
  // while a crash left it dirty. Once neither holds, index == object.
  val IndexRepaired = NAMES.forall(n =>
    pending.contains(n) or dirty.contains(n) or idxEntries.get(n) == objSeq.get(n))

  //= docs/spec/fleet.md#5-claims-and-fencing
  //# At most one claim MUST be Bound to an object at the object's current
  //# fence.
  val SingleHolder = NAMES.forall(n =>
    CLUSTERS.filter(c => phase.get((c, n)) == Bound and claimFence.get((c, n)) == fence.get(n)).size() <= 1)

  val FenceBounded = tuples(CLUSTERS, NAMES).forall(p => claimFence.get(p) <= fence.get(p._2))

  val NoStaleEffect = regressions == 0

  //= docs/spec/fleet.md#4-list-and-watch
  //# A watch from a resourceVersion MUST deliver every index change after
  //# that version, in order, with no gaps.
  // The watch starts at 0, so delivered is exactly 1..watchCursor.
  val WatchGapFree = and {
    watchCursor <= idxSeq,
    delivered.length() == watchCursor,
    delivered.indices().forall(i => delivered[i] == i + 1),
  }

  val Safety = IndexNeverAhead and IndexRepaired and SingleHolder and FenceBounded
    and NoStaleEffect and WatchGapFree

  // ---- negative controls: each drops one rule and must fail ------------------

  // Bug 1: write the index before the object.
  action writeIndexFirst(n) = all {
    pending == Set(),
    objSeq.get(n) < MAX_SEQ,
    idxEntries' = idxEntries.set(n, objSeq.get(n) + 1),
    idxSeq' = idxSeq + 1,
    pending' = Set(n),
    objSeq' = objSeq, dirty' = dirty,
    holder' = holder, fence' = fence, phase' = phase,
    claimFence' = claimFence, accepted' = accepted, regressions' = regressions,
    watchCursor' = watchCursor, delivered' = delivered,
  }
  action anyWriteIndexFirst = { nondet n = oneOf(NAMES) writeIndexFirst(n) }
  action stepIndexFirst = any { anyWriteIndexFirst, writeIndex, crash, sweep }

  // Bug 2: the index write increments instead of copying the object seq,
  // so a crashed write is never caught up.
  action writeIndexIncrement = {
    nondet n = oneOf(NAMES)
    all {
      pending.contains(n),
      idxEntries' = idxEntries.set(n, idxEntries.get(n) + 1),
      idxSeq' = idxSeq + 1,
      pending' = Set(),
      dirty' = dirty.exclude(Set(n)),
      objSeq' = objSeq,
      holder' = holder, fence' = fence, phase' = phase,
      claimFence' = claimFence, accepted' = accepted, regressions' = regressions,
      watchCursor' = watchCursor, delivered' = delivered,
    }
  }
  action stepIncrementIndex = any { anyWriteObject, writeIndexIncrement, crash }

  // Bug 3: the receiver applies whatever fence arrives.
  action applyEffectNoCheck(c, n) = all {
    phase.get((c, n)) == Bound,
    accepted' = accepted.set(n, claimFence.get((c, n))),
    regressions' = if (claimFence.get((c, n)) < accepted.get(n)) regressions + 1 else regressions,
    watchCursor' = watchCursor, delivered' = delivered,
    objSeq' = objSeq, idxEntries' = idxEntries, idxSeq' = idxSeq,
    pending' = pending, dirty' = dirty,
    holder' = holder, fence' = fence, phase' = phase, claimFence' = claimFence,
  }
  action anyApplyEffectNoCheck = { nondet c = oneOf(CLUSTERS) nondet n = oneOf(NAMES) applyEffectNoCheck(c, n) }
  action stepNoFenceCheck = any {
    anyWriteObject, writeIndex, anyAcquire, anyExpire, anyApplyEffectNoCheck,
  }

  // Bug 4: a delete removes the name from the index before it tombstones
  // the object register.
  action removeIndexFirst(n) = all {
    pending == Set(),
    objSeq.get(n) > 0,
    idxEntries' = idxEntries.set(n, TOMB),
    idxSeq' = idxSeq + 1,
    pending' = Set(n),
    objSeq' = objSeq, dirty' = dirty,
    holder' = holder, fence' = fence, phase' = phase,
    claimFence' = claimFence, accepted' = accepted, regressions' = regressions,
    watchCursor' = watchCursor, delivered' = delivered,
  }
  action anyRemoveIndexFirst = { nondet n = oneOf(NAMES) removeIndexFirst(n) }
  action stepDeleteIndexFirst = any { anyWriteObject, writeIndex, anyRemoveIndexFirst, crash, sweep }

  // Bug 5: the watch jumps to the latest index sequence and skips the
  // changes in between.
  action deliverJump = all {
    watchCursor < idxSeq,
    watchCursor' = idxSeq,
    delivered' = delivered.append(idxSeq),
    objSeq' = objSeq, idxEntries' = idxEntries, idxSeq' = idxSeq,
    pending' = pending, dirty' = dirty,
    holder' = holder, fence' = fence, phase' = phase,
    claimFence' = claimFence, accepted' = accepted, regressions' = regressions,
  }
  action stepWatchJump = any { anyWriteObject, writeIndex, deliverJump }

  // ---- witness runs (`quint test`) ---------------------------------------------

  // A zombie's late effect is rejected once a successor has applied.
  run zombieIsFencedTest =
    init
      .then(writeObject("a")).then(writeIndex)
      .then(acquire("east", "a"))
      .then(expire("a"))
      .then(acquire("west", "a"))
      .then(applyEffect("west", "a"))
      .then(applyEffect("east", "a"))
      .expect(accepted.get("a") == 2 and regressions == 0)

  // A crash between the two writes leaves the object invisible to lists
  // until the sweep runs. This is the window the spec's startup and
  // interval reconciliation exists to close.
  run crashLeavesIndexBehindTest =
    init
      .then(writeObject("a")).then(crash)
      .expect(objSeq.get("a") == 1 and idxEntries.get("a") == 0 and dirty == Set("a"))
      .then(sweep)
      .expect(idxEntries.get("a") == 1 and dirty == Set())

  // The next write to a dirty object also repairs it.
  run nextWriteRepairsTest =
    init
      .then(writeObject("a")).then(crash)
      .then(writeObject("a")).then(writeIndex)
      .expect(idxEntries.get("a") == 2 and dirty == Set())

  // A crash between the tombstone and the index removal leaves a
  // tombstoned name in the index. That is allowed. The sweep removes it.
  run deleteTombstonesFirstTest =
    init
      .then(writeObject("a")).then(writeIndex)
      .then(deleteObject("a"))
      .expect(objSeq.get("a") == TOMB and idxEntries.get("a") == 1 and IndexNeverAhead)
      .then(crash)
      .expect(dirty == Set("a") and IndexNeverAhead)
      .then(sweep)
      .expect(idxEntries.get("a") == TOMB and dirty == Set() and IndexNeverAhead)

  // Without a crash, the next index write is the removal.
  run deleteThenRemoveTest =
    init
      .then(writeObject("a")).then(writeIndex)
      .then(deleteObject("a")).then(writeIndex)
      .expect(idxEntries.get("a") == TOMB and idxSeq == 2 and pending == Set())

  // A watch that falls behind catches up one index change at a time,
  // including the sweep's repair and the delete's index removal.
  run watchDeliversEveryChangeTest =
    init
      .then(writeObject("a")).then(writeIndex)
      .then(writeObject("b")).then(crash).then(sweep)
      .then(deleteObject("a")).then(writeIndex)
      .expect(idxSeq == 3 and watchCursor == 0 and WatchGapFree)
      .then(deliver).then(deliver).then(deliver)
      .expect(watchCursor == 3 and delivered == [1, 2, 3] and WatchGapFree)
}
