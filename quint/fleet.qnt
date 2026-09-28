// fleet.qnt — Quint model of docs/spec/fleet.md sections 3 (storage model),
// 4 (list and watch), and 5 (claims and fencing).
//
// The model has two names, two concurrent writers, and two clusters for
// claims. It is small on purpose so `quint verify` finishes quickly.
//
// Storage. Each name has one object register: a head sequence, a live
// flag, and the sequence of the create that began the head's
// incarnation (its UID). A delete writes a tombstone, and a later create
// starts a new incarnation, so a name can be deleted and created again.
// The index register maps each name to an entry, and idxHist keeps every
// index version with (entry, index sequence that recorded it,
// incarnation). A writer writes the object register in one step and the
// index register in a later step, so writers interleave, crash between
// the steps, and overwrite each other's writes before those are indexed.
// A write compares and sets on the object sequence that the index entry
// records, as the Store does.
//
// What it checks:
//
//   IndexNeverAhead   the index never records a sequence the object lacks,
//                     and never drops the entry of a live incarnation
//   IndexRepaired     an interrupted index write is caught up by the next
//                     write, a repair, or the sweep, never left behind
//   SingleHolder      at most one claim is Bound at the current fence
//   FenceBounded      no claim carries a fence the lock has not minted
//   NoStaleEffect     a receiver never accepts a fence lower than one it
//                     already accepted (a zombie cannot overwrite a successor)
//   WatchGapFree      a watch delivers index sequences 1..cursor, in order,
//                     with no gaps
//   ObservedRVIsWatchPosition
//                     every resourceVersion a client observes (get, list,
//                     watch event) is an index sequence at or below idxSeq,
//                     and the index at that sequence holds the version the
//                     client saw
//   WriteResponseIsOwnVersion
//                     a create, update, or delete answers with its own
//                     version, at the index sequence that recorded it (for
//                     a delete: the step that removed the name)
//   NoIncarnationCollapse
//                     no index step changes a name from one live
//                     incarnation to another, so a watch reports a delete
//                     and a create as two events
//   ReflectorMatchesIndex
//                     a reflector that resumes after every step from the
//                     resourceVersion of the last event it got holds
//                     exactly the index at that resourceVersion: it has
//                     skipped and replayed nothing
//
// What it does not prove: the model has no compaction, so it does not
// cover RecordedAt or RemovedAt failing on a trimmed history. The watch
// has no predicate, no bookmarks, and no initial events. A write's object
// step is atomic with its read of the index entry; in the code a read and
// a compare-and-set are two steps, and the compare-and-set fails when
// anything moved. Claims and storage do not interact beyond "the object
// is live".
//
// Bounds: `quint verify` checks every behaviour up to 7 steps. It was 12
// before the model had two writers, recreate, and a reflector; 8 steps
// took about 25 minutes, too close to the 30-minute CI limit. The random
// simulation in the gate runs 2000 traces of 30 steps.
//
// Negative controls each drop one rule and MUST violate the matching
// invariant. scripts/quint-check.sh runs both directions.
//
// Gate checks, read by scripts/quint-check.sh:
// quint-check: invariants=IndexNeverAhead,IndexRepaired,SingleHolder,FenceBounded,NoStaleEffect,WatchGapFree,ObservedRVIsWatchPosition,WriteResponseIsOwnVersion,NoIncarnationCollapse,ReflectorMatchesIndex
// quint-check: control=stepIndexFirst:IndexNeverAhead
// quint-check: control=stepIncrementIndex:IndexRepaired
// quint-check: control=stepNoFenceCheck:NoStaleEffect
// quint-check: control=stepDeleteIndexFirst:IndexNeverAhead
// quint-check: control=stepWatchJump:WatchGapFree
// quint-check: control=stepReacquireAnyone:SingleHolder
// quint-check: control=stepObjectSeqRV:ObservedRVIsWatchPosition
// quint-check: control=stepOtherWriteResponse:WriteResponseIsOwnVersion
// quint-check: control=stepCreateUnguarded:NoIncarnationCollapse
// quint-check: control=stepReflectorObjectSeqRV:ReflectorMatchesIndex
// quint-check: verify-steps=7
// quint-check: verify-split=each
module fleet {
  pure val NAMES = Set("a", "b")
  pure val CLUSTERS = Set("east", "west")
  pure val WRITERS = Set("w1", "w2")
  pure val MAX_SEQ = 4
  pure val MAX_FENCE = 3

  type Phase = Pending | Bound | Lost
  type Holder = Nobody | Held(str)
  // A writer between its object write and its index write holds the name
  // and the index entry that its write produces: the object sequence of
  // a create or an update, or -t for a delete that wrote tombstone t.
  type WState = Idle | Wrote((str, int))

  // ---- storage model (spec §3) ------------------------------------------
  var objSeq: str -> int          // head sequence; 0 = absent. A tombstone is a version too.
  var objLive: str -> bool        // the head is live
  var objBorn: str -> int         // sequence of the create that began the head's incarnation
  // Index entry: > 0 records that object sequence; 0 = never named;
  // -t = removed after tombstone t.
  var idxEntries: str -> int
  var idxSeq: int                 // index register sequence (the list RV)
  // idxHist[k] is the index at index sequence k: per name (entry, index
  // sequence that recorded the entry, incarnation). idxHist[idxSeq] is
  // the current index.
  var idxHist: List[str -> (int, int, int)]
  var writer: str -> WState
  var dirty: Set[str]             // a crash lost the index write of this name
  // The last step's write responses: (name, own entry, entry of the
  // version returned, resourceVersion returned).
  var responses: Set[(str, int, int, int)]
  // The last step's reads: (name, entry seen, resourceVersion reported).
  // idxHist only grows, so checking each observation in the state where
  // it is made covers all of them, and keeps `quint verify` fast.
  var observed: Set[(str, int, int)]

  // ---- list and watch (spec §4) -----------------------------------------
  // One watch that starts at resourceVersion 0.
  var watchCursor: int            // last index sequence the watch delivered
  var delivered: List[int]        // index sequences delivered, in order
  // One reflector: its cache of entries and the resourceVersion it
  // resumes from.
  var rCache: str -> int
  var rPos: int

  // ---- claims and fencing (spec §5) -------------------------------------
  var holder: str -> Holder       // lock holder per object
  var fence: str -> int           // last fence minted per object
  var phase: (str, str) -> Phase  // (cluster, name) -> claim phase
  var claimFence: (str, str) -> int
  var accepted: str -> int        // highest fence the receiver accepted
  var regressions: int            // accepted effects with a lower fence

  action init = all {
    objSeq' = NAMES.mapBy(n => 0),
    objLive' = NAMES.mapBy(n => false),
    objBorn' = NAMES.mapBy(n => 0),
    idxEntries' = NAMES.mapBy(n => 0),
    idxSeq' = 0,
    idxHist' = [NAMES.mapBy(n => (0, 0, 0))],
    writer' = WRITERS.mapBy(w => Idle),
    dirty' = Set(),
    responses' = Set(),
    observed' = Set(),
    watchCursor' = 0,
    delivered' = List(),
    rCache' = NAMES.mapBy(n => 0),
    rPos' = 0,
    holder' = NAMES.mapBy(n => Nobody),
    fence' = NAMES.mapBy(n => 0),
    phase' = tuples(CLUSTERS, NAMES).mapBy(p => Pending),
    claimFence' = tuples(CLUSTERS, NAMES).mapBy(p => 0),
    accepted' = NAMES.mapBy(n => 0),
    regressions' = 0,
  }

  // ---- helpers --------------------------------------------------------------

  // The entry that the index write for n records now.
  def headEntry(n: str): int =
    if (objLive.get(n)) objSeq.get(n)
    else if (objSeq.get(n) == 0) 0
    else -objSeq.get(n)

  // The name and own entry of a writer that wrote. Idle gives ("", 0);
  // callers check for Idle first.
  pure def wrote(s: WState): (str, int) = match s {
    | Idle => ("", 0)
    | Wrote(p) => p
  }

  // Names that a writer wrote and has not indexed yet.
  val pendingNames: Set[str] = WRITERS.fold(Set(), (acc, w) =>
    match writer.get(w) {
      | Idle => acc
      | Wrote(p) => acc.union(Set(p._1))
    })

  // record appends the index version that holds entries. A changed entry
  // gets the new index sequence and the incarnation of the object head.
  // An entry that did not change keeps the index sequence that recorded it.
  pure def record(hist: List[str -> (int, int, int)], entries: str -> int,
                  born: str -> int): List[str -> (int, int, int)] = {
    val prev = hist[hist.length() - 1]
    val k = hist.length()
    hist.append(NAMES.mapBy(m =>
      if (prev.get(m)._1 == entries.get(m)) prev.get(m) else (entries.get(m), k, born.get(m))))
  }

  // The index sequence at which hist recorded entry own for n, or -1.
  pure def recordedAt(hist: List[str -> (int, int, int)], n: str, own: int): int =
    hist.indices().fold(-1, (acc, k) =>
      if (acc == -1 and hist[k].get(n)._1 == own) hist[k].get(n)._2 else acc)

  action keepClaims = all {
    holder' = holder, fence' = fence, phase' = phase,
    claimFence' = claimFence, accepted' = accepted, regressions' = regressions,
  }
  action keepWatch = all { watchCursor' = watchCursor, delivered' = delivered }
  action keepReflector = all { rCache' = rCache, rPos' = rPos }
  action keepObj = all { objSeq' = objSeq, objLive' = objLive, objBorn' = objBorn }
  action keepIdx = all { idxEntries' = idxEntries, idxSeq' = idxSeq, idxHist' = idxHist }
  action quiet = all { responses' = Set(), observed' = Set() }
  action keepStore = all { keepObj, keepIdx, writer' = writer, dirty' = dirty }
  // Everything outside storage stays.
  action keepRest = all { keepWatch, keepReflector, keepClaims }

  // An index write that sets entries. It is one index step, and it runs
  // only when an entry changes.
  action indexTo(entries: str -> int): bool = all {
    entries != idxEntries,
    idxEntries' = entries,
    idxSeq' = idxSeq + 1,
    idxHist' = record(idxHist, entries, objBorn),
  }

  // ---- §3 actions ---------------------------------------------------------

  // An update compares and sets on the object sequence that the index
  // entry records.
  //= docs/spec/fleet.md#3-storage-model
  //= type=implication
  //# A mutation MUST write the object register before the index register.
  action update(w, n) = all {
    writer.get(w) == Idle,
    objLive.get(n),
    idxEntries.get(n) == objSeq.get(n),
    objSeq.get(n) < MAX_SEQ,
    objSeq' = objSeq.set(n, objSeq.get(n) + 1),
    objLive' = objLive, objBorn' = objBorn,
    writer' = writer.set(w, Wrote((n, objSeq.get(n) + 1))),
    keepIdx, dirty' = dirty, quiet, keepRest,
  }

  //= docs/spec/fleet.md#3-storage-model
  //= type=implication
  //# A delete MUST tombstone the object register before it removes the name
  //# from the index register.
  // The tombstone is the first write. The writer's index write removes the
  // name next, and a crash may fall between the two.
  action delete(w, n) = all {
    writer.get(w) == Idle,
    objLive.get(n),
    idxEntries.get(n) == objSeq.get(n),
    objSeq.get(n) < MAX_SEQ,
    objSeq' = objSeq.set(n, objSeq.get(n) + 1),
    objLive' = objLive.set(n, false), objBorn' = objBorn,
    writer' = writer.set(w, Wrote((n, -(objSeq.get(n) + 1)))),
    keepIdx, dirty' = dirty, quiet, keepRest,
  }

  //= docs/spec/fleet.md#3-storage-model
  //= type=implication
  //# A create MUST use a compare-and-set that requires the object register
  //# to be absent.
  //= docs/spec/fleet.md#3-storage-model
  //= type=implication
  //# A create MUST NOT write over a tombstone while the index register
  //# still names the deleted object.
  // The create starts a new incarnation. When the index still names the
  // deleted object, the create waits: `repair` records the removal first.
  action create(w, n) = all {
    writer.get(w) == Idle,
    not(objLive.get(n)),
    idxEntries.get(n) <= 0,
    objSeq.get(n) < MAX_SEQ,
    objSeq' = objSeq.set(n, objSeq.get(n) + 1),
    objLive' = objLive.set(n, true),
    objBorn' = objBorn.set(n, objSeq.get(n) + 1),
    writer' = writer.set(w, Wrote((n, objSeq.get(n) + 1))),
    keepIdx, dirty' = dirty, quiet, keepRest,
  }

  //= docs/spec/fleet.md#3-storage-model
  //= type=implication
  //# When the index write of a mutation did not complete, the next index
  //# write for that object MUST record the object register's current
  //# sequence.
  //= docs/spec/fleet.md#3-storage-model
  //= type=implication
  //# Each index entry MUST record both the object register sequence and
  //# the index sequence at which the index register recorded it.
  //= docs/spec/fleet.md#3-storage-model
  //= type=implication
  //# A mutation whose object write committed MUST retry its index write
  //# when that index write loses a round or has an unknown outcome.
  // The writer's index write records the head, which may be newer than
  // its own write. The response looks its own entry up in the history.
  // An attempt that lost its round and still landed is a `repair` step
  // before `finish`. An attempt that did not land changes nothing.
  action finish(w) = all {
    writer.get(w) != Idle,
    { val p = wrote(writer.get(w))
      {
        val n = p._1
        val own = p._2
        val entries = idxEntries.set(n, headEntry(n))
        val changed = entries != idxEntries
        val hist = if (changed) record(idxHist, entries, objBorn) else idxHist
        all {
          idxEntries' = entries,
          idxSeq' = if (changed) idxSeq + 1 else idxSeq,
          idxHist' = hist,
          responses' = Set((n, own, own, recordedAt(hist, n, own))),
          observed' = Set(),
          writer' = writer.set(w, Idle),
          dirty' = dirty.exclude(Set(n)),
          keepObj, keepRest,
        }
      }
    },
  }

  // The process dies between the two writes.
  action crash(w) = all {
    writer.get(w) != Idle,
    { val p = wrote(writer.get(w))
      all {
        writer' = writer.set(w, Idle),
        dirty' = dirty.union(Set(p._1)),
        keepObj, keepIdx, quiet, keepRest,
      }
    },
  }

  // An index write for one name with no response: the index write that a
  // write runs after its compare-and-set fails, and the one that a create
  // runs to record a removal first.
  action repair(n) = all {
    indexTo(idxEntries.set(n, headEntry(n))),
    dirty' = dirty.exclude(Set(n)),
    writer' = writer, keepObj, quiet, keepRest,
  }

  //= docs/spec/fleet.md#3-storage-model
  //= type=implication
  //# The extension server MUST reconcile the index register against the
  //# object registers at startup.
  action sweep = {
    val entries = NAMES.mapBy(n => if (dirty.contains(n)) headEntry(n) else idxEntries.get(n))
    all {
      dirty != Set(),
      dirty' = Set(),
      if (entries != idxEntries) indexTo(entries) else keepIdx,
      writer' = writer, keepObj, quiet, keepRest,
    }
  }

  // ---- §4 actions ---------------------------------------------------------

  // The watch delivers the next index sequence, one at a time. Each name
  // that the step changed gives one event, which reports the index
  // sequence of the entry. A removal reports the removal step.
  //= docs/spec/fleet.md#4-list-and-watch
  //= type=implication
  //# A watch from a resourceVersion MUST deliver every index change after
  //# that version, in order, with no gaps.
  //= docs/spec/fleet.md#4-list-and-watch
  //= type=implication
  //# Each watch event MUST carry the object at the sequence the index
  //# recorded.
  action deliver = {
    val k = watchCursor + 1
    all {
      watchCursor < idxSeq,
      watchCursor' = k,
      delivered' = delivered.append(k),
      responses' = Set(),
      observed' = NAMES.filter(n => idxHist[k].get(n)._1 != idxHist[k - 1].get(n)._1)
        .map(n => (n, idxHist[k].get(n)._1, idxHist[k].get(n)._2)),
      keepStore, keepReflector, keepClaims,
    }
  }

  // A get reads the index entry, then the object at the object sequence
  // the entry records. It reports the entry's index sequence.
  //= docs/spec/fleet.md#3-storage-model
  //= type=implication
  //# A get MUST serve an object only once the index register records it.
  //= docs/spec/fleet.md#3-storage-model
  //= type=implication
  //# A get MUST read the object at the object register sequence that the
  //# index entry records.
  //= docs/spec/fleet.md#3-storage-model
  //= type=implication
  //# An object's resourceVersion MUST be the index sequence at which the
  //# index register recorded that object version.
  action observeGet(n) = {
    val e = idxHist[idxSeq].get(n)
    all {
      e._1 > 0,
      observed' = Set((n, e._1, e._2)),
      responses' = Set(),
      keepStore, keepRest,
    }
  }

  // A list reads one index version and reports each item at its entry.
  action observeList = {
    val cur = idxHist[idxSeq]
    all {
      observed' = NAMES.filter(n => cur.get(n)._1 > 0).map(n => (n, cur.get(n)._1, cur.get(n)._2)),
      responses' = Set(),
      keepStore, keepRest,
    }
  }

  // The reflector resumes from rPos, takes the events of the next index
  // step, and keeps the resourceVersion of the last event as its new
  // resume point. Each step is a stop and a resume.
  action reflect = {
    val k = rPos + 1
    val changed = NAMES.filter(n => idxHist[k].get(n)._1 != idxHist[k - 1].get(n)._1)
    all {
      rPos >= 0,
      rPos < idxSeq,
      rCache' = NAMES.mapBy(n => if (changed.contains(n)) idxHist[k].get(n)._1 else rCache.get(n)),
      rPos' = changed.fold(0, (acc, n) => if (idxHist[k].get(n)._2 > acc) idxHist[k].get(n)._2 else acc),
      keepStore, quiet, keepWatch, keepClaims,
    }
  }

  // ---- §5 actions ---------------------------------------------------------

  //= docs/spec/fleet.md#5-claims-and-fencing
  //= type=implication
  //# Every successful acquisition MUST mint a fence strictly greater than
  //# every fence previously minted for that object.
  action acquire(c, n) = all {
    objLive.get(n),
    holder.get(n) == Nobody,
    phase.get((c, n)) == Pending,
    fence.get(n) < MAX_FENCE,
    holder' = holder.set(n, Held(c)),
    fence' = fence.set(n, fence.get(n) + 1),
    phase' = phase.set((c, n), Bound),
    claimFence' = claimFence.set((c, n), fence.get(n) + 1),
    accepted' = accepted, regressions' = regressions,
    keepStore, quiet, keepWatch, keepReflector,
  }

  // The holder acquires again, as after a release that did not land. It
  // keeps its tenure, so the fence stays the same.
  //= docs/spec/fleet.md#5-claims-and-fencing
  //= type=implication
  //# An acquire by the session that already holds the lock MUST return the
  //# current fence and MUST NOT mint a new one.
  action reacquire(c, n) = all {
    holder.get(n) == Held(c),
    phase' = phase.set((c, n), Bound),
    claimFence' = claimFence.set((c, n), fence.get(n)),
    holder' = holder, fence' = fence,
    accepted' = accepted, regressions' = regressions,
    keepStore, quiet, keepWatch, keepReflector,
  }

  // The holder's session lapses. The holder does not know yet: its claim
  // stays Bound until it observes the loss. That is the zombie.
  action expire(n) = all {
    holder.get(n) != Nobody,
    holder' = holder.set(n, Nobody),
    fence' = fence, phase' = phase, claimFence' = claimFence,
    accepted' = accepted, regressions' = regressions,
    keepStore, quiet, keepWatch, keepReflector,
  }

  //= docs/spec/fleet.md#5-claims-and-fencing
  //= type=implication
  //# When a claim's session lapses, the controller MUST set the claim to
  //# Lost.
  action observeLost(c, n) = all {
    phase.get((c, n)) == Bound,
    holder.get(n) != Held(c),
    phase' = phase.set((c, n), Lost),
    holder' = holder, fence' = fence, claimFence' = claimFence,
    accepted' = accepted, regressions' = regressions,
    keepStore, quiet, keepWatch, keepReflector,
  }

  //= docs/spec/fleet.md#5-claims-and-fencing
  //= type=implication
  //# Deleting a Bound claim MUST release the object's lock.
  action release(c, n) = all {
    phase.get((c, n)) == Bound,
    holder.get(n) == Held(c),
    holder' = holder.set(n, Nobody),
    phase' = phase.set((c, n), Pending),
    fence' = fence, claimFence' = claimFence,
    accepted' = accepted, regressions' = regressions,
    keepStore, quiet, keepWatch, keepReflector,
  }

  // A Bound claim (possibly a zombie) applies an effect carrying its fence.
  //= docs/spec/fleet.md#5-claims-and-fencing
  //= type=implication
  //# A receiver MUST reject an effect whose fence is lower than the highest
  //# fence it has accepted for that object.
  action applyEffect(c, n) = all {
    phase.get((c, n)) == Bound,
    accepted' = if (claimFence.get((c, n)) >= accepted.get(n))
                  accepted.set(n, claimFence.get((c, n)))
                else accepted,
    regressions' = regressions,
    holder' = holder, fence' = fence, phase' = phase, claimFence' = claimFence,
    keepStore, quiet, keepWatch, keepReflector,
  }

  // ---- the good step --------------------------------------------------------
  action anyCreate = { nondet w = oneOf(WRITERS) nondet n = oneOf(NAMES) create(w, n) }
  action anyUpdate = { nondet w = oneOf(WRITERS) nondet n = oneOf(NAMES) update(w, n) }
  action anyDelete = { nondet w = oneOf(WRITERS) nondet n = oneOf(NAMES) delete(w, n) }
  action anyFinish = { nondet w = oneOf(WRITERS) finish(w) }
  action anyCrash = { nondet w = oneOf(WRITERS) crash(w) }
  action anyRepair = { nondet n = oneOf(NAMES) repair(n) }
  action anyObserveGet = { nondet n = oneOf(NAMES) observeGet(n) }
  action anyAcquire = { nondet c = oneOf(CLUSTERS) nondet n = oneOf(NAMES) acquire(c, n) }
  action anyReacquire = { nondet c = oneOf(CLUSTERS) nondet n = oneOf(NAMES) reacquire(c, n) }
  action anyExpire = { nondet n = oneOf(NAMES) expire(n) }
  action anyObserveLost = { nondet c = oneOf(CLUSTERS) nondet n = oneOf(NAMES) observeLost(c, n) }
  action anyRelease = { nondet c = oneOf(CLUSTERS) nondet n = oneOf(NAMES) release(c, n) }
  action anyApplyEffect = { nondet c = oneOf(CLUSTERS) nondet n = oneOf(NAMES) applyEffect(c, n) }

  // Writes, index writes, and crashes.
  action anyWrite = any { anyCreate, anyUpdate, anyDelete, anyFinish, anyRepair }

  action step = any {
    anyWrite, anyCrash, sweep, deliver, anyObserveGet, observeList, reflect,
    anyAcquire, anyReacquire, anyExpire, anyObserveLost, anyRelease, anyApplyEffect,
  }

  // ---- invariants -------------------------------------------------------------

  //= docs/spec/fleet.md#3-storage-model
  //= type=implication
  //# The index register MUST NOT record a sequence higher than the object
  //# register holds.
  //= docs/spec/fleet.md#3-storage-model
  //= type=implication
  //# A delete MUST tombstone the object register before it removes the name
  //# from the index register.
  // A removal entry -t names the tombstone t it recorded. It may stay
  // while a new incarnation is not indexed yet, but it must be older than
  // that incarnation: the index never drops a live incarnation's entry.
  val IndexNeverAhead = NAMES.forall(n => {
    val e = idxEntries.get(n)
    if (e >= 0) e <= objSeq.get(n)
    else and {
      -e <= objSeq.get(n),
      not(objLive.get(n)) or -e < objBorn.get(n),
    }
  })

  // An object may lag in the index only while its write is in flight or
  // while a crash left it dirty. Once neither holds, index == object.
  val IndexRepaired = NAMES.forall(n =>
    pendingNames.contains(n) or dirty.contains(n) or idxEntries.get(n) == headEntry(n))

  //= docs/spec/fleet.md#5-claims-and-fencing
  //= type=implication
  //# At most one claim MUST be Bound to an object at the object's current
  //# fence.
  val SingleHolder = NAMES.forall(n =>
    CLUSTERS.filter(c => phase.get((c, n)) == Bound and claimFence.get((c, n)) == fence.get(n)).size() <= 1)

  val FenceBounded = tuples(CLUSTERS, NAMES).forall(p => claimFence.get(p) <= fence.get(p._2))

  val NoStaleEffect = regressions == 0

  //= docs/spec/fleet.md#4-list-and-watch
  //= type=implication
  //# A watch from a resourceVersion MUST deliver every index change after
  //# that version, in order, with no gaps.
  // The watch starts at 0, so delivered is exactly 1..watchCursor.
  val WatchGapFree = and {
    watchCursor <= idxSeq,
    delivered.length() == watchCursor,
    delivered.indices().forall(i => delivered[i] == i + 1),
  }

  // atIndex says the index at sequence rv holds entry e for n.
  def atIndex(n: str, e: int, rv: int): bool =
    if (rv < 0 or rv > idxSeq) false else idxHist[rv].get(n)._1 == e

  //= docs/spec/fleet.md#4-list-and-watch
  //= type=implication
  //# Every resourceVersion that a get, a list, or a watch event reports
  //# MUST be an index sequence.
  // An observation (n, e, r) says a client saw entry e of n and was told
  // resourceVersion r. A watch resumed from r delivers the index changes
  // after r. It skips no change of n and replays none when r is an index
  // sequence the index has reached and the index at r holds e for n.
  val ObservedRVIsWatchPosition = observed.forall(o => atIndex(o._1, o._2, o._3))

  //= docs/spec/fleet.md#3-storage-model
  //= type=implication
  //# A create or an update MUST return the object version that it wrote,
  //# with the index sequence at which the index register recorded that
  //# version.
  //= docs/spec/fleet.md#3-storage-model
  //= type=implication
  //# A delete MUST return the index sequence at which the index register
  //# removed the name.
  val WriteResponseIsOwnVersion = responses.forall(r => r._3 == r._2 and atIndex(r._1, r._2, r._4))

  //= docs/spec/fleet.md#4-list-and-watch
  //= type=implication
  //# A watch MUST report the deletion of an object and the creation of a
  //# new object with the same name as separate events.
  // idxHist only grows, so checking the newest step in every state checks
  // every step.
  val NoIncarnationCollapse = idxSeq == 0 or NAMES.forall(n => {
    val p = idxHist[idxSeq - 1].get(n)
    val c = idxHist[idxSeq].get(n)
    not(p._1 > 0 and c._1 > 0) or p._3 == c._3
  })

  //= docs/spec/fleet.md#4-list-and-watch
  //= type=implication
  //# A watch that resumes from the resourceVersion of any event MUST NOT
  //# skip or replay a change.
  val ReflectorMatchesIndex =
    if (rPos < 0 or rPos > idxSeq) false
    else NAMES.forall(n => rCache.get(n) == idxHist[rPos].get(n)._1)

  val Safety = and {
    IndexNeverAhead, IndexRepaired, SingleHolder, FenceBounded, NoStaleEffect,
    WatchGapFree, ObservedRVIsWatchPosition, WriteResponseIsOwnVersion,
    NoIncarnationCollapse, ReflectorMatchesIndex,
  }

  // ---- negative controls: each drops one rule and must fail ------------------

  // Bug 1: write the index before the object.
  action writeIndexFirst(n) = all {
    objSeq.get(n) < MAX_SEQ,
    indexTo(idxEntries.set(n, objSeq.get(n) + 1)),
    writer' = writer, dirty' = dirty, keepObj, quiet, keepRest,
  }
  action anyWriteIndexFirst = { nondet n = oneOf(NAMES) writeIndexFirst(n) }
  action stepIndexFirst = any { anyWriteIndexFirst, anyWrite, anyCrash, sweep }

  // Bug 2: the index write increments the entry instead of copying the
  // object head, so it never catches up a delete or a create.
  action finishIncrement(w) = all {
    writer.get(w) != Idle,
    { val p = wrote(writer.get(w))
      all {
        indexTo(idxEntries.set(p._1, idxEntries.get(p._1) + 1)),
        writer' = writer.set(w, Idle),
        dirty' = dirty.exclude(Set(p._1)),
        keepObj, quiet, keepRest,
      }
    },
  }
  action anyFinishIncrement = { nondet w = oneOf(WRITERS) finishIncrement(w) }
  action stepIncrementIndex = any { anyCreate, anyUpdate, anyDelete, anyFinishIncrement }

  // Bug 3: the receiver applies whatever fence arrives.
  action applyEffectNoCheck(c, n) = all {
    phase.get((c, n)) == Bound,
    accepted' = accepted.set(n, claimFence.get((c, n))),
    regressions' = if (claimFence.get((c, n)) < accepted.get(n)) regressions + 1 else regressions,
    holder' = holder, fence' = fence, phase' = phase, claimFence' = claimFence,
    keepStore, quiet, keepWatch, keepReflector,
  }
  action anyApplyEffectNoCheck = { nondet c = oneOf(CLUSTERS) nondet n = oneOf(NAMES) applyEffectNoCheck(c, n) }
  action stepNoFenceCheck = any { anyCreate, anyFinish, anyAcquire, anyExpire, anyApplyEffectNoCheck }

  // Bug 4: a delete removes the name from the index before it tombstones
  // the object register.
  action removeIndexFirst(n) = all {
    objLive.get(n),
    objSeq.get(n) < MAX_SEQ,
    indexTo(idxEntries.set(n, -(objSeq.get(n) + 1))),
    writer' = writer, dirty' = dirty, keepObj, quiet, keepRest,
  }
  action anyRemoveIndexFirst = { nondet n = oneOf(NAMES) removeIndexFirst(n) }
  action stepDeleteIndexFirst = any { anyWrite, anyRemoveIndexFirst }

  // Bug 5: the watch jumps to the latest index sequence and skips the
  // changes in between.
  action deliverJump = all {
    watchCursor < idxSeq,
    watchCursor' = idxSeq,
    delivered' = delivered.append(idxSeq),
    keepStore, quiet, keepReflector, keepClaims,
  }
  action stepWatchJump = any { anyWrite, deliverJump }

  // Bug 6: the re-entrant acquire does not check that the caller holds the
  // lock, so a second cluster gets the current fence while the holder is
  // still Bound.
  action reacquireAnyone(c, n) = all {
    holder.get(n) != Nobody,
    phase' = phase.set((c, n), Bound),
    claimFence' = claimFence.set((c, n), fence.get(n)),
    holder' = holder, fence' = fence,
    accepted' = accepted, regressions' = regressions,
    keepStore, quiet, keepWatch, keepReflector,
  }
  action anyReacquireAnyone = { nondet c = oneOf(CLUSTERS) nondet n = oneOf(NAMES) reacquireAnyone(c, n) }
  action stepReacquireAnyone = any { anyCreate, anyFinish, anyAcquire, anyReacquireAnyone }

  // Bug 7: a get reads the object register head and reports its sequence
  // as the resourceVersion, as the store did before issue #150.
  action observeGetObjectSeq(n) = all {
    objLive.get(n),
    observed' = Set((n, objSeq.get(n), objSeq.get(n))),
    responses' = Set(),
    keepStore, keepRest,
  }
  action anyObserveGetObjectSeq = { nondet n = oneOf(NAMES) observeGetObjectSeq(n) }
  action stepObjectSeqRV = any { anyWrite, anyObserveGetObjectSeq }

  // Bug 8: when a newer write is indexed first, the write answers with the
  // version that the index now records, at its index sequence. The client
  // gets another write's object, and an update at that resourceVersion
  // overwrites a version the client never saw.
  action finishOtherResponse(w) = all {
    writer.get(w) != Idle,
    { val p = wrote(writer.get(w))
      {
        val n = p._1
        val entries = idxEntries.set(n, headEntry(n))
        val changed = entries != idxEntries
        val hist = if (changed) record(idxHist, entries, objBorn) else idxHist
        val cur = hist[hist.length() - 1].get(n)
        all {
          idxEntries' = entries,
          idxSeq' = if (changed) idxSeq + 1 else idxSeq,
          idxHist' = hist,
          responses' = Set((n, p._2, cur._1, cur._2)),
          observed' = Set(),
          writer' = writer.set(w, Idle),
          dirty' = dirty.exclude(Set(n)),
          keepObj, keepRest,
        }
      }
    },
  }
  action anyFinishOtherResponse = { nondet w = oneOf(WRITERS) finishOtherResponse(w) }
  action stepOtherWriteResponse = any { anyCreate, anyUpdate, anyDelete, anyRepair, anyFinishOtherResponse }

  // Bug 9: a create writes over a tombstone while the index still names
  // the deleted object. The next index write moves the entry from the old
  // incarnation to the new one in one step: one MODIFIED event with a new
  // UID.
  action createUnguarded(w, n) = all {
    writer.get(w) == Idle,
    not(objLive.get(n)),
    objSeq.get(n) < MAX_SEQ,
    objSeq' = objSeq.set(n, objSeq.get(n) + 1),
    objLive' = objLive.set(n, true),
    objBorn' = objBorn.set(n, objSeq.get(n) + 1),
    writer' = writer.set(w, Wrote((n, objSeq.get(n) + 1))),
    keepIdx, dirty' = dirty, quiet, keepRest,
  }
  action anyCreateUnguarded = { nondet w = oneOf(WRITERS) nondet n = oneOf(NAMES) createUnguarded(w, n) }
  action stepCreateUnguarded = any { anyCreateUnguarded, anyUpdate, anyDelete, anyFinish }

  // Bug 10: events carry the object sequence. The reflector resumes from
  // the object sequence of its last event, as client-go would before
  // issue #150.
  action reflectObjectSeq = {
    val k = rPos + 1
    val changed = NAMES.filter(n => idxHist[k].get(n)._1 != idxHist[k - 1].get(n)._1)
    all {
      rPos >= 0,
      rPos < idxSeq,
      rCache' = NAMES.mapBy(n => if (changed.contains(n)) idxHist[k].get(n)._1 else rCache.get(n)),
      rPos' = changed.fold(0, (acc, n) => {
        val e = idxHist[k].get(n)._1
        val s = if (e < 0) -e else e
        if (s > acc) s else acc
      }),
      keepStore, quiet, keepWatch, keepClaims,
    }
  }
  action stepReflectorObjectSeqRV = any { anyWrite, reflectObjectSeq }

  // ---- witness runs (`quint test`) ---------------------------------------------

  // A zombie's late effect is rejected once a successor has applied.
  run zombieIsFencedTest =
    init
      .then(create("w1", "a")).then(finish("w1"))
      .then(acquire("east", "a"))
      .then(expire("a"))
      .then(acquire("west", "a"))
      .then(applyEffect("west", "a"))
      .then(applyEffect("east", "a"))
      .expect(accepted.get("a") == 2 and regressions == 0)

  // A holder that acquires again keeps its fence. The next holder after a
  // release still gets a higher one.
  run reacquireKeepsFenceTest =
    init
      .then(create("w1", "a")).then(finish("w1"))
      .then(acquire("east", "a"))
      .then(reacquire("east", "a"))
      .expect(fence.get("a") == 1 and claimFence.get(("east", "a")) == 1)
      .then(release("east", "a"))
      .then(acquire("west", "a"))
      .expect(fence.get("a") == 2)

  // A crash between the two writes leaves the object invisible to lists
  // until the sweep runs.
  run crashLeavesIndexBehindTest =
    init
      .then(create("w1", "a")).then(crash("w1"))
      .expect(objSeq.get("a") == 1 and idxEntries.get("a") == 0 and dirty == Set("a"))
      .then(observeList)
      .expect(observed == Set())
      .then(sweep)
      .expect(idxEntries.get("a") == 1 and dirty == Set())

  // The next write to a dirty object fails its compare-and-set, and its
  // repair catches the index up.
  run repairCatchesUpTest =
    init
      .then(create("w1", "a")).then(finish("w1"))
      .then(update("w1", "a")).then(crash("w1"))
      .then(repair("a"))
      .expect(idxEntries.get("a") == 2 and dirty == Set())
      .then(update("w2", "a")).then(finish("w2"))
      .expect(idxEntries.get("a") == 3 and responses == Set(("a", 3, 3, 3)))

  // A crash between the tombstone and the index removal leaves a
  // tombstoned name in the index. That is allowed. The sweep removes it.
  run deleteTombstonesFirstTest =
    init
      .then(create("w1", "a")).then(finish("w1"))
      .then(delete("w1", "a"))
      .expect(not(objLive.get("a")) and idxEntries.get("a") == 1 and IndexNeverAhead)
      .then(crash("w1"))
      .expect(dirty == Set("a") and IndexNeverAhead)
      .then(sweep)
      .expect(idxEntries.get("a") == -2 and dirty == Set() and IndexNeverAhead)

  // A watch that falls behind catches up one index change at a time,
  // including the sweep's repair and the delete's index removal.
  run watchDeliversEveryChangeTest =
    init
      .then(create("w1", "a")).then(finish("w1"))
      .then(create("w1", "b")).then(crash("w1")).then(sweep)
      .then(delete("w2", "a")).then(finish("w2"))
      .expect(idxSeq == 3 and watchCursor == 0 and WatchGapFree)
      .then(deliver).then(deliver).then(deliver)
      .expect(watchCursor == 3 and delivered == [1, 2, 3] and WatchGapFree)
      .expect(observed == Set(("a", -2, 3)) and ObservedRVIsWatchPosition)

  // A get reports the index sequence of the entry, not the object
  // sequence. b's object sequence is 1 but the index recorded it at 2.
  run getReportsIndexSequenceTest =
    init
      .then(create("w1", "a")).then(finish("w1"))
      .then(create("w1", "b")).then(finish("w1"))
      .then(observeGet("b"))
      .expect(observed == Set(("b", 1, 2)) and ObservedRVIsWatchPosition)
      .then(observeGetObjectSeq("b"))
      .expect(not(ObservedRVIsWatchPosition))

  // w1 updates a to 2. A repair records 2, and w2 overwrites it with 3
  // and indexes 3 before w1's index write. w1 still answers with its own
  // version 2 at index sequence 2.
  run overwrittenWriteGetsOwnVersionTest =
    init
      .then(create("w1", "a")).then(finish("w1"))
      .then(update("w1", "a")).then(repair("a"))
      .then(update("w2", "a")).then(finish("w2"))
      .then(finish("w1"))
      .expect(responses == Set(("a", 2, 2, 2)) and WriteResponseIsOwnVersion)

  // The same interleaving with the bug gives w1 the version 3.
  run otherWriteResponseFailsTest =
    init
      .then(create("w1", "a")).then(finish("w1"))
      .then(update("w1", "a")).then(repair("a"))
      .then(update("w2", "a")).then(finish("w2"))
      .then(finishOtherResponse("w1"))
      .expect(not(WriteResponseIsOwnVersion))

  // A delete loses its index write. The create waits for the repair that
  // records the removal, so the index goes live, removed, live.
  run recreateIsTwoStepsTest =
    init
      .then(create("w1", "a")).then(finish("w1"))
      .then(delete("w1", "a")).then(crash("w1"))
      .expect(idxEntries.get("a") == 1 and not(objLive.get("a")))
      .then(repair("a"))
      .then(create("w2", "a")).then(finish("w2"))
      .expect(idxEntries.get("a") == 3 and NoIncarnationCollapse)
      .then(deliver).then(deliver).then(deliver)
      .expect(observed == Set(("a", 3, 3)))

  // Without the guard, the removal is never recorded.
  run unguardedCreateCollapsesTest =
    init
      .then(create("w1", "a")).then(finish("w1"))
      .then(delete("w1", "a")).then(crash("w1"))
      .then(createUnguarded("w2", "a")).then(finish("w2"))
      .expect(not(NoIncarnationCollapse))

  // A reflector that resumes from each event matches the index. Resuming
  // from object sequences does not: b's event carries object sequence 1,
  // and the index at 1 does not hold b.
  run reflectorResumesTest =
    init
      .then(create("w1", "a")).then(finish("w1"))
      .then(create("w1", "b")).then(finish("w1"))
      .then(reflect).then(reflect)
      .expect(rPos == 2 and ReflectorMatchesIndex)

  run reflectorObjectSeqFailsTest =
    init
      .then(create("w1", "a")).then(finish("w1"))
      .then(create("w1", "b")).then(finish("w1"))
      .then(reflectObjectSeq).then(reflectObjectSeq)
      .expect(not(ReflectorMatchesIndex))
}
