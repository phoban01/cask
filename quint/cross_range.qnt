// quint-check: invariants=NoSkewOverflow,UncertaintyContract,TypeOK
// quint-check: control=stepNoReadGuard:UncertaintyContract
// quint-check: control=stepNoSkewGuard:NoSkewOverflow
// quint-check: verify-steps=10
// quint-check: verify-split=each
//
// Inv is TypeOK, NoSkewOverflow and UncertaintyContract under the TLA+
// name, so the gate lists the three parts. TLC reports a search depth of
// 14, so every state is at most 13 steps from init. `quint verify` at 10
// steps finishes in about 7 minutes. At 13 steps it did not finish in 12
// minutes. So the bound does not reach every state; TLC does
// (quint/PARITY.md). The shortest violations under the controls have 2
// and 3 steps. Each control found its violation in 20 of 20 runs with
// different seeds at 30 steps and 2000 samples.
//
// cross_range.qnt — Quint port of tla/CrossRange.tla.
//
// Scope: cross-range snapshots stay off the fleet path. Lists come from
// the index register. This module covers the snapshot contract of
// mvcc.SnapshotAt and mvcc.SnapshotRead in internal/mvcc, and the HLC
// stamp that internal/hlc puts on every version. That code still ships.
// No binary calls SnapshotRead today. The proof stays while the code
// ships.
//
// Cask does not give strict cross-range linearizability for snapshots. It
// documents a bounded uncertainty window instead. Take a snapshot read at
// HLC time t. If the key has no write in [t - MaxOffset, t], the read
// returns the latest value committed at or before t. For a key with a
// write in the window, the contract says nothing. Fencing tokens give
// cross-key safety for coordination (lease.qnt), not snapshots.
//
// The model has two ranges. Each range has its own HLC, and the HLCs stay
// within MaxOffset of each other. A write commits at its range's HLC. A
// read takes one key at a time t and returns the latest entry with an HLC
// at or before t. The read needs t at or below the range's HLC. TLC found
// that guard: a read at a future t can be broken by a later write at or
// below t.
//
// One witness read is enough, as in the TLA+. A read does not change hlc
// or history. So a run with many reads is covered by the runs that take
// each read alone.
//
// Invariants:
//
//   NoSkewOverflow       the HLCs of two ranges differ by at most
//                        MaxOffset
//   UncertaintyContract  a read of a key with no write in the window
//                        returns the latest entry at or before t
//   TypeOK               the variables stay in their ranges
//   Inv                  TypeOK, NoSkewOverflow and UncertaintyContract, as
//                        in the TLA+
//
// Constants are the values from tla/CrossRange.cfg: ranges r1 and r2,
// keys k1 and k2, values v1 and v2, MaxOffset 2, MaxTime 6.
//
// Differences from the TLA+ source:
//   - Constants are pure vals, not CONSTANTS, so the model is fixed.
//     NoValue is the string "none".
//   - The .cfg sets RangeOfKey to MCRangeOfKey, a CHOOSE over the onto
//     maps from keys to ranges. Here RangeOfKey maps k1 to r1 and k2 to
//     r2. The other onto map is the same model with the ranges renamed,
//     so the state count is the same.
//   - The TLA+ snap is <<>> or <<t, k, value, hlc>>. Here it is a record
//     with a taken flag. An empty snap has taken false and fixed fields.
//     This is one to one with the TLA+, so the state count is the same.
//   - LatestAtTime is a fold over the history, not CHOOSE. The HLCs in
//     one history are distinct, so both give the same entry.
//   - The TLA+ has no Bug config. The negative controls are new. Each
//     drops one guard:
//       stepNoReadGuard  SnapshotRead without t <= hlc of the key's range.
//                        It MUST violate UncertaintyContract.
//       stepNoSkewGuard  TickHLC and Write without the skew bound. It MUST
//                        violate NoSkewOverflow.
module cross_range {
  pure val RANGES = Set("r1", "r2")
  pure val KEYS = Set("k1", "k2")
  pure val VALUES = Set("v1", "v2")
  pure val RANGE_OF_KEY = Map("k1" -> "r1", "k2" -> "r2")
  pure val MAX_OFFSET = 2
  pure val MAX_TIME = 6
  pure val NO_VALUE = "none"
  pure val TIMES = 0.to(MAX_TIME)

  type Snap = { taken: bool, t: int, k: str, v: str, h: int }
  pure val NO_SNAP: Snap = { taken: false, t: 0, k: "k1", v: NO_VALUE, h: 0 }

  var hlc: str -> int                     // hlc[r]: the range's local HLC
  var history: str -> List[(int, str)]    // history[k]: (HLC at commit, value)
  var snap: Snap                          // the one witness read, or NO_SNAP

  val TypeOK = and {
    hlc.keys() == RANGES,
    RANGES.forall(r => TIMES.contains(hlc.get(r))),
    history.keys() == KEYS,
    KEYS.forall(k => history.get(k).foldl(true, (ok, e) =>
      ok and TIMES.contains(e._1) and VALUES.contains(e._2))),
    snap.taken implies and {
      TIMES.contains(snap.t),
      KEYS.contains(snap.k),
      VALUES.union(Set(NO_VALUE)).contains(snap.v),
      TIMES.contains(snap.h),
    },
    not(snap.taken) implies snap == NO_SNAP,
  }

  action init = all {
    hlc' = RANGES.mapBy(r => 0),
    history' = KEYS.mapBy(k => List()),
    snap' = NO_SNAP,
  }

  // The HLCs of any two ranges differ by at most MaxOffset.
  def SkewOK(h: str -> int): bool =
    tuples(RANGES, RANGES).forall(((r1, r2)) =>
      h.get(r1) - h.get(r2) <= MAX_OFFSET and h.get(r2) - h.get(r1) <= MAX_OFFSET)

  // Range r may step its HLC by one and stay within the skew bound.
  def SkewRoom(r: str): bool =
    RANGES.exclude(Set(r)).forall(r2 => hlc.get(r) + 1 - hlc.get(r2) <= MAX_OFFSET)

  // ---- actions --------------------------------------------------------------

  // An idle tick of range r's HLC. With skewGuard it keeps the skew bound.
  action tickWith(skewGuard: bool, r: str): bool = all {
    hlc.get(r) < MAX_TIME,
    skewGuard implies SkewRoom(r),
    hlc' = hlc.setBy(r, x => x + 1),
    history' = history,
    snap' = snap,
  }

  action TickHLC(r: str): bool = tickWith(true, r)

  // A write of v to k. The key's range steps its HLC and stamps the entry.
  action writeWith(skewGuard: bool, k: str, v: str): bool = {
    val r = RANGE_OF_KEY.get(k)
    all {
      hlc.get(r) < MAX_TIME,
      skewGuard implies SkewRoom(r),
      hlc' = hlc.setBy(r, x => x + 1),
      history' = history.setBy(k, h => h.append((hlc.get(r) + 1, v))),
      snap' = snap,
    }
  }

  action Write(k: str, v: str): bool = writeWith(true, k, v)

  // The latest entry of k with an HLC at or before t, or (0, NO_VALUE).
  def LatestAtTime(k: str, t: int): (int, str) =
    history.get(k).foldl((0, NO_VALUE), (best, e) =>
      if (e._1 <= t and (best._2 == NO_VALUE or e._1 >= best._1)) e else best)

  // A snapshot read of k at t. With readGuard, t must be at or below the
  // HLC of k's range. Then every later write commits above t, so the read
  // stays correct. This is the load-bearing guard TLC found.
  action readWith(readGuard: bool, t: int, k: str): bool = {
    val pair = LatestAtTime(k, t)
    all {
      not(snap.taken),
      readGuard implies t <= hlc.get(RANGE_OF_KEY.get(k)),
      hlc' = hlc,
      history' = history,
      snap' = { taken: true, t: t, k: k, v: pair._2, h: pair._1 },
    }
  }

  action SnapshotRead(t: int, k: str): bool = readWith(true, t, k)

  action stepWith(skewGuard: bool, readGuard: bool): bool = any {
    { nondet r = RANGES.oneOf()
      tickWith(skewGuard, r) },
    { nondet k = KEYS.oneOf()
      nondet v = VALUES.oneOf()
      writeWith(skewGuard, k, v) },
    { nondet t = TIMES.oneOf()
      nondet k = KEYS.oneOf()
      readWith(readGuard, t, k) },
  }

  action step = stepWith(true, true)

  // ---- invariants -------------------------------------------------------------

  // The HLC skew across ranges stays within MaxOffset. The guards on
  // TickHLC and Write keep it. The invariant states the contract.
  val NoSkewOverflow = SkewOK(hlc)

  // A read of a key with no write in [t - MaxOffset, t] returns the latest
  // entry at or before t. The contract says nothing about other reads.
  val UncertaintyContract = snap.taken implies {
    val h = history.get(snap.k)
    val outsideWindow = h.foldl(true, (ok, e) =>
      ok and (e._1 < snap.t - MAX_OFFSET or e._1 > snap.t))
    val cands = h.select(e => e._1 <= snap.t)
    outsideWindow implies (
      if (cands.length() == 0) snap.v == NO_VALUE and snap.h == 0
      else cands.foldl(false, (found, e) => found or and {
        e._1 == snap.h,
        e._2 == snap.v,
        cands.foldl(true, (ok, e2) => ok and e2._1 <= e._1),
      }))
  }

  val Inv = and { TypeOK, NoSkewOverflow, UncertaintyContract }

  // ---- negative controls ---------------------------------------------------------

  // A read at a future t. A later write can then commit at or below t,
  // outside the window. It MUST violate UncertaintyContract.
  action stepNoReadGuard = stepWith(true, false)

  // The HLCs drift past MaxOffset. It MUST violate NoSkewOverflow.
  action stepNoSkewGuard = stepWith(false, true)

  // ---- witness runs ------------------------------------------------------------

  // A read at t = 2 after writes at 1 and 2 returns the write at 2.
  run readLatestTest =
    init
      .then(Write("k1", "v1"))
      .then(Write("k1", "v2"))
      .then(SnapshotRead(2, "k1"))
      .expect(snap.v == "v2" and snap.h == 2 and Inv)

  // A read at t = 1 ignores the later write at 2.
  run readAsOfTest =
    init
      .then(Write("k1", "v1"))
      .then(Write("k1", "v2"))
      .then(SnapshotRead(1, "k1"))
      .expect(snap.v == "v1" and snap.h == 1 and Inv)

  // The guard: k1's range is at 0, so a read at 4 is not allowed.
  run futureReadRejectedTest =
    init
      .then(SnapshotRead(4, "k1"))
      .fail()

  // The skew bound: r1 cannot run more than MaxOffset ahead of r2.
  run skewBoundTest =
    init
      .then(TickHLC("r1"))
      .then(TickHLC("r1"))
      .then(TickHLC("r1"))
      .fail()

  // Control: a read at 4 while k1's range is at 0 returns no value. A
  // write then commits at 1, below 4 - MaxOffset. The read is now wrong.
  run futureReadBrokenTest =
    init
      .then(readWith(false, 4, "k1"))
      .expect(snap.v == NO_VALUE and Inv)
      .then(Write("k1", "v1"))
      .expect(not(UncertaintyContract) and NoSkewOverflow)

  // Control: without the skew guard r1 runs 3 ahead of r2.
  run skewOverflowTest =
    init
      .then(tickWith(false, "r1"))
      .then(tickWith(false, "r1"))
      .then(tickWith(false, "r1"))
      .expect(not(NoSkewOverflow))
}
