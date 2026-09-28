// quint-check: invariants=FenceMonotone,ReceiverNotAhead,LiveBoundHoldsLock,KeptCoversLock,TypeOK
// quint-check: control=stepSeedAtZero:FenceMonotone
// quint-check: control=stepSeedAtZero:ReceiverNotAhead
// quint-check: control=stepRestoreEveryBound:LiveBoundHoldsLock
// quint-check: control=stepReleaseClearsKept:FenceMonotone
// quint-check: control=stepStaleWriteLowersKept:KeptCoversLock
// quint-check: verify-steps=6
//
// Verify bound: each CI verify job has a 30-minute timeout. On the dev VM,
// `quint verify` takes 3.5 minutes at 6 steps, 12.7 at 7, and 33 at 8.
// Six steps reach every witness run up to the cutover: the release of
// issue #200 (Bind, Effect, Release, Cutover) and the zombie's stale
// write (Bind, Lapse, Bind, StaleWrite). The simulation runs 30 steps.
//
// cutover.qnt — the claim lock of one object across the cutover from the
// CRD to cask (docs/spec/fleet.md sections 5 and 7, issues #196 and #200).
//
// Before the cutover, claims bind in the source world. Each bind mints
// the next fence, as a cask lock does. A receiver accepts an effect only
// when its fence is at least the highest fence it has accepted. Sessions
// can lapse. A lapsed claim stays Bound until its controller sees the
// lapse and sets it to Lost, so a zombie can still send effects.
//
// The Device status has two fence fields. The lease names the current
// holder and its fence. A release and a delete clear the lease. The
// lastFence keeps the highest fence that the Device has advertised. No
// write lowers it, and a release does not clear it. A zombie can write
// its stale lease to the Device status. That write keeps lastFence too.
//
// The Cutover step is the export and the import in one step. Writers are
// frozen between the two, so no bind can happen in between. The lock
// register then starts again in cask:
//
//   - The export sees only status: the Device lease fence, the Device
//     lastFence, and the fence of each claim that still exists. The
//     recorded fence is the highest of these. The model does not give
//     the export the lock fence.
//   - The lock fence is seeded at the recorded fence.
//   - A Bound claim that alone holds the recorded fence is restored: its
//     session is granted and it holds the lock at that fence.
//   - Every other Bound claim is written as Lost. Cask holds no session
//     from the source world.
//
// The receiver keeps its highest accepted fence across the cutover.
//
// Invariants:
//
//   FenceMonotone       the lock fence never decreases, and every bind
//                       mints a fence above the one before, across the
//                       cutover too
//   ReceiverNotAhead    no receiver has accepted a fence above the lock
//                       fence, so the next bind mints a fence that every
//                       receiver accepts
//   LiveBoundHoldsLock  a Bound claim with a live session holds the lock
//                       at the lock fence
//   KeptCoversLock      the Device lastFence is at least the lock fence,
//                       so the export sees every minted fence
//   TypeOK              the variables stay in their ranges
//
// Negative controls:
//
//   stepSeedAtZero           the import starts the lock again at fence 0,
//                            so the next bind mints fence 1. It MUST
//                            violate FenceMonotone and ReceiverNotAhead.
//   stepRestoreEveryBound    the import grants a session to every Bound
//                            claim, so a zombie from the source world
//                            comes back live without the lock. It MUST
//                            violate LiveBoundHoldsLock.
//   stepReleaseClearsKept    a release and a delete clear lastFence too,
//                            as the code did before issue #200. When the
//                            claim is gone, the export sees no fence, and
//                            the import seeds the lock too low. It MUST
//                            violate FenceMonotone.
//   stepStaleWriteLowersKept a zombie's stale lease write replaces
//                            lastFence with its own lower fence. It MUST
//                            violate KeptCoversLock.
//
// What the model does not cover:
//
//   - Per-device steps. The model has one object, and Cutover is atomic.
//     The code grants a session, seeds a lock, writes each object, writes
//     the marker, and renews each session, all as separate steps across
//     many devices.
//   - A bind in separate writes. Bind takes the lock and writes the claim
//     and Device status in one step. In code these are three writes. A
//     fence that the lock minted but that no status held can be minted
//     again after the cutover. No receiver saw it, because an effect
//     carries the fence from the claim status. In code, a claim delete
//     waits until lastFence covers the claim's fence (server tests).
//   - A deleted Device. The model has one Device, and it always exists. A
//     Device that is deleted takes its lastFence with it.
//   - A concurrent acquirer during the cutover. No Bind runs inside
//     Cutover. In code, Seed compares and sets on the register it read, and
//     the session is granted before the lock names it (lease tests).
//   - A rerun after a partial import. The model imports once. In code,
//     CheckSeed and Seed refuse a register that moved or names another live
//     holder (lease and migrate tests).
//   - Session time. A restored session is live at the cutover. The model
//     has no clock, TTL, or import grace, so it does not show a session
//     that lapses before its first renewal. That costs liveness, not
//     safety: Lapse and MarkLost cover the lapse itself. The slow-import
//     tests in cmd/cask-apiserver/cutover_test.go cover the grace.
module cutover {
  pure val CLAIMS = Set("c1", "c2")
  pure val NONE = "none"
  pure val MAX_FENCE = 5
  pure val FENCES = 0.to(MAX_FENCE)
  pure val PHASES = Set("Pending", "Bound", "Lost")

  var inCask: bool          // the cutover has run
  var fence: int            // the lock register fence
  var owner: str            // the claim that holds the lock, or NONE
  var live: str -> bool     // the claim's session is live
  var phase: str -> str     // the claim status phase
  var cfence: str -> int    // the fence in the claim status (0 = none)
  var adv: int              // the Device status lease fence (0 = none)
  var kept: int             // the Device status lastFence
  var seen: int             // the highest fence the receiver accepted
  // History variables for FenceMonotone.
  var prevFence: int        // the lock fence before the last step
  var minted: bool          // the last step was a bind

  val TypeOK = and {
    FENCES.contains(fence),
    CLAIMS.union(Set(NONE)).contains(owner),
    live.keys() == CLAIMS,
    phase.keys() == CLAIMS,
    CLAIMS.forall(c => PHASES.contains(phase.get(c))),
    cfence.keys() == CLAIMS,
    CLAIMS.forall(c => FENCES.contains(cfence.get(c))),
    FENCES.contains(adv),
    FENCES.contains(kept),
    FENCES.contains(seen),
    FENCES.contains(prevFence),
  }

  action init = all {
    inCask' = false,
    fence' = 0,
    owner' = NONE,
    live' = CLAIMS.mapBy(c => false),
    phase' = CLAIMS.mapBy(c => "Pending"),
    cfence' = CLAIMS.mapBy(c => 0),
    adv' = 0,
    kept' = 0,
    seen' = 0,
    prevFence' = 0,
    minted' = false,
  }

  pure def max2(a: int, b: int): int = if (a > b) a else b

  // The lock is free when no claim holds it or its holder's session has
  // lapsed (lazy expiry).
  val Free = owner == NONE or not(live.get(owner))

  // ---- claim steps, the same before and after the cutover ---------------

  //= docs/spec/fleet.md#5-claims-and-fencing
  //= type=implication
  //# A Device status MUST keep in its lastFence field the highest fence
  //# that the Device has advertised.
  // A Pending claim binds: it takes the lock and mints the next fence. The
  // Device advertises the lease and keeps its fence.
  action Bind(c: str): bool = all {
    phase.get(c) == "Pending",
    Free,
    fence < MAX_FENCE,
    fence' = fence + 1,
    owner' = c,
    live' = live.set(c, true),
    phase' = phase.set(c, "Bound"),
    cfence' = cfence.set(c, fence + 1),
    adv' = fence + 1,
    kept' = max2(kept, fence + 1),
    seen' = seen,
    inCask' = inCask,
    prevFence' = fence,
    minted' = true,
  }

  // A session lapses. The claim stays Bound until its controller sees it.
  action Lapse(c: str): bool = all {
    live.get(c),
    live' = live.set(c, false),
    fence' = fence, owner' = owner, phase' = phase, cfence' = cfence,
    adv' = adv, kept' = kept, seen' = seen, inCask' = inCask,
    prevFence' = fence, minted' = false,
  }

  // The controller sees a lapsed session and sets the claim to Lost.
  action MarkLost(c: str): bool = all {
    phase.get(c) == "Bound",
    not(live.get(c)),
    phase' = phase.set(c, "Lost"),
    fence' = fence, owner' = owner, live' = live, cfence' = cfence,
    adv' = adv, kept' = kept, seen' = seen, inCask' = inCask,
    prevFence' = fence, minted' = false,
  }

  // The new lastFence after a release or a delete of claim fence f. The
  // negative control clears it.
  pure def keptAfterRelease(k: int, f: int, clear: bool): int =
    if (clear) 0 else max2(k, f)

  // The live holder deletes its claim and releases the lock. The workload
  // then creates the claim again. The Device status write clears the
  // lease when the lease fence is not above the claim's fence.
  action releaseStep(c: str, clear: bool): bool = all {
    phase.get(c) == "Bound",
    live.get(c),
    owner == c,
    owner' = NONE,
    live' = live.set(c, false),
    phase' = phase.set(c, "Pending"),
    cfence' = cfence.set(c, 0),
    adv' = if (adv <= cfence.get(c)) 0 else adv,
    kept' = keptAfterRelease(kept, cfence.get(c), clear),
    fence' = fence, seen' = seen, inCask' = inCask,
    prevFence' = fence, minted' = false,
  }

  // A Lost claim is deleted and created again.
  action recreateStep(c: str, clear: bool): bool = all {
    phase.get(c) == "Lost",
    phase' = phase.set(c, "Pending"),
    cfence' = cfence.set(c, 0),
    adv' = if (adv <= cfence.get(c)) 0 else adv,
    kept' = keptAfterRelease(kept, cfence.get(c), clear),
    fence' = fence, owner' = owner, live' = live,
    seen' = seen, inCask' = inCask,
    prevFence' = fence, minted' = false,
  }

  //= docs/spec/fleet.md#5-claims-and-fencing
  //= type=implication
  //# Deleting a claim MUST NOT complete before the lastFence of its
  //# Device is at least the claim's fence.
  action Release(c: str): bool = releaseStep(c, false)
  action Recreate(c: str): bool = recreateStep(c, false)

  // A Bound claim, live or a zombie, writes its lease to the Device status
  // again. The lease moves only to a fence at least the lease fence and
  // lastFence. The negative control sets lastFence to the writer's fence.
  action staleWriteStep(c: str, lower: bool): bool = all {
    phase.get(c) == "Bound",
    cfence.get(c) > 0,
    adv' = if (cfence.get(c) >= adv and cfence.get(c) >= kept) cfence.get(c) else adv,
    kept' = if (lower) cfence.get(c) else max2(kept, cfence.get(c)),
    fence' = fence, owner' = owner, live' = live, phase' = phase,
    cfence' = cfence, seen' = seen, inCask' = inCask,
    prevFence' = fence, minted' = false,
  }

  //= docs/spec/fleet.md#5-claims-and-fencing
  //= type=implication
  //# A write to a Device MUST NOT lower its lastFence.
  action StaleWrite(c: str): bool = staleWriteStep(c, false)

  //= docs/spec/fleet.md#5-claims-and-fencing
  //= type=implication
  //# A receiver MUST reject an effect whose fence is lower than the
  //# highest fence it has accepted for that object.
  // A Bound claim, live or a zombie, sends an effect. The receiver accepts
  // it only when its fence is at least the highest it has accepted.
  action Effect(c: str): bool = all {
    phase.get(c) == "Bound",
    cfence.get(c) >= seen,
    seen' = cfence.get(c),
    fence' = fence, owner' = owner, live' = live, phase' = phase,
    cfence' = cfence, adv' = adv, kept' = kept, inCask' = inCask,
    prevFence' = fence, minted' = false,
  }

  // ---- the cutover --------------------------------------------------------

  //= docs/spec/fleet.md#7-migration
  //= type=implication
  //# The fences that the export records for a Device MUST include the
  //# lastFence in its status.
  // The export sees status only: the Device lease, the Device lastFence,
  // and the fence of each claim.
  val recorded = CLAIMS.fold(max2(adv, kept), (m, c) => max2(m, cfence.get(c)))

  // The claim that alone holds the recorded fence, if it is Bound.
  pure def holds(ph: str -> str, cf: str -> int, f: int, c: str): bool = and {
    f > 0,
    ph.get(c) == "Bound",
    cf.get(c) == f,
    CLAIMS.forall(d => (d != c and ph.get(d) == "Bound") implies cf.get(d) != f),
  }

  // The import with the seed fence and the rule for Bound claims as
  // parameters. keep(c) is true for a Bound claim that the import restores
  // as Bound with a live session. The lock holder is the claim that holds
  // the seed fence, if one is kept. The Device status is imported as is.
  action cutoverWith(seed: int, keep: str -> bool): bool = all {
    not(inCask),
    inCask' = true,
    fence' = seed,
    owner' = if (CLAIMS.exists(c => keep.get(c) and cfence.get(c) == seed))
               CLAIMS.filter(c => keep.get(c) and cfence.get(c) == seed).fold(NONE, (_, c) => c)
             else NONE,
    live' = CLAIMS.mapBy(c => keep.get(c)),
    phase' = CLAIMS.mapBy(c =>
      if (phase.get(c) == "Bound" and not(keep.get(c))) "Lost" else phase.get(c)),
    cfence' = cfence,
    adv' = adv,
    kept' = kept,
    seen' = seen,
    prevFence' = fence,
    minted' = false,
  }

  //= docs/spec/fleet.md#7-migration
  //= type=implication
  //# The import MUST NOT seed an object's lock at a fence below the
  //# highest fence that the export recorded for that object.
  //= docs/spec/fleet.md#7-migration
  //= type=implication
  //# The import MUST restore a Bound claim as Bound only when that claim
  //# alone holds the highest fence that the export recorded for its
  //# object.
  //= docs/spec/fleet.md#7-migration
  //= type=implication
  //# The import MUST write a Bound claim as Lost when it does not restore
  //# that claim as Bound.
  //= docs/spec/fleet.md#7-migration
  //= type=implication
  //# The import MUST grant a restored claim's session and seed its lock
  //# before it writes the claim.
  // The lock, the session, and the claim change in one step, so no other
  // step sees the claim Bound before its session and lock exist.
  action Cutover: bool =
    cutoverWith(recorded, CLAIMS.mapBy(c => holds(phase, cfence, recorded, c)))

  action step = any {
    { nondet c = CLAIMS.oneOf()
      any { Bind(c), Lapse(c), MarkLost(c), Release(c), Recreate(c), StaleWrite(c), Effect(c) } },
    Cutover,
  }

  // ---- invariants -----------------------------------------------------------

  val FenceMonotone = and {
    fence >= prevFence,
    minted implies fence > prevFence,
  }

  val ReceiverNotAhead = seen <= fence

  val LiveBoundHoldsLock = CLAIMS.forall(c =>
    (phase.get(c) == "Bound" and live.get(c)) implies (owner == c and cfence.get(c) == fence))

  val KeptCoversLock = kept >= fence

  // ---- negative controls ----------------------------------------------------

  // The import starts the lock again at fence 0. It restores no claim.
  action stepSeedAtZero = any {
    { nondet c = CLAIMS.oneOf()
      any { Bind(c), Lapse(c), MarkLost(c), Release(c), Recreate(c), StaleWrite(c), Effect(c) } },
    cutoverWith(0, CLAIMS.mapBy(c => false)),
  }

  // The import grants a session to every Bound claim and keeps it Bound.
  action stepRestoreEveryBound = any {
    { nondet c = CLAIMS.oneOf()
      any { Bind(c), Lapse(c), MarkLost(c), Release(c), Recreate(c), StaleWrite(c), Effect(c) } },
    cutoverWith(recorded, CLAIMS.mapBy(c => phase.get(c) == "Bound")),
  }

  // A release and a delete clear lastFence, as the code did before #200.
  action stepReleaseClearsKept = any {
    { nondet c = CLAIMS.oneOf()
      any { Bind(c), Lapse(c), MarkLost(c), releaseStep(c, true), recreateStep(c, true),
            StaleWrite(c), Effect(c) } },
    Cutover,
  }

  // A zombie's stale lease write sets lastFence to its own fence.
  action stepStaleWriteLowersKept = any {
    { nondet c = CLAIMS.oneOf()
      any { Bind(c), Lapse(c), MarkLost(c), Release(c), Recreate(c),
            staleWriteStep(c, true), Effect(c) } },
    Cutover,
  }

  // ---- witness runs ---------------------------------------------------------

  // c1 binds at fence 1 and sends an effect. The cutover restores it: it
  // stays Bound, holds the lock at fence 1, and its session is live.
  run restoreBoundTest =
    init
      .then(Bind("c1"))
      .then(Effect("c1"))
      .then(Cutover)
      .expect(phase.get("c1") == "Bound" and owner == "c1" and fence == 1
        and live.get("c1") and seen == 1
        and FenceMonotone and ReceiverNotAhead and LiveBoundHoldsLock)

  // After the cutover, c1 releases and c2 binds above the recorded fence.
  // The receiver accepts c2's effect.
  run nextBindAboveRecordedTest =
    init
      .then(Bind("c1"))
      .then(Effect("c1"))
      .then(Cutover)
      .then(Release("c1"))
      .then(Bind("c2"))
      .expect(cfence.get("c2") == 2 and FenceMonotone)
      .then(Effect("c2"))
      .expect(seen == 2 and ReceiverNotAhead)

  // A zombie: c1 lapses, c2 takes over at fence 2, and c1 is still Bound
  // at the cutover. The import writes c1 as Lost and restores c2.
  run zombieWrittenLostTest =
    init
      .then(Bind("c1"))
      .then(Lapse("c1"))
      .then(Bind("c2"))
      .then(Cutover)
      .expect(phase.get("c1") == "Lost" and phase.get("c2") == "Bound"
        and owner == "c2" and LiveBoundHoldsLock)

  // Issue #200: c1 binds at fence 1, sends an effect, and is deleted. At
  // the export no claim holds fence 1, and the lease is clear. lastFence
  // keeps fence 1, so the lock is seeded at 1 and the next bind mints 2.
  run releasedFenceKeptTest =
    init
      .then(Bind("c1"))
      .then(Effect("c1"))
      .then(Release("c1"))
      .expect(adv == 0 and kept == 1 and cfence.get("c1") == 0)
      .then(Cutover)
      .expect(fence == 1 and FenceMonotone and ReceiverNotAhead)
      .then(Bind("c2"))
      .expect(cfence.get("c2") == 2)

  // The zombie c1 writes its stale lease after c2 took over. The lease
  // and lastFence keep c2's fence.
  run staleWriteKeepsFenceTest =
    init
      .then(Bind("c1"))
      .then(Lapse("c1"))
      .then(Bind("c2"))
      .then(StaleWrite("c1"))
      .expect(adv == 2 and kept == 2 and KeptCoversLock)

  // Control: the seed at 0. The receiver saw fence 1, and the lock is now
  // behind it. The next bind mints fence 1 again.
  run seedAtZeroTest =
    init
      .then(Bind("c1"))
      .then(Effect("c1"))
      .then(cutoverWith(0, CLAIMS.mapBy(c => false)))
      .expect(not(FenceMonotone) and not(ReceiverNotAhead))

  // Control: the zombie c1 comes back live without the lock.
  run restoreEveryBoundTest =
    init
      .then(Bind("c1"))
      .then(Lapse("c1"))
      .then(Bind("c2"))
      .then(cutoverWith(2, CLAIMS.mapBy(c => phase.get(c) == "Bound")))
      .expect(not(LiveBoundHoldsLock))

  // Control: the release clears lastFence. The export sees no fence, and
  // the lock goes back to 0 behind the receiver.
  run releaseClearsKeptTest =
    init
      .then(Bind("c1"))
      .then(Effect("c1"))
      .then(releaseStep("c1", true))
      .then(Cutover)
      .expect(fence == 0 and not(FenceMonotone) and not(ReceiverNotAhead))

  // Control: the zombie's stale write lowers lastFence below the lock.
  run staleWriteLowersKeptTest =
    init
      .then(Bind("c1"))
      .then(Lapse("c1"))
      .then(Bind("c2"))
      .then(staleWriteStep("c1", true))
      .expect(not(KeptCoversLock))
}
