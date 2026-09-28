// quint-check: invariants=NoStaleRead,TypeOK
// quint-check: control=stepNaive:NoStaleRead
// quint-check: verify-steps=18
//
// Inv is TypeOK and NoStaleRead under the TLA+ name, so the gate lists the
// two parts. The shortest stale read under stepNaive has 5 steps. TLC
// reports a search depth of 19, so every state is at most 18 steps from
// init. `quint verify` at 18 steps reaches every state and finishes in
// about 4 minutes (quint/PARITY.md).
//
// owner_reads.qnt — Quint port of tla/OwnerReads.tla.
//
// An owner serves reads from its local cache with no consensus round. A
// write finds out that its owner lost the lock when the accept fails. A
// read finds out nothing, so the read must be safe by time alone. Each
// client sees true time plus a fixed clock offset of at most SKEW. The
// guard has two sides:
//
//   owner side:  serve only while localNow(c) + SKEW < expiry
//   taker side:  take a lapsed lock only once localNow(c) >= expiry + SKEW
//
// Under this guard, the last read the old owner serves comes before, in
// true time, the first write a successor commits. The naive guard checks
// only that the lease lapsed, on each side's own clock. An owner with a
// slow clock then serves its cache after a successor with a fast clock
// took the lock and wrote.
//
// Scope, as in the TLA+: writes go only through the current lock holder.
//
// Invariants:
//
//   NoStaleRead  every served local read returned the latest committed
//                value
//   TypeOK       the variables stay in their ranges
//   Inv          TypeOK and NoStaleRead, as in the TLA+
//
// Constants are the values from tla/OwnerReads.cfg: two clients, Skew 2,
// TTL 5, MaxTime 14, MaxWrites 2.
//
// Differences from the TLA+ source:
//   - Constants are pure vals, not CONSTANTS, so the model is fixed.
//     NoClient is the string "none".
//   - Guarded is not a constant. TakeableBy, CanServe, Acquire and
//     LocalRead take a `guarded` flag. `step` passes true. The negative
//     control `stepNaive` passes false. It stands for
//     tla/OwnerReadsBug.cfg and MUST violate NoStaleRead.
//   - init picks the clock offsets with nondet. initWith sets them, so
//     the witness runs can fix a slow and a fast clock.
module owner_reads {
  pure val CLIENTS = Set("c1", "c2")
  pure val NO_CLIENT = "none"
  pure val SKEW = 2
  pure val TTL = 5
  pure val MAX_TIME = 14
  pure val MAX_WRITES = 2
  pure val OFFSETS = (-SKEW).to(SKEW)

  var now: int            // true time; no process sees it directly
  var off: str -> int     // off[c]: c's fixed clock offset, |off[c]| <= SKEW
  var holder: str         // current lock holder, or NO_CLIENT
  var sexp: int           // the stored lease expiry, written at grant time
  var gexp: str -> int    // gexp[c]: the expiry from c's own last grant
  var regVal: int         // the register's committed value counter
  var cache: str -> int   // cache[c]: what c's fast-path cache holds
  var bad: bool           // some served local read returned a stale value

  def LocalNow(c: str): int = now + off.get(c)

  action initWith(o: str -> int): bool = all {
    now' = 0,
    off' = o,
    holder' = NO_CLIENT,
    sexp' = 0,
    gexp' = CLIENTS.mapBy(c => 0),
    regVal' = 0,
    cache' = CLIENTS.mapBy(c => 0),
    bad' = false,
  }

  // The adversary picks each clock offset in -SKEW..SKEW.
  action init = {
    nondet o = setOfMaps(CLIENTS, OFFSETS).oneOf()
    initWith(o)
  }

  // ---- actions --------------------------------------------------------------

  action Tick: bool = all {
    now < MAX_TIME,
    now' = now + 1,
    off' = off,
    holder' = holder,
    sexp' = sexp,
    gexp' = gexp,
    regVal' = regVal,
    cache' = cache,
    bad' = bad,
  }

  // The lock looks free to c: never granted, or lapsed on c's clock. The
  // guarded check adds the taker-side wait of SKEW.
  def TakeableBy(guarded: bool, c: str): bool = or {
    holder == NO_CLIENT and sexp == 0,
    if (guarded) LocalNow(c) >= sexp + SKEW else LocalNow(c) >= sexp,
  }

  // c takes the lock and records the expiry on its own clock. That is what
  // Sessions.Grant commits. c primes its cache from a phase-1 read.
  action Acquire(guarded: bool, c: str): bool = all {
    TakeableBy(guarded, c),
    holder' = c,
    sexp' = LocalNow(c) + TTL,
    gexp' = gexp.set(c, LocalNow(c) + TTL),
    cache' = cache.set(c, regVal),
    now' = now,
    off' = off,
    regVal' = regVal,
    bad' = bad,
  }

  // Only the current holder commits. In the implementation the epoch
  // fence stops a deposed owner's write. The writer's cache tracks its
  // own write.
  action Write(c: str): bool = all {
    holder == c,
    regVal < MAX_WRITES,
    regVal' = regVal + 1,
    cache' = cache.set(c, regVal + 1),
    now' = now,
    off' = off,
    holder' = holder,
    sexp' = sexp,
    gexp' = gexp,
    bad' = bad,
  }

  // c believes its lease covers reads. It judges only from its local clock
  // and its own granted expiry. It cannot see a successor.
  def CanServe(guarded: bool, c: str): bool = and {
    gexp.get(c) > 0,
    if (guarded) LocalNow(c) + SKEW < gexp.get(c) else LocalNow(c) < gexp.get(c),
  }

  // c serves its cache with no consensus round. The read is stale when the
  // cache is not the latest committed value at this true instant.
  action LocalRead(guarded: bool, c: str): bool = all {
    CanServe(guarded, c),
    bad' = (bad or cache.get(c) != regVal),
    now' = now,
    off' = off,
    holder' = holder,
    sexp' = sexp,
    gexp' = gexp,
    regVal' = regVal,
    cache' = cache,
  }

  // Each disjunct picks only its own client, as the TLA+ Next does.
  action stepWith(guarded: bool): bool = any {
    Tick,
    { nondet c = CLIENTS.oneOf()
      Acquire(guarded, c) },
    { nondet c = CLIENTS.oneOf()
      Write(c) },
    { nondet c = CLIENTS.oneOf()
      LocalRead(guarded, c) },
  }

  // The W4 double-sided MaxOffset guard (tla/OwnerReads.cfg).
  action step = stepWith(true)

  // ---- invariants -------------------------------------------------------------

  // Every served local read returned the latest committed value.
  val NoStaleRead = not(bad)

  val TypeOK = and {
    0 <= now and now <= MAX_TIME,
    CLIENTS.union(Set(NO_CLIENT)).contains(holder),
    0 <= regVal and regVal <= MAX_WRITES,
  }

  val Inv = and { TypeOK, NoStaleRead }

  // ---- negative control: naive lease checks ---------------------------------

  // The naive lapsed check on both sides (tla/OwnerReadsBug.cfg). It MUST
  // violate NoStaleRead.
  action stepNaive = stepWith(false)

  // ---- witness runs ------------------------------------------------------------

  pure val SLOW_FAST = Map("c1" -> -2, "c2" -> 2)

  // The bug: c1 has a slow clock and c2 a fast one. c1 takes the lock at
  // true time 0 with expiry 3 on its clock. At true time 1, c2's clock
  // reads 3, so the naive taker check lets c2 take over and write. c1's
  // clock reads -1, so the naive owner check lets c1 serve its old cache.
  run naiveStaleReadTest =
    initWith(SLOW_FAST)
      .then(Acquire(false, "c1"))
      .expect(sexp == 3 and gexp.get("c1") == 3)
      .then(Tick)
      .then(Acquire(false, "c2"))
      .then(Write("c2"))
      .then(LocalRead(false, "c1"))
      .expect(not(NoStaleRead) and TypeOK)

  // The guarded taker waits SKEW past the expiry on its own clock.
  run guardedTakerWaitsTest =
    initWith(SLOW_FAST)
      .then(Acquire(true, "c1"))
      .then(Tick)
      .then(Acquire(true, "c2"))
      .fail()

  // The guarded owner stops serving SKEW before its expiry. With the
  // guard, c1 serves at true time 2 at the latest (local 0 + 2 < 3), and
  // c2 takes over at true time 3 at the earliest (local 5 >= 3 + 2).
  run guardedHandoverTest =
    initWith(SLOW_FAST)
      .then(Acquire(true, "c1"))
      .then(Tick)
      .then(Tick)
      .then(LocalRead(true, "c1"))
      .expect(NoStaleRead)
      .then(Tick)
      .then(LocalRead(true, "c1"))
      .fail()

  run guardedSuccessorWritesTest =
    initWith(SLOW_FAST)
      .then(Acquire(true, "c1"))
      .then(Tick)
      .then(Tick)
      .then(Tick)
      .then(Acquire(true, "c2"))
      .then(Write("c2"))
      .then(LocalRead(true, "c2"))
      .expect(holder == "c2" and regVal == 1 and Inv)

  // Only the holder writes.
  run writeNeedsHolderTest =
    initWith(SLOW_FAST)
      .then(Acquire(true, "c1"))
      .then(Write("c2"))
      .fail()
}
