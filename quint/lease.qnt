// quint-check: invariants=SingleHolder,FenceLatest,FenceMonotone,TypeOK
// quint-check: control=stepBumpReuse:FenceMonotone
// quint-check: control=stepAcquireReuse:FenceMonotone
// quint-check: verify-steps=10
//
// The shortest violation under either control has 1 or 2 steps. TLC
// reports a search depth of 10, so every state is at most 9 steps from
// init. `quint verify` at 10 steps reaches every state and finishes in
// about 5 seconds (quint/PARITY.md).
//
// lease.qnt — Quint port of tla/Lease.tla.
//
// A lease is a CASPaxos register that holds {owner, expiry, fence}. Time is
// a counter `now`. A lease is live while its expiry is in the future.
// Acquire is enabled only when no live lease exists. This models lazy
// expiry: an expired lease is never seen as live.
//
// Invariants:
//
//   SingleHolder   at most one client holds a live lease
//   FenceLatest    the live holder carries the latest fence, and no client
//                  holds a fence above the latest
//   FenceMonotone  the fence never decreases, and every grant (Acquire or
//                  Bump) mints a fence above the one before
//   TypeOK         the variables stay in their ranges
//   Inv            TypeOK, SingleHolder and FenceLatest, as in the TLA+
//
// Constants are the values from tla/Lease.cfg: two clients, MaxTime 4,
// TTL 2, MaxFence 6.
//
// Differences from the TLA+ source:
//   - Constants are pure vals, not CONSTANTS, so the model is fixed.
//     NoClient is the string "none".
//   - FenceMonotone is a temporal property in the TLA+:
//     [][fence' >= fence]_vars. Quint checks state invariants, so the port
//     adds two history variables. prevFence is the fence before the last
//     step. minted is true when the last step was a grant. FenceMonotone
//     checks fence >= prevFence, and fence > prevFence after a grant.
//   - The strict half is new. It is the spec sentence in
//     docs/spec/fleet.md section 5. The TLA+ property cannot see it: a Bump
//     that keeps the fence is a stuttering step.
//   - The history variables add states. quint/PARITY.md compares the
//     counts with a TLC VIEW that leaves them out.
//   - Bump picks f from the constant set FENCES with f >= fence + 1, not
//     from (fence + 1)..MaxFence, so that Apalache accepts it. bumpTo takes
//     the lowest allowed fence and the picked fence as parameters.
//   - The negative controls are new. stepBumpReuse lets Bump keep the
//     current fence. stepAcquireReuse lets Acquire hand out the current
//     fence, so a new holder and an expired holder share one token. Both
//     MUST violate FenceMonotone. FenceLatest does not catch either.
module lease {
  pure val CLIENTS = Set("c1", "c2")
  pure val NO_CLIENT = "none"
  pure val MAX_TIME = 4
  pure val TTL = 2
  pure val MAX_FENCE = 6
  pure val FENCES = 0.to(MAX_FENCE)

  var owner: str         // the client that holds the grant, or NO_CLIENT
  var expiry: int        // absolute time the current grant expires
  var fence: int         // counter of fences minted
  var held: str -> int   // held[c] = the fence client c believes it holds (0 = none)
  var now: int           // current time
  // History variables for FenceMonotone. The TLA+ has neither.
  var prevFence: int     // the fence before the last step
  var minted: bool       // the last step was a grant (Acquire or Bump)

  val TypeOK = and {
    CLIENTS.union(Set(NO_CLIENT)).contains(owner),
    0 <= expiry and expiry <= MAX_TIME + TTL,
    FENCES.contains(fence),
    held.keys() == CLIENTS,
    CLIENTS.forall(c => FENCES.contains(held.get(c))),
    0 <= now and now <= MAX_TIME,
  }

  action init = all {
    owner' = NO_CLIENT,
    expiry' = 0,
    fence' = 0,
    held' = CLIENTS.mapBy(c => 0),
    now' = 0,
    prevFence' = 0,
    minted' = false,
  }

  // A live lease is one owned by a client whose grant has not expired.
  val Live = owner != NO_CLIENT and expiry > now

  // ---- actions --------------------------------------------------------------

  // Acquire with the minted fence as a parameter. Acquire passes fence + 1.
  // The negative control passes the current fence.
  action acquireAt(c: str, f: int): bool = all {
    not(Live),
    fence < MAX_FENCE,
    fence' = f,
    owner' = c,
    expiry' = now + TTL,
    held' = held.set(c, f),
    now' = now,
    prevFence' = fence,
    minted' = true,
  }

  //= docs/spec/fleet.md#5-claims-and-fencing
  //= type=implication
  //# Every successful acquisition MUST mint a fence strictly greater than
  //# every fence previously minted for that object.
  // Acquire is enabled only when no live lease exists (lazy expiry). It
  // mints fence + 1 and hands it to the acquirer.
  action Acquire(c: str): bool = acquireAt(c, fence + 1)

  // The holder c moves to fence f, with f at least lo. Bump passes
  // lo = fence + 1. The negative control passes lo = fence.
  action bumpTo(c: str, lo: int, f: int): bool = all {
    owner == c,
    Live,
    fence < MAX_FENCE,
    lo <= f,
    fence' = f,
    held' = held.set(c, f),
    owner' = owner,
    expiry' = expiry,
    now' = now,
    prevFence' = fence,
    minted' = true,
  }

  // The live holder raises its own fence, possibly by more than one: the
  // Locks.Bump operation (W0). When the consensus layer reports the
  // register at a higher epoch, the holder jumps its fence past it without
  // a release. The owner does not change and still carries the latest
  // fence.
  action Bump(c: str): bool = {
    nondet f = FENCES.oneOf()
    bumpTo(c, fence + 1, f)
  }

  // The live holder extends its expiry. The fence does not change.
  action Renew(c: str): bool = all {
    owner == c,
    Live,
    expiry' = now + TTL,
    owner' = owner,
    fence' = fence,
    held' = held,
    now' = now,
    prevFence' = fence,
    minted' = false,
  }

  // The holder gives the lease up.
  action Release(c: str): bool = all {
    owner == c,
    owner' = NO_CLIENT,
    held' = held.set(c, 0),
    expiry' = expiry,
    fence' = fence,
    now' = now,
    prevFence' = fence,
    minted' = false,
  }

  action Tick: bool = all {
    now < MAX_TIME,
    now' = now + 1,
    owner' = owner,
    expiry' = expiry,
    fence' = fence,
    held' = held,
    prevFence' = fence,
    minted' = false,
  }

  // Each disjunct picks only its own client, as the TLA+ Next does.
  action step = any {
    { nondet c = CLIENTS.oneOf()
      Acquire(c) },
    { nondet c = CLIENTS.oneOf()
      Renew(c) },
    { nondet c = CLIENTS.oneOf()
      Bump(c) },
    { nondet c = CLIENTS.oneOf()
      Release(c) },
    Tick,
  }

  // ---- invariants -------------------------------------------------------------

  //= docs/spec/fleet.md#5-claims-and-fencing
  //= type=implication
  //# At most one claim MUST be Bound to an object at the object's current
  //# fence.
  // At most one client holds a live lease. The check is trivial by
  // structure. The model checkers confirm that no action breaks it.
  val SingleHolder =
    tuples(CLIENTS, CLIENTS).forall(((c, d)) =>
      (owner == c and expiry > now and owner == d) implies c == d)

  // The live holder carries the latest fence, and no client believes it
  // holds a fence above the latest.
  val FenceLatest = and {
    Live implies held.get(owner) == fence,
    CLIENTS.forall(c => held.get(c) <= fence),
  }

  val Inv = and { TypeOK, SingleHolder, FenceLatest }

  // The fence never decreases across a step (the TLA+ FenceMonotone), and
  // a grant mints a fence above every earlier one.
  val FenceMonotone = and {
    fence >= prevFence,
    minted implies fence > prevFence,
  }

  // ---- negative controls: reuse the current fence -------------------------------

  // Bump keeps the current fence. The holder believes it moved past the
  // higher epoch, but its token did not change.
  action stepBumpReuse = any {
    { nondet c = CLIENTS.oneOf()
      Acquire(c) },
    { nondet c = CLIENTS.oneOf()
      Renew(c) },
    { nondet c = CLIENTS.oneOf()
      nondet f = FENCES.oneOf()
      bumpTo(c, fence, f) },
    { nondet c = CLIENTS.oneOf()
      Release(c) },
    Tick,
  }

  // Acquire hands out the current fence. A new holder and an expired
  // holder then share one token, so a receiver cannot reject the zombie.
  action stepAcquireReuse = any {
    { nondet c = CLIENTS.oneOf()
      acquireAt(c, fence) },
    { nondet c = CLIENTS.oneOf()
      Renew(c) },
    { nondet c = CLIENTS.oneOf()
      Bump(c) },
    { nondet c = CLIENTS.oneOf()
      Release(c) },
    Tick,
  }

  // ---- witness runs ------------------------------------------------------------

  // c1 takes the lease and it expires. c2 takes it with a higher fence. The
  // zombie c1 still holds fence 1, so a receiver can reject it.
  run expiryHandoverTest =
    init
      .then(Acquire("c1"))
      .expect(Live and owner == "c1" and held.get("c1") == 1 and FenceMonotone)
      .then(Tick)
      .then(Tick)
      .expect(not(Live))
      .then(Acquire("c2"))
      .expect(owner == "c2" and held.get("c2") == 2 and held.get("c1") < fence
        and Inv and FenceMonotone)

  // A live lease blocks a second acquirer.
  run liveLeaseBlocksAcquireTest =
    init
      .then(Acquire("c1"))
      .then(Acquire("c2"))
      .fail()

  // Renew extends the expiry and keeps the fence.
  run renewKeepsFenceTest =
    init
      .then(Acquire("c1"))
      .then(Tick)
      .then(Renew("c1"))
      .expect(expiry == 3 and fence == 1 and not(minted) and Inv and FenceMonotone)

  // Bump jumps the fence by more than one and the holder keeps the lease.
  run bumpJumpsFenceTest =
    init
      .then(Acquire("c1"))
      .then(bumpTo("c1", 2, 4))
      .expect(owner == "c1" and fence == 4 and held.get("c1") == 4
        and Inv and FenceMonotone)

  // Bump cannot keep the fence, and only the holder may bump.
  run bumpNeedsHigherFenceTest =
    init
      .then(Acquire("c1"))
      .then(bumpTo("c1", 2, 1))
      .fail()

  run bumpNeedsHolderTest =
    init
      .then(Acquire("c1"))
      .then(bumpTo("c2", 2, 2))
      .fail()

  // Release clears the holder's fence and lets another client acquire.
  run releaseTest =
    init
      .then(Acquire("c1"))
      .then(Release("c1"))
      .then(Acquire("c2"))
      .expect(owner == "c2" and held.get("c1") == 0 and fence == 2 and Inv)

  // Control: a Bump that keeps the fence. FenceLatest still holds, and the
  // TLA+ property sees a stuttering step. FenceMonotone fails.
  run bumpReuseBreaksMonotoneTest =
    init
      .then(Acquire("c1"))
      .then(bumpTo("c1", 1, 1))
      .expect(fence == 1 and Inv and not(FenceMonotone))

  // Control: c2 acquires with the fence c1 still holds after expiry. Both
  // clients hold fence 1. FenceLatest still holds. FenceMonotone fails.
  run acquireReuseBreaksMonotoneTest =
    init
      .then(Acquire("c1"))
      .then(Tick)
      .then(Tick)
      .then(acquireAt("c2", 1))
      .expect(held.get("c1") == 1 and held.get("c2") == 1 and Inv
        and not(FenceMonotone))
}
