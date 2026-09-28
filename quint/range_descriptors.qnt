// quint-check: invariants=NoSplitBrain,AuthorityExistsAndLive,TombstoneIsTerminal,TypeOK
// quint-check: control=stepNoEpochCheck:NoSplitBrain
// quint-check: control=stepCutoverFirst:AuthorityExistsAndLive
// quint-check: control=stepTombstoneKeepsLive:TombstoneIsTerminal
// quint-check: verify-steps=16
//
// Inv is TypeOK, NoSplitBrain, AuthorityExistsAndLive and
// TombstoneIsTerminal under the TLA+ name, so the gate lists the four
// parts. TLC reports a search depth of 17, so every state is at most 16
// steps from init. `quint verify` at 16 steps reaches every state and
// finishes in about 80 seconds (quint/PARITY.md). The shortest violations
// under the controls have 1 to 4 steps. Each control found its violation
// in 20 of 20 runs with different seeds at 30 steps and 2000 samples.
//
// range_descriptors.qnt — Quint port of RangeDescriptors.tla. The TLA+ source is
// retired. quint/PARITY.md names the commit that still holds it.
//
// Scope: ranges and splits stay off the fleet path. The fleet uses
// per-object registers and index registers. This module covers the split
// and merge code that cmd/cask still ships: internal/ranges
// Orchestrator.Split and Merge, the descriptor register, and the
// ErrRangeChanged epoch check. The proof stays while that code ships.
//
// A split of range O makes two new ranges, L and R. It runs four commits
// in order:
//
//   1. CreateL    write descriptor L, live
//   2. CreateR    write descriptor R, live
//   3. Cutover    swap the roster's range ids from O to L and R
//   4. Tombstone  mark descriptor O not live
//
// The model follows one key K. Its authoritative range is O before the
// cutover and L from the cutover on. A client writes to the range it
// believes is authoritative. The proposer accepts only when that belief
// matches the current authoritative range (the descriptor epoch check,
// which returns ErrRangeChanged) and the range is live. So a client with
// a stale belief never writes to the old range.
//
// caspaxos.qnt has the per-register agreement under each descriptor.
// reconfig.qnt has the replica-set change of one range.
//
// Invariants:
//
//   NoSplitBrain            every accepted write went to the range that
//                           was authoritative in the phase it was accepted
//   AuthorityExistsAndLive  the authoritative range exists and is live
//   TombstoneIsTerminal     once tombstoned, O is not live
//   TypeOK                  the variables stay in their ranges
//   Inv                     all four, as in the TLA+
//
// Liveness: EventuallyCaughtUp says that every client comes to believe
// the authoritative range, under weak fairness of RefreshBelief. It is a
// temporal property here. quint run and quint verify do not check it.
// TLC checks it on the compiled module (quint/PARITY.md).
//
// Constants are the values from RangeDescriptors.cfg: two clients.
//
// Differences from the TLA+ source:
//   - Client is a pure val, not a CONSTANT, so the model is fixed.
//   - The TLA+ variable exists is present here. exists is a Quint
//     built-in.
//   - A write is a tuple (client, range, phase), as in the TLA+.
//   - The TLA+ has no Bug config. The negative controls are new. Each
//     drops one rule of the protocol:
//       stepNoEpochCheck        ClientWrite skips the epoch check. It MUST
//                               violate NoSplitBrain.
//       stepCutoverFirst        Cutover runs before the new descriptors
//                               exist. It MUST violate
//                               AuthorityExistsAndLive.
//       stepTombstoneKeepsLive  Tombstone advances the phase but leaves O
//                               live. It MUST violate TombstoneIsTerminal.
module range_descriptors {
  pure val CLIENTS = Set("c1", "c2")
  pure val RANGES = Set("O", "L", "R")
  pure val PHASES = Set("initial", "createdL", "createdR", "cutover", "tombstoned")

  // The authoritative range for K in phase p. The roster names O until the
  // cutover, and L from then on. R holds the other half of the split, by
  // symmetry.
  pure def Authoritative(p: str): str =
    if (Set("initial", "createdL", "createdR").contains(p)) "O" else "L"

  var phase: str                          // the split protocol phase
  var present: str -> bool                 // present[r]: descriptor r has a value (the TLA+ exists)
  var alive: str -> bool                  // alive[r]: descriptor r is live
  var believed: str -> str                // believed[c]: the range c routes K to
  var writes: Set[(str, str, str)]        // accepted writes (client, range, phase)

  val TypeOK = and {
    PHASES.contains(phase),
    present.keys() == RANGES,
    alive.keys() == RANGES,
    believed.keys() == CLIENTS,
    CLIENTS.forall(c => RANGES.contains(believed.get(c))),
    writes.forall(w => CLIENTS.contains(w._1) and RANGES.contains(w._2) and PHASES.contains(w._3)),
  }

  action init = all {
    phase' = "initial",
    present' = RANGES.mapBy(r => r == "O"),
    alive' = RANGES.mapBy(r => r == "O"),
    believed' = CLIENTS.mapBy(c => "O"),
    writes' = Set(),
  }

  // ---- protocol actions: the four split steps ---------------------------------

  action CreateL: bool = all {
    phase == "initial",
    phase' = "createdL",
    present' = present.set("L", true),
    alive' = alive.set("L", true),
    believed' = believed,
    writes' = writes,
  }

  action CreateR: bool = all {
    phase == "createdL",
    phase' = "createdR",
    present' = present.set("R", true),
    alive' = alive.set("R", true),
    believed' = believed,
    writes' = writes,
  }

  // The cutover commit on the roster. The authoritative range flips here.
  // It needs the phase in which both new descriptors exist.
  action cutoverFrom(ready: bool): bool = all {
    ready,
    phase' = "cutover",
    present' = present,
    alive' = alive,
    believed' = believed,
    writes' = writes,
  }

  action Cutover: bool = cutoverFrom(phase == "createdR")

  // Mark O not live. oDead is the value O's live flag takes.
  action tombstoneWith(oDead: bool): bool = all {
    phase == "cutover",
    phase' = "tombstoned",
    alive' = alive.set("O", not(oDead)),
    present' = present,
    believed' = believed,
    writes' = writes,
  }

  action Tombstone: bool = tombstoneWith(true)

  // ---- client actions ----------------------------------------------------------

  // c refreshes its range map to the current authoritative range, after
  // ErrRangeChanged or a periodic poll.
  action RefreshBelief(c: str): bool = all {
    believed.get(c) != Authoritative(phase),
    believed' = believed.set(c, Authoritative(phase)),
    phase' = phase,
    present' = present,
    alive' = alive,
    writes' = writes,
  }

  // c writes K to the range it believes. With epochCheck the proposer
  // accepts only when the belief matches the authoritative range. A stale
  // belief gets ErrRangeChanged and no commit.
  action clientWriteWith(epochCheck: bool, c: str): bool = all {
    epochCheck implies believed.get(c) == Authoritative(phase),
    alive.get(believed.get(c)),
    writes' = writes.union(Set((c, believed.get(c), phase))),
    phase' = phase,
    present' = present,
    alive' = alive,
    believed' = believed,
  }

  action ClientWrite(c: str): bool = clientWriteWith(true, c)

  action step = any {
    CreateL,
    CreateR,
    Cutover,
    Tombstone,
    { nondet c = CLIENTS.oneOf()
      RefreshBelief(c) },
    { nondet c = CLIENTS.oneOf()
      ClientWrite(c) },
  }

  // ---- invariants -------------------------------------------------------------

  // Every accepted write went to the range that was authoritative in the
  // phase it was accepted. So a stale belief never writes to the old range.
  val NoSplitBrain = writes.forall(w => w._2 == Authoritative(w._3))

  // The authoritative range for K is always there and live.
  val AuthorityExistsAndLive = and {
    present.get(Authoritative(phase)),
    alive.get(Authoritative(phase)),
  }

  // Once tombstoned, O accepts no writes.
  val TombstoneIsTerminal = phase == "tombstoned" implies not(alive.get("O"))

  val Inv = and { TypeOK, NoSplitBrain, AuthorityExistsAndLive, TombstoneIsTerminal }

  // ---- liveness ----------------------------------------------------------------

  // Every client eventually believes the authoritative range, if each
  // RefreshBelief that stays enabled is taken.
  temporal Fairness =
    CLIENTS.forall(c => weakFair(RefreshBelief(c), (phase, present, alive, believed, writes)))

  temporal EventuallyCaughtUp =
    eventually(always(CLIENTS.forall(c => believed.get(c) == Authoritative(phase))))

  // ---- negative controls ---------------------------------------------------------

  // The proposer skips the epoch check. A client that still routes to O
  // writes there after the cutover. It MUST violate NoSplitBrain.
  action stepNoEpochCheck = any {
    CreateL,
    CreateR,
    Cutover,
    Tombstone,
    { nondet c = CLIENTS.oneOf()
      RefreshBelief(c) },
    { nondet c = CLIENTS.oneOf()
      clientWriteWith(false, c) },
  }

  // The cutover runs before the new descriptors exist. It MUST violate
  // AuthorityExistsAndLive.
  action stepCutoverFirst = any {
    CreateL,
    CreateR,
    cutoverFrom(Set("initial", "createdL", "createdR").contains(phase)),
    Tombstone,
    { nondet c = CLIENTS.oneOf()
      RefreshBelief(c) },
    { nondet c = CLIENTS.oneOf()
      ClientWrite(c) },
  }

  // The tombstone step leaves O live. It MUST violate TombstoneIsTerminal.
  action stepTombstoneKeepsLive = any {
    CreateL,
    CreateR,
    Cutover,
    tombstoneWith(false),
    { nondet c = CLIENTS.oneOf()
      RefreshBelief(c) },
    { nondet c = CLIENTS.oneOf()
      ClientWrite(c) },
  }

  // ---- witness runs ------------------------------------------------------------

  // The full split with one prompt and one lagging client. The lagging
  // client cannot write across the cutover until it refreshes.
  run splitWithLaggingClientTest =
    init
      .then(ClientWrite("c1"))
      .then(CreateL)
      .then(CreateR)
      .then(Cutover)
      .then(RefreshBelief("c1"))
      .then(ClientWrite("c1"))
      .expect(writes == Set(("c1", "O", "initial"), ("c1", "L", "cutover")))
      .then(Tombstone)
      .expect(believed.get("c2") == "O" and not(alive.get("O")) and Inv)
      .then(RefreshBelief("c2"))
      .then(ClientWrite("c2"))
      .expect(Inv)

  // A client that still believes O gets ErrRangeChanged after the cutover.
  run staleBeliefRejectedTest =
    init
      .then(CreateL)
      .then(CreateR)
      .then(Cutover)
      .then(ClientWrite("c2"))
      .fail()

  // The cutover waits for both new descriptors.
  run cutoverNeedsDescriptorsTest =
    init
      .then(CreateL)
      .then(Cutover)
      .fail()

  // Control: without the epoch check the stale write lands in O.
  run noEpochCheckTest =
    init
      .then(CreateL)
      .then(CreateR)
      .then(Cutover)
      .then(clientWriteWith(false, "c2"))
      .expect(not(NoSplitBrain) and AuthorityExistsAndLive)

  // Control: a cutover before CreateL leaves K with no descriptor.
  run cutoverFirstTest =
    init
      .then(cutoverFrom(true))
      .expect(not(AuthorityExistsAndLive))

  // Control: a tombstone that leaves O live.
  run tombstoneKeepsLiveTest =
    init
      .then(CreateL)
      .then(CreateR)
      .then(Cutover)
      .then(tombstoneWith(false))
      .expect(not(TombstoneIsTerminal) and NoSplitBrain)
}
