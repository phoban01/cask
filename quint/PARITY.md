# TLA+ to Quint parity

Each spec in the old `tla/` directory has a Quint port in `quint/`. A port
is done when TLC finds the same number of distinct states for the original
and for the Quint module compiled to TLA+, with the constants from the
original `.cfg`. Every row is filled, so `tla/` is retired (#17). Commit
`82ae0bb` is the last commit that holds it. "How to reproduce a row" shows
how to get the originals back.

| TLA+ spec | Quint module | Invariants | TLA+ distinct states (TLC) | Quint result | Notes |
|-----------|--------------|------------|----------------------------|--------------|-------|
| `CasPaxosMvcc.tla` | `caspaxos.qnt` | `Consistency`, `OneValuePerBallot`, `VotesSafe`, `OneVotePerBallot`, `TypeOK`, `Inv` | 7790 (115141 generated, depth 10) | TLC on the compiled module: 7790 distinct (128725 generated, depth 10), no violation. `quint run`: no violation. Control `stepNoPromise` violates `Consistency` and `VotesSafe`. `quint verify --max-steps=6` (Apalache 0.56.1): no violation in about 2 minutes; the control violates `Consistency`. | Same constants as the `.cfg`. More generated states because `step` picks a value for `IncreaseMaxBal` too. `SafeAt` ranges over `BALLOTS` with `c < b` so Apalache accepts it. |
| `OwnedRegister.tla` | `owned_register.qnt` | `ChosenChain` (the TLA+ `NoLostUpdate`), `OneValuePerBallot`, `Inv` | Without `SYMMETRY`: 5663662 (29622006 generated, depth 26). With `SYMMETRY Perms` as in the `.cfg`: 1034912 (5444229 generated, depth 26). `OwnedRegisterBug.cfg` violates `Inv` at depth 10 (58869 distinct without `SYMMETRY`). | TLC on the compiled module: 5663662 distinct (32981229 generated, depth 26), no violation. Control `stepBumpRule` violates `ChosenChain` at depth 10 (58869 distinct). `quint run`: no violation; the control violates `ChosenChain`. `quint verify --max-steps=6` (Apalache 0.56.1): no violation in about 3 minutes. | #10. Same constants as the `.cfg`. Acceptors are strings, so the compiled module cannot use `SYMMETRY`; compare the counts without it. The control stands for `OwnedRegisterBug.cfg`. Its shortest trace has 9 steps. `quint verify` at 9 steps did not finish in 15 minutes. |
| `Lease.tla` | `lease.qnt` | `SingleHolder`, `FenceLatest`, `FenceMonotone`, `TypeOK`, `Inv` | 635 (2013 generated, depth 10) | TLC on the compiled module with `VIEW` of the five TLA+ variables: 635 distinct (2013 generated, depth 10), no violation of `Inv`, `FenceMonotone`, or the TLA+ property `[][fence' >= fence]`. Without the `VIEW`: 1145 distinct (4011 generated, depth 10). Controls `stepBumpReuse` and `stepAcquireReuse` violate `FenceMonotone` at depth 3 and depth 2 (no `VIEW`); both keep `Inv` and the TLA+ property. `quint run`: no violation; both controls violate `FenceMonotone`. `quint verify --max-steps=10` (Apalache 0.56.1): no violation in about 5 seconds; both controls violate `FenceMonotone` at 3 steps. | #11. Same constants as the `.cfg`. The TLA+ `FenceMonotone` is a temporal property. The port adds history variables `prevFence` and `minted` and checks it as a state invariant, with a strict rise on every grant. The history variables add states, so compare with the `VIEW`. Do not use the `VIEW` for a control: a `Bump` that keeps the fence then looks like a seen state, and TLC does not check it. |
| `OwnerReads.tla` | `owner_reads.qnt` | `NoStaleRead`, `TypeOK`, `Inv` | 59775 (126737 generated, depth 19). `OwnerReadsBug.cfg` violates `Inv` at depth 6 (1232 distinct, 2258 generated). | TLC on the compiled module: 59775 distinct (126737 generated, depth 19), no violation. Control `stepNaive` violates `Inv` at depth 6 (1232 distinct, 2258 generated) with the same trace: c1 has clock offset -2 and c2 has offset 2. c1 acquires, time ticks, c2 acquires and writes, and c1 serves a stale read. `quint run`: no violation; the control violates `NoStaleRead` in 20 of 20 runs with different seeds. `quint verify --max-steps=18` (Apalache 0.56.1): no violation in about 4 minutes; the control violates `NoStaleRead` at 5 steps in about 3 seconds. | #12. Same constants as the `.cfg`. The control stands for `OwnerReadsBug.cfg`. At 18 steps `quint verify` reaches every state. |
| `Reconfig.tla` | `reconfig.qnt` | `NoLostValue`, `CatchUpHeld`, `TypeOK`, `Inv` | 173 (753 generated, depth 8). The `.cfg` has no `SYMMETRY`, and there is no Bug config. With the catch-up check removed from `LeaveJoint`, TLC finds `Inv` violated at depth 5 (61 distinct, 143 generated). | TLC on the compiled module: 173 distinct (753 generated, depth 8), no violation. Control `stepNoCatchUp` violates `CatchUpHeld` at depth 5 (61 distinct, 143 generated). The trace: a1 and a2 vote v1, so Cold chooses v1. The change enters joint and leaves it with no Cnew quorum for v1. `quint run`: no violation; the control violates `CatchUpHeld` in 20 of 20 runs with different seeds. `quint verify --max-steps=7` (Apalache 0.56.1): no violation in about 13 seconds; the control violates `CatchUpHeld` in about 3 seconds. | #13. Same constants as the `.cfg`. The control is new. It drops the catch-up check, as the issue asks. At 7 steps `quint verify` reaches every state. |
| `RosterReconfig.tla` | `roster_reconfig.qnt` | `NoLostMembership`, `AlwaysAvailable`, `TypeOK`, `Inv` | 117 (554 generated, depth 5). The `.cfg` has no `SYMMETRY`, and there is no Bug config. With the catch-up check removed from `LeaveJoint`, TLC finds `Inv` violated at depth 3 (28 distinct, 103 generated). With the joint write sent only to the old core, as issue #14 names, TLC finds no violation (24 distinct, 173 generated, depth 4). | TLC on the compiled module: 117 distinct (554 generated, depth 5), no violation. Control `stepNoCatchUp` violates `Inv` at depth 3 (28 distinct, 103 generated). The trace: the change enters joint and leaves it at once. Cnew holds no chosen version. `quint run`: no violation; the control violates `NoLostMembership` in 20 of 20 runs with different seeds, and `AlwaysAvailable` in 20 of 20. `quint verify --max-steps=4` (Apalache 0.56.1): no violation in about 73 seconds; the control violates `NoLostMembership` at 2 steps in about 12 seconds. | #14. Same constants as the `.cfg`. The control is new. It drops the catch-up check. The issue names a joint write that reaches only the old core. That write cannot break `NoLostMembership`, because the catch-up check blocks the release. It stops the change from finishing, which is a liveness loss. The witness run `oldOnlyWriteBlocksReleaseTest` shows it. At 4 steps `quint verify` reaches every state. |
| `RangeDescriptors.tla` | `range_descriptors.qnt` | `NoSplitBrain`, `AuthorityExistsAndLive`, `TombstoneIsTerminal`, `TypeOK`, `Inv`; liveness `EventuallyCaughtUp` | 2260 (5181 generated, depth 17), no violation of `Inv` or `EventuallyCaughtUp`. The `.cfg` has no `SYMMETRY`, and there is no Bug config. | TLC on the compiled module, with a wrapper `Spec == q_init /\ [][q_step]_vars /\ Fairness`: 2260 distinct (5181 generated, depth 17), no violation of `Inv` or `EventuallyCaughtUp`. Control `stepNoEpochCheck` violates `Inv` at depth 5 (27 distinct). Control `stepCutoverFirst` violates `Inv` at depth 2 (3 distinct). Control `stepTombstoneKeepsLive` violates `Inv` at depth 5 (24 distinct). `quint run`: no violation; each control violates its invariant (`NoSplitBrain`, `AuthorityExistsAndLive`, `TombstoneIsTerminal`) in 20 of 20 runs with different seeds. `quint verify --max-steps=16` (Apalache 0.56.1): no violation in about 80 seconds; each control violates its invariant in about 3 seconds. | #15. Ported, not retired. Ranges and splits stay off the fleet path, but `cmd/cask` still serves split and merge (`cmd/cask/cluster.go`) through `internal/ranges` (`Orchestrator.Split`, `Orchestrator.Merge`, the descriptor register, the `ErrRangeChanged` epoch check). The proof stays while that code ships. Same constants as the `.cfg`. The TLA+ variable `exists` is `present`, because `exists` is a Quint built-in. The three controls are new; each drops one rule. `quint run` and `quint verify` do not check the liveness property; TLC does, on the compiled module. At 16 steps `quint verify` reaches every state. |
| `CrossRange.tla` | `cross_range.qnt` | `NoSkewOverflow`, `UncertaintyContract`, `TypeOK`, `Inv` | 15993045 (43030083 generated, depth 14), no violation, in about 5 minutes. The `.cfg` has no `SYMMETRY`, and there is no Bug config. | TLC on the compiled module, with the JVM heap at 3 GB: 15993045 distinct (43030083 generated, depth 14), no violation, in about 5 minutes. Control `stepNoReadGuard` violates `Inv` at depth 3 (145 distinct, 192 generated). Control `stepNoSkewGuard` violates `Inv` at depth 4 (55 distinct, 76 generated). `quint run`: no violation; `stepNoReadGuard` violates `UncertaintyContract` and `stepNoSkewGuard` violates `NoSkewOverflow`, each in 20 of 20 runs with different seeds. `quint verify --max-steps=10` (Apalache 0.56.1): no violation in about 7 minutes. At 8 steps it takes about 80 seconds. At 13 steps it did not finish in 12 minutes. Each control violates its invariant in about 3 seconds. | #16. Ported, not retired. Cross-range snapshots stay off the fleet path, and no binary calls `SnapshotRead`. But `internal/mvcc` still ships `SnapshotAt` and `SnapshotRead`, and `internal/hlc` stamps every version that `cmd/cask` and `cmd/cask-apiserver` write. The proof stays while that code ships. Same constants as the `.cfg`. `RangeOfKey` maps k1 to r1 and k2 to r2 in place of the `CHOOSE` in `MCRangeOfKey`; the other onto map only renames the ranges. The TLA+ `snap` of `<<>>` or a 4-tuple is a record with a `taken` flag. Both controls are new; each drops one guard. `quint verify` at 10 steps does not reach every state; TLC does. The old TLA+ README called the read guard load-bearing, and `stepNoReadGuard` shows why. `SnapshotAt` enforces it since #182 (issue #181). |

## Add the port to the gate

`scripts/quint-check.sh` checks every module that has `quint-check:`
header comments. Do not edit the script. Put these lines above `module`:

```
// quint-check: invariants=Consistency,OneValuePerBallot,VotesSafe
// quint-check: control=stepNoPromise:Consistency
// quint-check: verify-steps=6
```

- `invariants=` lists the invariants that the good step must keep.
- `control=step:invariant` names one negative control. Add one line per
  control. The gate fails if a module has no control.
- `verify-steps=` sets the `quint verify` bound. The default is 12.
- `step=` names the good step. The default is `step`.

## How to reproduce a row

TLC needs `tla2tools.jar`. The compiled module also needs `Apalache.tla` and
`Variants.tla`, which ship inside the Apalache jar that Quint uses. Work in a
temporary directory outside the repo.

The originals are no longer in the tree. Get them from commit `82ae0bb`:

```sh
git archive 82ae0bb tla | tar -x -C "$WORKDIR"
cd "$WORKDIR/tla"
```

Each row names the `.cfg` it used. `OwnedRegisterBug.cfg` and
`OwnerReadsBug.cfg` are the two negative controls. TLC must report a
violation for each.

```sh
# The original.
java -cp tla2tools.jar tlc2.TLC -workers 1 -config CasPaxosMvcc.cfg CasPaxosMvcc.tla

# The port. Drop the Apalache log lines above the module header first.
quint compile --target tlaplus quint/caspaxos.qnt > caspaxos.tla
cat > caspaxos.cfg <<'EOF'
INIT q_init
NEXT q_step
INVARIANT Inv
CHECK_DEADLOCK FALSE
EOF
java -cp tla2tools.jar tlc2.TLC -workers 1 -config caspaxos.cfg caspaxos.tla
```

Use `-workers 1`. With more workers TLC reports a different search depth.
The distinct state count does not change.

For `quint verify`, keep the bound small. The shortest trace that breaks
`Consistency` under `stepNoPromise` has 6 steps. At 10 steps the good step
ran for more than 40 minutes without an answer.

## Notes kept from the TLA+ README

- The first TLC run of the suite (2026-07-14) found four defects in the
  specs. `Lease.tla` and `CrossRange.tla` used unbounded `CHOOSE`
  sentinels, which TLC cannot evaluate. They became model values.
  `CrossRange.cfg` assigned a function literal, which a `.cfg` does not
  allow. It became a `<-` substitution of `MCRangeOfKey`. `Reconfig.tla`
  evaluated "chosen in Cold" after the fact, so a vote after release
  completed a Cold quorum that could never commit. It became a snapshot
  taken at `LeaveJoint`. The ports keep all four fixes.
- The same run found one real design rule. A snapshot read at a time
  above the range's applied HLC breaks the uncertainty contract, because a
  later write can commit below that time. `cross_range.qnt` keeps the
  guard and its control `stepNoReadGuard`. `mvcc.SnapshotAt` enforces the
  guard since #182 (issue #181).
- `OwnedRegister.tla` had a precedence trap. In
  `bad' = bad \/ ~(...)`, `=` binds tighter than `\/`, so `bad'` stays
  unassigned on the one step that breaks the invariant. The fix is
  parentheses. `owned_register.qnt` keeps them and says why in
  `RecordCommit`.
- The scope notes moved to the module headers: agreement only in
  `caspaxos.qnt`, one register only in `lease.qnt`, the guard in
  `cross_range.qnt`, and the composition with `caspaxos.qnt` and
  `reconfig.qnt` in `range_descriptors.qnt`.
