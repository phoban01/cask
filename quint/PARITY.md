# TLA+ to Quint parity

Each spec in `tla/` gets a Quint port in `quint/`. A port is done when TLC
finds the same number of distinct states for the original and for the
Quint module compiled to TLA+, with the constants from the original `.cfg`.
When every row is filled, `tla/` is retired (#17).

| TLA+ spec | Quint module | Invariants | TLA+ distinct states (TLC) | Quint result | Notes |
|-----------|--------------|------------|----------------------------|--------------|-------|
| `CasPaxosMvcc.tla` | `caspaxos.qnt` | `Consistency`, `OneValuePerBallot`, `VotesSafe`, `OneVotePerBallot`, `TypeOK`, `Inv` | 7790 (115141 generated, depth 10) | TLC on the compiled module: 7790 distinct (128725 generated, depth 10), no violation. `quint run`: no violation. Control `stepNoPromise` violates `Consistency` and `VotesSafe`. `quint verify --max-steps=6` (Apalache 0.56.1): no violation in about 2 minutes; the control violates `Consistency`. | Same constants as the `.cfg`. More generated states because `step` picks a value for `IncreaseMaxBal` too. `SafeAt` ranges over `BALLOTS` with `c < b` so Apalache accepts it. |
| `OwnedRegister.tla` | `owned_register.qnt` | `ChosenChain` (the TLA+ `NoLostUpdate`), `OneValuePerBallot`, `Inv` | Without `SYMMETRY`: 5663662 (29622006 generated, depth 26). With `SYMMETRY Perms` as in the `.cfg`: 1034912 (5444229 generated, depth 26). `OwnedRegisterBug.cfg` violates `Inv` at depth 10 (58869 distinct without `SYMMETRY`). | TLC on the compiled module: 5663662 distinct (32981229 generated, depth 26), no violation. Control `stepBumpRule` violates `ChosenChain` at depth 10 (58869 distinct). `quint run`: no violation; the control violates `ChosenChain`. `quint verify --max-steps=6` (Apalache 0.56.1): no violation in about 3 minutes. | #10. Same constants as the `.cfg`. Acceptors are strings, so the compiled module cannot use `SYMMETRY`; compare the counts without it. The control stands for `OwnedRegisterBug.cfg`. Its shortest trace has 9 steps. `quint verify` at 9 steps did not finish in 15 minutes. |
| `Lease.tla` | `lease.qnt` | `SingleHolder`, `FenceLatest`, `FenceMonotone`, `TypeOK`, `Inv` | 635 (2013 generated, depth 10) | TLC on the compiled module with `VIEW` of the five TLA+ variables: 635 distinct (2013 generated, depth 10), no violation of `Inv`, `FenceMonotone`, or the TLA+ property `[][fence' >= fence]`. Without the `VIEW`: 1145 distinct (4011 generated, depth 10). Controls `stepBumpReuse` and `stepAcquireReuse` violate `FenceMonotone` at depth 3 and depth 2 (no `VIEW`); both keep `Inv` and the TLA+ property. `quint run`: no violation; both controls violate `FenceMonotone`. `quint verify --max-steps=10` (Apalache 0.56.1): no violation in about 5 seconds; both controls violate `FenceMonotone` at 3 steps. | #11. Same constants as the `.cfg`. The TLA+ `FenceMonotone` is a temporal property. The port adds history variables `prevFence` and `minted` and checks it as a state invariant, with a strict rise on every grant. The history variables add states, so compare with the `VIEW`. Do not use the `VIEW` for a control: a `Bump` that keeps the fence then looks like a seen state, and TLC does not check it. |
| `OwnerReads.tla` | `owner_reads.qnt` | `NoStaleRead`, `TypeOK`, `Inv` | 59775 (126737 generated, depth 19). `OwnerReadsBug.cfg` violates `Inv` at depth 6 (1232 distinct, 2258 generated). | TLC on the compiled module: 59775 distinct (126737 generated, depth 19), no violation. Control `stepNaive` violates `Inv` at depth 6 (1232 distinct, 2258 generated) with the same trace: c1 has clock offset -2 and c2 has offset 2. c1 acquires, time ticks, c2 acquires and writes, and c1 serves a stale read. `quint run`: no violation; the control violates `NoStaleRead` in 20 of 20 runs with different seeds. `quint verify --max-steps=18` (Apalache 0.56.1): no violation in about 4 minutes; the control violates `NoStaleRead` at 5 steps in about 3 seconds. | #12. Same constants as the `.cfg`. The control stands for `OwnerReadsBug.cfg`. At 18 steps `quint verify` reaches every state. |
| `Reconfig.tla` | `reconfig.qnt` | `NoLostValue`, `CatchUpHeld`, `TypeOK`, `Inv` | 173 (753 generated, depth 8). The `.cfg` has no `SYMMETRY`, and there is no Bug config. With the catch-up check removed from `LeaveJoint`, TLC finds `Inv` violated at depth 5 (61 distinct, 143 generated). | TLC on the compiled module: 173 distinct (753 generated, depth 8), no violation. Control `stepNoCatchUp` violates `CatchUpHeld` at depth 5 (61 distinct, 143 generated). The trace: a1 and a2 vote v1, so Cold chooses v1. The change enters joint and leaves it with no Cnew quorum for v1. `quint run`: no violation; the control violates `CatchUpHeld` in 20 of 20 runs with different seeds. `quint verify --max-steps=7` (Apalache 0.56.1): no violation in about 13 seconds; the control violates `CatchUpHeld` in about 3 seconds. | #13. Same constants as the `.cfg`. The control is new. It drops the catch-up check, as the issue asks. At 7 steps `quint verify` reaches every state. |
| `RosterReconfig.tla` | not ported | | | | #14 |
| `RangeDescriptors.tla` | not ported | | | | #15 |
| `CrossRange.tla` | not ported | | | | #16 |

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
