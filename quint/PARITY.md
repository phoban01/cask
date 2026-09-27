# TLA+ to Quint parity

Each spec in `tla/` gets a Quint port in `quint/`. A port is done when TLC
finds the same number of distinct states for the original and for the
Quint module compiled to TLA+, with the constants from the original `.cfg`.
When every row is filled, `tla/` is retired (#17).

| TLA+ spec | Quint module | Invariants | TLA+ distinct states (TLC) | Quint result | Notes |
|-----------|--------------|------------|----------------------------|--------------|-------|
| `CasPaxosMvcc.tla` | `caspaxos.qnt` | `Consistency`, `OneValuePerBallot`, `VotesSafe`, `OneVotePerBallot`, `TypeOK`, `Inv` | 7790 (115141 generated, depth 10) | TLC on the compiled module: 7790 distinct (128725 generated, depth 10), no violation. `quint run`: no violation. Control `stepNoPromise` violates `Consistency` and `VotesSafe`. `quint verify --max-steps=6` (Apalache 0.56.1): no violation in about 2 minutes; the control violates `Consistency`. | Same constants as the `.cfg`. More generated states because `step` picks a value for `IncreaseMaxBal` too. `SafeAt` ranges over `BALLOTS` with `c < b` so Apalache accepts it. |
| `OwnedRegister.tla` | `owned_register.qnt` | `ChosenChain` (the TLA+ `NoLostUpdate`), `OneValuePerBallot`, `Inv` | Without `SYMMETRY`: 5663662 (29622006 generated, depth 26). With `SYMMETRY Perms` as in the `.cfg`: 1034912 (5444229 generated, depth 26). `OwnedRegisterBug.cfg` violates `Inv` at depth 10 (58869 distinct without `SYMMETRY`). | TLC on the compiled module: 5663662 distinct (32981229 generated, depth 26), no violation. Control `stepBumpRule` violates `ChosenChain` at depth 10 (58869 distinct). `quint run`: no violation; the control violates `ChosenChain`. `quint verify --max-steps=6` (Apalache 0.56.1): no violation in about 3 minutes. | #10. Same constants as the `.cfg`. Acceptors are strings, so the compiled module cannot use `SYMMETRY`; compare the counts without it. The control stands for `OwnedRegisterBug.cfg`. Its shortest trace has 9 steps. `quint verify` at 9 steps did not finish in 15 minutes. |
| `Lease.tla` | not ported | | | | #11 |
| `OwnerReads.tla` | not ported | | | | #12. `OwnerReadsBug.cfg` is the negative control. |
| `Reconfig.tla` | not ported | | | | #13 |
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
