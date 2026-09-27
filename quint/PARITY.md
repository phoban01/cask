# TLA+ to Quint parity

Each spec in `tla/` gets a Quint port in `quint/`. A port is done when TLC
finds the same number of distinct states for the original and for the
Quint module compiled to TLA+, with the constants from the original `.cfg`.
When every row is filled, `tla/` is retired (#17).

| TLA+ spec | Quint module | Invariants | TLA+ distinct states (TLC) | Quint result | Notes |
|-----------|--------------|------------|----------------------------|--------------|-------|
| `CasPaxosMvcc.tla` | `caspaxos.qnt` | `Consistency`, `OneValuePerBallot`, `VotesSafe`, `OneVotePerBallot`, `TypeOK`, `Inv` | 7790 (115141 generated, depth 10) | TLC on the compiled module: 7790 distinct (128725 generated, depth 10), no violation. `quint run`: no violation. Control `stepNoPromise` violates `Consistency` and `VotesSafe`. | Same constants as the `.cfg`. More generated states because `step` picks a value for `IncreaseMaxBal` too. `SafeAt` ranges over `BALLOTS` with `c < b` so Apalache accepts it. |
| `OwnedRegister.tla` | not ported | | | | #10. `OwnedRegisterBug.cfg` is the negative control. |
| `Lease.tla` | not ported | | | | #11 |
| `OwnerReads.tla` | not ported | | | | #12. `OwnerReadsBug.cfg` is the negative control. |
| `Reconfig.tla` | not ported | | | | #13 |
| `RosterReconfig.tla` | not ported | | | | #14 |
| `RangeDescriptors.tla` | not ported | | | | #15 |
| `CrossRange.tla` | not ported | | | | #16 |

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
