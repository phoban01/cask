# TLA+ specifications

Machine-checkable safety specs for cask's most dangerous protocols. Per the
plan, these are **spec-first**: a protocol is model-checked here before the Go
code that depends on it is trusted.

| Spec | Protocol | Key invariants |
|------|----------|----------------|
| `CasPaxosMvcc.tla` | the CASPaxos consensus engine under each key | `Consistency` (agreement — one chosen value), `OneValuePerBallot`, `VotesSafe` |
| `Lease.tla` | leases / locks + fencing | `SingleHolder`, `FenceLatest`, `FenceMonotone` |
| `Reconfig.tla` | per-range joint-consensus reconfiguration under elastic churn | `NoLostValue` (agreement across the transition), `CatchUpHeld` (Cold's chosen values are in Cnew before release) |

## Running TLC

TLC (the model checker) needs a JVM. It is **not** installed in this dev
environment yet — add a JDK to `devbox.json` (`"packages": ["go@latest",
"jdk@latest"]`) and fetch `tla2tools.jar`, then:

```sh
# one-time: grab the toolbox jar
curl -L -o tla2tools.jar https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar

# check a spec against its model
java -jar tla2tools.jar -config CasPaxosMvcc.cfg CasPaxosMvcc.tla
java -jar tla2tools.jar -config Lease.cfg        Lease.tla
java -jar tla2tools.jar -config Reconfig.cfg     Reconfig.tla
```

Both models are deliberately small (3 acceptors / 2 clients, tiny ballot/time
horizons) so they finish quickly while still exercising the cross-ballot
carry-forward and lease-contention interleavings that defeat naive
implementations. CI should run these on every change to the corresponding
protocol code.

## Scope / honesty notes

- `CasPaxosMvcc.tla` proves the **agreement** property each register update
  relies on, in Lamport's abstract voting style. The MVCC version chain and HLC
  timestamp ride inside the agreed value; per-key version/HLC monotonicity is
  enforced in `internal/mvcc` and exercised by the Go property tests. A full
  refinement mapping from this spec to register **linearizability** is future
  work.
- These specs have **not** been run through TLC in the authoring environment
  (no JVM present). They are written to be checkable and are gated behind the
  devbox/JDK setup above before being relied upon.
