# cask

## Goal

Cask is a lightweight, leaderless coordination store that lives inside the
Kubernetes extension API server of every management cluster in a fleet.
It serves cluster-scoped global objects and fenced claims as ordinary
Kubernetes resources, identically from every cluster, with no hub cluster
and no external runtime dependency. A fleet starts from one node and grows
in place.

The job is to prove this, not just build it. Every claim the design makes
must trace from a spec sentence to a Quint invariant to a line of code to a
test. That chain is what makes the case to a manager who is worried about
complexity.

The project name is cask. Do not use any other name for it.

## Constraints that do not move

- No external runtime dependencies. No managed databases, no hosted
  services on the critical path. Consensus is embedded in the API server.
- Zero-ops means embedded topology, not a separate cluster to run.
- Every fleet resource is cluster-scoped.
- No multi-key transactions and no global revision. Fencing tokens are the
  cross-key story.
- Ranges, splits, and cross-range snapshots stay off the fleet path. The
  fleet uses per-object registers, one index register per resource type,
  the roster, leases, and watch.

## How work happens

1. **Spec first.** `docs/spec/fleet.md` is the requirement source. Change
   the spec before the code. Re-extract with `devbox run spec` and commit
   the regenerated `docs/spec/fleet/*.toml`.
2. **Model in Quint.** `quint/` holds the models. TLA+ is being retired in
   favour of Quint at every level. Every safety rule is an invariant, and
   every invariant has a negative control that must fail. Run
   `devbox run quint`. Each module declares its invariants and controls
   in `// quint-check:` header lines. Bounded verification with Apalache
   runs with `--verify` in CI and also works on aarch64 with the Apalache
   that Quint 0.32 installs. `devbox run quint` is the gate script, not
   the binary; call `quint` directly for one-off commands.
3. **Cite with Duvet.** Every requirement has a citation in code or a
   `type=exception` that names the issue tracking it. Tests cite with
   `type=test`. Quint citations use `type=implication`, because a model
   shows that a rule holds, not that code does it. In Go, put citations inside the function body, because
   gofmt rewrites `//=` in doc comments and Duvet then cannot parse them.
   `devbox run duvet-ci` must pass; the committed snapshot makes a coverage
   drop a failure.
4. **Small issues.** Work is filed as GitHub issues an agent can finish in
   under five minutes. Each issue names its spec sentence, the exact task,
   the files, and the command that proves it is done. `docs/issues/fleet.md`
   is the source; `devbox run issues` files new ones. An issue filed
   straight with `gh` also goes into that file. One issue is one PR.
   If an issue turns out larger, do the first slice, open a follow-up, stop.
5. **Every bug becomes a check.** A bug found anywhere adds a negative
   control in Quint, a fault in `testutil/sim/faults/`, or a regression seed,
   and a Duvet test citation. The catalog only grows.

## Tests

- Unit tests run with the race detector: `devbox run test`.
- The simulator gate runs on every protocol change: `devbox run sim-gate`.
- End-to-end tests use `sigs.k8s.io/e2e-framework` against kind clusters,
  with plain `testing`. No Ginkgo. No envtest. `devbox run e2e`.
- Model-based tests replay Quint traces against the real API server.
- `devbox run verify` runs unit tests, Quint checks, and the Duvet gate.

## Tooling

Use devbox for everything. Do not install tools globally or call `nix-shell`
by hand. `devbox.json` lists the packages and the scripts above.
`devbox run duvet-install` installs Duvet through cargo on first use.

## Architecture decisions already made

- Storage: one cask register per object, one index register per resource
  type. Each index entry records the object sequence and the index
  sequence at which the index recorded it. The object register is written
  before the index register. A sweep at startup and on an interval
  repairs an index write lost to a crash. Every resourceVersion a client
  sees is an index sequence. An object's resourceVersion is its entry's index sequence, a
  list's is the index register sequence, and a watch event's is the index
  step that made the change. Get reads the entry, then the object at the
  entry's object sequence, so an unindexed write is not visible. An
  update or delete checks the client's resourceVersion against the entry,
  then compares and sets on the entry's object sequence.
- Claims: binding a claim is acquiring the object's cask lock. The fence
  travels in status and in every downstream effect. A receiver rejects a
  lower fence. A status write never lowers an advertised fence.
- Membership: one founding member; grow voters one to three to five, never
  one to two, only through roster reconfiguration, never by restarting with
  a longer static peer list. Extra clusters join as participants.
- Migration: same API group as the CRD; preserve uid and creationTimestamp;
  readiness waits for the import; first index sequence above the source
  etcd revision; continuous export from day one.
- Fallback for the design review: embedded etcd behind the same storage
  seam. Cask is the implementation with the strongest evidence, not an
  irreplaceable part.

## Writing

Specs, issues, commit messages, and docs follow Simplified Technical
English: short sentences, one idea each, active voice, plain words. No
filler, no marketing adjectives, no emoji.

## Where things are

- `docs/spec/fleet.md` requirements; `docs/spec/fleet/` extracted rules
- `quint/` models; `scripts/quint-check.sh` positive and negative checks
- `.duvet/config.toml` and `.duvet/snapshot.txt` traceability gate
- `docs/issues/fleet.md` issue source; `scripts/file-issues.sh`
- `cmd/cask-apiserver/` the extension server; `demo/kind/` three-cluster demo
- `docs/confidence.md` the trust ladder; `docs/sim-gate.md` the simulator
- `.github/workflows/verify.yml` and `sim-gate.yml` the CI gates
