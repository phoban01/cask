# Cask fleet issues

One issue per `## ` heading. `scripts/file-issues.sh` files them on GitHub
and skips titles that already exist. Every issue is sized for an agent to
finish in under five minutes. If one turns out larger, do the first slice,
open a follow-up with the rest, and stop.

Every issue follows the same shape: the spec sentence it serves, the exact
task, the files to touch, and the command that proves it is done. Cite the
spec sentence with a Duvet annotation in the code you add:

```
//= docs/spec/fleet.md#<section-id>
//= type=test            (omit for an implementation citation)
//# <sentence quoted exactly>
```

In Go, put the citation inside the function body, not in the doc comment
above it: gofmt rewrites `//=` in doc comments to `// =`, which Duvet does
not parse.

## ci: make go test -race green

labels: ci

Spec: docs/spec/fleet.md#10-verification
> CI MUST run the unit tests with the race detector.

`TestPebbleGroupCommitCoalesces` in `internal/store/pebble_test.go` fails on
fast disks: 64 writers produce 64 flushes because each sync finishes before
the next write arrives. It asserts a throughput property, not safety.

Task: make the writers overlap deterministically (hold the first sync with a
gate in the test, or inject a slow `Sync` through the existing SlowStore
decorator), or move the assertion behind a `perf` build tag.

Files: `internal/store/pebble_test.go`

Done when: `go test -race -count=3 ./internal/store/` passes.

## quint: model delete and the index removal order

labels: quint

Spec: docs/spec/fleet.md#3-storage-model
> A delete MUST tombstone the object register before it removes the name from the index register.

Task: add `deleteObject(n)` to `quint/fleet.qnt` that sets `objSeq` to a
tombstone marker and a follow-up index removal, with a crash between them.
Extend `IndexNeverAhead` so a name whose object is tombstoned may still be
in the index, but never the reverse. Add a negative control that removes
from the index first.

Files: `quint/fleet.qnt`, `scripts/quint-check.sh` (add the negative control)

Done when: `scripts/quint-check.sh` passes and the new control is violated.

## quint: model the watch cursor

labels: quint

Spec: docs/spec/fleet.md#4-list-and-watch
> A watch from a resourceVersion MUST deliver every index change after that version, in order, with no gaps.

Task: add `watchCursor: int` and `delivered: List[int]` to `quint/fleet.qnt`.
`deliver` advances the cursor by one when `watchCursor < idxSeq`. Add
`WatchGapFree`: `delivered` is exactly `1..watchCursor`. Add a negative
control whose deliver jumps to `idxSeq`.

Files: `quint/fleet.qnt`, `scripts/quint-check.sh`

Done when: `scripts/quint-check.sh` passes with the new invariant listed.

## quint: model compaction and 410 Gone

labels: quint

Spec: docs/spec/fleet.md#4-list-and-watch
> A watch whose start version is compacted MUST end with 410 Gone.

Task: add `compacted: int` and a `compact` action that may advance it to any
value at most `idxSeq`. A watch that starts below `compacted` sets a
`gone` flag instead of delivering. Invariant: a delivered event is never
below `compacted` at the time of delivery.

Files: `quint/fleet.qnt`, `scripts/quint-check.sh`

Done when: `scripts/quint-check.sh` passes.

## quint: export ITF traces for model-based testing

labels: quint

Spec: docs/spec/fleet.md#10-verification
> Traces generated from the Quint model MUST be replayed against the extension server in a Go test.

Task: add `scripts/quint-traces.sh` that runs
`quint run quint/fleet.qnt --mbt --n-traces=20 --max-steps=15 --out-itf=cmd/cask-apiserver/testdata/traces/roto_{seq}.itf.json`
and commit the output.

Files: `scripts/quint-traces.sh`, `cmd/cask-apiserver/testdata/traces/`

Done when: the script runs and twenty trace files exist.

## quint: Go reader for ITF traces

labels: quint, apiserver

Spec: docs/spec/fleet.md#10-verification
> Traces generated from the Quint model MUST be replayed against the extension server in a Go test.

Task: add `cmd/cask-apiserver/itf_test.go` with a type that decodes the
Informal Trace Format (`states[]`, each a map of variable name to value,
with `#meta.index` and the `mbt::actionTaken` field). Include a test that
parses every file under `testdata/traces/`.

Files: `cmd/cask-apiserver/itf_test.go`

Done when: `go test -run TestITFParse ./cmd/cask-apiserver/` passes.

## quint: replay storage traces against fleetStore

labels: quint, apiserver

Spec: docs/spec/fleet.md#10-verification
> Traces generated from the Quint model MUST be replayed against the extension server in a Go test.

Task: for each trace, map `writeObject` to `store.update` or `create`,
`writeIndex` to the index write, `crash` to skipping the index write, and
`sweep` to the sweep function. After each step compare the store's index
entries and object sequences to the trace state. Skip claim actions.

Files: `cmd/cask-apiserver/mbt_store_test.go`

Done when: `go test -run TestReplayStorageTraces ./cmd/cask-apiserver/` passes.

## quint: replay claim traces against claimController

labels: quint, apiserver

Spec: docs/spec/fleet.md#10-verification
> Traces generated from the Quint model MUST be replayed against the extension server in a Go test.

Task: map `acquire` to `tryBind`, `expire` to letting the session lapse on
the sim clock, `observeLost` to `renew`, `release` to `release`, and
`applyEffect` to `setDeviceLease` with the claim's fence. After each step
compare the device's advertised fence to the trace's `accepted`.

Files: `cmd/cask-apiserver/mbt_claims_test.go`

Done when: `go test -run TestReplayClaimTraces ./cmd/cask-apiserver/` passes.

## quint: port CasPaxosMvcc.tla

labels: quint

Spec: docs/spec/fleet.md#10-verification
> Every safety rule in sections 3 and 5 MUST be an invariant in the Quint model.

Recipe, one PR per step if any step passes five minutes:
1. `quint/caspaxos.qnt`: state and `init` from `tla/CasPaxosMvcc.tla`.
2. Actions, one to one with the TLA+ actions.
3. Invariants `Consistency`, `OneValuePerBallot`, `VotesSafe`, plus a negative control that drops the promise check.
4. `quint compile --target tlaplus` and run TLC on the output with the original `.cfg` values. Record the state count next to the original's in `quint/PARITY.md`.

Done when: `quint run quint/caspaxos.qnt --invariants Consistency OneValuePerBallot VotesSafe` passes and the negative control fails.

## quint: port OwnedRegister.tla

labels: quint

Spec: docs/spec/fleet.md#10-verification
> The Quint model MUST include a negative control for each invariant that fails when the rule is omitted.

Same recipe as the CasPaxosMvcc port. The negative control is
`tla/OwnedRegisterBug.cfg`, the pre-W0 ballot rule; it must still find the
lost update as a short trace.

Files: `quint/owned_register.qnt`, `quint/PARITY.md`

Done when: `ChosenChain` and `OneValuePerBallot` pass on the good step and the bug step violates `ChosenChain`.

## quint: port Lease.tla

labels: quint

Spec: docs/spec/fleet.md#5-claims-and-fencing
> Every successful acquisition MUST mint a fence strictly greater than every fence previously minted for that object.

Same recipe. Invariants `SingleHolder`, `FenceLatest`, `FenceMonotone`.
Add the negative control the TLA+ never had: a `Bump` that reuses the
current fence.

Files: `quint/lease.qnt`, `quint/PARITY.md`

Done when: the three invariants pass and the control violates `FenceMonotone`.

## quint: port OwnerReads.tla

labels: quint

Spec: docs/spec/fleet.md#10-verification
> Every safety rule in sections 3 and 5 MUST be an invariant in the Quint model.

Same recipe. Invariant `NoStaleRead`; negative control is
`tla/OwnerReadsBug.cfg` (naive lease checks).

Files: `quint/owner_reads.qnt`, `quint/PARITY.md`

Done when: `NoStaleRead` passes on the good step and fails on the bug step.

## quint: port Reconfig.tla

labels: quint

Spec: docs/spec/fleet.md#6-membership
> The voter set MUST change only by joint-consensus reconfiguration of the roster.

Same recipe. Invariants `NoLostValue`, `CatchUpHeld`. Negative control:
release the old configuration before catch-up.

Files: `quint/reconfig.qnt`, `quint/PARITY.md`

Done when: both invariants pass and the control violates `CatchUpHeld`.

## quint: port RosterReconfig.tla

labels: quint

Spec: docs/spec/fleet.md#6-membership
> The voter set MUST change only by joint-consensus reconfiguration of the roster.

Same recipe. Invariants `NoLostMembership`, `AlwaysAvailable`. Negative
control: a membership write that goes only to the old core during the
joint phase.

Files: `quint/roster_reconfig.qnt`, `quint/PARITY.md`

Done when: both invariants pass and the control violates `NoLostMembership`.

## quint: port RangeDescriptors.tla or record it as retired

labels: quint

Spec: docs/spec/fleet.md#3-storage-model
> Each object MUST be stored in one cask register keyed by resource type and name.

Cask uses per-key registers and index registers, not ranges. Decide in
`quint/PARITY.md` whether the split protocol stays in scope. If it stays,
port with the same recipe (`NoSplitBrain`, `AuthorityExistsAndLive`,
`TombstoneIsTerminal`). If not, record the decision and the commit that
last checked the TLA+.

Done when: `quint/PARITY.md` has a row for RangeDescriptors.

## quint: port CrossRange.tla or record it as retired

labels: quint

Spec: docs/spec/fleet.md#3-storage-model
> A list's resourceVersion MUST be the sequence of the index register.

Lists come from the index register, so the cross-range snapshot contract is
off the fleet's path. Same decision as the RangeDescriptors issue.

Done when: `quint/PARITY.md` has a row for CrossRange.

## quint: retire tla/ once parity is recorded

labels: quint, spec

Spec: docs/spec/fleet.md#10-verification
> Every safety rule in sections 3 and 5 MUST be an invariant in the Quint model.

Task: when every row in `quint/PARITY.md` is filled, delete `tla/`, point
`README.md`, `docs/confidence.md`, and the paper's §4 at `quint/`, and
change "TLA+ specifications" to "Quint specifications checked with TLC and
Apalache".

Done when: `grep -r 'tla/' README.md docs/ | grep -v PARITY` is empty.

## duvet: exceptions for section 2 until the generic server lands

labels: duvet, apiserver

Spec: docs/spec/fleet.md#2-resources

Task: in `cmd/cask-apiserver/server.go`, add a `type=exception` citation for
each of the seven section 2 sentences with
`reason=tracked in docs/issues/fleet.md "apiserver: ..."` naming the issue
that will implement it.

Done when: `duvet report --ci` passes and the snapshot shows section 2 with no uncited rows.

## duvet: exceptions for section 6 membership

labels: duvet, membership

Spec: docs/spec/fleet.md#6-membership

Task: add `type=exception` citations in `cmd/cask-apiserver/main.go` for the
twelve section 6 sentences, each naming its membership issue.

Done when: `duvet report --ci` passes and section 6 has no uncited rows.

## duvet: exceptions for section 7 migration

labels: duvet, migration

Spec: docs/spec/fleet.md#7-migration

Task: add a `cmd/cask-migrate/doc.go` with a package comment holding a
`type=exception` citation for each section 7 sentence, naming its issue.

Done when: `duvet report --ci` passes and section 7 has no uncited rows.

## duvet: exceptions for sections 8 and 9

labels: duvet, ops

Spec: docs/spec/fleet.md#8-security

Task: add `type=exception` citations for sections 8 and 9 in
`cmd/cask-apiserver/main.go` next to the TLS and data-dir flags.

Done when: `duvet report --ci` passes and sections 8 and 9 have no uncited rows.

## duvet: test citations for section 3 create, update, delete

labels: duvet, apiserver

Spec: docs/spec/fleet.md#3-storage-model
> An update whose compare-and-set fails MUST return a conflict.

Task: add a test in `cmd/cask-apiserver/server_test.go` that creates an
object, updates it with a stale resourceVersion, and expects 409. Cite the
create, update, and conflict sentences with `type=test`.

Done when: `go test ./cmd/cask-apiserver/` passes and the three rows show a test.

## duvet: cite section 4 in serveList and serveWatch

labels: duvet, apiserver

Spec: docs/spec/fleet.md#4-list-and-watch

Task: cite the list sentence in `serveList`, the gap-free and event
sentences in `serveWatch`, and add a `type=exception` for the push
sentence with `reason=poll-diff until the index change feed lands`.

Files: `cmd/cask-apiserver/server.go`

Done when: `duvet report --ci` passes and section 4 has no uncited rows.

## apiserver: split cmd/cask-apiserver into its own Go module

labels: apiserver

Spec: docs/spec/fleet.md#2-resources
> The extension server MUST be built on the generic server in k8s.io/apiserver.

Task: add `cmd/cask-apiserver/go.mod` with a `replace` to the repo root and
`k8s.io/apiserver`, `k8s.io/apimachinery`, `k8s.io/client-go` at the same
Kubernetes minor. Build and test the module in `devbox run test` and CI.

Done when: `cd cmd/cask-apiserver && go build ./...` passes.

## apiserver: Device and DeviceClaim as runtime.Object

labels: apiserver

Spec: docs/spec/fleet.md#2-resources
> Every fleet resource MUST be cluster-scoped.

Task: move the types to `cmd/cask-apiserver/apis/fleet/v1alpha1/types.go`
using `metav1.TypeMeta` and `metav1.ObjectMeta`, mark them
`+genclient:nonNamespaced`, and generate `zz_generated.deepcopy.go` with
controller-gen. Cite the cluster-scoped sentence on the types.

Done when: `go vet ./cmd/cask-apiserver/...` passes.

## apiserver: register the scheme

labels: apiserver

Spec: docs/spec/fleet.md#7-migration
> The extension server MUST serve the same API group, version, and kinds that the CRD served.

Task: add `register.go` with `SchemeGroupVersion` for
`fleet.cask.dev/v1alpha1`, `AddToScheme`, and the internal version needed
by the generic server.

Done when: a test builds the scheme and round-trips a Device through JSON.

## apiserver: storage.Interface skeleton

labels: apiserver

Spec: docs/spec/fleet.md#3-storage-model
> Each object MUST be stored in one cask register keyed by resource type and name.

Task: add `cmd/cask-apiserver/storage/cask.go` with a type that satisfies
`k8s.io/apiserver/pkg/storage.Interface`. Every method returns a `not
implemented` error. Add a compile-time interface assertion.

Done when: `go build ./cmd/cask-apiserver/...` passes.

## apiserver: implement storage Get

labels: apiserver

Spec: docs/spec/fleet.md#3-storage-model
> An object's resourceVersion MUST be the sequence of its object register.

Task: `Get` reads the object register, decodes into `out`, and sets
resourceVersion to the register sequence. Honour `IgnoreNotFound`.

Done when: a test gets a created object and sees its sequence as the RV.

## apiserver: implement storage Create

labels: apiserver

Spec: docs/spec/fleet.md#3-storage-model
> A create MUST use a compare-and-set that requires the object register to be absent.

Task: `Create` encodes `obj`, CASes against absent, then writes the index
entry with the new sequence. Map `ErrConflict` to
`storage.NewKeyExistsError`.

Done when: a test creates twice and gets a key-exists error the second time.

## apiserver: implement GuaranteedUpdate

labels: apiserver

Spec: docs/spec/fleet.md#3-storage-model
> An update MUST use a compare-and-set on the resourceVersion the client supplied.

Task: loop: read, run `tryUpdate`, CAS on the read value, retry on
conflict when the caller allows it, then write the index entry. Map a
precondition failure to `storage.NewInvalidObjError`.

Done when: a test with two concurrent updaters ends with both applied and the final RV equal to the object sequence.

## apiserver: implement storage Delete

labels: apiserver

Spec: docs/spec/fleet.md#3-storage-model
> A delete MUST tombstone the object register before it removes the name from the index register.

Task: check preconditions, tombstone the object register, then remove the
index entry.

Done when: a test deletes and a following Get returns not found and the index no longer lists the name.

## apiserver: index register carries per-object sequence

labels: apiserver

Spec: docs/spec/fleet.md#3-storage-model
> Each resource type MUST have one index register that maps every object name to that object's latest sequence.

Task: change the index value from a name list to a map of name to
sequence. `indexMutate` becomes `indexSet(name, seq)` and
`indexRemove(name)`. Update `list` to return the recorded sequences.

Files: `cmd/cask-apiserver/store.go`

Done when: `go test ./cmd/cask-apiserver/` passes.

## apiserver: implement GetList from the index

labels: apiserver

Spec: docs/spec/fleet.md#4-list-and-watch
> A list MUST return every object that the index register names at the index sequence the list reports.

Task: `GetList` reads the index register once, reads each named object,
applies the predicate, and sets the list resourceVersion to the index
sequence. Honour `ResourceVersionMatch` by returning the index sequence for
`NotOlderThan`.

Done when: a test lists three objects and the list RV equals the index sequence.

## apiserver: implement Watch from the index change feed

labels: apiserver

Spec: docs/spec/fleet.md#4-list-and-watch
> A watch from a resourceVersion MUST deliver every index change after that version, in order, with no gaps.

Task: `Watch` opens a `watch.KeyWatcher` on the index register from the
given sequence and emits Added, Modified, or Deleted per index diff,
reading each object at its recorded sequence. Return
`storage.NewTooLargeResourceVersionError` for a future RV.

Done when: a test watches from an old RV and receives every change in order.

## apiserver: implement the Versioner

labels: apiserver

Spec: docs/spec/fleet.md#3-storage-model
> A list's resourceVersion MUST be the sequence of the index register.

Task: implement `storage.Versioner` over decimal sequences for objects and
lists. Reject non-numeric RVs with `ErrInvalidResourceVersion`.

Done when: unit tests cover parse, update object, and update list.

## apiserver: index sweep at startup

labels: apiserver

Spec: docs/spec/fleet.md#3-storage-model
> The extension server MUST reconcile the index register against the object registers at startup.

Task: add `sweepIndex(ctx, resource)` that scans object registers and
writes any missing or stale index entries. Call it before readiness.

Done when: a test writes an object without its index entry, starts the server, and finds the entry after startup.

## apiserver: index sweep at an interval

labels: apiserver

Spec: docs/spec/fleet.md#3-storage-model
> The extension server MUST reconcile the index register against the object registers at a fixed interval.

Task: run `sweepIndex` on a ticker (default 60 seconds, flag
`--index-sweep-interval`).

Done when: a test with a one-second interval observes a repair without a restart.

## apiserver: wire the generic server with delegated auth

labels: apiserver

Spec: docs/spec/fleet.md#2-resources
> The extension server MUST delegate authentication and authorization to the local kube-apiserver.

Task: build a `genericapiserver.RecommendedOptions` server, install the API
group with the cask storage, and enable delegating authentication and
authorization. Keep the old mux behind `--legacy-http` for one release.

Done when: `go run ./cmd/cask-apiserver --help` lists the standard recommended flags.

## apiserver: readiness waits for storage

labels: apiserver

Spec: docs/spec/fleet.md#2-resources
> The extension server MUST report not ready until its storage is reachable.

Task: add a `healthz.HealthChecker` that proposes a no-op to the index
register and fails until it commits. Register it as a readyz check.

Done when: a test with an unreachable peer set sees readyz fail.

## apiserver: readiness waits for the import

labels: apiserver, migration

Spec: docs/spec/fleet.md#2-resources
> The extension server MUST report not ready until any pending migration import is complete.

Task: add an `import-complete` marker register. The readyz check fails
while `--expect-import` is set and the marker is absent.

Done when: a test starts with `--expect-import`, sees readyz fail, writes the marker, and sees it pass.

## apiserver: serving certificate flags

labels: apiserver

Spec: docs/spec/fleet.md#8-security
> The extension server MUST serve HTTPS with a certificate the kube-apiserver can verify.

Task: use the generic server's `SecureServing` options and remove
`--self-signed-tls` from the demo manifests in favour of a cert-manager
issued secret. Update the APIService to carry `caBundle`.

Done when: the kind demo runs without `insecureSkipTLSVerify`.

## apiserver: reject objects over 1 MiB

labels: apiserver

Spec: docs/spec/fleet.md#3-storage-model
> An object value MUST NOT exceed 1 MiB.

Task: check the encoded size in Create and GuaranteedUpdate and return
`storage.NewInvalidObjError` above the limit.

Done when: a test with a 2 MiB object gets an invalid-object error.

## membership: dynamic join replaces the static peer list

labels: membership

Spec: docs/spec/fleet.md#6-membership
> The extension server MUST join the fleet through the dynamic roster path.

Task: add `--bootstrap` and `--seed` flags to the apiserver that call the
same roster-founding and join code `cmd/cask` uses on the Nebula path.
Keep `--cask-peers` for tests only, with a deprecation log line.

Done when: two apiservers, one with `--bootstrap` and one with `--seed`, form a roster of two members in a test.

## membership: voter and participant roles

labels: membership

Spec: docs/spec/fleet.md#6-membership
> Every member MUST be either a voter or a participant.

Task: add a `Role` field to `roster.Member` with values voter and
participant. Placement selects replicas from voters only. Cite the
"Only voters MUST hold register replicas" sentence at the placement filter.

Done when: a test with three voters and two participants places every register on voters.

## membership: promote and demote through roster reconfig

labels: membership

Spec: docs/spec/fleet.md#6-membership
> The voter set MUST change only by joint-consensus reconfiguration of the roster.

Task: add `POST /admin/promote` and `POST /admin/demote` on the apiserver.
Each takes a list of member ids in the request body. Apply the whole list
in one `Roster.Reconfigure` call with the new voter set. Refuse the request
if the resulting voter count is even or above five. One call must be able
to grow the voters from one to three, because a single-id promote would
pass through two voters, which the spec forbids.

Done when: a test promotes two participants in one call and observes the
core grow from one to three voters by joint consensus.

## membership: refuse static growth from a founded data dir

labels: membership

Spec: docs/spec/fleet.md#6-membership
> An operator MUST NOT grow the voter set by restarting members with a longer static peer list.

Task: at startup, if the data dir holds a roster register and `--cask-peers`
names members that are not in it, exit with a message pointing at the
promote endpoint.

Done when: a test starts a founded node with a longer peer list and gets a non-zero exit.

## membership: mutual TLS flags for consensus

labels: membership

Spec: docs/spec/fleet.md#8-security
> Consensus traffic between members MUST use mutual TLS.

Task: add `--consensus-cert`, `--consensus-key`, and `--consensus-ca` to
`cmd/cask` and the apiserver. Wire them into a `tls.Config` with
`ClientAuth: RequireAndVerifyClientCert`. Leave the transport plaintext when
the flags are absent, with a warning.

Done when: flags parse and a config test builds a valid `tls.Config`.

## membership: TLS on the ConnectRPC listener and dialer

labels: membership

Spec: docs/spec/fleet.md#8-security
> Consensus traffic between members MUST use mutual TLS.

Task: use the `tls.Config` from the flags issue on the consensus listener
and the client transport in `internal/transport`. Cite the sentence there.

Done when: a test forms a three-node cluster over TLS and a client without a certificate is refused.

## migrate: export command

labels: migration

Spec: docs/spec/fleet.md#7-migration
> The migration MUST preserve each object's uid.

Task: add `cmd/cask-migrate export --kubeconfig ... --group fleet.cask.dev`
that lists every object of each kind through the CRD API and writes JSON
lines with full metadata to a file. Print the source etcd revision from
the list response.

Done when: a test against a fake dynamic client writes one line per object with uid present.

## migrate: import command

labels: migration

Spec: docs/spec/fleet.md#7-migration
> The migration MUST preserve each object's creationTimestamp.

Task: add `cmd/cask-migrate import --file ... --cask-seed ...` that writes
each object register directly with uid and creationTimestamp intact, sets
the index entries, and writes the import-complete marker last.

Done when: a test imports two objects and a Get returns the original uid and creationTimestamp.

## migrate: initial index sequence above the etcd revision

labels: migration

Spec: docs/spec/fleet.md#7-migration
> The initial index sequence for each resource type MUST be greater than the source etcd revision at export time.

Task: `import` takes `--min-index-seq` (the exported revision plus one) and
seeds each index register so its first sequence is at least that value.

Done when: a test imports with `--min-index-seq 1000` and the first list RV is above 1000.

## migrate: cutover runbook

labels: migration

Spec: docs/spec/fleet.md#7-migration
> Writers MUST be frozen from the start of the export until the APIService is available.

Task: write `docs/runbooks/cutover.md` with the ordered steps: freeze
writers, export, delete the CRD, start the apiserver with
`--expect-import`, import, register the APIService, unfreeze. Include the
garbage-collector warning about owner references.

Done when: the file exists and each step names its command.

## migrate: continuous export CronJob

labels: migration, ops

Spec: docs/spec/fleet.md#7-migration
> A continuous export of all fleet objects MUST run from the first day of phase one.

Task: add `demo/kind/manifests/export-cronjob.yaml` that runs
`cask-migrate export` every ten minutes into a PersistentVolumeClaim.

Done when: `kubectl apply --dry-run=client -f` accepts it.

## migrate: rehearsal on a kind copy

labels: migration, e2e

Spec: docs/spec/fleet.md#7-migration
> The cutover MUST be rehearsed on a copy of the management cluster before it runs on the real one.

Task: add an e2e feature that creates CRD objects on a kind cluster, runs
the runbook steps, and checks that every object is served by the
extension server with the same uid.

Done when: `go test ./test/e2e -run TestCutover` passes on kind.

## e2e: add e2e-framework and the kind skeleton

labels: e2e

Spec: docs/spec/fleet.md#10-verification
> End-to-end tests MUST use sigs.k8s.io/e2e-framework against kind clusters.

Task: add `test/e2e/main_test.go` with a `TestMain` that uses
`env.NewWithConfig`, `envfuncs.CreateCluster(kind.NewProvider(), ...)`
for three clusters named east, west, and north, and tears them down. Plain
`testing` only. Cite the two MUST NOT sentences with an exception-free
implementation citation on the file.

Done when: `go test ./test/e2e -run TestMain -count=1` creates and deletes the clusters.

## e2e: create in one cluster, read in another

labels: e2e

Spec: docs/spec/fleet.md#2-resources
> A client MUST be able to use kubectl, client-go informers, field selectors, and watch bookmarks against fleet resources without fleet-specific code.

Task: a feature that creates a Device through the east cluster's client-go
dynamic client and reads it through west with the same resourceVersion.

Done when: `go test ./test/e2e -run TestCrossClusterRead` passes.

## e2e: two claims race, one binds

labels: e2e

Spec: docs/spec/fleet.md#5-claims-and-fencing
> At most one claim MUST be Bound to an object at the object's current fence.

Task: a feature that creates a DeviceClaim for the same device from east
and west at once and asserts exactly one is Bound and the other Pending.
Cite with `type=test`.

Done when: `go test ./test/e2e -run TestClaimRace` passes.

## e2e: zombie fence rejection

labels: e2e

Spec: docs/spec/fleet.md#5-claims-and-fencing
> A receiver MUST reject an effect whose fence is lower than the highest fence it has accepted for that object.

Task: a feature that scales the holding apiserver to zero, waits for the
successor to bind, scales the zombie back, and asserts the device's
advertised fence never drops. Cite with `type=test`.

Done when: `go test ./test/e2e -run TestZombieFence` passes.

## e2e: grow from one voter to three

labels: e2e, membership

Spec: docs/spec/fleet.md#6-membership
> The founding member MUST remain available until the voter set has grown to three.

Task: a feature that starts east alone with `--bootstrap`, writes objects,
joins west and north with `--seed`, promotes both, and asserts the objects
are readable from all three with the founder stopped afterwards.

Done when: `go test ./test/e2e -run TestGrowToThree` passes.

## e2e: informer list-then-watch has no gaps

labels: e2e

Spec: docs/spec/fleet.md#4-list-and-watch
> A watch from a resourceVersion MUST deliver every index change after that version, in order, with no gaps.

Task: a feature that starts a client-go informer on Devices, creates fifty
objects from another cluster, and asserts the informer store holds all
fifty with no re-list. Cite with `type=test`.

Done when: `go test ./test/e2e -run TestInformerNoGaps` passes.

## ci: e2e workflow on kind

labels: ci, e2e

Spec: docs/spec/fleet.md#10-verification
> CI MUST run the Quint checks, the Duvet report, the simulator gate, and the end-to-end suite.

Task: add `.github/workflows/e2e.yml` that installs kind and kubectl, builds
the apiserver image, and runs `go test ./test/e2e -timeout 30m`.

Done when: the workflow file passes `actionlint`.

## ops: quorum health signal

labels: ops

Spec: docs/spec/fleet.md#9-operations
> Cask MUST expose a health signal for quorum state.

Task: add `cask_quorum_reachable` (0 or 1) and `cask_voters_reachable` to
the apiserver's metrics endpoint, computed from the last consensus round.

Done when: a test scrapes the endpoint and sees both series.

## ops: lease expiry and fence regression counters

labels: ops

Spec: docs/spec/fleet.md#9-operations
> Cask MUST expose counters for lease expiries and for rejected fence regressions.

Task: increment `cask_lease_expiries_total` in the session reaper and
`cask_fence_regressions_rejected_total` in `setDeviceLease` when it refuses
a lower fence. Cite the sentence at both sites.

Done when: the zombie test in `server_test.go` sees the regression counter rise.

## ops: majority-loss recovery runbook

labels: ops

Spec: docs/spec/fleet.md#9-operations
> A majority-loss recovery procedure MUST be documented.

Task: write `docs/runbooks/majority-loss.md`: stop survivors, pick the
survivor with the highest roster generation, run
`cask-apiserver --force-new-fleet` to rewrite the roster core to itself,
restart, re-promote. Include the data-loss statement for uncommitted
proposals.

Done when: the file exists and the `--force-new-fleet` flag is tracked in a follow-up issue.

## ops: add --force-new-fleet to cask-apiserver

labels: ops

Spec: docs/spec/fleet.md#9-operations
> A majority-loss recovery procedure MUST be documented.

Task: add a `--force-new-fleet` flag to `cmd/cask-apiserver/main.go`.
The flag needs `--data-dir`. It opens the Pebble store and builds a
one-acceptor proposer over the local acceptor. Through that proposer it
rewrites the roster register at `roster.Key`: `Core` and `Members` become
this node only, `Joint` becomes nil, and `ConfigGen` and `Epoch` go up by
one. Then the process exits. Put the rewrite in a `roster.ForceCore`
helper. Add a Duvet citation for the sentence above. This depends on #43,
because the apiserver has no roster until then.

The runbook `docs/runbooks/majority-loss.md` (#64) calls this flag.

Files: `cmd/cask-apiserver/main.go`, `internal/roster/roster.go`,
`internal/roster/roster_test.go`

Done when: a test writes a three-member roster to a Pebble dir, runs
`ForceCore` for one member, and reads back `Core` equal to that member
with a higher `ConfigGen`.

## ops: PodDisruptionBudget for voters

labels: ops

Spec: docs/spec/fleet.md#9-operations
> A rolling upgrade MUST keep a majority of voters available at all times.

Task: add a PDB with `maxUnavailable: 1` to `demo/kind/manifests` for the
apiserver and a StatefulSet with a volumeClaimTemplate for `--data-dir`.

Done when: `kubectl apply --dry-run=client -f` accepts both.

## spec: add the owner-reference warning to section 7

labels: spec

Spec: docs/spec/fleet.md#7-migration

Task: add a sentence: "The APIService MUST NOT become available while any
imported object that other objects reference by ownerReference is
missing." Re-extract with `duvet extract -f markdown docs/spec/fleet.md -o docs/spec`
and commit the regenerated toml.

Done when: `duvet report --ci` passes and the new sentence appears in the snapshot.

## ci: quarantine TestDuelingProposersConvergeWithBackoff until the retry contract is fixed

labels: ci, agent-5m

Spec: docs/spec/fleet.md#10-verification
> CI MUST run the unit tests with the race detector.

`TestDuelingProposersConvergeWithBackoff` in `internal/caspaxos/backoff_test.go`
fails about one run in four with "committed 31 ops, want 30". The test is
not flaky. It asserts a property CASPaxos does not give: exactly-once for a
non-idempotent change function. See the follow-up issue
"caspaxos: report an unknown outcome after a failed accept phase".

Task: keep the livelock check and drop the exact count. Make each writer
append a unique op id and assert every id is present at least once and
that the list is not shorter than `writers*ops`. Add a comment that names
the follow-up issue.

Files: `internal/caspaxos/backoff_test.go`

Done when: `go test -race -count=50 -run TestDuelingProposers ./internal/caspaxos/` passes.

## caspaxos: report an unknown outcome after a failed accept phase

labels: quint, spec

Spec: docs/spec/fleet.md#3-storage-model
> An update MUST use a compare-and-set on the resourceVersion the client supplied.

A proposer whose accept phase reaches a minority can still have its value
chosen: the next prepare adopts the highest-ballot accepted value
(`prepare` in `internal/caspaxos/proposer.go`). `Propose` then retries,
reads its own change back, and applies the change again. For an append
that is a duplicate. For the fleet path every change is a compare-and-set,
so the worst case is a spurious conflict for a write that did land.

Task, first slice: add a spec sentence to section 3: "A write that returns
a conflict MAY have been committed, and the client MUST re-read before it
retries." Model it in Quint as a negative control that applies a
non-idempotent change on retry and shows a duplicate. Follow-ups: make
`Propose` return `ErrUnknownOutcome` instead of retrying after a failed
accept, and cite the sentence from `update` in `cmd/cask-apiserver/store.go`.

Files: `docs/spec/fleet.md`, `quint/fleet.qnt`

Done when: `devbox run spec` and `devbox run quint` pass and the new control fails as expected.

## caspaxos: return ErrUnknownOutcome instead of reapplying after a failed accept

labels: quint

Spec: docs/spec/fleet.md#3-storage-model
> A retried write MUST be a compare-and-set, never a blind reapplication of a change.

`Propose` in `internal/caspaxos/proposer.go` retries after an accept phase
that reached only a minority. The retry applies the change function again.
For a non-idempotent change that duplicates the write. `quint/retry.qnt`
shows this as the `stepBlindRetry` control.

Task: add `ErrUnknownOutcome` to `internal/caspaxos/errors.go`. When an
accept fails after at least one acceptor accepted, return it instead of
starting a new round. Callers that only use compare-and-set changes may
keep retrying; document that on `Propose`.

Files: `internal/caspaxos/proposer.go`, `internal/caspaxos/errors.go`

Done when: `go test -race ./internal/caspaxos/` and `devbox run sim-gate` pass.

## e2e: use cluster names that do not collide with the demo

labels: e2e

Spec: docs/spec/fleet.md#10-verification
> End-to-end tests MUST use sigs.k8s.io/e2e-framework against kind clusters.

`test/e2e/main_test.go` creates kind clusters named east, west, and north.
These are the demo's cluster names. If the demo clusters exist, the suite
reuses them and deletes them when it finishes.

Task: prefix the e2e cluster names, for example `e2e-east`, and read an
optional prefix from `CASK_E2E_PREFIX` so parallel runs do not collide.

Files: `test/e2e/main_test.go`

Done when: `go vet -tags e2e ./test/e2e/` passes and `kind get clusters` shows only `e2e-*` names during a run.

## apiserver: field selectors and watch bookmarks

labels: apiserver

Spec: docs/spec/fleet.md#2-resources
> A client MUST be able to use kubectl, client-go informers, field selectors, and watch bookmarks against fleet resources without fleet-specific code.

The legacy mux ignores `fieldSelector` and `allowWatchBookmarks`. kubectl works today. Field selectors and bookmarks do not.

Task: in the generic server storage, return `metadata.name` from `GetAttrs` so a field selector on the name filters list and watch. Emit a Bookmark event with the current index sequence when the client sets `allowWatchBookmarks`.

Files: `cmd/cask-apiserver/server.go` (or the storage file that #27 adds), `cmd/cask-apiserver/server_test.go`

Done when: `go test -race ./cmd/cask-apiserver/ -run 'TestFieldSelector|TestWatchBookmark'` passes.

## ops: refuse an embedded acceptor without --data-dir

labels: ops

Spec: docs/spec/fleet.md#9-operations
> Every voter MUST persist its acceptor state to a durable volume.

`cmd/cask-apiserver/main.go` starts an embedded acceptor on `store.NewMem()` when `--data-dir` is empty. It only logs a warning. The single-node mode with no `--cask-peers` also keeps its acceptor in memory.

Task: refuse to start an embedded acceptor without `--data-dir`. Add an `--ephemeral` flag that allows the in-memory store for tests and the local demo. Keep the warning when `--ephemeral` is set.

Files: `cmd/cask-apiserver/main.go`, `cmd/cask-apiserver/main_test.go`

Done when: `go test -race ./cmd/cask-apiserver/ -run TestAcceptorNeedsDataDir` passes.

## ops: rolling upgrade runbook across clusters

labels: ops

Spec: docs/spec/fleet.md#9-operations
> A rolling upgrade MUST keep a majority of voters available at all times.

The PodDisruptionBudget in `demo/kind/manifests/apiserver.yaml` covers one cluster. The demo runs one voter per cluster, so the budget does not stop two clusters from upgrading at the same time.

Task: write `docs/runbooks/rolling-upgrade.md`. Upgrade one cluster at a time. Before the next cluster, wait for the voter to be Ready and for the quorum health signal from #62 to report a quorum. Add a `demo/kind/upgrade.sh` that runs these steps on the kind demo.

Files: `docs/runbooks/rolling-upgrade.md`, `demo/kind/upgrade.sh`

Done when: `demo/kind/upgrade.sh` upgrades all three clusters and `kubectl get devices` answers from every cluster during the run.

## e2e: rehearse majority-loss recovery on kind

labels: ops, e2e

Spec: docs/spec/fleet.md#9-operations
> The majority-loss recovery procedure MUST be rehearsed before phase two.

`docs/runbooks/majority-loss.md` describes the procedure. Nothing runs it yet. It needs `--force-new-fleet` (#74), the dynamic roster path (#43), and the promote endpoint (#45).

Task: add an e2e feature on the three-cluster kind setup. It deletes two voters and their data, runs the runbook steps on the survivor, rejoins the other clusters, and promotes back to three voters. Cite the sentence with `type=test`.

Files: `test/e2e/majority_loss_test.go`

Done when: `go test ./test/e2e -run TestMajorityLossRecovery` passes on kind.

## caspaxos: close the remaining unknown-outcome paths after #72

labels: quint

Spec: docs/spec/fleet.md#3-storage-model
> A retried write MUST be a compare-and-set, never a blind reapplication of a change.

Follow-up to #72. `Propose` now returns `ErrUnknownOutcome` after an
accept that some acceptor may hold. Some paths still hide an unknown
outcome or retry without a check. Each item below is one small PR.

1. `ErrRangeChanged` during the accept phase. `accept` returns it at
   once, even when another acceptor already accepted. `agent.Router`
   then retries the same change once on the new replica set. Task:
   return `ErrUnknownOutcome` (wrapping `ErrRangeChanged`) when the
   write may be held, and do not retry the change in the router.
   Files: `internal/caspaxos/proposer.go`, `internal/agent/router.go`.
2. Context cancel or deadline during the accept phase. `Propose`
   returns `ctx.Err()`, which is also an unknown outcome. Task: decide
   whether to wrap it as `ErrUnknownOutcome` and document it on
   `Propose`. Files: `internal/caspaxos/proposer.go`.
3. `OwnedProposer.Write` (the 1-RTT path). Check what it returns after
   a minority accept, and whether `owner.Manager.FastPropose` falls back
   to a full round that applies the change again. Files:
   `internal/caspaxos/owned.go`, `internal/owner/manager.go`.
4. mvcc exactly-once depends on the OpID staying in the chain. If
   `Compact` drops the version between two retries, the retry appends a
   second copy. Task: keep the newest versions of in-flight ops, or
   bound the retry window. Files: `internal/mvcc/mvcc.go`.
5. The API server returns a plain error when mvcc exhausts its
   unknown-outcome retries. Task: map `caspaxos.ErrUnknownOutcome` to a
   Kubernetes timeout status, so a client re-reads. Files:
   `cmd/cask-apiserver/store.go`.
6. Every bug becomes a check. Task: add a `minority_accept` fault in
   `testutil/sim/faults/` that makes an accept reach one acceptor, and
   assert that each op lands once. Add a `type=test` Duvet citation.

Done when: each item has a test, `devbox run test`, `devbox run
sim-gate`, and `devbox run duvet-ci` pass.


## lease: make Sessions.Revoke a compare-and-set on owner and expiry

labels: quint

Spec: docs/spec/fleet.md#3-storage-model
> A retried write MUST be a compare-and-set, never a blind reapplication of a change.

`Sessions.Revoke` in `internal/lease/session.go` clears the record with a
change that ignores the current value. Its comment says clearing twice
changes nothing. That is false when a Grant lands between two attempts:
Revoke's first attempt reaches a minority, another round commits it, a new
owner Grants the same id, and Revoke's retry clears the live session.

Task: make Revoke read the record first and clear it only if owner and
expiry still match what it read. Fix the comment. Add a test that grants
between two Revoke attempts and expects the new session to survive.

Files: `internal/lease/session.go`, `internal/lease/*_test.go`

Done when: `go test -race -count=20 ./internal/lease/` passes.

## caspaxos: correct the Propose contract for the all-rejected retry

labels: quint

Spec: docs/spec/fleet.md#3-storage-model
> A write that returned a conflict MAY have been committed.

`accept` in `internal/caspaxos/proposer.go` returns as soon as a quorum
is impossible, with stragglers still undecided. So "every acceptor
rejected" is only reachable with one acceptor. For three or more, every
contended write whose accept fails returns ErrUnknownOutcome. That is
safe, but the doc comment on `Propose` promises a retry that never
happens.

Task: rewrite the comment to state the real rule. Either drop the
all-rejected branch or keep it with a note that it applies to a single
acceptor. Add a test with three acceptors where two reject and one is
slow, and assert ErrUnknownOutcome.

Files: `internal/caspaxos/proposer.go`, `internal/caspaxos/unknown_test.go`

Done when: `go test -race ./internal/caspaxos/` passes.

## quint: add caspaxos.qnt to the Quint gate

labels: quint

Spec: docs/spec/fleet.md#10-verification
> The Quint model MUST include a negative control for each invariant that fails when the rule is omitted.

`quint/caspaxos.qnt` (from #9) is not in the Quint gate yet. Another PR was
changing the gate script, so #9 left it alone.

Task: in `scripts/quint-check.sh`, add a `quint/caspaxos.qnt` block like the
`retry.qnt` block. Run typecheck, `quint test`, and `quint run` with
`--invariants Consistency OneValuePerBallot VotesSafe`. Add
`must_fail --spec quint/caspaxos.qnt stepNoPromise Consistency`. Under
`--verify`, run `quint verify` on the same invariants with `--max-steps=6`.
At 10 steps Apalache ran for more than 40 minutes.

Files: `scripts/quint-check.sh`

Done when: `devbox run quint` prints `quint: ok` and reports the
`stepNoPromise` control as violated.


## demo: start the kind demo with --bootstrap and --seed

labels: membership

Spec: docs/spec/fleet.md#6-membership
> The extension server MUST join the fleet through the dynamic roster path.

`demo/kind/manifests/apiserver.yaml` still starts three voters with the
deprecated `--cask-peers`. The apiserver now has `--bootstrap` and
`--seed` (#43). A joined member is a participant until the promote
endpoint (#45) makes it a voter. This depends on #45, or the demo drops
from three voters to one.

Task: start east with `--bootstrap` and west and north with `--seed`
pointing at east. After both join, call the promote endpoint once with
both ids. Update `demo/kind/README.md`.

Files: `demo/kind/manifests/apiserver.yaml`, `demo/kind/demo.sh`,
`demo/kind/README.md`

Done when: the demo comes up and `GET /roster` on each apiserver shows a
core of three.

## membership: move data registers when the core changes

labels: membership

Spec: docs/spec/fleet.md#6-membership
> The voter set MUST change only by joint-consensus reconfiguration of the roster.

In the apiserver, every data register uses the roster core as its
acceptor set (`cmd/cask-apiserver/membership.go`). `Roster.Reconfigure`
carries forward only the roster key. A promote (#45) that changes the core
without moving the data registers can lose a committed value.

Task: before the core release step, carry forward every data key from the
old core to the new core with a joint Identity round, the way
`internal/reconfig` does for ranges. Data proposals during the joint phase
must use the joint quorum.

Files: `cmd/cask-apiserver/membership.go`, `internal/roster/reconfig.go`

Done when: a test writes keys on a one-voter core, grows it to three, stops
the founder, and reads every key back.

## roster: carry range descriptors when cmd/cask changes the core

labels: membership

Spec: docs/spec/fleet.md#6-membership
> A core change MUST carry every data register forward to the new core before it releases the old core.

In `cmd/cask`, every range descriptor register (`\x00rd/<id>`) uses the
roster core as its acceptor set (`descriptorStore` in
`cmd/cask/cluster.go`). The roster core change carries only the roster
key, and descriptor writes use the plain core in the joint phase. A core
change can lose a committed descriptor. The apiserver fix for #110 adds
`Roster.SetCarry`; `cmd/cask` does not set it yet.

Task: set a carry hook in `cmd/cask` that carries every descriptor key to
the new core, and make `descriptorStore` propose to both cores while the
roster is joint.

Files: `cmd/cask/cluster.go`

Done when: a test writes a descriptor on a one-voter core, grows it to
three, stops the founder, and reads the descriptor back.

## ci: TestDuelingProposersConvergeWithBackoff fails when unknown outcomes repeat

## ci: TestRevokeRetryKeepsNewSession fails when the minority accept lands late

labels: ci, agent-5m

Spec: docs/spec/fleet.md#3-storage-model
> A retried write MUST be a compare-and-set, never a blind reapplication of a change.

`TestDuelingProposersConvergeWithBackoff` in `internal/caspaxos/backoff_test.go`
fails on CI with `writer 2: caspaxos: write outcome unknown; re-read before
retry (livelock: backoff failed to converge)`. Since #86, a contended write
whose accept fails returns `ErrUnknownOutcome`. The test retries such a write
only four times. Each retry is a new `Propose` call, so its backoff starts
again at attempt 0 and the delay never grows. Six writers on one key can
then collide four times in a row. A stress run fails 18 of 3000 runs.

Task: give each op one retry budget. After `ErrUnknownOutcome` or
`ErrPreempted`, the writer backs off with an attempt count that grows across
retries, then proposes again. The retry reads the register and skips an op id
that already landed. Keep the exactly-once check on op ids. A writer that
spends its budget still fails the test as a livelock.

Files: `internal/caspaxos/backoff_test.go`

Done when: `go test -race -count=500 -cpu=1,2,4 -run TestDuelingProposersConvergeWithBackoff ./internal/caspaxos/`
passes in two copies at once, 3000 of 3000 runs.

`TestRevokeRetryKeepsNewSession` in `internal/lease/revoke_test.go` fails on
CI with `grant between attempts: caspaxos: change precondition failed`. The
test downs the accept path to acceptors 1 and 2, so only acceptor 0 can take
the first Revoke write. The proposer returns as soon as 1 and 2 fail. The
accept to acceptor 0 can still be in flight. The in-between Grant can then
read acceptor 0 before that accept lands, see agentA's live session, and fail
with `ErrConflict`. Sixteen parallel stress copies fail 35 of 48000 runs.

Task: make the test wait until acceptor 0 holds the first Revoke write
before it runs the Grant. Keep what the test proves: a
Grant lands between two Revoke attempts, agentB's session survives, and the
test fails against the Revoke from before #101.

Files: `internal/lease/revoke_test.go`

Done when: `go test -race -count=1000 -cpu=1,2,4 -run TestRevokeRetryKeepsNewSession ./internal/lease/`
passes in 16 copies at once, and the test fails against the pre-#101 Revoke.

## ci: TestHungOldVoterDoesNotWedgeCoreChange times out intermittently

labels: agent-5m, ci, fleet

Spec: docs/spec/fleet.md#6-membership
> A core change MUST finish while a majority of the old core and a majority of the new core answer.

`TestHungOldVoterDoesNotWedgeCoreChange` in `cmd/cask-apiserver/regress_test.go`
(from #117) timed out once in a `go test -race -count=5 ./...` run of the
apiserver module. It passed on rerun and 10 times alone, so it depends on
timing or load. A flaky required check blocks every merge.

Task: reproduce under load (several copies at once, `-cpu=1,2,4`). Decide
whether the test's deadline is too tight for the race detector, or the
core change really stalls (for example the progress window from #133, the
10 s peer client timeout, or the run loop). If the change stalls, that is
a liveness bug: fix it in code and keep the test strict. Otherwise widen
the test's deadline with a comment that states the budget.

Files: `cmd/cask-apiserver/regress_test.go`, maybe `corechange.go`, `membership.go`

Done when: 8 copies of `go -C cmd/cask-apiserver test -race -count=50 -run TestHungOldVoter ./...` pass together.

## caspaxos: end the accept phase at the first rejection

labels: fleet, agent-5m, membership

Spec: docs/spec/fleet.md#6-membership
> A core change MUST finish while a majority of the old core and a majority of the new core answer.

#143 found that `prepare` in `internal/caspaxos/proposer.go` waited forever
when one acceptor promised, one rejected, and one hung. The fix returns at
the first rejection. The `accept` phase has the same shape. One accept, one
rejection, and one hung acceptor keep `quorumStillPossible` true, so the
phase waits on the hung acceptor. A rejection in accept needs a competing
proposer between the two phases, so it is rarer, but it can still stall a
round while a majority answers.

Task: make `accept` return at the first rejection. Keep the `mayHold` rule:
an acceptor that accepted, or one still in flight, may hold the value, so a
changed value still ends in `ErrUnknownOutcome`. Add a test next to
`TestRejectionEndsPrepareWithHungPeer` with one acceptor that accepts, one
that rejects, and one that hangs.

Files: `internal/caspaxos/proposer.go`, `internal/caspaxos/fanout_test.go`

Done when: the new test passes, `go test -race -count=500 -cpu=1,2,4 ./internal/caspaxos/`
passes, and `devbox run sim-gate` passes.

## apiserver: push index writes from other clusters to watches

labels: apiserver

Spec: docs/spec/fleet.md#4-list-and-watch
> Watch events SHOULD be pushed from the index register's change feed rather than polled.

`Store.Watch` (#34) wakes at once after an index write through the same
Store. A write from another cluster reaches the watch only at the next read
of the index history. The default poll interval is 250 ms.

Task: wake the watch when any proposer commits an index write, for example
from the register owner's commit notification. Keep the poll as a fallback.
Then replace the `type=exception` in `cmd/cask-apiserver/storage/watch.go`
with an implementation citation.

Files: `cmd/cask-apiserver/storage/watch.go`, `internal/watch/watch.go`

Done when: a test with a 1 h poll interval sees a write through a second
Store's proposer within 1 s, and `devbox run duvet-ci` passes.


## apiserver: one index history reader per Store for all watches

labels: apiserver

Spec: docs/spec/fleet.md#3-storage-model
> A mutation MUST write the object register before the index register.

Each watch from `Store.Watch` (#34) reads the index history with its own
identity round after every index write. These rounds preempt index writes.
In `TestWatchFromOldVersionDeliversEveryChangeInOrder`, three writers and
two watches made an index write lose all 12 rounds about once in 150
runs. The object write had committed, so the index lagged until the next
write for that name. The test now repairs the index write the way the
sweep does.

Task: give each Store one reader of the index history. It fans each index
step out to the open watches through a bounded buffer per watch, as
`internal/watch.FanOut` does. A watch whose buffer fills ends with 410
Gone.

Files: `cmd/cask-apiserver/storage/watch.go`, `cmd/cask-apiserver/storage/watch_test.go`

Done when: a test with 20 open watches counts one index history read per
index write, and the watch tests pass with `-count=300`.


## spec: say which resourceVersion a watch event carries

labels: spec

Spec: docs/spec/fleet.md#4-list-and-watch
> A watch from a resourceVersion MUST deliver every index change after that version, in order, with no gaps.

Section 3 says an object's resourceVersion is its object register sequence.
Section 4 says a watch event carries the object at the sequence the index
recorded. So a watch event carries an object sequence.

A client-go reflector resumes a watch from the resourceVersion of the last
event it received. `Store.Watch` (#34) reads that value as an index
sequence. The two sequences are not related:

- An object sequence below the index sequence replays old index steps.
- An object sequence above the index sequence gets "too large resource
  version", and the reflector retries it until the index catches up.
- An object sequence in between can skip index steps.

Bookmarks carry the index sequence, so a resume from a bookmark is safe.

Task: decide the rule in `docs/spec/fleet.md` section 4 before any code
change. Options: send a bookmark after every event when the client allows
bookmarks; or carry the index sequence on watch events and map it back on
update. Add a Quint action for a resume from an event, with an invariant
and a negative control.

Files: `docs/spec/fleet.md`, `quint/fleet.qnt`

Done when: the spec states which resourceVersion a watch event carries,
and `devbox run quint` shows the resume control fails.

## ci: fail when Go files are not gofmt-clean

labels: ci

Spec: docs/spec/fleet.md#10-verification
> CI MUST run the unit tests with the race detector.

Several files on main are not gofmt-clean, for example
`internal/caspaxos/ballotspace_test.go`, `internal/caspaxos/unknown_test.go`,
`internal/owner/manager.go`, `cmd/cask/mint.go`, and
`cmd/cask-apiserver/types.go`. Nothing in CI checks formatting, so the
drift grows and agents keep reporting it.

Task: run `gofmt -w` on every Go file in both modules. Add a step to the
`go` job in `.github/workflows/verify.yml` that fails when
`gofmt -l .` prints anything. Add the same check to `devbox run verify`.
Check that gofmt does not rewrite any `//=` Duvet line (citations must
stay inside function bodies).

Files: `.github/workflows/verify.yml`, `devbox.json`, the unformatted files

Done when: `test -z "$(gofmt -l .)"` passes and `devbox run duvet-ci` passes.



## roster: Remove must not change the core outside a joint change

labels: membership

Spec: docs/spec/fleet.md#6-membership
> The voter set MUST change only by joint-consensus reconfiguration of the roster.

Found in the second review of #117. A quiescent `Roster.Remove`
(`internal/roster/roster.go`) drops a voter from Core directly and bumps
ConfigGen. It never runs joint consensus, so the carry hook never runs.
Reproduced: core {1..5}, voters 1 and 2 hung while 5 keys commit on
{3,4,5}, then Remove(5) and Remove(4) leave core {1,2,3}; with voter 3
stopped, all 5 keys read back empty.

Task: when a carry hook is set, route a core-changing Remove through
`Reconfigure` (joint change plus carry), or refuse it. Add the review's
`TestReviewRemoveShrinkNoCarry` as a regression test. Add a shrink to
`quint/core_change.qnt` with a negative control that removes a voter
without a joint change.

Files: `internal/roster/roster.go`, `quint/core_change.qnt`, tests

Done when: the regression test passes and `devbox run quint` prints `quint: ok`.



## apiserver: legacy fleetStore update and delete must compare on sequence

labels: apiserver

Spec: docs/spec/fleet.md#3-storage-model
> An update MUST use a compare-and-set on the resourceVersion the client supplied.

Found while building the storage.Interface in #127 to #131. The legacy
`fleetStore` in `cmd/cask-apiserver/store.go` still has two races:

- `update` compares values, not sequences. If a value changes and then
  changes back, a stale resourceVersion passes the check.
- `delete` reads, then deletes without a condition. A concurrent update
  between the two is silently deleted.

Task: switch `update` to `mvcc.CASSeq` and `delete` to `mvcc.DeleteSeq`
on the sequence read (both added in #130 and #131). Add a test for each
race. Once the generic server (#38) replaces the legacy handlers, this
code goes away; until then it serves the demo.

Files: `cmd/cask-apiserver/store.go`, `cmd/cask-apiserver/server_test.go`

Done when: `go -C cmd/cask-apiserver test -race ./...` passes with both new tests.



## apiserver: read the index entry without a round on the index register

labels: fleet, apiserver, agent-5m

Spec: docs/spec/fleet.md#3-storage-model
> A get MUST serve an object only once the index register records it.

Since #150, `Store.Get`, `GuaranteedUpdate`, and `Delete` read the index
entry first. Each read is an identity round on the index register of the
resource type. The watches and the index writes use the same register, so
the extra rounds make an index round lose more often. The watch test now
runs a mutation again when it loses before its object write commits.

Task: serve the index entry read from the owner cache (`mvcc.WithLocalReader`)
when it vouches for itself, or from the one index reader of #149. Count
the identity rounds on the index register in a test.

Files: `cmd/cask-apiserver/storage/cask.go`, `cmd/cask-apiserver/storage/index.go`

Done when: a get costs no identity round on the index register when the
owner cache serves it, and `go -C cmd/cask-apiserver test -race -count=300
-run TestWatch ./storage/` passes.

## storage: index write landed at a lower sequence than the index read during a voter outage

labels: apiserver, fleet

Spec: docs/spec/fleet.md#3-storage-model
> The index register MUST NOT record a sequence higher than the object register holds.

Seen once in three e2e runs of `TestZombieFenceRejection` (#58, PR #169),
while east's apiserver was scaled to zero:

```
create claim e2e-zombie-fence-west: cask storage: deviceclaims index write landed at 6, want 9
```

The error comes from `writeIndex` in `cmd/cask-apiserver/storage/index.go`
(added in #165). The index register's sequence appears to go backwards
across a read and a compare-and-set during a one-voter outage. If real,
this is a linearizability violation: a stale read of the index register
(for example an owner-cached or local read), or a lost committed write.
No server logs were captured for that run.

Task: reproduce in-process with three acceptors, one stopped and later
restarted empty-state-free (its Pebble data kept), and concurrent
creates on one resource type through two apiservers. Log every index
read and CASSeq with ballot and sequence. Decide whether a committed
index value was lost, a read was stale, or the error check itself is
wrong. If it is a consensus or read-path bug, add a Quint negative
control or a simulator fault that finds it, and fix it. If the check is
wrong, fix the check and explain why the sequence may legitimately be
lower.

Files: `cmd/cask-apiserver/storage/index.go`, `internal/mvcc`, `internal/caspaxos`, `internal/owner`

Done when: the reproduction runs 500 times with no regression, and the cause is written in the PR.

## storage: a committed mutation fails when its index write loses a round

labels: fleet, apiserver, agent-5m

Spec: docs/spec/fleet.md#3-storage-model
> When the index write of a mutation did not complete, the next index write for that object MUST record the object register's current sequence.

CI on PR #167 failed in `TestWatchFromOldVersionDeliversEveryChangeInOrder`:

```
cask storage: delete ".../devices/gpu-1": object tombstoned, index write failed: caspaxos: write outcome unknown; re-read before retry
```

The delete's tombstone committed. Its index write then lost its round
with `ErrUnknownOutcome`, and `writeIndex` returned the error. A client
gets a 500 for a delete that happened. Create and update have the same
shape. `writeIndex` retries only `ErrConflict`, and with no backoff.

Task: retry an index write that ends in `ErrPreempted` or
`ErrUnknownOutcome`, with jittered backoff and a bounded budget. Each
attempt re-reads the index and the object head and compares and sets on
the index sequence it read. Add a test that injects `ErrUnknownOutcome`
into the index write of a create, an update, and a delete, and checks
the returned resourceVersion.

Files: `cmd/cask-apiserver/storage/index.go`, `cmd/cask-apiserver/storage/lostround_test.go`

Done when: the new test passes, and fails without the change.

## security: refuse to serve consensus without mutual TLS

labels: membership

Spec: docs/spec/fleet.md#8-security
> The cask client API and control endpoints MUST NOT be reachable outside the pod without authentication.

Since #48, `--consensus-cert`, `--consensus-key`, and `--consensus-ca` put
mutual TLS on the consensus listener. The acceptor RPC, `/roster`,
`/roster/join`, `/roster/keys`, and the admin endpoints then need a client
certificate that the fleet CA signed. Without the flags, both binaries log
a warning and serve all of these in plaintext to anyone who can reach the
port. The demo and e2e now pass the flags, so the fallback only serves
unit tests.

Task: make `cmd/cask-apiserver` exit when `--listen-consensus` is set
without the three consensus flags, unless `--insecure-consensus` is set.
Keep the plaintext path for tests behind that flag. Then remove the
control-endpoint half of the Duvet exception in
`cmd/cask-apiserver/main.go`; the API half stays on #38.

Files: `cmd/cask-apiserver/main.go`, `internal/mtls/mtls.go`

Done when: `go -C cmd/cask-apiserver test -race ./...` passes, and a test
shows that start fails without the flags and without
`--insecure-consensus`.

## security: rotate and revoke consensus certificates

labels: membership

Spec: docs/spec/fleet.md#8-security
> Consensus traffic between members MUST use mutual TLS.

Since #48, each member reads `--consensus-cert`, `--consensus-key`, and
`--consensus-ca` once at start. `cask-apiserver gen-consensus-certs`
discards the CA key, and member certificates live for one year. So an
operator cannot add a member to the demo CA, cannot rotate a certificate
without a restart, and cannot revoke a stolen certificate.

Task: reload the three files when they change on disk. Use
`tls.Config.GetCertificate`, `GetClientCertificate`, and a CA pool that
`VerifyConnection` and `VerifyPeerCertificate` read under a lock. Add a
deny list of certificate serial numbers, read from an optional
`--consensus-deny` file, that both sides check. Keep the CA key in
`gen-consensus-certs` behind `--ca-key-out`.

Files: `internal/mtls/mtls.go`, `cmd/cask-apiserver/certs.go`

Done when: `go test -race ./internal/mtls/` shows that a member picks up a
new certificate without a restart, and that a denied serial is refused on
both sides.

## security: disable TLS session tickets on the consensus listener

labels: membership

Spec: docs/spec/fleet.md#8-security
> Consensus traffic between members MUST use mutual TLS.

A TLS 1.3 session ticket lets a client resume without a new certificate
check. Once consensus certificates can be revoked, a revoked member could
resume a session it opened before the revocation. This depends on the
rotation and revocation issue, #160.

Task: set `SessionTicketsDisabled: true` on the server config in
`mtls.New`.

Files: `internal/mtls/mtls.go`, `internal/mtls/mtls_test.go`

Done when: `go test -race ./internal/mtls/` shows that the server config
disables session tickets and that a second connection runs a full
handshake.

## security: bind the consensus certificate to the node id

labels: membership

Spec: docs/spec/fleet.md#8-security
> Consensus traffic between members MUST use mutual TLS.

Mutual TLS checks only that the fleet CA signed the peer certificate. Any
member certificate can then join the roster as any node id, or propose
with any ballot node id. A stolen certificate from one member can
impersonate every member.

Task: put the node id in each member certificate, as a URI SAN
`cask://node/<id>`. On the server, read the verified peer certificate from
`r.TLS` in `/roster/join` and reject a join whose node id differs. In the
acceptor handler, reject a ballot whose node id differs. Keep the check
off when the listener is plaintext.

Files: `internal/mtls/ca.go`, `internal/cluster/cluster.go`,
`internal/transport/connect.go`, `cmd/cask-apiserver/membership.go`

Done when: `go -C cmd/cask-apiserver test -race ./...` passes, and a test
shows that a member with the certificate of node 2 cannot join as node 9
or prepare with ballot node id 9.

## security: stop serving consensus on the host port in cmd/cask overlay mode

labels: membership

Spec: docs/spec/fleet.md#8-security
> Consensus traffic between members MUST use mutual TLS.

With `--nebula-config` or `--mint`, `cmd/cask` serves the consensus mux on
the Nebula overlay. It also serves the same mux, with the acceptor RPC,
`/roster`, `/roster/join`, `/rangekeys`, `/health`, and the admin
endpoints, in plaintext on the host `--listen` port. A client that reaches
the host port bypasses the overlay's peer authentication.

Task: in overlay mode, serve only the client API (`/kv/`, `/cas/`,
`/session/`, `/lock/`) on `--listen`. Keep the consensus, roster, health,
and admin handlers on the overlay listener only.

Files: `cmd/cask/main.go`

Done when: `go test -race ./cmd/cask/` passes, and a test shows that the
host listener answers 404 for the acceptor route and `/roster` in overlay
mode.
