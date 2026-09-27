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
Kubernetes minor. Add the module to `go.work`.

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
