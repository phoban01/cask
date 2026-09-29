# Runbook: cutover from the CRD to cask

Spec: [docs/spec/fleet.md#7-migration](../spec/fleet.md#7-migration)

> Writers MUST be frozen from the start of the export until the APIService is available.

## When to use this runbook

Use it to move the objects of the `fleet.cask.dev` group from a CRD to the
cask extension server. The CRD keeps its objects in the management
cluster's etcd. After the cutover, the extension server serves the same
group, version, and kinds from cask, in every cluster of the fleet.

The cutover keeps each object's `metadata.uid` and
`metadata.creationTimestamp`. Clients see the same objects under the same
names.

Rehearse the whole procedure on a copy of the management cluster first.
The spec requires it.

## Current state

Parts of this procedure do not exist yet. Do not run it on a real cluster
until these are done:

- **Claims and fences.** Bound claims and the lock fences they hold are
  carried across the cutover by [#201](https://github.com/phoban01/cask/pull/201)
  and [#206](https://github.com/phoban01/cask/pull/206), which are not merged.
  Without them, every Bound claim goes to Lost after the import, and the
  next fence starts at 1.
- **Metadata on update.** The prototype server on `main` keeps only
  `name`, `resourceVersion`, and `labels` when it decodes an object. The
  first update through it drops the uid, the creationTimestamp, the
  annotations, the ownerReferences, and the finalizers. The claim
  controller also writes status through it. The generic server in
  [#191](https://github.com/phoban01/cask/pull/191) fixes this (#38).
- **resourceVersion above the etcd revision.** The import starts each
  index register at sequence 1
  ([#51](https://github.com/phoban01/cask/issues/51)). A resourceVersion
  that a client got from the CRD is higher than any that cask serves. So
  every client of the group must start again with a fresh list (step 10).
- **Continuous export.** No CronJob runs `cask-migrate export` yet
  ([#53](https://github.com/phoban01/cask/issues/53)). Run it by hand.
- **Rehearsal.** No e2e test runs this runbook on kind yet
  ([#54](https://github.com/phoban01/cask/issues/54)).
- **One import per fleet.** The import writes a marker register. It
  refuses a second export file that differs from the first. So cask takes
  the objects of one source cluster. No issue tracks a merge of several
  source clusters.
- **Rollback with the same uids.** No tool writes the export file back
  into a CRD. See [Rollback](#rollback).

## What can go wrong

Read this before you start.

### The garbage collector deletes dependents

The Kubernetes garbage collector deletes an object when every owner in its
`metadata.ownerReferences` is gone. It checks each owner by uid. An owner
is gone when the API answers `404`, or when it answers with an object that
has a different uid.

The cutover makes owners disappear twice:

1. Deleting the CRD deletes every object of the group. The garbage
   collector sees each owner go. It then deletes every dependent that
   names a fleet object as its owner. A dependent in another group, such
   as a ConfigMap or a Job, is not in the export file. It is lost.
2. After the APIService is registered, the garbage collector reads owners
   from cask. If an owner is not imported yet, cask answers `404`. If an
   owner has a new uid, the uid check fails. Either way, the garbage
   collector deletes its dependents.

The spec forbids the second case:

> The APIService MUST NOT become available while any imported object that
> other objects reference by ownerReference is missing.

This runbook prevents it by order: the APIService is registered only after
the import finishes, and the import keeps every uid. Step 1 deals with the
first case.

A fleet object that owns another fleet object is safe. Both are in the
export file, and both come back with the same uids.

### Finalizers hold the CRD in Terminating

A CRD is deleted only after all its objects are gone. An object with
`metadata.finalizers` stays until its controller removes them. The
controllers are frozen, so the CRD stays in `Terminating`. Step 5 removes
the finalizers after the export. The export file keeps them, and the
import writes them back.

### Bound claims lose their lease

A claim that is Bound in the export file holds no cask lock after the
import. Cask has no session for it. The claim controller of the cluster
named in `status.cluster` fails its next renewal and sets the claim to
Lost (`renew` in `cmd/cask-apiserver/claims.go`). The workload must create
a new claim.

A new cask lock mints its fences from the start. A receiver that kept a
higher fence from before the cutover rejects the new, lower fence. No
issue tracks fence continuity across the cutover. Reset such receivers
by hand, or release claims before the freeze.

### Old resourceVersions go back in time

Until #51 lands, cask serves lower resourceVersions than the CRD did. A
client that resumes a watch from a CRD resourceVersion gets wrong results.
Restart every client of the group after the cutover.

## Terms

- **Source cluster:** the management cluster that holds the CRD and its
  objects.
- **Writer:** any client that creates, updates, or deletes objects of the
  group. This includes controllers, CI jobs, and people with `kubectl`.
- **Export file:** the `cask-export/v1` file that `cask-migrate export`
  writes. Line 1 is a header with the source etcd revision. Every other
  line is one object, as the API returned it.

## Before you start

- The cask fleet runs, and every member is Ready. No cluster has the
  `v1alpha1.fleet.cask.dev` APIService pointing at cask yet.
- You have a `cask-migrate` binary. The demo image has one at
  `/usr/local/bin/cask-migrate`. To build it:

  ```sh
  go -C cmd/cask-apiserver build -o /tmp/cask-migrate ./cmd/cask-migrate
  ```

- You have an etcd backup of the source cluster. It is the only way back
  to the CRD with the same uids.
- No object of the group has a `metadata.deletionTimestamp`:

  ```sh
  kubectl --context <source> get devices,deviceclaims -o json \
    | jq -r '.items[] | select(.metadata.deletionTimestamp) | .metadata.name'
  ```

  Wait until this prints nothing.

## Procedure

### 1. Find dependents outside the group

List every object in the source cluster that names a fleet object as its
owner:

```sh
kubectl --context <source> api-resources --verbs=list -o name \
  | xargs -n1 kubectl --context <source> get -A -o json --ignore-not-found \
  | jq -r '.items[]
      | select(any(.metadata.ownerReferences[]?; .apiVersion | startswith("fleet.cask.dev/")))
      | "\(.apiVersion) \(.kind) \(.metadata.namespace // "-") \(.metadata.name)"'
```

Ignore the lines whose `apiVersion` is `fleet.cask.dev/...`. The export
file carries those objects.

If no other line is left, go to step 2.

If other lines are left, pick one of these before you continue:

- **Stop the garbage collector.** Restart `kube-controller-manager` with
  `--controllers=*,-garbage-collector-controller` (older releases name it
  `garbagecollector`). Turn it back on after step 10. This keeps the
  ownerReferences in place. It needs access to the control plane.
- **Orphan the dependents.** Delete each fleet owner with
  `--cascade=orphan` in step 6. The garbage collector then removes the
  ownerReference from each dependent, and keeps the dependent. After
  step 10, add the ownerReferences back by hand. The uids do not change,
  so the old references are valid again.

If you cannot do either, stop. Cask has no other safe path.

### 2. Freeze the writers

Stop every writer of the group in every cluster of the fleet. Scale each
controller to zero:

```sh
kubectl --context <cluster> -n <namespace> scale deployment/<controller> --replicas=0
```

Tell the people who use `kubectl` on the group to stop. Pause CI jobs that
write to it. The freeze lasts until step 10.

### 3. Export

```sh
cask-migrate export --context <source> --output fleet-export.jsonl
```

The command only reads from the cluster. It prints one line:

```text
exported 42 objects in 2 resources of fleet.cask.dev at source etcd revision 918273
```

Write down the object count and the revision.

The command stops with an error when an object has a namespace, no uid,
or no creationTimestamp. Do not edit the file to get past the error. Fix
the source object, and start again at step 3.

### 4. Check that the freeze holds

Export again to a second file:

```sh
cask-migrate export --context <source> --output fleet-export-check.jsonl
diff <(tail -n +2 fleet-export.jsonl) <(tail -n +2 fleet-export-check.jsonl)
```

The export writes the same bytes for the same objects. Each object line
keeps its own resourceVersion, so any write to any object shows up here.
The header can differ, because the revision counts writes to the whole
cluster. `tail -n +2` skips it.

If `diff` prints anything, a writer is still running. Find it, stop it,
and start again at step 3.

Copy `fleet-export.jsonl` to safe storage.

### 5. Remove the finalizers

```sh
kubectl --context <source> get devices,deviceclaims -o name \
  | xargs -r -n1 kubectl --context <source> patch --type=merge -p '{"metadata":{"finalizers":null}}'
```

The export file still holds the finalizers. The import writes them back.

### 6. Orphan the dependents (only if you chose it in step 1)

```sh
kubectl --context <source> delete devices,deviceclaims --all --cascade=orphan
```

Wait until the dependents from step 1 have no fleet ownerReference left.

### 7. Delete the CRD

```sh
kubectl --context <source> delete crd devices.fleet.cask.dev deviceclaims.fleet.cask.dev
kubectl --context <source> wait --for=delete crd/devices.fleet.cask.dev crd/deviceclaims.fleet.cask.dev --timeout=5m
```

The CRD registers its own APIService with the same name. Wait until it is
gone:

```sh
kubectl --context <source> get apiservice v1alpha1.fleet.cask.dev
```

Continue when this answers `NotFound`.

### 8. Import

Make the export file readable inside one apiserver pod. A file under
1 MiB fits in a ConfigMap. A larger file needs a volume.

Add `--import-file` to that one apiserver. Also add `--expect-import`
with the revision from step 3. Then restart it:

```sh
kubectl --context <cluster> -n cask-system patch statefulset/cask-apiserver --type=json -p '[
  {"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--import-file=/import/fleet-export.jsonl"},
  {"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--expect-import"},
  {"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--expect-import-revision=<revision>"}]'
kubectl --context <cluster> -n cask-system rollout status statefulset/cask-apiserver --timeout=10m
```

The import runs before the server listens. It writes through consensus,
so one apiserver imports for the whole fleet. It writes a marker register
last.

Check the log:

```sh
kubectl --context <cluster> -n cask-system logs statefulset/cask-apiserver | grep 'import done'
```

`written` plus `unchanged` must equal the object count from step 3.

The import checks the whole file before its first write. If the file is
invalid, or it conflicts with an object that cask already holds, the
import writes nothing and the process exits. The pod then restarts in a
loop. Read the log, fix the cause, and start step 8 again. A second run
with the same file is safe.

Remove `--import-file` after the import. A later restart does not need it.
Keep `--expect-import`. The marker stays in cask, so the check passes at
once after a restart.

Add `--expect-import` and `--expect-import-revision` to the apiserver in
each other cluster of the fleet too, in the same way.

### 9. Register the APIService

With `--expect-import`, an apiserver reports not ready until three things
are true:

- The import marker is in cask.
- The marker names the `cask-export/v1` format, the `fleet.cask.dev`
  group, and the revision from step 3.
- Every Device or DeviceClaim that an object names in its
  `ownerReferences` exists in cask with the same uid.

A pod that is not ready gets no traffic from its Service. So the
APIService cannot become Available before the import is complete. You do
not have to hold it back by hand.

Apply the APIService in the source cluster. Use the APIService object
from `demo/kind/manifests/apiserver.yaml` as the pattern.

```sh
kubectl --context <source> apply -f apiservice.yaml
kubectl --context <source> wait --for=condition=Available apiservice/v1alpha1.fleet.cask.dev --timeout=5m
```

If the wait times out, read the readiness checks of the apiserver:

```sh
kubectl --context <source> -n cask-system exec cask-apiserver-0 -- \
  wget -qO- --no-check-certificate 'https://localhost:9443/readyz?verbose'
```

A failed `cask-import` check names the missing marker, the wrong
revision, or the missing owners. A failed `cask-storage` check means the
apiserver cannot reach a majority of the voters.

Do the same in each other cluster of the fleet that does not serve the
group yet.

Check that cask serves every object with its old uid and creationTimestamp:

```sh
tail -n +2 fleet-export.jsonl \
  | jq -r '[.kind, .metadata.name, .metadata.uid, .metadata.creationTimestamp] | @tsv' \
  | sort > before.tsv
kubectl --context <source> get devices,deviceclaims -o json \
  | jq -r '.items[] | [.kind, .metadata.name, .metadata.uid, .metadata.creationTimestamp] | @tsv' \
  | sort > after.tsv
diff before.tsv after.tsv
```

If `diff` prints anything, do not unfreeze. Go to [Rollback](#rollback).

### 10. Unfreeze

- If you stopped the garbage collector in step 1, turn it back on.
- If you orphaned the dependents in step 6, add their ownerReferences
  back now.
- Restart every client of the group, readers too. Each one must start
  from a fresh list, not from a resourceVersion it kept (#51).
- Scale the writers back up.

### 11. Check the result

- `kubectl get devices` answers from every cluster and shows the same
  objects.
- A new DeviceClaim binds and shows its fence in its status.
- The dependents from step 1 still exist.

## Rollback

What you can do depends on how far you got.

**Before step 5.** Nothing has changed in the source cluster. Unfreeze
the writers.

**Step 5 done, CRD objects still there.** Put the finalizers back from
the export file. Then unfreeze the writers.

**From step 6, before step 10.** The CRD objects are gone.

1. Delete the cask APIService in every cluster where you applied it.
2. If the CRD is gone, apply it again.
3. Choose one:
   - Restore the source cluster's etcd from the backup. This brings back
     the objects with their uids. It also rolls back every other object
     in the cluster to the time of the backup.
   - Create the objects again from the export file. Remove
     `metadata.uid`, `metadata.creationTimestamp`, and
     `metadata.resourceVersion` first. The API server sets a new uid and
     creationTimestamp on create, and it refuses a create that sets
     resourceVersion. The garbage collector then deletes any dependent
     that still points at an old uid. Stop it, or orphan the dependents
     first.
4. Unfreeze the writers.

Cask can keep the objects of a failed try. A new try with a different
export file conflicts with the import marker. Start a new try on an empty
fleet.

**After step 10.** Writes after the cutover exist only in cask. To keep
them, export from cask with `cask-migrate export` before you roll back.
The same limits on uids apply.
