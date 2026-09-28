# Runbook: rolling upgrade across clusters

Spec: [docs/spec/fleet.md#9-operations](../spec/fleet.md#9-operations)

> A rolling upgrade MUST keep a majority of voters available at all times.

## When to use this runbook

Use it to move every `cask-apiserver` in a fleet to a new image. The same
steps apply to a change of flags or of the pod template.

## Why one cluster at a time

Each management cluster runs one `cask-apiserver`. Once three or more
clusters exist, each cluster holds at most one voter. The voters form the
quorum for every read and every write.

Each cluster has a PodDisruptionBudget with `maxUnavailable: 1` (see
`demo/kind/manifests/apiserver.yaml`). The budget counts pods in its own
cluster only. With one pod per cluster, it lets that pod go. It does not
see the other clusters. Two clusters that upgrade at the same time take
two voters down, and three voters then have no majority.

So the order across clusters is your job. Nothing in cask enforces it.

A node drain or a node upgrade in another cluster also takes a voter
down. Treat it as a second upgrade.

## Current state

Some parts of this procedure do not exist yet:

- **Quorum health signal.** Cask exposes no metric for quorum state
  ([#62](https://github.com/phoban01/cask/issues/62)). Until it lands,
  step 1 uses a read through each cluster's API and `GET /roster`. After
  #62, use `cask_quorum_reachable` and `cask_voters_reachable`.
- **Readiness.** The pod's readiness probe does not check storage
  ([#39](https://github.com/phoban01/cask/issues/39)). `/healthz` answers
  `ok` as soon as the server listens. A Ready pod does not prove that
  its voter takes part in the quorum.
- **Demo script.** No script runs this runbook on the kind demo
  ([#194](https://github.com/phoban01/cask/issues/194)).
- **Demo topology.** The kind demo still starts each apiserver with a
  static `--cask-peers` list ([#109](https://github.com/phoban01/cask/issues/109)).
  In that mode, `GET /roster` and the promote and demote endpoints do not
  exist. Use only the read check in step 1, and skip
  [Replace a lost voter](#replace-a-lost-voter).
- **Version check.** Members do not check each other's version. During
  the upgrade, old and new members run side by side. Read the release
  notes for any change to the wire format or the data dir format before
  you start.

## Terms

- **Voter:** a member in `core` of the roster. It holds register
  replicas.
- **Participant:** a member that is not a voter. It reads and proposes,
  but holds no replicas. Its restart does not touch the quorum.
- **Driver:** the voter with the highest node id. Only the driver adds
  members and changes the core. While it restarts, joins and core changes
  wait.
- **Roster generation:** `cfg_gen` in the roster value. It goes up on
  every change to the core.

## Before you start

- **Count the voters.** With one voter, every restart stops all reads and
  writes. The spec keeps the founding member up until the voters grow to
  three. Grow to three voters first, or plan for the outage.
- **Stop other changes.** Do not promote, demote, or join members during
  the upgrade. Pause node upgrades and node drains in every cluster.
- **Check the data dirs.** Every voter runs with `--data-dir` on a
  PersistentVolumeClaim. A voter that restarts without its data forgets
  its promises. That is not safe.
- **Switch the founder to `--seed`.** The founder started with
  `--bootstrap`. On a restart, `--bootstrap` reads the roster from the
  founder's own acceptor only, and keeps the value it finds there. That
  value can be old. Before you upgrade the founder, replace `--bootstrap`
  with `--seed` and the consensus addresses of two other members. A member
  that is already in the roster rejoins through `--seed` without a change
  to the roster.
- **Pick the order.** Upgrade the participants first. A bad image then
  shows itself before it can touch the quorum. Then upgrade the voters one
  at a time. Upgrade the driver last, so it restarts once.

## Procedure

Do steps 1 to 4 for one cluster. Then start again at step 1 for the next
cluster.

### 1. Check the quorum before you start

Read through the API of every cluster:

```sh
for c in <cluster> <cluster> <cluster>; do
  kubectl --context "$c" get devices > /dev/null && echo "$c ok"
done
```

Each read is a consensus round on a majority of the voters. A read that
answers proves that the cluster's apiserver reaches a majority. A read
that hangs or fails means it does not.

Read the roster from every voter:

```sh
curl -s http://<consensus-addr>/roster | jq '{core, cfg_gen, joint}'
```

This read is local to the member. It does not need a quorum. Check that:

- every voter answers;
- every voter shows the same `core` and the same `cfg_gen`;
- no voter shows a `joint` value. A `joint` value means a core change is
  in flight. Wait for it to finish.

The consensus address is the `--advertise-consensus` value. When the
member runs with the consensus TLS flags, use `https://` and pass a
client certificate that the fleet CA signed.

After #62, also check the metrics of every member:
`cask_quorum_reachable` is 1, and `cask_voters_reachable` equals the
number of voters.

Do not continue if any check fails. Find out why first.

### 2. Upgrade one cluster

```sh
kubectl --context <cluster> -n cask-system set image statefulset/cask-apiserver apiserver=<new-image>
kubectl --context <cluster> -n cask-system rollout status statefulset/cask-apiserver --timeout=10m
```

The StatefulSet stops the old pod and starts a new one. The new pod
mounts the same PersistentVolumeClaim, so the acceptor keeps its state.

During the restart this cluster's voter is down. The other voters still
hold a majority. The API in this cluster does not answer until the pod is
back.

### 3. Wait for the cluster to serve

```sh
kubectl --context <cluster> wait --for=condition=Available apiservice/v1alpha1.fleet.cask.dev --timeout=5m
```

A Ready pod is not enough (#39). Go on to step 4.

### 4. Check the quorum again

Repeat step 1 for every cluster, the upgraded one too. The upgraded voter
must show the same `core` and `cfg_gen` as the others.

Only then start step 1 for the next cluster.

## If a voter does not come back

Stop. Do not upgrade another cluster. The fleet has a majority, but no
margin: one more voter down stops it.

### The new image fails

The pod crashes in a loop or never serves. Roll the StatefulSet back:

```sh
kubectl --context <cluster> -n cask-system rollout undo statefulset/cask-apiserver
kubectl --context <cluster> -n cask-system delete pod cask-apiserver-0
```

The StatefulSet does not replace a pod that never became Ready. Delete
the pod, so the old template takes its place. The data dir stays on the
PersistentVolumeClaim.

Then repeat step 1. Stop the upgrade until you know why the image failed.

### The pod does not start

The pod stays `Pending`. A local volume ties the pod to one node. Check
that the node is up and the PersistentVolumeClaim is `Bound`:

```sh
kubectl --context <cluster> -n cask-system describe pod cask-apiserver-0
kubectl --context <cluster> -n cask-system get pvc data-cask-apiserver-0
```

Bring the node back. Do not delete the PersistentVolumeClaim.

### The data dir is lost

Do not start the voter again with an empty data dir under its old
`--id`. It would forget its promises and can break consensus. Treat the
voter as lost. Go to [Replace a lost voter](#replace-a-lost-voter).

### A majority of voters is down

The fleet stops reading and writing. If the voters can come back, bring
them back with their data dirs. The fleet then heals by itself. If they
are lost for good, follow [majority-loss.md](majority-loss.md).

## Replace a lost voter

The voter count must stay odd, so you cannot swap one voter for one new
member in one step. Use the promote and demote endpoints on the
consensus address of any member. See step 9 of
[majority-loss.md](majority-loss.md) for their rules.

1. Join a new member. Start an apiserver with an empty data dir, a new
   `--id`, and `--seed` set to live members. It joins as a participant.
2. If you have two participants, promote both. Three voters become five:

   ```sh
   curl -sL -X POST http://<consensus-addr>/admin/promote -d '[<new-id>, <other-id>]'
   ```

   Then demote the lost voter and one other voter. Five voters become
   three:

   ```sh
   curl -sL -X POST http://<consensus-addr>/admin/demote -d '[<lost-id>, <voter-id>]'
   ```

3. If you have only one participant, demote the lost voter and one live
   voter. Three voters become one. Then promote two participants. One
   voter becomes three. While one voter is left, the fleet has no margin
   at all. Prefer the path in step 2.

Each change must finish while a majority of the old voters and a majority
of the new voters answer. Wait until `core` in `GET /roster` shows the
new voters before the next request.

The lost member stays in `members` of the roster. No endpoint removes it
yet. Do not reuse its `--id`.
