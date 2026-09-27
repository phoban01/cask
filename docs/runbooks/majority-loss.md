# Runbook: majority loss

Spec: [docs/spec/fleet.md#9-operations](../spec/fleet.md#9-operations)

> A majority-loss recovery procedure MUST be documented.

## When to use this runbook

Use it when a majority of the voters are lost and will not come back.
Examples: two of three voters, or three of five, lost their disks or
their clusters.

Do not use it when the voters are only down or partitioned. The fleet
stalls, but it heals when a majority returns. Bring the voters back
instead. This procedure throws away the lost voters' state for good.

## What you lose

Read this before you start.

- A write that only the lost voters accepted is gone. The client got a
  success response, but no survivor holds the value.
- A proposal that was in flight when the voters failed can go either way.
  The founder can hold it as its last accepted value. After recovery that
  value becomes the committed value, although no client got a success
  response for it.
- A lock can lose its most recent acquisitions. A fence minted after the
  founder's last accepted value is lost. The next acquisition can mint
  that same fence again. A holder from before the loss can then carry a
  fence equal to the new holder's fence, and receivers accept both. Stop
  every controller that held a claim in a lost cluster before you restart
  the fleet.
- The roster generation is the best hint for the most recent survivor. It
  is not a guarantee. Another survivor can hold a newer value for a data
  key.

## Current state

The procedure below is the target. Two parts of it do not exist yet:

- `cask-apiserver --force-new-fleet` is tracked in
  [#74](https://github.com/phoban01/cask/issues/74).
- The promote endpoint is tracked in
  [#45](https://github.com/phoban01/cask/issues/45).

The apiserver joins the fleet through the dynamic roster path:
`--bootstrap`, `--seed`, and `GET /roster` on the `--listen-consensus`
address. `cmd/cask` uses the same code. `--cask-peers` is deprecated and
kept for tests.

Until the two missing parts land, the apiserver has no supported
majority-loss recovery. Do not shorten `--cask-peers` by hand to force a
quorum.

## Terms

- **Roster:** the register at key `\x00roster` that holds the membership.
  See `internal/roster/roster.go`.
- **Core:** the voters that store the roster register (`core` in the
  roster value).
- **Roster generation:** `cfg_gen` in the roster value. It goes up on every
  change to the core. `epoch` goes up on every membership change.

## Procedure

### 1. Confirm the majority loss

Read `core` from any survivor with `GET /roster`. Count the core members
that answer. Continue only if fewer than a majority answer and the rest
are permanently lost.

### 2. Record the roster generation of each survivor

Each node serves its last known roster value at `GET /roster`. This read
is local. It does not need a quorum, so it works during a majority loss.

```sh
curl -s http://<survivor-addr>/roster | jq '{cfg_gen, epoch, core}'
```

Do this before you stop the survivors. Write down `cfg_gen` and `epoch`
for each one.

### 3. Stop all survivors

Stop every survivor, voters and participants. No node may propose while
you rewrite the roster. On Kubernetes, scale each survivor to zero:

```sh
kubectl --context <cluster> -n cask-system scale statefulset/cask-apiserver --replicas=0
```

### 4. Pick the founder

Pick the survivor with the highest `cfg_gen`. On a tie, pick the highest
`epoch`. On a second tie, pick any of them.

### 5. Back up every survivor's data dir

Copy the `--data-dir` of every survivor to safe storage. An
investigation can later need a value that only one survivor holds.

### 6. Rewrite the roster core to the founder

On the founder only, run the apiserver once with its normal `--data-dir`
and `--force-new-fleet`:

```sh
cask-apiserver --cluster <name> --data-dir /var/lib/cask --force-new-fleet
```

The command sets the core and the members to the founder alone. It
clears any joint configuration and raises `cfg_gen` and `epoch`. Then it
exits.

### 7. Restart the founder

Start the founder with its normal flags and `--bootstrap`. The founder
finds the rewritten roster and keeps it, so it forms a one-voter fleet.
Check that it serves reads and writes:

```sh
kubectl --context <founder-cluster> get devices
```

### 8. Rejoin the other survivors as new members

Empty the `--data-dir` of every other survivor. You backed it up in
step 5. Start each one with `--seed` set to the founder. It joins as a
new participant.

Do not restart a survivor with its old data dir. It still holds the old
core, and it can form a second fleet with other nodes that hold it.

Do not start a lost voter with its old data. If a lost voter comes back,
empty its data dir first and join it as a new member.

### 9. Re-promote voters

Grow the voters from one to three. Then grow from three to five if you
had five. Never grow from one to two. The promote endpoint changes the
core by joint-consensus reconfiguration, and it refuses an even voter
count:

```sh
curl -X POST https://<founder-addr>/admin/promote/<id>
```

Wait until `core` in `GET /roster` shows the new voters before the next
step.

### 10. Check the result

- `GET /roster` shows the same `core` and `cfg_gen` on every member.
- Each cluster's APIService reports `Available`.
- A claim created after recovery binds and shows its fence in its status.

## Rehearsal

The spec requires a rehearsal of this procedure before phase two. Use the
three-cluster kind demo in `demo/kind/`.
