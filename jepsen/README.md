# Jepsen tests for cask

Out-of-process Jepsen tests that drive **running `cask` nodes** under a nemesis
(partitions, clock skew, process kills, membership churn) and check the recorded
history with Jepsen's Elle/Knossos checkers.

## Status

This directory is a **scaffold/artifact** (like `quint/`). The CI-enforced version
of the headline gate already runs **in-process and deterministically** at
[`test/jepsen/fencing_test.go`](../test/jepsen/fencing_test.go) — the
fencing-token monotonicity invariant under a partition nemesis. The Clojure
harness here is for the full out-of-process story and needs a JVM + Leiningen +
the `cask` binary; it is not run in the authoring environment.

## Workloads (planned)

- **lin-kv** — `Put`/`Get`/`CAS` over the `/kv` and `/cas` HTTP API, checked for
  linearizability (Knossos/Elle).
- **lock + fencing** — the headline: many clients `POST /lock/<name>` and assert
  no two hold simultaneously and fencing tokens are monotonic with respect to
  grant order (Kleppmann fencing). This is the **release gate**.
- **set / append** — Elle, for the snapshot-isolation claims.

## Nemeses (matched to cask's risks)

- **partition** — split the storage tier; a quorum must remain for progress.
- **clock-skew** — stresses HLC snapshots and lease expiry.
- **kill/pause** — process crashes and restarts (durability).
- **membership churn** — add/remove storage nodes mid-test to exercise elastic
  placement + catch-up-before-release (M5). The partition+churn lock-fencing run
  is the v1 release gate.

## Client mapping

Jepsen's client speaks to a node's HTTP API:

| op       | request                                         |
|----------|-------------------------------------------------|
| write    | `PUT /kv/<k>` body=value                         |
| read     | `GET /kv/<k>`                                     |
| cas      | `POST /cas/<k>?expect=<old>` body=new            |
| session  | `POST /session/<id>?ttl=<s>` (+ `/keepalive`)    |
| acquire  | `POST /lock/<name>?session=<id>` -> `{token}`    |
| release  | `DELETE /lock/<name>?session=<id>`               |

## Running (once fleshed out)

```sh
# needs: jdk, leiningen, the cask binary on the test nodes
lein run test --workload lock --nemesis partition,kill --time-limit 300
```
