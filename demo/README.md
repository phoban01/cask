# cask multi-cloud demo

Forms a single cask cluster across **three different clouds** over the encrypted
Nebula overlay, then joins **this laptop behind NAT** as a client that holds zero
replicas yet reads and writes the globally-coordinated state. The point: cask
self-forms across clouds and NAT with no firewall/VPN config beyond one open UDP
port — write a key or take a fenced lock on one cloud, read it on another and on
your laptop.

```
GCP  e2-micro  (x86,  FREE)  ┐
Oracle A1.Flex (arm64, FREE) ├─ lighthouses + RF=3 replica set (the data lives here)
AWS  t4g.small (arm64, FREE) ┘
this laptop  (NAT, client-only) ── routes to the replicas, stores nothing
```

## Cost

Designed to be ~free: Oracle A1 and AWS t4g.small are free (the AWS Graviton free
trial runs through **Dec 31 2026**), GCP e2-micro is Always Free. GCP has no free
arm, so that node is x86 — the cluster is mixed-arch and the deploy ships each VM
the matching binary. Tear down with `down.sh` when finished so nothing lingers.

## Prerequisites

- Authenticated CLIs on your PATH: **`gcloud`**, **`oci`**, **`aws`** (each logged in
  to an account with permission to create a VM + firewall/security rules).
- Go toolchain (to build the binaries), `ssh`/`scp`, `curl`.
- Set the account-specific values in [config.sh](config.sh) (or via the environment):
  - `GCP_PROJECT` — your gcloud project id (**required**)
  - `OCI_COMPARTMENT` — Oracle compartment OCID (**required**); optionally `OCI_SUBNET`
    to reuse an existing public subnet instead of creating a throwaway VCN
  - `AWS_REGION` (default `us-east-1`); AWS uses the default VPC
  - Optional overrides: zones/regions, instance types, overlay ports.

## Run

```sh
demo/up.sh          # provision 3 clouds (parallel) -> build -> deploy -> start laptop
demo/interact.sh    # cross-cloud read-after-write, fenced lock, replica failover
demo/down.sh        # destroy all cloud resources + stop the laptop node
# or:
demo/demo.sh        # up.sh + interact.sh in one go (leaves the cluster running)
```

`demo/interact.sh --no-failover` skips the stop-a-replica act.
`demo/down.sh --purge` also removes the generated certs and demo ssh key.

## How it works

1. **`up.sh`** generates a one-off ed25519 ssh key, runs the three `clouds/<c>-up.sh`
   scripts in parallel (each creates one VM, opens inbound UDP `4242` + TCP `22`,
   injects the ssh key, and records the public IP to `state/<c>.env`), builds the
   binaries, then runs `deploy.sh`.
2. **`gen-spec.sh`** turns the state files into `clusterconf/nodes.json` — the three
   clouds as lighthouses advertised at their **public IPs**, plus the laptop as a
   non-lighthouse client.
3. **`deploy.sh`** runs `cask gen-certs -spec` to mint the CA + per-node Nebula
   configs, scps the arch-matched binary + config to each VM and starts it under
   `systemd` (`cask.service`), then starts the laptop locally with `--client-only`.
4. **`interact.sh`** drives the cluster: cloud APIs are reached via `ssh <vm> curl
   localhost:8080`; the laptop API is local.

Each node's KV/lock API binds to `127.0.0.1:8080` on its own host — only the Nebula
underlay (UDP `4242`) is exposed publicly; consensus rides the encrypted overlay.

## Files

| file | role |
|------|------|
| `config.sh` | shared config; **set the REQUIRED account values here** |
| `lib.sh` | logging, state files, ssh/scp helpers |
| `build.sh` | host + `linux/{amd64,arm64}` binaries |
| `clouds/<c>-up.sh` / `-down.sh` | provision / destroy one cloud (gcp, oci, aws) |
| `gen-spec.sh` | state files → `clusterconf/nodes.json` |
| `deploy.sh` | certs → push/start every node |
| `interact.sh` | the scripted demo |
| `up.sh` / `down.sh` / `demo.sh` | orchestrators |
| `state/` | generated: ssh key, per-cloud `.env`, laptop pid/log (gitignored) |
```
