# Multi-cloud cask demo: GCP + Oracle + AWS + a NAT'd laptop

## Context

We want a compelling, runnable demo of cask's headline capability: the **self-forming
Nebula overlay** lets one binary form a single encrypted consensus cluster *across
different clouds and across NAT, with no firewall/VPN plumbing*. The demo provisions one
free-tier node on each of GCP, Oracle, and AWS (all three are lighthouses and the RF=3
replica set), then joins our **local laptop behind NAT as a client-only node that holds
zero replicas** yet reads and writes the globally-coordinated state. The punchline:
*write a key/lock on the AWS node, read it on the laptop and on the GCP node; the laptop
stores none of the data.*

Two gaps block this today:
1. `cask gen-certs` only emits **loopback** configs ([gencerts.go:41](cmd/cask/gencerts.go#L41) hardcodes `127.0.0.1`; lighthouse advertise == listen host). Real cross-cloud needs lighthouses to **listen on `0.0.0.0`** but be **advertised at their public IP**, with arbitrary per-node lighthouse/zone/overlay-IP assignment. Public IPs only exist after provisioning, so they must be injected at generation time.
2. The self-forming path always calls `rost.Add(self)` ([cluster.go:75-79](cmd/cask/cluster.go#L75-L79)), making any Nebula node roster-eligible. With RF=3 over 4 members, HRW could place a replica on the NAT'd laptop — breaking "holds nothing" and forcing cloud→laptop hole-punching. We need a **client-only** mode.

Decisions (confirmed with user): **bash + cloud CLIs** for provisioning; **extend `gen-certs` in Go**; topology = **3 cloud lighthouses + NAT'd laptop**.

All Go changes were validated against the code by a design pass — discovery derives identity from overlay IPs only (the listen/advertise split is invisible to it), `rost.Get` is a pure linearizable read usable by a non-member, a non-roster proposer drives consensus on a replica set it isn't part of, HRW never selects a non-member, and the tree cross-compiles clean with `CGO_ENABLED=0` for amd64 + arm64.

---

## Part A — Go changes (make the binary multi-cloud capable)

### A1. `internal/transport/nebula/certgen.go` — split listen from advertise
- Add a field to `NodeSpec` (struct ~L19-25): `Advertise string` — public underlay addr for `static_host_map`; falls back to `UDP` when empty.
- In `GenerateConfigs`, the **only** place `static_host_map` originates is the `lhStatic` loop (~L63-68). Change `lhStatic[overlayIP] = []string{n.UDP}` to use `Advertise` when set:
  ```go
  adv := n.Advertise; if adv == "" { adv = n.UDP }
  lhStatic[n.OverlayIP.String()] = []string{adv}
  ```
  Leave `listen.host/port` deriving from `n.UDP` (~L93-97, L124). Net effect: `UDP="0.0.0.0:4242"` → binds all interfaces; `Advertise="<public-ip>:4242"` → what peers dial. No other line changes; member/lighthouse `static` maps already copy from `lhStatic`.

### A2. `cmd/cask/gencerts.go` — `-spec <file.json>` flag
- Add `import "encoding/json"` and a flag `-spec` (keep existing `-n/-base-ip/-udp-base/...` as the default loopback path when `-spec` is empty).
- When `-spec` is set, read a JSON array and build `[]nebula.NodeSpec` from it instead of the loopback loop. JSON shape (snake_case):
  ```json
  [{"name":"gcp","overlay_ip":"10.42.0.1","zone":"gcp","udp":"0.0.0.0:4242","advertise":"<gcp-pub>:4242","lighthouse":true},
   {"name":"aws","overlay_ip":"10.42.0.2","zone":"aws","udp":"0.0.0.0:4242","advertise":"<aws-pub>:4242","lighthouse":true},
   {"name":"oci","overlay_ip":"10.42.0.3","zone":"oci","udp":"0.0.0.0:4242","advertise":"<oci-pub>:4242","lighthouse":true},
   {"name":"laptop","overlay_ip":"10.42.0.100","zone":"edge","udp":"0.0.0.0:4242","advertise":"","lighthouse":false}]
  ```
  Parse via a small `specJSON` struct (string `overlay_ip` → `netip.ParseAddr`). Drive the printed run-script footer off `len(specs)` (not `*n`) to avoid an index panic.

### A3. `cmd/cask/main.go` + `cmd/cask/cluster.go` — client-only node
- **main.go**: add `--client-only` bool flag; pass it into `nebulaCluster(...)` (new trailing param at the call ~L79). Guard the health route — a client returns `mon == nil`:
  ```go
  if mon != nil { mux.HandleFunc("/health", mon.serveHealth) }
  ```
  Overlay listener goroutine and the local `--listen` KV/lock API stay unchanged.
- **cluster.go**:
  - `nebulaCluster(...)` gains a `clientOnly bool` param.
  - Skip the self-join when client-only: change the guard at ~L75 to `if !clientOnly && !containsID(genesis, self.NodeID)`. Everything else (`rost.Genesis` idempotent no-op, `rost.Get` pure read, `dyn.set(routerFor(self.NodeID, val, dialer))` over the 3 replicas) already works with `self ∉ val.Members`.
  - When client-only: return `nil` monitor and start a read-only loop instead of `reconcileLoop`:
    ```go
    if clientOnly { go reconcileLoopReadOnly(ctx, log, rost, dialer, dyn, val); return dyn, ln, self, nil, nil }
    ```
  - New `reconcileLoopReadOnly`: ticker every `reconcileInterval`; each tick `rost.Get`, and on epoch change `dialer.learn(v.Members)` + `dyn.set(routerFor(dialer.self, v, dialer))`. No `Controller`, no `mon.scan`, no `Reconcile`. The `dialer.learn` is required so newly-joined replica IDs resolve (else `ErrNoReplica`).
- Existing `cluster_test.go` only calls `routerFor`/`placeRange`/`dynamicProposer`, not `nebulaCluster`, so the new param doesn't break it. Add a small test: `placeRange` with `self` absent from members still yields a working router (optional but cheap).

### A4. Build (mixed arch — free tier dictates the split)
Free-tier reality (verified June 2026): **Oracle A1** (arm64) and **AWS t4g.small** (arm64,
Graviton free trial through Dec 31 2026) are free; **GCP has no free arm** — its Always-Free
`e2-micro` is **x86-only**. User chose the free GCP e2-micro, so the cluster is mixed-arch and
we build both Linux arches:
- `bin/cask` — local laptop (host arch; plain `go build`)
- `bin/cask-linux-arm64` — Oracle A1 + AWS t4g (`CGO_ENABLED=0 GOOS=linux GOARCH=arm64`)
- `bin/cask-linux-amd64` — GCP e2-micro (`CGO_ENABLED=0 GOOS=linux GOARCH=amd64`)

Deploy mechanism (user choice): **static binary + systemd over scp** — `deploy.sh` ships each
VM the binary matching the `ARCH` recorded in its state file. (Serverless container PaaS —
Cloud Run / App Runner — was ruled out: no inbound UDP / stable public IP for Nebula
lighthouses. Docker-on-VM multi-arch was offered but not chosen.)

---

## Part B — Provisioning + orchestration scripts (`demo/`)

Bash + native CLIs (`gcloud`, `oci`, `aws`). Each cloud's `*-up.sh` creates one always-free
instance, opens **inbound UDP 4242** (overlay) and **TCP 22** (SSH) from `0.0.0.0/0`,
installs a demo SSH key at create time, and writes `demo/state/<cloud>.env`
(`PUBLIC_IP`, `SSH_TARGET`, `ARCH`, `ZONE`, plus any created resource IDs for teardown).
`*-down.sh` deletes everything it created by reading that state file.

```
demo/
  README.md          prereqs (gcloud/oci/aws authed), what runs, est. $0 cost, cleanup note
  config.sh          shared: UDP_PORT=4242, OVERLAY_PORT=8001, API_PORT=8080,
                     overlay /24=10.42.0, instance names/tags, regions, SSH key path
  lib.sh             logging, ssh/scp wrappers (-i demo key), state read/write helpers
  build.sh           cross-compile the three binaries (A4)
  clouds/                (mixed arch: GCP amd64, OCI+AWS arm64 — deploy picks by ARCH)
    gcp-up.sh        gcloud compute instances create (e2-micro x86, FREE us-* region) + firewall
    gcp-down.sh      delete instance + firewall rule
    oci-up.sh        oci compute instance launch (VM.Standard.A1.Flex, arm64, FREE) + VCN/subnet/
                     internet-gw/security-list ingress (or reuse OCI_SUBNET from config.sh)
    oci-down.sh      terminate instance (+ teardown VCN if we created it)
    aws-up.sh        aws ec2 run-instances (t4g.small Graviton arm64, FREE trial) + security group
    aws-down.sh      terminate instance + delete security group
  gen-spec.sh        read state/*.env → write clusterconf/nodes.json (3 cloud LH + laptop)
  deploy.sh          cask gen-certs -spec clusterconf/nodes.json -out clusterconf/;
                     scp matching-arch binary + <node>.yml to each VM; install+start a
                     systemd unit running: cask --nebula-config <node>.yml
                     --overlay-port 8001 --listen 127.0.0.1:8080;
                     start the laptop locally: bin/cask --nebula-config clusterconf/laptop.yml
                     --overlay-port 8001 --listen 127.0.0.1:8080 --client-only  (PID to state)
  interact.sh        the scripted demo (below)
  up.sh              orchestrator: clouds up (parallel) → build → gen-spec → deploy
  down.sh            kill laptop node → clouds down (parallel) → rm state/ + clusterconf/
  demo.sh            up.sh && interact.sh   (one-shot)
```

`demo/state/` and `demo/clusterconf/` are gitignored (the latter holds **private keys**).

### interact.sh — the narrative
Cloud client APIs bind to `127.0.0.1:8080` on each VM, so we drive them via `ssh <vm> curl
localhost:8080/...`; the laptop API is local.
1. **Cross-cloud write/read**: `PUT /kv/greeting "hello from AWS"` on the AWS node → `GET`
   it on the **laptop** (holds zero replicas) and on the **GCP** node → same value.
2. **CAS across clouds**: `POST /cas/greeting?expect=...` on GCP, observe on Oracle.
3. **Fenced lock**: `POST /session/payments` + `POST /lock/widget?session=payments` on GCP
   → `{"token":N}`; read the holder from the laptop and AWS.
4. **(Act 2) Failover + fencing monotonicity**: stop cask on the lock-holder's cloud node
   (one replica; 2/3 majority survives). The reaper cascade-frees the dead session's locks;
   re-acquire from another node → strictly higher token `N+1`. Print the tokens side by side.
   (Don't kill a 2nd replica — `store/mem` is in-memory; 2 of 3 retain the replicated state.)

---

## Key files
- Modify: [internal/transport/nebula/certgen.go](internal/transport/nebula/certgen.go), [cmd/cask/gencerts.go](cmd/cask/gencerts.go), [cmd/cask/main.go](cmd/cask/main.go), [cmd/cask/cluster.go](cmd/cask/cluster.go)
- Reuse as-is: `roster.Get/Genesis` ([internal/roster/roster.go](internal/roster/roster.go)), `agent.NewRouter` ([internal/agent/router.go](internal/agent/router.go)), `placement.TargetReplicas`, `nebula.SelfMember/GenesisMembers/SeedMembers` ([internal/transport/nebula/discovery.go](internal/transport/nebula/discovery.go))
- New: everything under `demo/`, plus `.gitignore` entries for `demo/state/` and `demo/clusterconf/`

## Verification
1. **Unit/build**: `go build ./... && go test ./...`; `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ./cmd/cask` and `GOARCH=arm64` both succeed.
2. **Local end-to-end (no clouds)**: prove the client-only path before spending on VMs — `cask gen-certs -spec` a 4-node loopback spec (1 lighthouse + 3 members, one member `lighthouse:false` started with `--client-only` on distinct ports), then `PUT` on a replica and `GET` on the client-only node returns the value; confirm the client never appears in the roster (log line / `placeRange` excludes it).
3. **Full demo**: `demo/up.sh` then `demo/interact.sh` → cross-cloud read-after-write, fenced lock visible from the laptop, and token `N → N+1` across the simulated failover. Tear down with `demo/down.sh` and confirm all three cloud instances + firewall/SG/VCN artifacts are gone (re-run `*-down.sh` is idempotent).

> Phase 1 above (multi-cloud demo + the consensus fixes) is **implemented and verified live**. Phase 2 below is the next, larger piece.

---

# Phase 2 — Zero-conf registry control plane + private mesh + kind replica

## Context

Phase 1 baked each lighthouse's public IP into every node's config (`gen-certs -spec`) — brittle, and it requires nodes to be publicly reachable. The goal now is the **production topology**: a **private data plane with zero public inbound**, coordinated by a small **public control/relay plane**, and **zero-conf** nodes that ship with only a registration hostname + token (the Tailscale/Nebula-DERP model, and the natural fit for separate Kubernetes clusters). New demo shape: keep the 3 clouds (now private, full replicas), add a **kind-cluster pod as a 4th full replica**, keep the **laptop as a client**, and front everything with a **coordinator** (registry + lighthouse + relay) on one public VM with an `sslip.io` name.

**Central trade-off (accept for the demo, note for prod):** with no inbound on any data node, *all* consensus traffic relays through the one coordinator, so it's a data-plane SPOF/bottleneck. Production fix = ≥2 relays (`relay.relays` lists several) and a durable registry. Out of scope here; documented.

## Target architecture

- **Coordinator** (one public VM — the only reachable thing): `cask coordinator` runs (a) a **registry HTTP API** (persistent CA, mints per-node certs, serves host map + roster seeds; TLS via `autocert` on `reg.<ip>.sslip.io`, plain-HTTP+token fallback) and (b) an embedded **Nebula lighthouse + relay** (`am_lighthouse:true, am_relay:true`, overlay `10.42.0.1`, holds no consensus data).
- **Data plane — private, outbound-only:** 3 cloud replicas + 1 kind-pod replica + 1 laptop client. Firewalls closed to inbound overlay (egress + operator SSH only); every node `use_relays:true, relays:[10.42.0.1]`, so they reach each other only via the relay.
- **Zero-conf join:** `cask --registry https://reg.<ip>.sslip.io --token <T> --role replica|client --name X --zone Z` → `POST /register` → `{ca, cert, key, overlay_ip, lighthouses[], relays[], members[]}` → build config in memory → existing `nebulaCluster` path.

## Phases (each independently verifiable)

### A — certgen refactor ([internal/transport/nebula/certgen.go](internal/transport/nebula/certgen.go))
Split `GenerateConfigs` into reusable primitives; **keep `GenerateConfigs`'s signature** (loopback demo/`gen-certs` unaffected, re-implemented on top):
- `CreateCA(name, ttl) (*CA, error)`, `(*CA).MarshalCA()`/`LoadCA(certPEM,keyPEM)` — persist+reload the CA (uses `cert.MarshalSigningPrivateKeyToPEM` / `UnmarshalSigningPrivateKeyFromPEM` / `UnmarshalCertificateFromPEM`, confirmed in nebula v1.10.3).
- `(*CA).MintNodeCert(spec, ttl) (certPEM,keyPEM,err)` — the per-node body, `tbs.Sign(ca.Cert, Curve25519, ca.key)`.
- `BuildNebulaConfig(spec, caPEM,certPEM,keyPEM, lhHosts, lhStatic) (string,error)` — the conf map, now also emitting **relay keys**: `relay.am_relay` (coordinator) or `relay.use_relays:true`+`relay.relays:[...]` (data nodes). Extend `NodeSpec` with `AmRelay bool` + `Relays []netip.Addr`.

### B — registry service (new `internal/registry/` + `cmd/cask/coordinator.go`)
- `store.go`: mutex-guarded in-memory node store; **overlay-IP pool allocator that is idempotent on `name`** (a restart/re-roll rejoins as the *same* overlay IP → same NodeID — load-bearing, since `nodeIDFromIP` derives identity from the IP). `Members()` returns **replicas only** (clients + coordinator excluded → never in roster/placement). `LoadOrCreateCA(dir)` persists the CA under `/opt/cask/registry`.
- `api.go`: `RegisterReq{token,role,name,zone}` / `RegisterResp{ca,cert,key,overlay_ip,lighthouses[],relays[],members[]}` / `MembersResp`.
- `server.go`: `POST /register` (constant-time token check → `Allocate` → `MintNodeCert` → respond) and `GET /members`; mirror `transport`'s `decode`/`writeJSON` JSON helpers.
- `cmd/cask/coordinator.go`: `cask coordinator` subcommand (dispatch beside `gen-certs` in [main.go](cmd/cask/main.go)). Flags `--token --listen --public-addr <ip>:4242 --state --overlay-prefix --overlay-port --genesis-replicas gcp,oci,aws --tls-host`. Self-mints the `10.42.0.1` lighthouse+relay cert, `nebula.New(cfg)` (runs lighthouse/relay, never `Listen`s — holds no acceptor), and `http.Serve` the registry on the host TCP stack (public).

### C — registry client bootstrap ([cmd/cask/main.go](cmd/cask/main.go) + new `cmd/cask/registry_client.go`)
- Flags `--registry --token --role --name --zone`; a branch parallel to the `--nebula-config` branch.
- `registerNode(...)` POSTs `/register`, then builds the config with the **same `nebula.BuildNebulaConfig`** from the response; `clientOnly = role=="client"`.
- `registryDiscovery` implements `roster.Discovery` by `GET /members` — the seam that lets the kind pod (joining late) be picked up by running replicas' reconcile.

### D — genesis/roster with a registry ([cmd/cask/cluster.go](cmd/cask/cluster.go))
- Decouple genesis from `lighthouse.hosts` (the lone lighthouse is the coordinator, which must **not** be a roster member). Add `nebulaClusterWith(..., disco roster.Discovery, genesis []roster.Member)`; `nebulaCluster` delegates with `SeedDiscovery(seeds)` + `GenesisMembers(...)` (legacy path unchanged). Registry path passes `resp.Members` as genesis and `registryDiscovery` as `disco`.
- Determinism: coordinator's `--genesis-replicas` returns a fixed replica seed set to the cloud trio (idempotent name→octet pinning makes `.4`=AWS the stable `highestID` seeder); the existing single-seeder + `retryBootstrap` handle the rest. The kind pod is never in genesis — it joins via `rost.Add` through discovery.
- Coordinator stays out of placement automatically (`Members()` excludes it); it serves only as the Nebula relay (`relay.relays`), which cask never consults for placement. The `overlayDialer` dials `http://<overlayIP>:<caskPort>` so relaying is invisible to cask.

### E — demo wiring (`demo/`)
- `demo/clouds/coordinator-up.sh` (new): one public VM; open **UDP 4242 + TCP 443 + TCP 22**; start `cask coordinator`; record `reg.<ip>.sslip.io` + token to `demo/state/`.
- `demo/clouds/{aws,gcp,oci}-up.sh`: **remove the inbound UDP 4242 ingress** (egress + SSH only) — the "no public inbound" enforcement (pragmatic; note prod = private subnet + NAT gateway).
- `demo/deploy.sh`: data-node `ExecStart` becomes `cask --registry … --role replica --name <cloud> --zone <cloud>` (no per-node YAML pushed); laptop → `--role client --name laptop`. `gen-spec`/`gen-certs -spec` dropped from the registry path (kept for legacy `--nebula-config`).
- `demo/kind/` (new): `Dockerfile` (distroless + `cask-linux-arm64`), `deployment.yaml` (`cask --registry … --role replica --name kind-pod`, token from a `Secret`, no Service), `kind-up.sh`/`kind-down.sh`.
- `demo/up.sh`/`down.sh`: bring the **coordinator up first**, then the private data nodes, deploy, then `kind-up.sh`; tear down in reverse.

## OPEN QUESTION (resolve first in the new chat): relays vs. direct consensus
"Why does consensus go via relays?" — it doesn't *have* to. Nebula with `use_relays:true`
tries a **direct** tunnel first and only relays when direct fails. Consensus is
bidirectional (a proposer dials *into* each replica for prepare/accept), so the relay
dependence is purely a consequence of **how private the nodes are**:
- If data nodes accept **no inbound at all** (strict reading of "not reachable from
  outside"), they can't even hole-punch to each other → *all* mutual consensus relays
  through the coordinator → the SPOF below.
- If data nodes can form **direct** tunnels (hole-punching between cloud egress NATs, or
  one tier allows inbound), consensus among them is **direct**; only the truly
  unreachable node (the double-NAT kind pod) relays. No core SPOF.
**Decision needed:** strict zero-inbound everywhere (simple, but coordinator = data-plane
SPOF) vs. allow direct where possible + relays only as fallback (more resilient, closer to
Nebula's default). This choice changes Phase E firewalls and the SPOF risk below.

## Key risks (and demo stance)
- **Coordinator SPOF/throughput** (all traffic relays through it) — accept for demo, size it up a tier; prod = ≥2 relays.
- **Genesis convergence on async registration** — solved by `--genesis-replicas` pinning + idempotent name→IP allocation; reconcile is the safety net.
- **Registry confidentiality** — `autocert` TLS on the sslip.io name; token-gated; plain-HTTP fallback documented as demo-only.
- **kind double-NAT** — handled by relays (the reason the kind pod is the showcase for the relay path).

## Verification
1. **Build/test**: `go build ./... && go test ./...` (certgen refactor must keep loopback/`gen-certs` output byte-stable — diff before/after).
2. **Local loopback (no clouds)**: run `cask coordinator` on `127.0.0.1`, then a couple of `cask --registry http://127.0.0.1:… --role replica` on distinct ports + one `--role client`; confirm register → join → cross-node read/write, client holds no replica, and a late `--role replica` is picked up via `/members`. Proves A–D before spending.
3. **Full mixed demo**: `demo/up.sh` → coordinator + 3 private cloud replicas + kind pod replica + laptop client; `demo/interact.sh` shows write-on-one-cloud → read-on-kind-pod and on laptop, fenced lock across all, and (stretch) a replica drop. Confirm **no data node has any open inbound overlay port** (all traffic via the relay). `demo/down.sh` removes everything incl. the kind cluster and coordinator.
