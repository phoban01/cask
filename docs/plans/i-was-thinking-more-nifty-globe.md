# Phase 2 (revised): dynamic membership — stateless mint, DNS discovery, reflexive roster reconfiguration

## Context

The original Phase 2 ([warm-plotting-biscuit.md](warm-plotting-biscuit.md), lines 145–215) proposed a
stateful HTTP **registry** + Nebula **relay** coordinator, with every data node relaying consensus
through it (a permanent SPOF), and kept the Phase-1 **static genesis** member set baked into config.

Three decisions reshape it:
- **No relays.** Nebula's lighthouse already coordinates UDP **hole-punching** for *direct* peer
  tunnels; relays are only a symmetric-NAT fallback. Teams ensure the network path between nodes.
- **No stateful registry.** Replaced by a **stateless `cask mint`** endpoint that issues a random
  identity + signed cert against a persistent CA. Discovery moves to **DNS** (re-resolving lighthouse
  hostname + a DNS-SRV `roster.Discovery` adapter).
- **No static genesis.** The roster register becomes **reflexive**: it stores the membership, and a
  bounded *core* of that membership is the register's own CASPaxos acceptor set. The cluster
  bootstraps from a **single founder** and grows/shrinks the core via **joint-consensus
  reconfiguration**, reusing `internal/reconfig` (TLA-checked in `tla/Reconfig.tla`).

The hard constraint that shapes the bootstrap: CASPaxos safety requires every proposer to a register
to use quorums from the *same* acceptor set, and there is no coordination-free way to establish the
*first* register without risking split-brain. So founding is gated on one explicit act (a one-shot
`--bootstrap` flag); everything after is dynamic.

Outcome: a node ships with only **a mint URL + token + zone** (or a pre-minted cert) and a discovery
name — no baked IPs, no member list, no relays, no central control plane.

Sources: [Nebula lighthouse discovery](https://deepwiki.com/slackhq/nebula/3.5-lighthouse-discovery),
[Nebula relays](https://www.defined.net/blog/announcing-relay-support-in-nebula/),
[Nebula lighthouse DNS](https://nebula.defined.net/docs/guides/using-lighthouse-dns/) (A/TXT only — no SRV),
[Kubernetes DNS](https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/).

## Architecture

- **Lighthouse** (one public VM, hostname-addressed): Nebula `am_lighthouse:true`, **no relay**.
  Co-hosts `cask mint`. **No consensus role** (genesis is gone — see below).
- **`cask mint`** (stateless): persistent CA; `POST /mint` → random overlay-IP identity + cert.
- **Roster register**: founded by one node, replicated on a dynamic *core* (size `registerRF`, e.g. 3)
  that tracks membership via joint reconfiguration. Full membership drives HRW data placement; the
  core is a bounded subset that physically stores the register.
- **Discovery**: DNS-SRV (operator/K8s-published, resolves to overlay IPs) ∪ static seeds, feeding the
  reconcile ADD path and acting as the recovery anchor for a node with a stale core view.

---

## Work item 1 — drop relays + DNS-name lighthouse addressing ([internal/transport/nebula/certgen.go](internal/transport/nebula/certgen.go))

Split `GenerateConfigs` into reusable primitives (the minter needs runtime signing), **no relay keys**.
Keep `GenerateConfigs`'s signature (loopback/`gen-certs` re-implemented on top, byte-stable output):
- `CreateCA(name,ttl)`, `(*CA).MarshalCA()`/`LoadCA(certPEM,keyPEM)`, `(*CA).MintNodeCert(spec,ttl)`,
  `BuildNebulaConfig(spec, ca,cert,key, lhHosts, lhStatic)`.
- `Advertise` already flows verbatim into `static_host_map` ([certgen.go:69-75](internal/transport/nebula/certgen.go#L69-L75))
  and Nebula re-resolves hostnames there — DNS lighthouse addressing needs no new plumbing. Add a
  `static_map` block (`network:"ip"`, `lookup_timeout:"1s"`) when an Advertise is a hostname.

## Work item 2 — stateless `cask mint` + enrollment client ([cmd/cask/mint.go](cmd/cask/mint.go), [cmd/cask/mint_client.go](cmd/cask/mint_client.go))

- `cask mint` subcommand: `LoadOrCreateCA(--ca-dir)` (the only persisted state — a signing key).
  `POST /mint {token,zone,role}` → constant-time token check → **random** overlay IP in a large
  `--overlay-prefix` (IPv6 /64 or a generous IPv4 prefix — a /24 birthday-collides at ~19 nodes) →
  hostname `cask-<hex>` → `MintNodeCert` → `{ca,cert,key,overlay_ip,lighthouse_hosts[],static_host_map,
  discovery_srv}`. Pure function of input + CA key; no node store, no IP-pool allocator. `autocert`
  TLS on `--tls-host`, plain-HTTP+token fallback documented demo-only.
  - Re-mint = **new** identity (no idempotent name→IP). Nodes **persist their cert to disk** and reuse
    it across restarts; a re-minted node rejoins as new and the stale one ages out via the failure
    detector. Document the trade-off.
- Node-side: `--mint <url> --token --zone --role replica|client` (parallel to `--nebula-config`).
  `enroll()` POSTs, builds config in memory via the same `BuildNebulaConfig`, persists the cert,
  `clientOnly = role=="client"`, falls into the cluster path.

## Work item 3 — DNS-SRV discovery adapter ([internal/discovery/srv.go](internal/discovery/srv.go))

New package (`net` + `roster` only). `SRVDiscovery{Service,Proto,Domain,Resolver}` implements
`roster.Discovery`: `LookupSRV` → per target trim trailing `.`, `LookupNetIP(ctx,"ip",target)`,
**`.Unmap()`** each result (load-bearing: a 4-in-6 address must collapse to canonical v4 or
`NodeIDFromIP` takes the IPv6 fold path and computes a *different id*), build
`Member{NodeID:nebula.NodeIDFromIP(ip), Addr:ip:srvPort, Zone:""}`. SRV target A records resolve to
**overlay IPs**; port = caskPort. Unresolvable targets skipped; `LookupSRV` error propagates so the
loop's `if err != nil { continue }` ([cluster.go:253](cmd/cask/cluster.go#L253)) self-heals. **Export
`nebula.NodeIDFromIP`** ([discovery.go:144](internal/transport/nebula/discovery.go#L144)) — identity
is a single-sourced cluster contract. Wired as `unionDiscovery{SeedDiscovery(seeds), SRVDiscovery{}}`
(dedup by NodeID, seeds first) into the reconcile loop. Empty Zone is the established pattern for
discovered peers; the node's real zone overwrites it on self-join (last-write-wins, [roster.go:148](internal/roster/roster.go#L148)).

## Work item 4 — reflexive roster reconfiguration (the core feature)

The roster register at `\x00roster` becomes self-describing about its own acceptor set, and that set
is changed by running joint consensus **on the roster key itself**. `internal/reconfig` and
`caspaxos.NewJointProposer` are reused **unchanged** — the safety-critical machinery is already
TLA-checked; only orchestration is new.

**a. Register value** ([internal/roster/roster.go](internal/roster/roster.go)). Extend `Value`:
```go
type Value struct {
    Epoch     uint64   // bumped on Members change (data-placement input — unchanged role)
    Members   []Member // FULL membership (HRW placement over data ranges)
    Core      []uint64 // the register's acceptor set (bounded subset, sorted ids)
    Joint     *Joint   // {Old,New} — non-nil only mid-reconfiguration
    ConfigGen uint64   // bumped ONLY when Core/Joint changes
}
```
`Core ⊆ Members`, `len(Core) = min(registerRF, len(Members))` (`registerRF`=3, odd). `desiredCore` =
the `registerRF` highest-NodeID members (aligns with CASPaxos's id tiebreak, [ballot tiebreak](internal/caspaxos)),
deterministic so every node computes the same target. `CASPaxos never interprets values` — `Core`/`Joint`
are opaque bytes to the engine; meaning lives in the pure change functions.

**b. Founding — single founder, race-safe.** One operator-chosen node runs `cask serve --bootstrap`
→ `Roster.Founder()` writes `{Epoch:1, Members:[self], Core:[self], ConfigGen:1}` against the singleton
`{self}` quorum (trivially available, no ballot-dueling). The flag is a **one-shot creation gate**,
not an ongoing role. Every other node (and a restarted founder *without* the flag) **joins**: discover
a seed → read the register → `Add(self)`. A non-founder that finds no register **retries discovery,
never founds** — this asymmetry is the entire split-brain defense. Deletes the static-genesis
`highestID`-seeds/others-adopt dance ([cluster.go:94-147](cmd/cask/cluster.go#L94-L147)) and
`GenesisMembers`'s consensus role.

**c. Reconfiguration orchestration** (new [internal/roster/reconfig.go](internal/roster/reconfig.go),
`Roster.Reconfigure(ctx, target)`), the reconfig 3-step ([reconfig.go:9-15](internal/reconfig/reconfig.go#L9-L15))
on the roster key:
1. **Publish JOINT** against `old=Core`: set `Joint={Old:Core,New:target}`, bump `ConfigGen`.
2. **CarryForward** the roster key against the joint quorum `{old,new}` — literally
   `reconfig.CarryForward(ctx, self, roster.Key, oldClients, newClients)` — installs the value
   (incl. the `Joint` marker) into the new acceptors. Idempotent.
3. **Publish new-only** against the joint quorum: `Core=Joint.New`, `Joint=nil`, bump `ConfigGen`.
   (Must still use a joint quorum to safely supersede any writer still on joint rules.)
The **register value is the journal**: a crash mid-reconfig is resumed by any node on its next tick by
reading `Joint` and re-entering the matching step. `Add`/`Remove` become joint-aware (propose against
`{Core}` or `{Joint.Old,Joint.New}` per the current value); `Remove` also strips the id from
`Core`/`Joint.New` so we never finalize onto a removed node.

**d. Stale-config reads** (the chicken-and-egg: you must target current `Core` but learn it *by*
reading). Each node caches `believedCore`/`believedConfigGen`. Read order: (a) `believedCore`;
(b) on a seen `Joint`, the **union `old∪new`** via a joint proposer (the joint quorum overlaps every
old and new quorum — the `Reconfig.tla` lemma — so the union always sees the latest committed value);
(c) a fully-stale node whose believed set was reconfigured away can't reach a quorum and **recovers via
discovery** (DNS-SRV ∪ seeds anchor it to a live core member, then it adopts the current `Core`). This
is why §c keeps departed core nodes in `Members` and why discovery is unioned with seeds.

**e. Reconcile-loop / dialer integration** ([cmd/cask/cluster.go](cmd/cask/cluster.go),
[internal/roster/controller.go](internal/roster/controller.go)). `Controller.Reconcile` keeps its
ADD-on-discovered / REMOVE-on-`down`-only semantics ([controller.go:34-61](internal/roster/controller.go#L34-L61)),
then gains a tail step: after reconciling `Members`, compute `desiredCore(Members)` and call
`Reconfigure` only when the core is under-filled (grow 1→3→5) or a **condemned member is in the core**
(replace it — else the register loses an acceptor). Removal is processed before recomputing
`desiredCore`, so a condemned node is never re-selected. The one-time fixed roster proposer
([cluster.go:112](cmd/cask/cluster.go#L112)) becomes a **proposer factory** closure
`func(groups [][]uint64) Proposer` that resolves ids via the existing `dialer` at call time, so every
reconfig automatically targets the current core; the loop `dialer.learn`s new core members when
`ConfigGen` advances. **Data placement is untouched** — `placeRange` reads `val.Members`, never `Core`
([cluster.go:284-295](cmd/cask/cluster.go#L284-L295)).

## Work item 5 — demo rewiring (`demo/`)

- Replace `coordinator-up.sh` with a **lighthouse + mint VM** (open UDP 4242 + TCP 443/22), hostname
  recorded to `demo/state/`. No relay, no registry process (only the stateless minter + a CA file).
- Data nodes: `cask serve --mint https://<host>/ --token <T> --role replica --zone <cloud>
  --discovery-srv _cask._tcp.<domain>`; **one** node also gets `--bootstrap`. Laptop → `--role client`.
- Operator publishes `_cask._tcp.<domain>` SRV+A (overlay IPs) for stable members; the K8s pod replica
  via a **headless Service**. Kind pod gets a **real network path** (host networking / NodePort) per
  the no-relay decision.
- `demo/up.sh`: lighthouse+mint → `--bootstrap` node → other replicas → kind pod; teardown reverse.

---

## Key files

- **New:** [internal/roster/reconfig.go](internal/roster/reconfig.go) (`Reconfigure`, `desiredCore`), [internal/discovery/srv.go](internal/discovery/srv.go), [cmd/cask/mint.go](cmd/cask/mint.go), [cmd/cask/mint_client.go](cmd/cask/mint_client.go), `demo/` lighthouse+mint/kind scripts.
- **Modify:** [internal/roster/roster.go](internal/roster/roster.go) (extend `Value`; `Proposer` field → proposer-factory; `Founder`; joint-aware `Add`/`Remove`; stale-read follow), [internal/roster/controller.go](internal/roster/controller.go) (core-reconfig tail step), [cmd/cask/cluster.go](cmd/cask/cluster.go) (delete static genesis, dynamic proposer factory, `--bootstrap` gate, founder-or-join), [internal/transport/nebula/certgen.go](internal/transport/nebula/certgen.go) (primitive split, `static_map`, no relays), [internal/transport/nebula/discovery.go](internal/transport/nebula/discovery.go) (export `NodeIDFromIP`), [cmd/cask/main.go](cmd/cask/main.go) (`mint` dispatch + flags).
- **Reuse UNCHANGED:** [internal/reconfig/reconfig.go](internal/reconfig/reconfig.go) (`CarryForward`, `JointProposer`), `caspaxos.NewProposer`/`NewJointProposer`, `roster.Get` read shape, the client-only path, `cask gen-certs -spec` (legacy/offline).

## Verification

1. **Build/test:** `go build ./... && go test ./...`; cross-compile amd64+arm64; `gen-certs`/loopback
   output byte-stable after the certgen split (diff).
2. **Unit — discovery & mint:** SRV parser (fake resolver): trailing-dot trim, unresolvable target
   skipped, `NodeID==nebula.NodeIDFromIP(...)`, `::ffff:10.42.0.7` `.Unmap()`s to the same id as v4,
   empty Zone, `LookupSRV` error propagates. Mint: CA round-trip, bad token→401, good token→cert
   verifies against returned CA with a random in-prefix IP, two mints→two identities.
3. **Sim — reflexive reconfiguration** (new `internal/roster/reconfig_test.go`, modeled on
   [reconfig_test.go](internal/reconfig/reconfig_test.go) + `testutil/sim`): (a) founder bootstraps a
   singleton register; (b) grow 1→3→5, asserting the prior value survives each transition read against
   the *new* core only; (c) a core member fails (`nw.SetReachable(c,false)`) and is replaced, value
   intact; (d) concurrent `Add` between Step 1 and Step 3 survives (issued through the joint proposer);
   (e) a stale-core node fails its stale read then follows discovery to the current core.
4. **TLA:** the existing config-agnostic `tla/Reconfig.tla` already models this transition (instantiate
   `Cold/Cnew` = old/new core, single key = `\x00roster`); optionally extend it for concurrent
   membership writes during the joint phase and resume-after-crash. Note the reflexive reuse in
   `tla/README.md`.
5. **Local loopback e2e (no clouds):** `cask mint` on 127.0.0.1; one `--bootstrap` replica founds;
   add replicas + a `--role client`; assert core grows to `registerRF`, cross-node read-after-write,
   client holds no replica; kill a core member and confirm reconfig replaces it and the cluster keeps
   serving.
6. **Full demo:** `demo/up.sh` → lighthouse+mint + `--bootstrap` cloud replica + more replicas + a K8s
   pod replica + laptop client; `demo/interact.sh` shows write→read across clouds/pod/laptop, fenced
   lock, token `N→N+1` across a core-member failover. Confirm **no relay config, no registry process,
   no baked member list** anywhere. `demo/down.sh` removes everything incl. the kind cluster.
