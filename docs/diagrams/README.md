# cask multi-cloud demo — diagrams

Visual companions to [`demo/`](../../demo/). The hero diagram is a standalone SVG;
the rest are Mermaid (they render inline on GitHub and stay easy to edit).

## 1. Architecture

One cask cluster spanning GCP + Oracle + AWS as the RF=3 replica set, with a NAT'd
laptop joining as a client-only node — all over one encrypted Nebula overlay.

![architecture](architecture.svg)

A Mermaid view of the same topology:

```mermaid
flowchart TB
  classDef gcp   fill:#e8f0fe,stroke:#4285F4,color:#16222e
  classDef oci   fill:#fbecea,stroke:#C74634,color:#16222e
  classDef aws   fill:#fff4e0,stroke:#FF9900,color:#16222e
  classDef lap   fill:#eef1f4,stroke:#5A6B7B,color:#16222e
  classDef net   fill:#0e7c86,stroke:#0e7c86,color:#ffffff

  subgraph clouds[" RF = 3 replica set — the data lives here "]
    direction LR
    G["☁️ GCP · us-central1<br/>e2-micro · x86<br/><b>lighthouse + replica</b><br/>10.42.0.1:8001"]:::gcp
    O["☁️ Oracle · us-ashburn-1<br/>E2.1.Micro · arm64<br/><b>lighthouse + replica</b><br/>10.42.0.2:8001"]:::oci
    A["☁️ AWS · us-east-1<br/>t4g.small · arm64<br/><b>lighthouse + replica</b><br/>10.42.0.3:8001"]:::aws
  end

  NET["🔒 Nebula encrypted overlay · 10.42.0.0/24<br/>one public UDP port :4242 · userspace · no TUN/root"]:::net
  L["💻 Laptop behind NAT<br/><b>client-only · 0 replicas</b><br/>10.42.0.100 · outbound-only"]:::lap

  G <--> NET
  O <--> NET
  A <--> NET
  L <-->|routes ops to replicas| NET
```

## 2. Read-after-write across clouds

Write on the AWS node; read it back on the laptop (which stores nothing) and on GCP.
Consensus commits on a majority and rides the overlay; the client API is local to each node.

```mermaid
sequenceDiagram
  autonumber
  participant C as curl @ AWS node
  participant A as AWS replica<br/>10.42.0.3
  participant G as GCP replica<br/>10.42.0.1
  participant O as Oracle replica<br/>10.42.0.2
  participant L as Laptop<br/>(client-only)

  C->>A: PUT /kv/greeting "hello"  (127.0.0.1:8080)
  par CASPaxos over the overlay
    A->>G: prepare / accept
    A->>O: prepare / accept
  end
  Note over A,O: committed on majority (2 of 3)
  A-->>C: 204 No Content

  L->>G: GET /kv/greeting  (routed to a replica)
  G-->>L: "hello"
  Note over L: the laptop holds no data,<br/>yet reads globally-coordinated state
```

## 3. Self-forming bootstrap (the genesis fix)

Every node derives the **same** genesis set from its config, so only one node writes
it — the highest node id, whose ballots dominate. Everyone else adopts the known
value locally instead of issuing write-imposing reads that would preempt the seeder
(the dueling-proposer livelock that took the real multi-lighthouse deploy down).

```mermaid
sequenceDiagram
  autonumber
  participant A as AWS (highest id)<br/><b>sole seeder</b>
  participant G as GCP
  participant O as Oracle
  Note over A,O: each computes genesis = {gcp, oci, aws} from its own config

  A->>A: rost.Genesis(...) — the only writer (jittered-backoff retry)
  A->>G: replicate roster register
  A->>O: replicate roster register
  Note over G,O: adopt the deterministic genesis value locally<br/>(no register read → no preemption of the seeder)
  Note over A,O: roster = 3 members · RF = 3 · range placed
```

## 4. Fault tolerance — a replica fails, the cluster keeps serving

Stop Oracle's node: its overlay tunnel dies, so consensus RPCs to it time out fast
(bounded, ~4s) and count as non-votes. GCP + AWS are still a majority, so writes
commit and the laptop reads the new value.

```mermaid
flowchart LR
  classDef ok   fill:#e7f6ec,stroke:#2e9e5b,color:#16222e
  classDef bad  fill:#fdeaea,stroke:#d64545,color:#16222e
  classDef step fill:#eef1f4,stroke:#5A6B7B,color:#16222e

  W["PUT /kv/failover<br/>via GCP"]:::step --> Q{"quorum reached?"}
  G["GCP ✓"]:::ok --> Q
  A["AWS ✓"]:::ok --> Q
  O["Oracle ✗ down<br/>RPC times out → non-vote"]:::bad -.-> Q
  Q -->|"2 of 3"| Cm["commit on majority"]:::ok
  Cm --> R["laptop GET → 'written with Oracle down'"]:::step
  R --> Re["Oracle restarts → rejoins & catches up"]:::step
```

## 5. Deploy pipeline (`demo/up.sh`)

How `up.sh` goes from three empty accounts to a running, interacting cluster.

```mermaid
flowchart LR
  classDef s fill:#eef1f4,stroke:#5A6B7B,color:#16222e
  classDef c fill:#e8f0fe,stroke:#4285F4,color:#16222e

  PF["preflight.sh<br/>tools · creds · live auth"]:::s --> UP["up.sh"]:::s
  UP --> PR["provision 3 clouds<br/>(parallel): VM + firewall + ssh key"]:::c
  PR --> B["build.sh<br/>host · linux/amd64 · linux/arm64"]:::s
  B --> SP["gen-spec.sh<br/>state → nodes.json"]:::s
  SP --> CG["cask gen-certs -spec<br/>CA + per-node Nebula configs"]:::s
  CG --> DP["deploy.sh<br/>scp arch-matched binary → systemd"]:::c
  DP --> LP["start laptop<br/>cask --client-only"]:::s
  LP --> IN["interact.sh<br/>cross-cloud read/write · lock · failover"]:::s
```
