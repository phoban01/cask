#!/usr/bin/env bash
# run.sh — the head-to-head throughput/latency harness behind the paper's
# "vs etcd, same hardware" row. Stands up a real 3-node cask cluster (cmd/cask
# processes, Pebble on tmpfs-or-disk) AND a real 3-node etcd cluster (Docker),
# runs the SAME workloads through cmd/cask-bench against each, and prints a
# side-by-side table.
#
# Both systems run as real clustered processes on this host, so both pay real
# client RPC, real inter-node consensus RPC, and real fsync. See bench/README.md
# for the fairness rationale (fresh keys for writes, linearizable reads, etc.).
#
# Requirements: go, docker. Usage:
#   bench/run.sh                       # default workloads + params
#   WORKLOADS="put get" CLIENTS=128 DURATION=15s bench/run.sh
set -euo pipefail
cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"

WORKLOADS="${WORKLOADS:-put get cas lock}"
CLIENTS="${CLIENTS:-64}"
DURATION="${DURATION:-8s}"
WARMUP="${WARMUP:-2s}"
KEYS="${KEYS:-10000}"
VALUE_SIZE="${VALUE_SIZE:-256}"
ETCD_IMG="${ETCD_IMG:-quay.io/coreos/etcd:v3.5.16}"

WORK="$(mktemp -d)"
CASK_PIDS=()
RESULTS="$WORK/results.jsonl"
: >"$RESULTS"

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

cleanup() {
  step "Teardown"
  for pid in "${CASK_PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  docker rm -f caskbench-etcd1 caskbench-etcd2 caskbench-etcd3 >/dev/null 2>&1 || true
  docker network rm caskbench-net >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

step "Building cask + cask-bench"
go build -o "$WORK/cask" ./cmd/cask
go build -o "$WORK/cask-bench" ./cmd/cask-bench

# ---- cask: 3 real processes on localhost, durable Pebble ----
CASK_PEERS="127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003"
CASK_EPS="http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003"
step "Starting 3-node cask cluster ($CASK_PEERS)"
i=1
for port in 8001 8002 8003; do
  mkdir -p "$WORK/cask$i"
  "$WORK/cask" --id "$i" --listen "127.0.0.1:$port" --peers "$CASK_PEERS" --insecure-consensus \
    --data-dir "$WORK/cask$i" >"$WORK/cask$i.log" 2>&1 &
  CASK_PIDS+=($!)
  i=$((i + 1))
done
# Readiness: a committed write needs a quorum of acceptors listening.
for _ in $(seq 1 50); do
  if curl -fsS -X PUT "http://127.0.0.1:8001/kv/__probe__" -d ok >/dev/null 2>&1; then
    echo "   cask ready"; break
  fi
  sleep 0.3
done
curl -fsS -X PUT "http://127.0.0.1:8001/kv/__probe__" -d ok >/dev/null || {
  echo "FAIL: cask cluster never became ready"; tail -n 20 "$WORK/cask1.log"; exit 1; }

# ---- etcd: 3 real containers on a shared docker network ----
step "Starting 3-node etcd cluster ($ETCD_IMG)"
docker network create caskbench-net >/dev/null 2>&1 || true
ETCD_CLUSTER="caskbench-etcd1=http://caskbench-etcd1:2380,caskbench-etcd2=http://caskbench-etcd2:2380,caskbench-etcd3=http://caskbench-etcd3:2380"
hostport=12379
n=1
for name in caskbench-etcd1 caskbench-etcd2 caskbench-etcd3; do
  docker run -d --name "$name" --network caskbench-net -p "$hostport:2379" \
    "$ETCD_IMG" etcd \
    --name "$name" \
    --data-dir /etcd-data \
    --listen-peer-urls http://0.0.0.0:2380 \
    --listen-client-urls http://0.0.0.0:2379 \
    --initial-advertise-peer-urls "http://$name:2380" \
    --advertise-client-urls "http://$name:2379" \
    --initial-cluster "$ETCD_CLUSTER" \
    --initial-cluster-state new \
    --initial-cluster-token caskbench >/dev/null
  hostport=$((hostport + 10000))
  n=$((n + 1))
done
ETCD_EPS="127.0.0.1:12379,127.0.0.1:22379,127.0.0.1:32379"
for _ in $(seq 1 50); do
  if docker exec caskbench-etcd1 etcdctl --endpoints=http://127.0.0.1:2379 endpoint health >/dev/null 2>&1; then
    echo "   etcd ready"; break
  fi
  sleep 0.3
done
docker exec caskbench-etcd1 etcdctl --endpoints=http://127.0.0.1:2379 endpoint health >/dev/null 2>&1 || {
  echo "FAIL: etcd cluster never became healthy"; docker logs caskbench-etcd1 2>&1 | tail -n 20; exit 1; }

# ---- run every workload against both targets ----
bench() { # target endpoints workload
  "$WORK/cask-bench" --target "$1" --endpoints "$2" --workload "$3" \
    --clients "$CLIENTS" --duration "$DURATION" --warmup "$WARMUP" \
    --keys "$KEYS" --value-size "$VALUE_SIZE" --json 2>>"$WORK/$1-$3.log" \
    | tee -a "$WORK/console.log" | grep '^RESULT ' | sed 's/^RESULT //' >>"$RESULTS" || true
}

for wl in $WORKLOADS; do
  step "workload: $wl"
  bench cask "$CASK_EPS" "$wl"
  bench etcd "$ETCD_EPS" "$wl"
done

# ---- comparison table ----
step "Results (clients=$CLIENTS, duration=$DURATION, value=${VALUE_SIZE}B)"
"$WORK/cask-bench" table "$RESULTS"

echo
echo "raw results ($RESULTS):"
cat "$RESULTS"
