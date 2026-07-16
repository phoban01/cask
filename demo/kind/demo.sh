#!/usr/bin/env bash
# demo.sh — the cask multi-cluster device-fleet demo on kind.
#
# Topology: two kind clusters (east, west), each running a cask-apiserver
# that serves the fleet.cask.dev/v1alpha1 group; both apiservers share ONE
# 3-node cask consensus fleet running as plain containers on the kind docker
# network. A Device created via either cluster is visible from both, and a
# device can be leased by AT MOST ONE DeviceClaim globally — enforced by
# cask's fenced locks, not by anything in Kubernetes.
#
# Requirements: docker, kind, kubectl. Run from anywhere; paths are
# script-relative. `teardown.sh` removes everything.
set -euo pipefail
cd "$(dirname "$0")"
REPO_ROOT="$(cd ../.. && pwd)"

CLUSTERS=(east west)
CASK_NODES=3
IMG=cask-demo:latest

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

step "Building the demo image (cask + cask-apiserver)"
docker build -q -f "$REPO_ROOT/demo/kind/Dockerfile" -t "$IMG" "$REPO_ROOT"

step "Creating kind clusters: ${CLUSTERS[*]}"
for c in "${CLUSTERS[@]}"; do
  kind get clusters 2>/dev/null | grep -qx "$c" || kind create cluster --name "$c" --wait 120s
done

step "Starting the shared cask fleet ($CASK_NODES nodes on the kind network)"
# kind's clusters share the docker network named "kind"; the cask containers
# join it so every cluster's pods can reach them by container IP.
PEER_NAMES=()
for i in $(seq 1 "$CASK_NODES"); do PEER_NAMES+=("cask-$i:8001"); done
PEERS_BY_NAME=$(IFS=,; echo "${PEER_NAMES[*]}")
for i in $(seq 1 "$CASK_NODES"); do
  docker rm -f "cask-$i" >/dev/null 2>&1 || true
  docker run -d --name "cask-$i" --network kind "$IMG" \
    --id "$i" --listen ":8001" --peers "$PEERS_BY_NAME" --transport connect >/dev/null
done
# Pods reach the cask nodes by IP (pod DNS is cluster DNS, not docker DNS).
PEER_IPS=()
for i in $(seq 1 "$CASK_NODES"); do
  ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "cask-$i")
  PEER_IPS+=("$ip:8001")
done
CASK_PEERS=$(IFS=,; echo "${PEER_IPS[*]}")
echo "   cask fleet at: $CASK_PEERS"

step "Deploying the fleet.cask.dev apiserver into each cluster"
kind load docker-image "$IMG" --name east
kind load docker-image "$IMG" --name west
id=101
for c in "${CLUSTERS[@]}"; do
  sed -e "s/__CLUSTER__/$c/" -e "s/__ID__/$id/" -e "s/__CASK_PEERS__/$CASK_PEERS/" \
    manifests/apiserver.yaml | kubectl --context "kind-$c" apply -f -
  id=$((id + 1))
done
for c in "${CLUSTERS[@]}"; do
  kubectl --context "kind-$c" -n cask-system rollout status deploy/cask-apiserver --timeout=120s
  # The APIService goes Available once the kube-apiserver can proxy to it.
  kubectl --context "kind-$c" wait --for=condition=Available \
    apiservice/v1alpha1.fleet.cask.dev --timeout=120s
done

step "1/4 — one fleet, one truth: a Device created in EAST is visible in WEST"
kubectl --context kind-east create -f - <<'EOF'
apiVersion: fleet.cask.dev/v1alpha1
kind: Device
metadata: {name: gpu-7}
spec: {model: h100, zone: rack-12}
EOF
kubectl --context kind-west get devices gpu-7 -o yaml | sed -n '1,12p'

step "2/4 — both clusters race to claim gpu-7: exactly ONE lease exists globally"
kubectl --context kind-east create -f - <<'EOF'
apiVersion: fleet.cask.dev/v1alpha1
kind: DeviceClaim
metadata: {name: train-job}
spec: {deviceName: gpu-7, ttlSeconds: 15}
EOF
kubectl --context kind-west create -f - <<'EOF'
apiVersion: fleet.cask.dev/v1alpha1
kind: DeviceClaim
metadata: {name: render-job}
spec: {deviceName: gpu-7, ttlSeconds: 15}
EOF
sleep 6 # two reconcile ticks
echo "--- all claims, as seen from EITHER cluster (one Bound, one Pending):"
kubectl --context kind-west get deviceclaims -o custom-columns=NAME:.metadata.name,CLUSTER:.status.cluster,PHASE:.status.phase,FENCE:.status.fence

step "3/4 — handover: deleting the winner passes the lease at a HIGHER fence"
WINNER=$(kubectl --context kind-east get deviceclaims -o jsonpath='{range .items[?(@.status.phase=="Bound")]}{.metadata.name}{end}')
WINNER_CLUSTER=$(kubectl --context kind-east get deviceclaims "$WINNER" -o jsonpath='{.status.cluster}')
echo "   winner: $WINNER (cluster $WINNER_CLUSTER)"
kubectl --context "kind-$WINNER_CLUSTER" delete deviceclaim "$WINNER"
sleep 6
kubectl --context kind-east get deviceclaims -o custom-columns=NAME:.metadata.name,CLUSTER:.status.cluster,PHASE:.status.phase,FENCE:.status.fence
kubectl --context kind-east get devices gpu-7 -o jsonpath='   device lease: cluster={.status.lease.cluster} fence={.status.lease.fence}{"\n"}'

step "4/4 — the zombie: pause the holder's apiserver; its lease lapses and is fenced"
HOLDER_CLUSTER=$(kubectl --context kind-east get devices gpu-7 -o jsonpath='{.status.lease.cluster}')
OTHER=east; [ "$HOLDER_CLUSTER" = east ] && OTHER=west
echo "   holder is $HOLDER_CLUSTER; pausing its apiserver (no more renewals)..."
kubectl --context "kind-$HOLDER_CLUSTER" -n cask-system scale deploy/cask-apiserver --replicas=0
kubectl --context "kind-$OTHER" create -f - <<EOF
apiVersion: fleet.cask.dev/v1alpha1
kind: DeviceClaim
metadata: {name: takeover-job}
spec: {deviceName: gpu-7, ttlSeconds: 15}
EOF
echo "   waiting out the lapsed TTL (~20s)..."
sleep 22
kubectl --context "kind-$OTHER" get deviceclaims takeover-job -o custom-columns=NAME:.metadata.name,CLUSTER:.status.cluster,PHASE:.status.phase,FENCE:.status.fence
kubectl --context "kind-$OTHER" get devices gpu-7 -o jsonpath='   device lease: cluster={.status.lease.cluster} fence={.status.lease.fence}{"\n"}'
echo "   waking the zombie: its claim discovers it is Lost (higher fence exists)"
kubectl --context "kind-$HOLDER_CLUSTER" -n cask-system scale deploy/cask-apiserver --replicas=1
kubectl --context "kind-$HOLDER_CLUSTER" -n cask-system rollout status deploy/cask-apiserver --timeout=120s
sleep 6
kubectl --context "kind-$HOLDER_CLUSTER" get deviceclaims -o custom-columns=NAME:.metadata.name,CLUSTER:.status.cluster,PHASE:.status.phase,FENCE:.status.fence

step "Done. Cleanup: demo/kind/teardown.sh"
