#!/usr/bin/env bash
# demo.sh — the cask multi-cluster device-fleet demo on kind.
#
# Topology: three kind clusters (east, west, north), each running ONE
# cask-apiserver that serves the fleet.cask.dev/v1alpha1 group. cask is
# EMBEDDED in the apiservers — each carries its own acceptor, and the three
# apiservers form the consensus group among themselves. There is no external
# coordination infrastructure of any kind: the extension servers ARE the
# fleet. A Device created via any cluster is visible from all, and a device
# can be leased by AT MOST ONE DeviceClaim globally — enforced by cask's
# fenced locks, not by anything in Kubernetes.
#
# Requirements: docker, kind, kubectl. Run from anywhere; paths are
# script-relative. `teardown.sh` removes everything.
set -euo pipefail
cd "$(dirname "$0")"
REPO_ROOT="$(cd ../.. && pwd)"

CLUSTERS=(east west north)
IMG=cask-demo:latest

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

step "Building the demo image (cask-apiserver with embedded cask)"
docker build -q -f "$REPO_ROOT/demo/kind/Dockerfile" -t "$IMG" "$REPO_ROOT"

step "Creating kind clusters: ${CLUSTERS[*]}"
for c in "${CLUSTERS[@]}"; do
  kind get clusters 2>/dev/null | grep -qx "$c" || kind create cluster --name "$c" --wait 120s
done

# kind's clusters share the docker network named "kind". The apiservers run
# with hostNetwork, so consensus rides the node IPs — mutually reachable
# across clusters, unlike pod IPs.
PEER_IPS=()
for c in "${CLUSTERS[@]}"; do
  ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$c-control-plane")
  PEER_IPS+=("$ip:8001")
done
CASK_PEERS=$(IFS=,; echo "${PEER_IPS[*]}")
echo "   consensus fleet (the apiservers themselves): $CASK_PEERS"

step "Deploying the fleet.cask.dev apiserver into each cluster"
id=101
for c in "${CLUSTERS[@]}"; do
  kind load docker-image "$IMG" --name "$c"
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

step "1/4 — one fleet, one truth: a Device created in EAST is visible in WEST and NORTH"
kubectl --context kind-east create -f - <<'EOF'
apiVersion: fleet.cask.dev/v1alpha1
kind: Device
metadata: {name: gpu-7}
spec: {model: h100, zone: rack-12}
EOF
kubectl --context kind-west get devices gpu-7 -o yaml | sed -n '1,12p'
kubectl --context kind-north get devices gpu-7 -o jsonpath='   north sees: {.metadata.name} rv={.metadata.resourceVersion}{"\n"}'

step "2/4 — clusters race to claim gpu-7: exactly ONE lease exists globally"
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
echo "--- all claims, as seen from ANY cluster (one Bound, one Pending):"
kubectl --context kind-north get deviceclaims -o custom-columns=NAME:.metadata.name,CLUSTER:.status.cluster,PHASE:.status.phase,FENCE:.status.fence

step "3/4 — handover: deleting the winner passes the lease at a HIGHER fence"
WINNER=$(kubectl --context kind-east get deviceclaims -o jsonpath='{range .items[?(@.status.phase=="Bound")]}{.metadata.name}{end}')
WINNER_CLUSTER=$(kubectl --context kind-east get deviceclaims "$WINNER" -o jsonpath='{.status.cluster}')
echo "   winner: $WINNER (cluster $WINNER_CLUSTER)"
kubectl --context "kind-$WINNER_CLUSTER" delete deviceclaim "$WINNER"
sleep 6
kubectl --context kind-east get deviceclaims -o custom-columns=NAME:.metadata.name,CLUSTER:.status.cluster,PHASE:.status.phase,FENCE:.status.fence
kubectl --context kind-east get devices gpu-7 -o jsonpath='   device lease: cluster={.status.lease.cluster} fence={.status.lease.fence}{"\n"}'

step "4/4 — the zombie: kill the holder's apiserver; its lease lapses and is fenced"
HOLDER_CLUSTER=$(kubectl --context kind-east get devices gpu-7 -o jsonpath='{.status.lease.cluster}')
OTHER=east; [ "$HOLDER_CLUSTER" = east ] && OTHER=west
echo "   holder is $HOLDER_CLUSTER; killing its apiserver (no more renewals)."
echo "   NOTE: that apiserver is also one of the three consensus nodes — the"
echo "   remaining two are a quorum, so the fleet keeps committing without it."
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
echo "   waking the zombie: it rejoins consensus (durable acceptor state) and"
echo "   its claim discovers it is Lost (a higher fence exists)"
kubectl --context "kind-$HOLDER_CLUSTER" -n cask-system scale deploy/cask-apiserver --replicas=1
kubectl --context "kind-$HOLDER_CLUSTER" -n cask-system rollout status deploy/cask-apiserver --timeout=120s
sleep 6
kubectl --context "kind-$HOLDER_CLUSTER" get deviceclaims -o custom-columns=NAME:.metadata.name,CLUSTER:.status.cluster,PHASE:.status.phase,FENCE:.status.fence

step "Done. Cleanup: demo/kind/teardown.sh"
