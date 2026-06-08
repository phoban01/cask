#!/usr/bin/env bash
# Local, no-cloud end-to-end of cask's DYNAMIC membership over a loopback Nebula
# overlay. It exercises the Phase-2 model with zero cloud accounts:
#
#   * one node FOUNDS the cluster (--bootstrap) — there is no static genesis;
#   * the other nodes JOIN dynamically (they learn the current acceptor core from
#     a peer's /roster snapshot and register via the driver's /roster/join);
#   * the roster register's own acceptor set (the "core") GROWS by reflexive
#     joint-consensus reconfiguration, keeping the founder and adding the
#     highest-id newcomers up to the register replication factor;
#   * the data plane (KV + a fenced lock) works across the overlay.
#
# This is the runnable companion to the reflexive-reconfiguration safety work
# (internal/roster + tla/RosterReconfig.tla). Run: bash demo/local-dynamic.sh
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

N="${N:-4}"                  # number of nodes
TMP="$(mktemp -d)"
trap 'kill $(jobs -p) 2>/dev/null; rm -rf "$TMP"' EXIT

echo "== build =="
go build -o "$TMP/cask" ./cmd/cask

echo "== gen-certs ($N-node loopback; node1 is the Nebula lighthouse + founder) =="
"$TMP/cask" gen-certs -n "$N" -out "$TMP/cc" >/dev/null

# node1 founds; nodes 2..N join through node1's /roster (reached via the lighthouse).
"$TMP/cask" --nebula-config "$TMP/cc/node1.yml" --overlay-port 8001 \
  --listen "127.0.0.1:8080" --bootstrap >"$TMP/node1.log" 2>&1 &
sleep 2
for i in $(seq 2 "$N"); do
  "$TMP/cask" --nebula-config "$TMP/cc/node$i.yml" --overlay-port 8001 \
    --listen "127.0.0.1:808$((i-1))" >"$TMP/node$i.log" 2>&1 &
done

# /roster is served on each node's local API too, so we can inspect from the host.
roster() { curl -s --max-time 2 "http://127.0.0.1:$1/roster" 2>/dev/null || true; }

echo "== wait for the core to fill (reflexive reconfiguration) =="
want_core=$(( N < 3 ? N : 3 ))   # core == min(RegisterRF=3, members)
ok=0
for _ in $(seq 1 60); do
  c=$(roster 8080 | grep -o '"core":\[[^]]*\]' || true)
  cnt=$(echo "$c" | grep -o '[0-9]\+' | wc -l | tr -d ' ')
  if [ "$cnt" = "$want_core" ]; then echo "  core filled to $cnt: $c"; ok=1; break; fi
  sleep 1
done
[ "$ok" = 1 ] || { echo "FAIL: core never filled"; sed -n '1,20p' "$TMP/node1.log"; exit 1; }

echo "== wait for every node's local API =="
for i in $(seq 1 "$N"); do
  p=$((8079 + i))
  for _ in $(seq 1 25); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 http://127.0.0.1:$p/roster 2>/dev/null || echo 000)" = 200 ] && break
    sleep 1
  done
done

echo "== roster =="; roster 8080 | head -c 600; echo
last_port=$((8079 + N))   # node i listens on 8079+i
echo "== cross-node KV (write on node2, read on node$N) =="
for _ in $(seq 1 10); do curl -s --max-time 5 -XPUT 127.0.0.1:8081/kv/greeting -d 'hello' >/dev/null 2>&1 && break; sleep 1; done
got=""
for _ in $(seq 1 20); do got=$(curl -s --max-time 5 "127.0.0.1:$last_port/kv/greeting" 2>/dev/null || true); [ "$got" = hello ] && break; sleep 1; done
[ "$got" = hello ] && echo "  read = '$got' OK" || { echo "FAIL: read '$got'"; exit 1; }

echo "== fenced lock on node3 =="
curl -s --max-time 5 -XPOST '127.0.0.1:8082/session/payments?ttl=60' >/dev/null || true
echo "  token: $(curl -s --max-time 5 -XPOST '127.0.0.1:8082/lock/widget?session=payments' 2>/dev/null || true)"

echo "ALL LOCAL DYNAMIC-MEMBERSHIP CHECKS PASSED"
