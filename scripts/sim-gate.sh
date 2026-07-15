#!/usr/bin/env bash
# sim-gate.sh — run the cask deterministic-simulator release gate (roadmap §6.6).
#
# Runs the regression set first, then explores fresh seeds for each fault
# profile. Because the buggify hook is a process-global, seeds are sharded
# ACROSS PROCESSES (not goroutines) for parallelism. Any invariant violation
# fails fast with a non-zero exit; the seed + trace are printed (and written to
# $RECORD_DIR if set) so the failure can be replayed.
#
# Env:
#   PROFILES    space-separated profiles (default: "smoke consensus lease cluster contention")
#   WORKLOADS   space-separated workloads: mvcc (full rounds) and/or owned
#               (the W1 ownership fast-path topology) (default: "mvcc owned")
#   SEEDS       seeds per profile-workload pair (default: 1000)
#   SHARDS      parallel processes per profile (default: nproc)
#   RECORD_DIR  directory for JSON violation records (optional)
#
# Usage:
#   scripts/sim-gate.sh                 # local quick run
#   SEEDS=10000 scripts/sim-gate.sh     # CI tier
set -euo pipefail

cd "$(dirname "$0")/.."

PROFILES="${PROFILES:-smoke consensus lease cluster contention}"
WORKLOADS="${WORKLOADS:-mvcc owned}"
SEEDS="${SEEDS:-1000}"
if command -v nproc >/dev/null 2>&1; then
  DEFAULT_SHARDS="$(nproc)"
else
  DEFAULT_SHARDS=4
fi
SHARDS="${SHARDS:-$DEFAULT_SHARDS}"
PKG="./testutil/sim/"

echo "== sim-gate: regressions =="
go test -run '^TestGateRegressions$' -count=1 -v "$PKG"

fail=0
for profile in $PROFILES; do
  for workload in $WORKLOADS; do
    echo "== sim-gate: profile=$profile workload=$workload seeds=$SEEDS shards=$SHARDS =="
    per=$(( (SEEDS + SHARDS - 1) / SHARDS ))
    pids=()
    for ((shard = 0; shard < SHARDS; shard++)); do
      start=$(( 1 + shard * per ))
      count=$per
      # Trim the last shard so the total is exactly SEEDS.
      if (( start - 1 + count > SEEDS )); then
        count=$(( SEEDS - (start - 1) ))
      fi
      (( count <= 0 )) && continue
      SIM_PROFILE="$profile" SIM_WORKLOAD="$workload" \
        SIM_SEED_START="$start" SIM_SEEDS="$count" \
        ${RECORD_DIR:+SIM_RECORD_DIR="$RECORD_DIR"} \
        go test -run '^TestGate$' -count=1 "$PKG" \
        >"/tmp/sim-gate-$profile-$workload-$shard.log" 2>&1 &
      pids+=($!)
    done
    for pid in "${pids[@]}"; do
      if ! wait "$pid"; then
        fail=1
      fi
    done
    # Surface any shard logs that recorded a violation.
    if (( fail )); then
      echo "!! sim-gate FAILED in profile=$profile workload=$workload — shard logs:"
      cat /tmp/sim-gate-"$profile"-"$workload"-*.log || true
      break 2
    fi
    echo "   profile=$profile workload=$workload clean"
  done
done

if (( fail )); then
  echo "sim-gate: FAIL"
  exit 1
fi
echo "sim-gate: PASS"
