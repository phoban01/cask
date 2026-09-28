#!/usr/bin/env bash
# Run the e2e suite, one run at a time on this machine.
#
# Each run starts three kind clusters (about 3 GB). Two runs at once can
# exhaust a 16 GB development VM, and the kernel then kills processes. A
# lock directory makes a second run wait for the first.
#
# Usage: scripts/e2e.sh [go test flags...]
set -euo pipefail
cd "$(dirname "$0")/.."
LOCK=${CASK_E2E_LOCK:-${TMPDIR:-/tmp}/cask-e2e.lock}
until mkdir "$LOCK" 2>/dev/null; do
  echo "e2e: waiting for another run to finish ($LOCK)" >&2
  sleep 15
done
trap 'rmdir "$LOCK"' EXIT
# -count=1 bypasses the Go test cache. A cached result would report a
# pass without creating any cluster.
go test -tags e2e -count=1 ./test/e2e -timeout 30m "$@"
