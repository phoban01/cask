#!/usr/bin/env bash
# Run the Quint checks for quint/fleet.qnt and quint/retry.qnt in both
# directions:
#   1. the good step keeps every invariant (random simulation, and
#      `quint verify` when Apalache + a JVM are available);
#   2. each negative control violates its invariant. A negative control
#      that passes means the model has lost its teeth, so that is a failure.
#
# Usage: scripts/quint-check.sh [--verify]
#
#= docs/spec/fleet.md#10-verification
## The Quint model MUST include a negative control for each invariant that
## fails when the rule is omitted.
set -euo pipefail

cd "$(dirname "$0")/.."
SPEC=quint/fleet.qnt
STEPS=${STEPS:-30}
SAMPLES=${SAMPLES:-2000}
INVARIANTS="IndexNeverAhead IndexRepaired SingleHolder FenceBounded NoStaleEffect WatchGapFree"

if command -v quint >/dev/null 2>&1; then
  Q=quint
else
  Q="nix-shell -p quint --run"
fi
q() { if [ "$Q" = quint ]; then quint "$@"; else nix-shell -p quint --run "quint $*"; fi; }

echo "== typecheck"
q typecheck "$SPEC"

echo "== witness runs"
q test "$SPEC"

echo "== good step keeps invariants ($SAMPLES samples x $STEPS steps)"
q run "$SPEC" --max-steps="$STEPS" --max-samples="$SAMPLES" --invariants $INVARIANTS

must_fail() {
  local spec=$SPEC
  if [ "$1" = --spec ]; then spec=$2; shift 2; fi
  local step=$1 inv=$2
  echo "== negative control: $step must violate $inv"
  if q run "$spec" --step="$step" --invariant="$inv" --max-steps="$STEPS" --max-samples="$SAMPLES" >/dev/null 2>&1; then
    echo "FAIL: $step did not violate $inv; the model has lost its teeth" >&2
    exit 1
  fi
  echo "   violated as required"
}
must_fail stepIndexFirst IndexNeverAhead
must_fail stepIncrementIndex IndexRepaired
must_fail stepNoFenceCheck NoStaleEffect
must_fail stepDeleteIndexFirst IndexNeverAhead
must_fail stepWatchJump WatchGapFree

RETRY=quint/retry.qnt
echo "== retry contract: typecheck, witness runs, good step"
q typecheck "$RETRY"
q test "$RETRY"
q run "$RETRY" --max-steps="$STEPS" --max-samples="$SAMPLES" --invariant=AppliedAtMostOnce
must_fail --spec "$RETRY" stepBlindRetry AppliedAtMostOnce

if [ "${1:-}" = "--verify" ]; then
  echo "== quint verify (Apalache, bounded)"
  q verify "$SPEC" --max-steps=12 --invariants $INVARIANTS
  q verify "$RETRY" --max-steps=12 --invariant=AppliedAtMostOnce
fi
echo "quint: ok"
