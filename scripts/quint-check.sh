#!/usr/bin/env bash
# Run the Quint checks for every quint/*.qnt module in both directions:
#   1. the good step keeps every invariant (random simulation, and
#      `quint verify` when Apalache + a JVM are available);
#   2. each negative control violates its invariant. A negative control
#      that passes means the model has lost its teeth, so that is a failure.
#
# Each module declares its checks in header comments. The script reads
# them, so a new module needs no change here:
#
#   // quint-check: invariants=Inv1,Inv2     required, comma-separated
#   // quint-check: control=stepBad:Inv1     one line per negative control;
#                                            at least one is required
#   // quint-check: step=step                good step (default: step)
#   // quint-check: verify-steps=12          `quint verify` bound (default: 12)
#
# A module without a quint-check line is not checked. A module with
# invariants but no control fails the gate.
#
# Usage: scripts/quint-check.sh [--verify]
#
#= docs/spec/fleet.md#10-verification
## The Quint model MUST include a negative control for each invariant that
## fails when the rule is omitted.
set -euo pipefail

cd "$(dirname "$0")/.."
STEPS=${STEPS:-30}
SAMPLES=${SAMPLES:-2000}
VERIFY=0
if [ "${1:-}" = "--verify" ]; then VERIFY=1; fi

if command -v quint >/dev/null 2>&1; then
  Q=quint
else
  Q="nix-shell -p quint --run"
fi
q() { if [ "$Q" = quint ]; then quint "$@"; else nix-shell -p quint --run "quint $*"; fi; }

die() { echo "FAIL: $*" >&2; exit 1; }

# parse SPEC sets INVARIANTS (array), CONTROLS (array of step:inv), GOOD,
# and VSTEPS from the quint-check header lines of SPEC.
parse() {
  local spec=$1 line key val inv found
  INVARIANTS=() CONTROLS=() GOOD=step VSTEPS=12
  while IFS= read -r line; do
    line=${line#*quint-check:}
    line=${line#"${line%%[![:space:]]*}"}
    line=${line%"${line##*[![:space:]]}"}
    key=${line%%=*} val=${line#*=}
    [ "$key" != "$line" ] && [ -n "$val" ] || die "$spec: bad quint-check line: $line"
    case $key in
      invariants)
        [ ${#INVARIANTS[@]} -eq 0 ] || die "$spec: more than one invariants line"
        IFS=, read -r -a INVARIANTS <<<"$val" ;;
      control)
        [[ $val =~ ^[A-Za-z_][A-Za-z0-9_]*:[A-Za-z_][A-Za-z0-9_]*$ ]] \
          || die "$spec: control must be step:invariant, got $val"
        CONTROLS+=("$val") ;;
      step) GOOD=$val ;;
      verify-steps)
        [[ $val =~ ^[0-9]+$ ]] || die "$spec: verify-steps must be a number, got $val"
        VSTEPS=$val ;;
      *) die "$spec: unknown quint-check key: $key" ;;
    esac
  done < <(grep -E '^[[:space:]]*//[[:space:]]*quint-check:' "$spec")

  [ ${#INVARIANTS[@]} -gt 0 ] || die "$spec: no invariants line"
  [ ${#CONTROLS[@]} -gt 0 ] || die "$spec: invariants have no negative control; add a control=step:invariant line"
  for val in "${CONTROLS[@]}"; do
    inv=${val#*:} found=0
    for key in "${INVARIANTS[@]}"; do [ "$key" = "$inv" ] && found=1; done
    [ $found -eq 1 ] || die "$spec: control $val names an invariant not in the invariants line"
  done
}

must_fail() {
  local spec=$1 step=$2 inv=$3
  echo "== negative control: $step must violate $inv"
  if q run "$spec" --step="$step" --invariant="$inv" --max-steps="$STEPS" --max-samples="$SAMPLES" >/dev/null 2>&1; then
    die "$step did not violate $inv; the model has lost its teeth"
  fi
  echo "   violated as required"
}

SPECS=()
for spec in quint/*.qnt; do
  grep -qE '^[[:space:]]*//[[:space:]]*quint-check:' "$spec" && SPECS+=("$spec")
done
[ ${#SPECS[@]} -gt 0 ] || die "no quint/*.qnt module has a quint-check line"

# Parse every header first, so a bad header fails before the slow runs.
for spec in "${SPECS[@]}"; do parse "$spec"; done

for spec in "${SPECS[@]}"; do
  parse "$spec"
  echo "=== $spec"
  echo "== typecheck"
  q typecheck "$spec"
  echo "== witness runs"
  q test "$spec"
  echo "== $GOOD keeps ${INVARIANTS[*]} ($SAMPLES samples x $STEPS steps)"
  q run "$spec" --step="$GOOD" --max-steps="$STEPS" --max-samples="$SAMPLES" --invariants "${INVARIANTS[@]}"
  for c in "${CONTROLS[@]}"; do
    must_fail "$spec" "${c%%:*}" "${c#*:}"
  done
  if [ $VERIFY -eq 1 ]; then
    echo "== quint verify (Apalache, bounded at $VSTEPS steps)"
    q verify "$spec" --step="$GOOD" --max-steps="$VSTEPS" --invariants "${INVARIANTS[@]}"
  fi
done
echo "quint: ok"
