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
#   // quint-check: verify-split=each        verify each invariant in its own
#                                            CI job (default: none, one job)
#   // quint-check: verify-init=initA,initB  verify from each init in its own
#                                            CI job; together they must cover init
#
# A module without a quint-check line is not checked. A module with
# invariants but no control fails the gate.
#
# Usage: scripts/quint-check.sh [--verify] [--verify-only] [--module FILE]
#          [--invariants A,B] [--list] [--verify-jobs]
#   --verify       also run bounded `quint verify` for each module
#   --verify-only  run only `quint verify` (CI runs one module per job)
#   --module FILE  check only FILE
#   --list         print the modules that have quint-check lines
#   --invariants A,B  with --verify-only: verify only these invariants
#   --init NAME    with --verify-only: verify from this init action
#   --verify-jobs  print the CI verify jobs as JSON: one per module, or one
#                  per invariant for a module with verify-split=each
#
#= docs/spec/fleet.md#10-verification
## The Quint model MUST include a negative control for each invariant that
## fails when the rule is omitted.
set -euo pipefail

cd "$(dirname "$0")/.."
STEPS=${STEPS:-30}
SAMPLES=${SAMPLES:-2000}
VERIFY=0
CHECKS=1
ONLY=""
LIST=0
VJOBS=0
ONLYINV=""
ONLYINIT=""
while [ $# -gt 0 ]; do
  case $1 in
    --verify) VERIFY=1 ;;
    --verify-only) VERIFY=1; CHECKS=0 ;;
    --module) ONLY=${2:?--module needs a file}; shift ;;
    --list) LIST=1 ;;
    --verify-jobs) VJOBS=1 ;;
    --invariants) ONLYINV=${2:?--invariants needs a list}; shift ;;
    --init) ONLYINIT=${2:?--init needs an action}; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

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
  INVARIANTS=() CONTROLS=() GOOD=step VSTEPS=12 VSPLIT=none VINITS=()
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
      verify-init)
        IFS=, read -r -a VINITS <<<"$val" ;;
      verify-split)
        [[ $val =~ ^(each|none)$ ]] || die "$spec: verify-split must be each or none, got $val"
        VSPLIT=$val ;;
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

# must_fail passes only when quint exits non-zero and its output reports an
# invariant violation: a line that starts with "[violation]" and the line
# "error: Invariant violated". Any other failure (parse or type error,
# unknown step or invariant, runtime error) fails the gate.
must_fail() {
  local spec=$1 step=$2 inv=$3 out rc=0
  echo "== negative control: $step must violate $inv"
  out=$(q run "$spec" --step="$step" --invariant="$inv" --max-steps="$STEPS" --max-samples="$SAMPLES" 2>&1) || rc=$?
  if [ $rc -eq 0 ]; then
    die "$step did not violate $inv; the model has lost its teeth"
  fi
  if ! grep -qE '^\[violation\]' <<<"$out" || ! grep -qxF 'error: Invariant violated' <<<"$out"; then
    {
      echo "FAIL: $spec: control $step:$inv exited $rc without an invariant violation"
      echo "first lines of quint output:"
      printf '%s\n' "$out" | head -n 10 | sed 's/^/   /' || true
    } >&2
    exit 1
  fi
  echo "   violated as required"
}

SPECS=()
for spec in quint/*.qnt; do
  grep -qE '^[[:space:]]*//[[:space:]]*quint-check:' "$spec" && SPECS+=("$spec")
done
[ ${#SPECS[@]} -gt 0 ] || die "no quint/*.qnt module has a quint-check line"
if [ -n "$ONLY" ]; then
  printf "%s\n" "${SPECS[@]}" | grep -qxF "$ONLY" || die "$ONLY has no quint-check line"
  SPECS=("$ONLY")
fi
if [ $LIST -eq 1 ]; then printf "%s\n" "${SPECS[@]}"; exit 0; fi

# --verify-jobs prints the CI verify matrix. A module with verify-split=each
# gets one job per invariant, so its slowest invariant sets the wall time.
if [ $VJOBS -eq 1 ]; then
  jobs=()
  for spec in "${SPECS[@]}"; do
    parse "$spec"
    invs=("")
    [ "$VSPLIT" = each ] && invs=("${INVARIANTS[@]}")
    inits=("")
    [ ${#VINITS[@]} -gt 0 ] && inits=("${VINITS[@]}")
    for ini in "${inits[@]}"; do
      for inv in "${invs[@]}"; do
        jobs+=("{\"module\":\"$spec\",\"invariants\":\"$inv\",\"init\":\"$ini\"}")
      done
    done
  done
  (IFS=,; echo "[${jobs[*]}]")
  exit 0
fi

# Parse every header first, so a bad header fails before the slow runs.
for spec in "${SPECS[@]}"; do parse "$spec"; done

for spec in "${SPECS[@]}"; do
  parse "$spec"
  echo "=== $spec"
  if [ $CHECKS -eq 1 ]; then
    echo "== typecheck"
    q typecheck "$spec"
    echo "== witness runs"
    q test "$spec"
    echo "== $GOOD keeps ${INVARIANTS[*]} ($SAMPLES samples x $STEPS steps)"
    q run "$spec" --step="$GOOD" --max-steps="$STEPS" --max-samples="$SAMPLES" --invariants "${INVARIANTS[@]}"
    for c in "${CONTROLS[@]}"; do
      must_fail "$spec" "${c%%:*}" "${c#*:}"
    done
  fi
  if [ $VERIFY -eq 1 ]; then
    VINV=("${INVARIANTS[@]}")
    if [ -n "$ONLYINV" ]; then
      IFS=, read -r -a VINV <<<"$ONLYINV"
      for inv in "${VINV[@]}"; do
        found=0
        for key in "${INVARIANTS[@]}"; do [ "$key" = "$inv" ] && found=1; done
        [ $found -eq 1 ] || die "$spec: --invariants names $inv, which is not in the invariants line"
      done
    fi
    echo "== quint verify (Apalache, bounded at $VSTEPS steps): ${VINV[*]} ${ONLYINIT:+from $ONLYINIT}"
    initarg=()
    [ -n "$ONLYINIT" ] && initarg=(--init="$ONLYINIT")
    q verify "$spec" "${initarg[@]}" --step="$GOOD" --max-steps="$VSTEPS" --invariants "${VINV[@]}"
  fi
done
echo "quint: ok"
