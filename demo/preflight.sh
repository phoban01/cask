#!/usr/bin/env bash
# Verify everything the demo needs BEFORE up.sh creates any cloud resources:
# required tools on PATH, required config values, and that each cloud CLI can
# actually authenticate. Exits non-zero (listing what to fix) if anything fails, so
# up.sh can bail before spending money. Run inside `devbox shell` (for the CLIs).
set -uo pipefail   # not -e: we want to run every check and report them all
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/config.sh"; source "$HERE/lib.sh"

fails=0
pass() { ok "$*"; }
fail() { printf '%sFAIL%s %s\n' "$(_c '1;31')" "$(_r)" "$*" >&2; fails=$((fails+1)); }

hr() { printf '\n\033[1;36m# %s\033[0m\n' "$*"; }

# --- tools -------------------------------------------------------------------
hr "tools"
for t in go ssh scp ssh-keygen curl gcloud aws oci yq; do
  if command -v "$t" >/dev/null 2>&1; then pass "$t present"
  else fail "$t missing (run inside \`devbox shell\`)"; fi
done

# --- credentials source ------------------------------------------------------
hr "credentials"
if [ -f "${CASK_CREDENTIALS:-$DEMO_DIR/credentials.yaml}" ]; then
  pass "credentials.yaml loaded"
else
  warn "no credentials.yaml — using ambient CLI auth (cp credentials.example.yaml credentials.yaml to change)"
fi

# --- required config values --------------------------------------------------
hr "config values"
[ -n "${GCP_PROJECT:-}" ]    && pass "GCP_PROJECT=$GCP_PROJECT"       || fail "GCP_PROJECT is empty (set gcp.project or env)"
[ -n "${OCI_COMPARTMENT:-}" ] && pass "OCI_COMPARTMENT set"            || fail "OCI_COMPARTMENT is empty (set oci.compartment or env)"
[ -n "${AWS_REGION:-}" ]     && pass "AWS_REGION=$AWS_REGION"          || fail "AWS_REGION is empty"

# --- per-cloud authentication ------------------------------------------------
hr "cloud auth (live API calls)"

# GCP: can we read the project?
if [ -n "${GCP_PROJECT:-}" ]; then
  if gcloud projects describe "$GCP_PROJECT" >/dev/null 2>&1; then pass "gcp: project reachable"
  else fail "gcp: cannot describe project $GCP_PROJECT (check service_account_key / gcloud auth)"; fi
fi

# AWS: who am I?
if who="$(aws sts get-caller-identity --query Arn --output text 2>/dev/null)"; then
  pass "aws: authenticated as $who"
else
  fail "aws: get-caller-identity failed (check access_key_id/secret or aws configure)"
fi

# OCI: can we read the compartment?
if [ -n "${OCI_COMPARTMENT:-}" ]; then
  if oci iam compartment get --compartment-id "$OCI_COMPARTMENT" >/dev/null 2>&1; then pass "oci: compartment reachable"
  else fail "oci: cannot get compartment (check oci.* fields / key_file path / region)"; fi
fi

# --- summary -----------------------------------------------------------------
echo
if [ "$fails" -eq 0 ]; then
  ok "preflight passed — safe to run demo/up.sh"
else
  die "$fails check(s) failed — fix the above before demo/up.sh"
fi
