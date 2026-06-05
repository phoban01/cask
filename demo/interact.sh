#!/usr/bin/env bash
# Drive the live demo against the running multi-cloud cluster. Each cloud node's
# KV/lock API is bound to 127.0.0.1 on its VM, so we reach it via `ssh <vm> curl
# localhost`; the laptop's API is local. The point: state written on one cloud is
# read on another and on the laptop (which holds zero replicas).
#
# Usage: demo/interact.sh [--no-failover]
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/config.sh"; source "$HERE/lib.sh"

WITH_FAILOVER=1
[ "${1:-}" = "--no-failover" ] && WITH_FAILOVER=0

# API helpers ---------------------------------------------------------------
cloud_curl() { local cloud="$1"; shift; cask_ssh "$cloud" "curl -fsS $*"; }     # on a VM
laptop_curl() { curl -fsS "$@"; }                                               # locally
L="127.0.0.1:$API_PORT"

hr() { printf '\n\033[1;35m── %s ──\033[0m\n' "$*"; }
say() { printf '   %s\n' "$*"; }

for c in "${CLOUDS[@]}"; do state_has "$c" || die "$c not provisioned; run demo/up.sh first"; done
[ -f "$STATE_DIR/laptop.pid" ] || die "laptop node not running; run demo/deploy.sh"

hr "cluster"
say "GCP    $(cloud_ip gcp)   overlay $GCP_OVERLAY_IP  zone $GCP_ZONE_TAG  (amd64, replica)"
say "Oracle $(cloud_ip oci)   overlay $OCI_OVERLAY_IP  zone $OCI_ZONE_TAG  (arm64, replica)"
say "AWS    $(cloud_ip aws)   overlay $AWS_OVERLAY_IP  zone $AWS_ZONE_TAG  (arm64, replica)"
say "laptop (this machine)    overlay $LAPTOP_OVERLAY_IP  zone $LAPTOP_ZONE_TAG  (client-only, 0 replicas)"

hr "1. write on AWS, read on the laptop and on GCP"
MSG="hello from AWS @ $(state_get aws PUBLIC_IP)"
cloud_curl aws "-XPUT $L/kv/greeting -d '$MSG'"; say "[AWS] PUT /kv/greeting = '$MSG'"
sleep 1
say "[laptop] GET /kv/greeting -> '$(laptop_curl "$L/kv/greeting")'"
say "[GCP]    GET /kv/greeting -> '$(cloud_curl gcp "$L/kv/greeting")'"

hr "2. compare-and-swap on GCP, observe on Oracle"
cloud_curl gcp "-XPOST '$L/cas/greeting?expect=$(printf %s "$MSG" | sed 's/ /%20/g')' -d 'updated from GCP'" \
  && say "[GCP] CAS greeting -> 'updated from GCP'"
sleep 1
say "[Oracle] GET /kv/greeting -> '$(cloud_curl oci "$L/kv/greeting")'"

hr "3. fenced lock — monotonic token across clouds"
cloud_curl gcp "-XPOST '$L/session/payments?ttl=30'" >/dev/null; say "[GCP] session 'payments' granted"
T1="$(cloud_curl gcp "-XPOST '$L/lock/widget?session=payments'")"; say "[GCP] acquire lock/widget -> $T1"
cloud_curl gcp "-XDELETE '$L/lock/widget?session=payments'" >/dev/null; say "[GCP] released"
cloud_curl aws "-XPOST '$L/session/billing?ttl=30'" >/dev/null; say "[AWS] session 'billing' granted"
T2="$(cloud_curl aws "-XPOST '$L/lock/widget?session=billing'")"; say "[AWS] re-acquire lock/widget -> $T2"
say "fencing tokens are strictly increasing across clouds: $T1 -> $T2"

if [ "$WITH_FAILOVER" = "1" ]; then
  hr "4. fault tolerance — stop a replica, the cluster keeps serving"
  say "[Oracle] systemctl stop cask  (1 of 3 replicas down; 2/3 majority remains)"
  cask_ssh oci "sudo systemctl stop cask.service" || true
  sleep 3
  FMSG="written with Oracle down"
  # --max-time bounds the write: a downed replica is a fast non-vote (the overlay
  # RPC timeout), so this commits on the gcp+aws majority within a few seconds.
  if cask_ssh gcp "curl -fsS --max-time 30 -XPUT $L/kv/failover -d '$FMSG'"; then
    say "[GCP] PUT /kv/failover = '$FMSG' (committed on the gcp+aws majority)"
    sleep 1
    say "[laptop] GET /kv/failover -> '$(laptop_curl --max-time 30 "$L/kv/failover")'"
  else
    say "[GCP] write did not complete in time (see overlay RPC timeout)"
  fi
  say "[Oracle] systemctl start cask  (replica rejoins, catches up)"
  cask_ssh oci "sudo systemctl start cask.service" || true
fi

hr "done"
say "All reads/writes/locks above crossed clouds over the encrypted Nebula overlay,"
say "with zero firewall/VPN config beyond one open UDP port — and the laptop, holding"
say "no replicas, participated as a full client."
