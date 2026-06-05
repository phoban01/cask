#!/usr/bin/env bash
# Tear down the GCP node + firewall rule. Idempotent: missing resources are warned,
# not fatal.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/../config.sh"; source "$HERE/../lib.sh"

require_cmd gcloud
[ -n "$GCP_PROJECT" ] || die "GCP_PROJECT is required"

ZONE="$(state_get gcp GCP_ZONE)"; ZONE="${ZONE:-$GCP_ZONE}"
FW_RULE="$(state_get gcp FW_RULE)"; FW_RULE="${FW_RULE:-$NAME-allow}"

log "gcp: deleting instance $NAME ($ZONE)"
gcloud compute instances delete "$NAME" --project "$GCP_PROJECT" --zone "$ZONE" --quiet \
  2>/dev/null || warn "gcp: instance $NAME not found"

log "gcp: deleting firewall rule $FW_RULE"
gcloud compute firewall-rules delete "$FW_RULE" --project "$GCP_PROJECT" --quiet \
  2>/dev/null || warn "gcp: firewall rule $FW_RULE not found"

rm -f "$(state_file gcp)"
ok "gcp: torn down"
