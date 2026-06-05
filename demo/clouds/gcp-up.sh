#!/usr/bin/env bash
# Provision the GCP node: one arm64 Tau T2A VM (NOT free tier) with inbound UDP
# (Nebula) and TCP 22 (ssh) open, the demo ssh key injected, and its public IP
# recorded to demo/state/gcp.env.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/../config.sh"; source "$HERE/../lib.sh"

require_cmd gcloud
[ -n "$GCP_PROJECT" ] || die "GCP_PROJECT is required (set it in demo/config.sh or the env)"
[ -f "$SSH_KEY.pub" ] || die "ssh key $SSH_KEY.pub missing (run demo/up.sh, which generates it)"

FW_RULE="$NAME-allow"
TAG="$NAME"

log "gcp: creating firewall rule $FW_RULE (udp:$NEBULA_UDP_PORT, tcp:22)"
gcloud compute firewall-rules create "$FW_RULE" \
  --project "$GCP_PROJECT" --network default --direction INGRESS --action ALLOW \
  --rules "udp:$NEBULA_UDP_PORT,tcp:22" --source-ranges 0.0.0.0/0 --target-tags "$TAG" \
  2>/dev/null || warn "gcp: firewall rule $FW_RULE already exists"

log "gcp: creating instance $NAME ($GCP_MACHINE, $GCP_ZONE)"
gcloud compute instances create "$NAME" \
  --project "$GCP_PROJECT" --zone "$GCP_ZONE" \
  --machine-type "$GCP_MACHINE" \
  --image-family "$GCP_IMAGE_FAMILY" --image-project "$GCP_IMAGE_PROJECT" \
  --tags "$TAG" \
  --metadata "ssh-keys=$GCP_SSH_USER:$(ssh_pubkey)" \
  >/dev/null

IP="$(gcloud compute instances describe "$NAME" --project "$GCP_PROJECT" --zone "$GCP_ZONE" \
  --format 'get(networkInterfaces[0].accessConfigs[0].natIP)')"
[ -n "$IP" ] || die "gcp: could not read public IP"

state_set gcp PUBLIC_IP   "$IP"
state_set gcp SSH_USER    "$GCP_SSH_USER"
state_set gcp ARCH        "amd64"
state_set gcp ZONE_TAG    "$GCP_ZONE_TAG"
state_set gcp OVERLAY_IP  "$GCP_OVERLAY_IP"
state_set gcp GCP_ZONE    "$GCP_ZONE"
state_set gcp FW_RULE     "$FW_RULE"
ok "gcp: $NAME up at $IP"
