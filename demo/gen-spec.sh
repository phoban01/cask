#!/usr/bin/env bash
# Assemble clusterconf/nodes.json from the provisioned cloud state files. The three
# clouds become Nebula lighthouses advertised at their public IPs (the RF=3 replica
# set); this laptop is appended as a non-lighthouse client-only node.
#
# Output feeds: cask gen-certs -spec clusterconf/nodes.json
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/config.sh"; source "$HERE/lib.sh"

mkdir -p "$CONF_DIR"
OUT="$CONF_DIR/nodes.json"

emit_cloud() { # cloud  -> one JSON object (lighthouse)
  local cloud="$1"
  local ip overlay zone
  ip="$(cloud_ip "$cloud")"; overlay="$(state_get "$cloud" OVERLAY_IP)"; zone="$(state_get "$cloud" ZONE_TAG)"
  [ -n "$ip" ] || die "$cloud: not provisioned (no PUBLIC_IP); run demo/up.sh"
  printf '  {"name":"%s","overlay_ip":"%s","zone":"%s","udp":"0.0.0.0:%s","advertise":"%s:%s","lighthouse":true}' \
    "$cloud" "$overlay" "$zone" "$NEBULA_UDP_PORT" "$ip" "$NEBULA_UDP_PORT"
}

log "writing $OUT"
{
  echo "["
  first=1
  for c in "${CLOUDS[@]}"; do
    [ $first -eq 1 ] || echo ","
    first=0
    emit_cloud "$c"
  done
  # The laptop: client-only, behind NAT, no advertise address (never dialed).
  echo ","
  printf '  {"name":"laptop","overlay_ip":"%s","zone":"%s","udp":"0.0.0.0:%s","advertise":"","lighthouse":false}\n' \
    "$LAPTOP_OVERLAY_IP" "$LAPTOP_ZONE_TAG" "$NEBULA_UDP_PORT"
  echo "]"
} > "$OUT"

ok "wrote $OUT"
cat "$OUT"
