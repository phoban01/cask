#!/usr/bin/env bash
# Tear down the Oracle node and, if oci-up.sh created the VCN, the whole network.
# Idempotent: missing resources are warned, not fatal.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/../config.sh"; source "$HERE/../lib.sh"

require_cmd oci
state_has oci || { warn "oci: no state file, nothing to tear down"; exit 0; }

INST="$(state_get oci OCI_INSTANCE)"
VCN="$(state_get oci OCI_VCN)"
CREATED_NET="$(state_get oci OCI_CREATED_NET)"
SUBNET="$(state_get oci OCI_SUBNET)"

if [ -n "$INST" ]; then
  log "oci: terminating instance $INST"
  oci compute instance terminate --instance-id "$INST" --force \
    --wait-for-state TERMINATED 2>/dev/null || warn "oci: instance terminate failed/already gone"
fi

# Only dismantle the network if we created it (a reused OCI_SUBNET is left alone).
if [ "$CREATED_NET" = "1" ] && [ -n "$VCN" ]; then
  if [ -n "$SUBNET" ]; then
    log "oci: deleting subnet $SUBNET"
    oci network subnet delete --subnet-id "$SUBNET" --force --wait-for-state TERMINATED \
      2>/dev/null || warn "oci: subnet delete failed/already gone"
  fi
  # The default route table's rule references the IG, which blocks VCN delete with a
  # 409 — clear the rules first, then drop the gateway.
  RT="$(oci network vcn get --vcn-id "$VCN" --query 'data."default-route-table-id"' --raw-output 2>/dev/null)"
  if [ -n "$RT" ]; then
    log "oci: clearing default route table rules"
    oci network route-table update --rt-id "$RT" --force --route-rules '[]' >/dev/null 2>&1 || true
  fi
  log "oci: deleting internet gateways in $VCN"
  for ig in $(oci network internet-gateway list --compartment-id "$OCI_COMPARTMENT" --vcn-id "$VCN" \
                --query 'data[].id' --raw-output 2>/dev/null | tr -d '[],"'); do
    [ -n "$ig" ] && oci network internet-gateway delete --ig-id "$ig" --force \
      --wait-for-state TERMINATED 2>/dev/null || true
  done
  log "oci: deleting VCN $VCN"
  oci network vcn delete --vcn-id "$VCN" --force --wait-for-state TERMINATED \
    2>/dev/null || warn "oci: vcn delete failed (may have lingering resources)"
fi

rm -f "$(state_file oci)"
ok "oci: torn down"
