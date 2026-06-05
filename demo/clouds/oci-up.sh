#!/usr/bin/env bash
# Provision the Oracle node: one arm64 Ampere A1 VM (Always Free eligible). If no
# existing public subnet is supplied (OCI_SUBNET), a self-contained VCN + internet
# gateway + route + security list (UDP Nebula + TCP 22) is created. Records the
# public IP and any created resource ids to demo/state/oci.env for teardown.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/../config.sh"; source "$HERE/../lib.sh"

require_cmd oci
[ -n "$OCI_COMPARTMENT" ] || die "OCI_COMPARTMENT is required (compartment OCID)"
[ -f "$SSH_KEY.pub" ] || die "ssh key missing (run demo/up.sh)"
C="$OCI_COMPARTMENT"
q() { oci "$@" --query "$Q" --raw-output; }   # helper: run oci with the current $Q JMESPath

# --- availability domain -----------------------------------------------------
AD="$OCI_AD"
if [ -z "$AD" ]; then
  Q='data[0].name' AD="$(q iam availability-domain list --compartment-id "$C")"
fi
log "oci: availability domain $AD"

# --- latest Ubuntu 24.04 arm64 image (A1 shape => aarch64) --------------------
Q='data[0].id'
IMG="$(oci compute image list --compartment-id "$C" \
  --operating-system "Canonical Ubuntu" --operating-system-version "24.04" \
  --shape "$OCI_SHAPE" --sort-by TIMECREATED --sort-order DESC --limit 1 \
  --query "$Q" --raw-output)"
[ -n "$IMG" ] || die "oci: no Ubuntu 24.04 image for shape $OCI_SHAPE"
log "oci: image $IMG"

# --- network: reuse OCI_SUBNET or build a throwaway VCN ----------------------
SUBNET="$OCI_SUBNET"
if [ -z "$SUBNET" ]; then
  log "oci: creating VCN $NAME-vcn"
  Q='data.id' VCN="$(oci network vcn create --compartment-id "$C" \
    --cidr-blocks '["10.0.0.0/16"]' --display-name "$NAME-vcn" \
    --query "$Q" --raw-output --wait-for-state AVAILABLE)"
  state_set oci OCI_VCN "$VCN"; state_set oci OCI_CREATED_NET 1

  Q='data.id' IG="$(oci network internet-gateway create --compartment-id "$C" --vcn-id "$VCN" \
    --is-enabled true --display-name "$NAME-ig" --query "$Q" --raw-output --wait-for-state AVAILABLE)"

  Q='data."default-route-table-id"' RT="$(oci network vcn get --vcn-id "$VCN" --query "$Q" --raw-output)"
  oci network route-table update --rt-id "$RT" --force \
    --route-rules "[{\"destination\":\"0.0.0.0/0\",\"destinationType\":\"CIDR_BLOCK\",\"networkEntityId\":\"$IG\"}]" >/dev/null

  Q='data."default-security-list-id"' SL="$(oci network vcn get --vcn-id "$VCN" --query "$Q" --raw-output)"
  oci network security-list update --security-list-id "$SL" --force \
    --egress-security-rules '[{"destination":"0.0.0.0/0","protocol":"all","isStateless":false}]' \
    --ingress-security-rules "[
      {\"source\":\"0.0.0.0/0\",\"protocol\":\"6\",\"isStateless\":false,\"tcpOptions\":{\"destinationPortRange\":{\"min\":22,\"max\":22}}},
      {\"source\":\"0.0.0.0/0\",\"protocol\":\"17\",\"isStateless\":false,\"udpOptions\":{\"destinationPortRange\":{\"min\":$NEBULA_UDP_PORT,\"max\":$NEBULA_UDP_PORT}}}
    ]" >/dev/null

  Q='data.id' SUBNET="$(oci network subnet create --compartment-id "$C" --vcn-id "$VCN" \
    --cidr-block 10.0.1.0/24 --display-name "$NAME-subnet" \
    --route-table-id "$RT" --security-list-ids "[\"$SL\"]" \
    --prohibit-public-ip-on-vnic false \
    --query "$Q" --raw-output --wait-for-state AVAILABLE)"
  ok "oci: network ready (subnet $SUBNET)"
else
  log "oci: reusing subnet $SUBNET (ensure it allows ingress udp:$NEBULA_UDP_PORT + tcp:22)"
fi
state_set oci OCI_SUBNET "$SUBNET"

# --- launch instance ---------------------------------------------------------
log "oci: launching $NAME ($OCI_SHAPE ${OCI_OCPUS}ocpu/${OCI_MEM_GB}GB)"
Q='data.id' INST="$(oci compute instance launch --compartment-id "$C" \
  --availability-domain "$AD" --shape "$OCI_SHAPE" \
  --shape-config "{\"ocpus\":$OCI_OCPUS,\"memoryInGBs\":$OCI_MEM_GB}" \
  --image-id "$IMG" --subnet-id "$SUBNET" --assign-public-ip true \
  --display-name "$NAME" \
  --metadata "{\"ssh_authorized_keys\":\"$(ssh_pubkey)\"}" \
  --query "$Q" --raw-output --wait-for-state RUNNING)"
state_set oci OCI_INSTANCE "$INST"

Q='data[0]."public-ip"' IP="$(oci compute instance list-vnics --instance-id "$INST" --query "$Q" --raw-output)"
[ -n "$IP" ] || die "oci: could not read public IP"

state_set oci PUBLIC_IP  "$IP"
state_set oci SSH_USER   "$OCI_SSH_USER"
state_set oci ARCH       "arm64"
state_set oci ZONE_TAG   "$OCI_ZONE_TAG"
state_set oci OVERLAY_IP "$OCI_OVERLAY_IP"
ok "oci: $NAME up at $IP"
