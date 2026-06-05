#!/usr/bin/env bash
# Tear down the AWS node, security group, and imported key pair. Idempotent.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/../config.sh"; source "$HERE/../lib.sh"

require_cmd aws
state_has aws || { warn "aws: no state file, nothing to tear down"; exit 0; }
export AWS_DEFAULT_REGION="$(state_get aws AWS_REGION)"; AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-$AWS_REGION}"

INST="$(state_get aws AWS_INSTANCE)"
SG_ID="$(state_get aws AWS_SG)"
KEY_NAME="$(state_get aws AWS_KEY)"

if [ -n "$INST" ]; then
  log "aws: terminating instance $INST"
  aws ec2 terminate-instances --instance-ids "$INST" >/dev/null 2>&1 \
    && aws ec2 wait instance-terminated --instance-ids "$INST" 2>/dev/null \
    || warn "aws: instance terminate failed/already gone"
fi

# SG can't be deleted until the instance is fully gone (the wait above handles that).
if [ -n "$SG_ID" ]; then
  log "aws: deleting security group $SG_ID"
  aws ec2 delete-security-group --group-id "$SG_ID" 2>/dev/null \
    || warn "aws: security group delete failed (may still be in use)"
fi

if [ -n "$KEY_NAME" ]; then
  log "aws: deleting key pair $KEY_NAME"
  aws ec2 delete-key-pair --key-name "$KEY_NAME" 2>/dev/null || true
fi

rm -f "$(state_file aws)"
ok "aws: torn down"
