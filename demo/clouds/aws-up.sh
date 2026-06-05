#!/usr/bin/env bash
# Provision the AWS node: one arm64 Graviton t4g.small (free trial, 750h/mo through
# Dec 31 2026) in the default VPC, with a security group opening inbound UDP (Nebula)
# and TCP 22, the demo ssh key imported, and the public IP recorded to state.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/../config.sh"; source "$HERE/../lib.sh"

require_cmd aws
[ -f "$SSH_KEY.pub" ] || die "ssh key missing (run demo/up.sh)"
export AWS_DEFAULT_REGION="$AWS_REGION"
SG_NAME="$NAME-sg"
KEY_NAME="$NAME-key"

# Latest Ubuntu 24.04 arm64 AMI from the public SSM parameter (never goes stale).
AMI="$(aws ssm get-parameters --names "$AWS_SSM_AMI" --query 'Parameters[0].Value' --output text)"
[ -n "$AMI" ] && [ "$AMI" != "None" ] || die "aws: could not resolve Ubuntu arm64 AMI"
log "aws: ami $AMI ($AWS_REGION)"

# Import the demo ssh public key as an EC2 key pair (idempotent).
if ! aws ec2 describe-key-pairs --key-names "$KEY_NAME" >/dev/null 2>&1; then
  log "aws: importing key pair $KEY_NAME"
  aws ec2 import-key-pair --key-name "$KEY_NAME" \
    --public-key-material "fileb://$SSH_KEY.pub" >/dev/null
fi

# Security group in the default VPC with ingress udp:4242 + tcp:22.
SG_ID="$(aws ec2 describe-security-groups --group-names "$SG_NAME" \
  --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)"
if [ -z "$SG_ID" ] || [ "$SG_ID" = "None" ]; then
  log "aws: creating security group $SG_NAME"
  SG_ID="$(aws ec2 create-security-group --group-name "$SG_NAME" \
    --description "cask demo overlay" --query 'GroupId' --output text)"
  aws ec2 authorize-security-group-ingress --group-id "$SG_ID" \
    --ip-permissions \
      "IpProtocol=udp,FromPort=$NEBULA_UDP_PORT,ToPort=$NEBULA_UDP_PORT,IpRanges=[{CidrIp=0.0.0.0/0}]" \
      "IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=0.0.0.0/0}]" >/dev/null
fi
log "aws: security group $SG_ID"

log "aws: launching $NAME ($AWS_INSTANCE_TYPE)"
INST="$(aws ec2 run-instances \
  --image-id "$AMI" --instance-type "$AWS_INSTANCE_TYPE" \
  --key-name "$KEY_NAME" --security-group-ids "$SG_ID" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$NAME}]" \
  --query 'Instances[0].InstanceId' --output text)"
state_set aws AWS_INSTANCE "$INST"
state_set aws AWS_SG "$SG_ID"
state_set aws AWS_KEY "$KEY_NAME"

log "aws: waiting for $INST to run"
aws ec2 wait instance-running --instance-ids "$INST"
IP="$(aws ec2 describe-instances --instance-ids "$INST" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)"
[ -n "$IP" ] && [ "$IP" != "None" ] || die "aws: could not read public IP"

state_set aws PUBLIC_IP  "$IP"
state_set aws SSH_USER   "$AWS_SSH_USER"
state_set aws ARCH       "arm64"
state_set aws ZONE_TAG   "$AWS_ZONE_TAG"
state_set aws OVERLAY_IP "$AWS_OVERLAY_IP"
state_set aws AWS_REGION "$AWS_REGION"
ok "aws: $NAME up at $IP"
