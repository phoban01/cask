#!/usr/bin/env bash
# Shared configuration for the cask multi-cloud demo.
#
# Sourced by every other script. Values marked REQUIRED are account-specific and
# must be set (here or via the environment) before running demo/up.sh. Everything
# else has a sensible default.
#
# Topology: one arm64 node on each of GCP, Oracle, and AWS — all three are Nebula
# lighthouses and the RF=3 replica set — plus this machine joining behind NAT as a
# --client-only node that holds zero replicas.

set -euo pipefail

# ---- repo paths -------------------------------------------------------------
DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$DEMO_DIR/.." && pwd)"
STATE_DIR="$DEMO_DIR/state"          # per-cloud .env files + ssh key + laptop PID
CONF_DIR="$REPO_DIR/clusterconf"     # gen-certs output (gitignored)
BIN_LOCAL="$REPO_DIR/bin/cask"               # laptop binary (host arch)
BIN_CLOUD="$REPO_DIR/bin/cask-linux-arm64"   # cloud binary (arm64)

# ---- overlay layout ---------------------------------------------------------
NEBULA_UDP_PORT="${NEBULA_UDP_PORT:-4242}"   # underlay UDP (must be open in cloud firewalls)
OVERLAY_PORT="${OVERLAY_PORT:-8001}"         # consensus RPC port on each node's overlay IP
API_PORT="${API_PORT:-8080}"                 # local KV/lock API (bound to 127.0.0.1 on every node)
OVERLAY_PREFIX="${OVERLAY_PREFIX:-10.42.0}"  # overlay /24

# Per-role overlay IP + zone. Clouds are lighthouses; the laptop is the client.
GCP_OVERLAY_IP="$OVERLAY_PREFIX.1";  GCP_ZONE_TAG="gcp"
OCI_OVERLAY_IP="$OVERLAY_PREFIX.2";  OCI_ZONE_TAG="oci"
AWS_OVERLAY_IP="$OVERLAY_PREFIX.3";  AWS_ZONE_TAG="aws"
LAPTOP_OVERLAY_IP="$OVERLAY_PREFIX.100"; LAPTOP_ZONE_TAG="edge"

# Clouds we provision. Loops iterate this list.
CLOUDS=(gcp oci aws)

# ---- naming + ssh -----------------------------------------------------------
NAME="${CASK_DEMO_NAME:-cask-demo}"          # instance / firewall / SG / tag name
SSH_KEY="$STATE_DIR/id_cask"                 # generated ed25519 keypair (demo/up.sh creates it)
SSH_OPTS="-o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=$STATE_DIR/known_hosts -o ConnectTimeout=10"

# ---- GCP (x86 e2-micro — Always Free; GCP has no free arm64) -----------------
# The Always Free e2-micro is x86-only, so the GCP node runs the amd64 binary
# (the rest of the cluster is arm64). Free only in us-west1 / us-central1 / us-east1.
GCP_PROJECT="${GCP_PROJECT:-}"               # REQUIRED: gcloud project id
GCP_ZONE="${GCP_ZONE:-us-central1-a}"        # must be in a free-tier region
GCP_MACHINE="${GCP_MACHINE:-e2-micro}"       # Always Free (x86)
GCP_IMAGE_FAMILY="${GCP_IMAGE_FAMILY:-ubuntu-2404-lts-amd64}"
GCP_IMAGE_PROJECT="${GCP_IMAGE_PROJECT:-ubuntu-os-cloud}"
GCP_SSH_USER="${GCP_SSH_USER:-cask}"

# ---- Oracle (arm64 Ampere A1 — FREE tier) -----------------------------------
OCI_COMPARTMENT="${OCI_COMPARTMENT:-}"       # REQUIRED: compartment OCID
OCI_AD="${OCI_AD:-}"                         # availability domain name; empty => first in region
OCI_SHAPE="${OCI_SHAPE:-VM.Standard.A1.Flex}"
OCI_OCPUS="${OCI_OCPUS:-1}"
OCI_MEM_GB="${OCI_MEM_GB:-6}"
OCI_SUBNET="${OCI_SUBNET:-}"                 # optional: existing public subnet OCID; empty => create a VCN
OCI_SSH_USER="${OCI_SSH_USER:-ubuntu}"
# Canonical Ubuntu 24.04 arm64 image is resolved at launch via `oci compute image list`.

# ---- AWS (arm64 Graviton t4g.small — FREE trial, 750h/mo through 2026) -------
AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_INSTANCE_TYPE="${AWS_INSTANCE_TYPE:-t4g.small}"   # arm64; the free-trial instance type
AWS_SSH_USER="${AWS_SSH_USER:-ubuntu}"
# Latest Ubuntu 24.04 arm64 AMI is resolved at launch from the public SSM parameter.
AWS_SSM_AMI="${AWS_SSM_AMI:-/aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id}"

# Remote layout on every cloud VM.
REMOTE_DIR="/opt/cask"
