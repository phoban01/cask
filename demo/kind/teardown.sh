#!/usr/bin/env bash
# teardown.sh — remove everything demo.sh created.
set -uo pipefail
for c in east west north; do kind delete cluster --name "$c" 2>/dev/null; done
rm -rf "$(dirname "$0")/.consensus-certs" "$(dirname "$0")/.serving-certs"
echo "demo torn down"
