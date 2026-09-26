#!/usr/bin/env bash
# teardown.sh — remove everything demo.sh created.
set -uo pipefail
for c in east west north; do kind delete cluster --name "$c" 2>/dev/null; done
echo "demo torn down"
