#!/usr/bin/env bash
# teardown.sh — remove everything demo.sh created.
set -uo pipefail
for c in east west; do kind delete cluster --name "$c" 2>/dev/null; done
for i in 1 2 3; do docker rm -f "cask-$i" >/dev/null 2>&1; done
echo "demo torn down"
