#!/usr/bin/env bash
# Build the cask binaries for the multi-cloud demo.
#
#   bin/cask              -> this machine (the NAT'd laptop / client-only node)
#   bin/cask-linux-arm64  -> every cloud VM (all instances are arm64, so one build)
#
# cask is pure Go (Nebula rides gvisor netstack, no cgo), so CGO_ENABLED=0 gives a
# static binary that runs on any arm64 Linux without cross-compile toolchains.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p bin

echo "==> building bin/cask (host: $(go env GOOS)/$(go env GOARCH))"
go build -o bin/cask ./cmd/cask

echo "==> building bin/cask-linux-arm64 (cloud VMs)"
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o bin/cask-linux-arm64 ./cmd/cask

echo "==> done:"
ls -lh bin/cask bin/cask-linux-arm64
