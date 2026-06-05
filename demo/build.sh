#!/usr/bin/env bash
# Build the cask binaries for the multi-cloud demo.
#
#   bin/cask              -> this machine (the NAT'd laptop / client-only node)
#   bin/cask-linux-arm64  -> Oracle A1 + AWS t4g (arm64, both free)
#   bin/cask-linux-amd64  -> GCP e2-micro (x86, free tier is x86-only)
#
# The cluster is mixed-arch: deploy.sh ships each VM the binary matching the ARCH
# recorded in its state file. cask is pure Go (Nebula rides gvisor netstack, no
# cgo), so CGO_ENABLED=0 cross-compiles cleanly to either Linux arch.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p bin

echo "==> building bin/cask (host: $(go env GOOS)/$(go env GOARCH))"
go build -o bin/cask ./cmd/cask

echo "==> building bin/cask-linux-arm64 (Oracle, AWS)"
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o bin/cask-linux-arm64 ./cmd/cask

echo "==> building bin/cask-linux-amd64 (GCP e2-micro)"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/cask-linux-amd64 ./cmd/cask

echo "==> done:"
ls -lh bin/cask bin/cask-linux-arm64 bin/cask-linux-amd64
