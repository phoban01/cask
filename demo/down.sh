#!/usr/bin/env bash
# Tear everything down: stop the local client-only node, then destroy all cloud
# resources (in parallel). Safe to run repeatedly. Leaves clusterconf/ and the ssh
# key unless --purge is given.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/config.sh"; source "$HERE/lib.sh"

# 1. stop the laptop node
if [ -f "$STATE_DIR/laptop.pid" ]; then
  pid="$(cat "$STATE_DIR/laptop.pid")"
  log "stopping local client-only node (pid $pid)"
  kill "$pid" 2>/dev/null || warn "laptop node already stopped"
  rm -f "$STATE_DIR/laptop.pid"
fi

# 2. tear down clouds in parallel
log "tearing down clouds: ${CLOUDS[*]}"
pids=()
for c in "${CLOUDS[@]}"; do ( "$HERE/clouds/$c-down.sh" ) & pids+=("$!"); done
for p in "${pids[@]}"; do wait "$p" || warn "a cloud teardown reported an error (continuing)"; done

# 3. optional cleanup of generated material
if [ "${1:-}" = "--purge" ]; then
  log "purging clusterconf/ and demo ssh key"
  rm -rf "$CONF_DIR"
  rm -f "$SSH_KEY" "$SSH_KEY.pub" "$STATE_DIR/known_hosts" "$STATE_DIR/laptop.log"
fi

ok "torn down"
