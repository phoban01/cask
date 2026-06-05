#!/usr/bin/env bash
# Bring up the whole demo: provision the three cloud VMs (in parallel), build the
# binaries, then deploy + start every node (clouds under systemd, laptop as a local
# client-only node). Idempotent-ish: re-running after a partial failure re-uses any
# state already written.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/config.sh"; source "$HERE/lib.sh"

mkdir -p "$STATE_DIR"

# Fail fast before creating anything if tools/creds/config aren't ready.
"$HERE/preflight.sh"

# Generate the demo ssh keypair once; it's injected into every VM at create time.
if [ ! -f "$SSH_KEY" ]; then
  log "generating demo ssh key $SSH_KEY"
  ssh-keygen -t ed25519 -N "" -C "cask-demo" -f "$SSH_KEY" >/dev/null
fi

# 1. provision clouds in parallel (skip any already up)
log "provisioning clouds: ${CLOUDS[*]}"
pids=()
for c in "${CLOUDS[@]}"; do
  if state_has "$c" && [ -n "$(cloud_ip "$c")" ]; then
    ok "$c: already provisioned ($(cloud_ip "$c")), skipping"
    continue
  fi
  ( "$HERE/clouds/$c-up.sh" ) & pids+=("$!")
done
fail=0
for p in "${pids[@]}"; do wait "$p" || fail=1; done
[ "$fail" -eq 0 ] || die "one or more clouds failed to provision (see output above)"

# 2. build binaries (host + both linux arches)
"$HERE/build.sh"

# 3. deploy + start everything
"$HERE/deploy.sh"

echo
ok "demo up. run: demo/interact.sh    (tear down with: demo/down.sh)"
