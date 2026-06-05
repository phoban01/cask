#!/usr/bin/env bash
# Mint the overlay certs from the provisioned state, push the arch-matched binary +
# config to each cloud VM and start it under systemd, then bring up this laptop as a
# local --client-only node. Assumes demo/up.sh already provisioned the clouds and
# built the binaries.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/config.sh"; source "$HERE/lib.sh"

[ -x "$BIN_LOCAL" ] || die "missing $BIN_LOCAL (run demo/build.sh)"

# 1. spec -> certs/configs (clusterconf/<name>.yml, one per node)
log "generating overlay spec + certs"
"$HERE/gen-spec.sh" >/dev/null
"$BIN_LOCAL" gen-certs -spec "$CONF_DIR/nodes.json" -out "$CONF_DIR" >/dev/null
ok "certs written to $CONF_DIR"

# 2. deploy each cloud node
deploy_cloud() {
  local cloud="$1"
  local arch bin
  arch="$(state_get "$cloud" ARCH)"
  bin="$REPO_DIR/bin/cask-linux-$arch"
  [ -x "$bin" ] || die "$cloud: missing $bin (run demo/build.sh)"
  [ -f "$CONF_DIR/$cloud.yml" ] || die "$cloud: missing $CONF_DIR/$cloud.yml"

  wait_for_ssh "$cloud"
  log "$cloud: pushing binary ($arch) + config"
  cask_ssh "$cloud" "sudo mkdir -p $REMOTE_DIR && sudo chown \$(id -un) $REMOTE_DIR"
  cask_scp "$cloud" "$bin" "$REMOTE_DIR/cask"
  cask_scp "$cloud" "$CONF_DIR/$cloud.yml" "$REMOTE_DIR/node.yml"
  cask_ssh "$cloud" "chmod +x $REMOTE_DIR/cask"

  log "$cloud: installing + starting cask.service"
  cask_ssh "$cloud" "sudo tee /etc/systemd/system/cask.service >/dev/null" <<EOF
[Unit]
Description=cask node ($cloud)
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=$REMOTE_DIR/cask --nebula-config $REMOTE_DIR/node.yml --overlay-port $OVERLAY_PORT --listen 127.0.0.1:$API_PORT
Restart=on-failure
RestartSec=2

[Install]
WantedBy=multi-user.target
EOF
  cask_ssh "$cloud" "sudo systemctl daemon-reload && sudo systemctl enable --now cask.service"
  ok "$cloud: cask running (overlay $(state_get "$cloud" OVERLAY_IP):$OVERLAY_PORT, api 127.0.0.1:$API_PORT)"
}

for c in "${CLOUDS[@]}"; do deploy_cloud "$c"; done

# 3. wait for the lighthouses to settle, then start the laptop client-only node
log "waiting for cloud roster to form ..."
sleep 8

[ -f "$CONF_DIR/laptop.yml" ] || die "missing $CONF_DIR/laptop.yml"
LAPTOP_LOG="$STATE_DIR/laptop.log"
log "starting local client-only node (api 127.0.0.1:$API_PORT)"
nohup "$BIN_LOCAL" --nebula-config "$CONF_DIR/laptop.yml" \
  --overlay-port "$OVERLAY_PORT" --listen "127.0.0.1:$API_PORT" --client-only \
  >"$LAPTOP_LOG" 2>&1 &
echo $! > "$STATE_DIR/laptop.pid"
sleep 4
ok "laptop client-only node started (pid $(cat "$STATE_DIR/laptop.pid"), log $LAPTOP_LOG)"

echo
ok "cluster up. try: demo/interact.sh"
