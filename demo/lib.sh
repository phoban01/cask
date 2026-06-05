#!/usr/bin/env bash
# Shared helpers: logging, prerequisite checks, per-cloud state files, and SSH/SCP
# wrappers. Source after config.sh.

# ---- logging ----------------------------------------------------------------
_c() { printf '\033[%sm' "$1"; }   # color on
_r() { printf '\033[0m'; }         # reset
log()  { printf '%s==>%s %s\n' "$(_c '1;36')" "$(_r)" "$*"; }
ok()   { printf '%s ok %s %s\n' "$(_c '1;32')" "$(_r)" "$*"; }
warn() { printf '%swarn%s %s\n' "$(_c '1;33')" "$(_r)" "$*" >&2; }
die()  { printf '%sFAIL%s %s\n' "$(_c '1;31')" "$(_r)" "$*" >&2; exit 1; }

require_cmd() { command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"; }

# ---- state ------------------------------------------------------------------
# Each cloud writes demo/state/<cloud>.env with shell-assignable KEY=VALUE lines:
#   PUBLIC_IP, SSH_USER, ARCH, ZONE_TAG, OVERLAY_IP, and any cloud resource ids
#   needed for teardown (e.g. GCP_INSTANCE, OCI_VCN, AWS_SG).
state_file() { echo "$STATE_DIR/$1.env"; }

state_set() { # cloud KEY VALUE  (idempotent upsert)
  local f; f="$(state_file "$1")"; mkdir -p "$STATE_DIR"; touch "$f"
  local key="$2" val="$3"
  if grep -q "^$key=" "$f" 2>/dev/null; then
    local tmp; tmp="$(mktemp)"; grep -v "^$key=" "$f" >"$tmp"; mv "$tmp" "$f"
  fi
  printf '%s=%q\n' "$key" "$val" >>"$f"
}

state_get() { # cloud KEY  -> value on stdout (empty if absent)
  local f; f="$(state_file "$1")"; [ -f "$f" ] || return 0
  ( set +u; # shellcheck disable=SC1090
    source "$f"; eval "printf '%s' \"\${$2:-}\"" )
}

state_has() { [ -f "$(state_file "$1")" ]; }

# Public IP / ssh user for a provisioned cloud.
cloud_ip()   { state_get "$1" PUBLIC_IP; }
cloud_user() { state_get "$1" SSH_USER; }

# ---- ssh / scp --------------------------------------------------------------
# cask_ssh <cloud> <remote command...>
cask_ssh() {
  local cloud="$1"; shift
  local ip user; ip="$(cloud_ip "$cloud")"; user="$(cloud_user "$cloud")"
  [ -n "$ip" ] || die "$cloud: no PUBLIC_IP in state (provision first)"
  # shellcheck disable=SC2086
  ssh $SSH_OPTS -i "$SSH_KEY" "$user@$ip" "$@"
}

# cask_scp <cloud> <local-src> <remote-dst>
cask_scp() {
  local cloud="$1" src="$2" dst="$3"
  local ip user; ip="$(cloud_ip "$cloud")"; user="$(cloud_user "$cloud")"
  [ -n "$ip" ] || die "$cloud: no PUBLIC_IP in state (provision first)"
  # shellcheck disable=SC2086
  scp $SSH_OPTS -i "$SSH_KEY" "$src" "$user@$ip:$dst"
}

# Wait until SSH answers (cloud-init / boot can lag the API returning an IP).
wait_for_ssh() { # cloud [timeout_s]
  local cloud="$1" timeout="${2:-180}" waited=0
  log "$cloud: waiting for ssh ..."
  until cask_ssh "$cloud" true 2>/dev/null; do
    sleep 5; waited=$((waited+5))
    [ "$waited" -ge "$timeout" ] && die "$cloud: ssh not ready after ${timeout}s"
  done
  ok "$cloud: ssh ready"
}

# Curl a node's local KV/lock API over ssh (the API binds to 127.0.0.1 on the VM).
node_api() { # cloud  curl-args...
  local cloud="$1"; shift
  cask_ssh "$cloud" "curl -fsS $*"
}

# Read the generated demo ssh public key (created by up.sh).
ssh_pubkey() { cat "$SSH_KEY.pub"; }
