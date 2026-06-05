#!/usr/bin/env bash
# Load demo/credentials.yaml (if present) into the environment so the cloud CLIs
# authenticate non-interactively — WITHOUT mutating any global CLI state (no
# `gcloud auth activate`, no ~/.aws or ~/.oci edits). It only exports per-invocation
# overrides and writes a throwaway OCI config under demo/state/.
#
# Sourced by config.sh (after the path vars are defined). No-op when the file is
# absent: the demo then falls back to whatever ambient CLI auth / env you already
# have. Requires yq (yq-go); added to devbox.
#
# Override the location with CASK_CREDENTIALS=/path/to/creds.yaml.

CRED_FILE="${CASK_CREDENTIALS:-$DEMO_DIR/credentials.yaml}"
[ -f "$CRED_FILE" ] || return 0
if ! command -v yq >/dev/null 2>&1; then
  printf 'creds: %s exists but yq is not on PATH (run inside `devbox shell`)\n' "$CRED_FILE" >&2
  return 0
fi

# _cv <yq-path> -> value, or "" if missing/null
_cv() { yq -r "$1 // \"\"" "$CRED_FILE" 2>/dev/null; }
# _abs <path> -> absolute (paths in the yaml are relative to demo/)
_abs() { case "$1" in /*) printf '%s' "$1";; "") :;; *) printf '%s/%s' "$DEMO_DIR" "$1";; esac; }

# --- GCP: per-invocation credential + project override (no global activation) --
_v="$(_cv .gcp.project)"
if [ -n "$_v" ]; then export GCP_PROJECT="$_v"; export CLOUDSDK_CORE_PROJECT="$_v"; fi
_v="$(_abs "$(_cv .gcp.service_account_key)")"
if [ -n "$_v" ]; then export CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE="$_v"; fi

# --- AWS: standard credential env vars ----------------------------------------
_v="$(_cv .aws.access_key_id)";     [ -n "$_v" ] && export AWS_ACCESS_KEY_ID="$_v"
_v="$(_cv .aws.secret_access_key)"; [ -n "$_v" ] && export AWS_SECRET_ACCESS_KEY="$_v"
_v="$(_cv .aws.region)"
if [ -n "$_v" ]; then export AWS_REGION="$_v"; export AWS_DEFAULT_REGION="$_v"; fi

# --- OCI ids: exported regardless of how you authenticate ---------------------
_v="$(_cv .oci.compartment)"; [ -n "$_v" ] && export OCI_COMPARTMENT="$_v"
_v="$(_cv .oci.subnet)";      [ -n "$_v" ] && export OCI_SUBNET="$_v"
_v="$(_cv .oci.region)";      [ -n "$_v" ] && { export OCI_REGION="$_v"; export OCI_CLI_REGION="$_v"; }

# --- OCI api-key auth: synthesize a config file only when key creds are present.
# Leave the key fields blank to use ambient auth (e.g. `oci setup bootstrap`).
_tenancy="$(_cv .oci.tenancy)"
_okey="$(_abs "$(_cv .oci.key_file)")"
if [ -n "$_tenancy" ] && [ -n "$_okey" ]; then
  mkdir -p "$STATE_DIR"
  _ocfg="$STATE_DIR/oci_config"
  ( umask 077
    {
      printf '[DEFAULT]\n'
      printf 'user=%s\n'        "$(_cv .oci.user)"
      printf 'tenancy=%s\n'     "$_tenancy"
      printf 'fingerprint=%s\n' "$(_cv .oci.fingerprint)"
      printf 'key_file=%s\n'    "$_okey"
      printf 'region=%s\n'      "$(_cv .oci.region)"
    } > "$_ocfg" )
  export OCI_CLI_CONFIG_FILE="$_ocfg"
  export OCI_CLI_PROFILE="DEFAULT"
fi

unset -f _cv _abs 2>/dev/null || true
unset _v _tenancy _okey _ocfg 2>/dev/null || true
