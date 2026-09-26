#!/usr/bin/env bash
# File the issues in docs/issues/fleet.md on GitHub with `gh`.
#
# The file holds one issue per `## ` heading. The first body line is
# `labels: a, b`. The rest is the issue body. An issue whose exact title
# already exists (open or closed) is skipped, so the script is safe to
# re-run after adding issues to the file.
#
# Usage: scripts/file-issues.sh [docs/issues/fleet.md] [--dry-run]
set -euo pipefail

cd "$(dirname "$0")/.."
FILE=${1:-docs/issues/fleet.md}
DRY=${2:-}

ensure_label() {
  local name=$1 color=$2 desc=$3
  gh label list --limit 200 --json name -q '.[].name' | grep -qx "$name" && return
  echo "creating label $name"
  [ "$DRY" = --dry-run ] || gh label create "$name" --color "$color" --description "$desc"
}
ensure_label fleet      0e8a16 "fleet objects over cask"
ensure_label agent-5m  1d76db "finishable by an agent in under five minutes"
ensure_label spec      5319e7 "docs/spec/fleet.md"
ensure_label quint     5319e7 "quint/ models"
ensure_label duvet     5319e7 "requirement citations"
ensure_label apiserver c5def5 "cmd/cask-apiserver"
ensure_label membership c5def5 "voters, participants, join"
ensure_label migration c5def5 "etcd to cask cutover"
ensure_label e2e       fbca04 "sigs.k8s.io/e2e-framework on kind"
ensure_label ci        fbca04 "workflows and gates"
ensure_label ops       fbca04 "durability, recovery, health"

existing=$(gh issue list --state all --limit 500 --json title -q '.[].title')

title=""; labels=""; body=""
flush() {
  [ -n "$title" ] || return 0
  if grep -qxF "$title" <<<"$existing"; then
    echo "skip (exists): $title"
  else
    echo "create: $title [$labels]"
    if [ "$DRY" != --dry-run ]; then
      gh issue create --title "$title" --label "$labels" --body "$body" >/dev/null
    fi
  fi
  title=""; labels=""; body=""
}

while IFS= read -r line || [ -n "$line" ]; do
  case "$line" in
    "## "*)
      flush
      title=${line#"## "}
      ;;
    "labels: "*)
      if [ -n "$title" ] && [ -z "$labels" ]; then
        labels="fleet,agent-5m,$(echo "${line#labels: }" | tr -d ' ')"
        continue
      fi
      body+="$line"$'\n'
      ;;
    *)
      [ -n "$title" ] && body+="$line"$'\n'
      ;;
  esac
done <"$FILE"
flush
