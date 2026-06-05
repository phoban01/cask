#!/usr/bin/env bash
# One-shot: bring the cluster up and run the scripted interaction. Tear down
# afterwards with demo/down.sh (this script does NOT auto-destroy, so you can poke
# at the live cluster).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"$HERE/up.sh"
"$HERE/interact.sh" "$@"
echo
echo "cluster is still running. tear down with: demo/down.sh"
