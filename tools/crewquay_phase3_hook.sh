#!/usr/bin/env bash
# MUSE_HATCH_HOOK_COMPAT=1
# DEPRECATED historical entry. Never fork Hook behavior; always invoke the
# tracked canonical Hook from the same repository at this commit.
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$SCRIPT_DIR/crewquay_hatch_hook.sh" "$@"
