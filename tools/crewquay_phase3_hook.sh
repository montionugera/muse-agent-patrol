#!/usr/bin/env bash
# Native HATCH 60s Hook adapter: one bounded Runtime cycle and an optional wake().
# The Agent owns one-time installation/authorization/SYN -> SYN-ACK -> ACK.
# SQLite scheduling, permissions, notification cooldown, exact remote receipts,
# retries and social policy belong to Muse Runtime, never this shell.
set -euo pipefail
source "${HATCH_HOOK_RUNTIME:?real native HATCH runtime required}"

REPO="${CREWQUAY_REPO:?repo required}"
DB="${CREWQUAY_DB:?sqlite required}"
CONFIG="${CREWQUAY_CONFIG:?config required}"
LOCK="${CREWQUAY_HOOK_LOCK:-${HOME}/hooks/state/crewquay-phase3-hook.lock}"
HOOK_OWNER="${CREWQUAY_HOOK_OWNER:-crewquay-hatch-hook}"
mkdir -p "$(dirname "$LOCK")"

exec 9>"$LOCK"
if ! flock -n 9; then
  silent "CrewQuay runtime is already running" '{"status":"busy"}'
  exit 0
fi

# HATCH wake() existence supports best-effort notification only.
# reliable_ack requires a separately validated Host Adapter and real receipt;
# no return code, env flag or model self-report is an ACK.
HOST_CAPABILITY="${CREWQUAY_HOST_CAPABILITY:-best_effort}"
if ! type wake >/dev/null 2>&1; then
  HOST_CAPABILITY="unavailable"
fi
case "$HOST_CAPABILITY" in
  best_effort|reliable_ack|unavailable) ;;
  *) silent "Unsupported Host capability" '{"status":"host_unavailable"}'; exit 0 ;;
esac

RESULT=""
if ! RESULT="$(cd "$REPO" && timeout 50s python3 -m patrol \
    --config "$CONFIG" --db "$DB" runtime-cycle \
    --owner "$HOOK_OWNER" --host-capability "$HOST_CAPABILITY" 2>/dev/null)"; then
  log "CrewQuay runtime-cycle failed or reached deadline; inspect SQLite run diagnostics"
  silent "Muse Runtime degraded" '{"status":"degraded"}'
  exit 0
fi

# Only route an opaque, Runtime-authorized notification to the native Host.
# No Shell SQL, deployment bootstrap, approval decision, lease or ACK.
HANDOFF=""
HANDOFF="$(printf '%s' "$RESULT" | python3 -c '
import json,sys
try:
    d=json.load(sys.stdin)
    h=d.get("host") or {}
    if (h.get("should_wake") is True and
        h.get("status") in ("notification_intent","reserved") and
        h.get("lease_version") and
        h.get("wakeup_id") and
        h.get("host_capability") in ("best_effort","reliable_ack")):
        print(json.dumps(h,ensure_ascii=False,separators=(",",":")))
except (ValueError,TypeError):
    pass
' 2>/dev/null)" || HANDOFF=""
if [ -n "$HANDOFF" ] && type wake >/dev/null 2>&1; then
  # Calling wake() does NOT certify Host accepted or Worker started.
  wake "CrewQuay Muse work ready: run muse-consume using wakeup_id and lease_version from the Host handoff; enforce Scheduler fencing and Outbox confirmed" "$HANDOFF"
else
  silent "Muse Runtime: no host notification due" '{"status":"idle"}'
fi
