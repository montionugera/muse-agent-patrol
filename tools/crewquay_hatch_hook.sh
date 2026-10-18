#!/usr/bin/env bash
# MUSE_HATCH_HOOK_ABI=1
# One canonical HATCH 60-second Hook executable for ALL Muse agents.
# The native HATCH registry invokes this existing tracked path; this file
# never creates a timer, grants authority, acknowledges a Host wake, or
# runs the model. Each Agent supplies ITS OWN config, DB and Host runtime.
set -euo pipefail

# Native HATCH registrations without CREWQUAY_* use a private, data-only
# binding beside THIS installed canonical script. No old Hook is ever sourced.
if [ -z "${CREWQUAY_REPO:-}" ] || [ -z "${CREWQUAY_DB:-}" ] || \
   [ -z "${CREWQUAY_CONFIG:-}" ] || [ -z "${HATCH_HOOK_RUNTIME:-}" ]; then
  BINDING="${BASH_SOURCE[0]}.binding.json"
  BOUND="$(python3 - "$BINDING" <<'PY'
import json, os, pathlib, stat, sys
try:
    p=pathlib.Path(sys.argv[1])
    info=p.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid()
        or stat.S_IMODE(info.st_mode)!=0o600 or info.st_size>4096):
        raise ValueError()
    data=json.loads(p.read_text(encoding="utf-8"))
    names=("agent_id","host_id","repo","db","config","hatch_runtime")
    if (not isinstance(data,dict)
        or set(data)!={"schema_version",*names}
        or type(data["schema_version"]) is not int or data["schema_version"]!=1):
        raise ValueError()
    if not all(isinstance(data[k],str) and data[k] and len(data[k])<=1024
               and not any(ord(c)<32 for c in data[k]) for k in names):
        raise ValueError()
    paths=("repo","db","config","hatch_runtime")
    if not all(pathlib.Path(data[k]).is_absolute() for k in paths):
        raise ValueError()
    cfg=json.loads(pathlib.Path(data["config"]).read_text(encoding="utf-8"))
    if (not isinstance(cfg,dict) or cfg.get("actor_id")!=data["agent_id"]
        or not isinstance(cfg.get("database"),str)
        or pathlib.Path(cfg["database"]).resolve()!=pathlib.Path(data["db"]).resolve()):
        raise ValueError()
    if not pathlib.Path(data["repo"]).is_dir() or not all(
        pathlib.Path(data[k]).is_file() for k in ("db","config","hatch_runtime")):
        raise ValueError()
    for k in paths:
        print(data[k])
except (OSError,ValueError,TypeError,KeyError,UnicodeError):
    sys.exit(2)
PY
  )" || { printf '%s\n' 'Muse Hook: missing/invalid private Agent binding' >&2; exit 2; }
  mapfile -t _agent_binding <<< "$BOUND"
  [ "${#_agent_binding[@]}" -eq 4 ] || exit 2
  values=( "${CREWQUAY_REPO:-}" "${CREWQUAY_DB:-}" "${CREWQUAY_CONFIG:-}" "${HATCH_HOOK_RUNTIME:-}" )
  for n in 0 1 2 3; do
    if [ -n "${values[$n]}" ] && [ "${values[$n]}" != "${_agent_binding[$n]}" ]; then
      printf '%s\n' 'Muse Hook: conflicting Agent binding and HATCH environment' >&2
      exit 2
    fi
  done
  CREWQUAY_REPO="${_agent_binding[0]}"
  CREWQUAY_DB="${_agent_binding[1]}"
  CREWQUAY_CONFIG="${_agent_binding[2]}"
  HATCH_HOOK_RUNTIME="${_agent_binding[3]}"
fi

REPO="${CREWQUAY_REPO:?Agent-specific repository path required}"
DB="${CREWQUAY_DB:?Agent-specific SQLite path required}"
CONFIG="${CREWQUAY_CONFIG:?Agent-specific runtime config path required}"
HATCH_RUNTIME="${HATCH_HOOK_RUNTIME:?Native HATCH runtime injection required}"
LOCK="${CREWQUAY_HOOK_LOCK:-${DB}.hatch-hook.lock}"
HOOK_OWNER="${CREWQUAY_HOOK_OWNER:-crewquay-hatch-hook}"
HOST_CAPABILITY="${CREWQUAY_HOST_CAPABILITY:-best_effort}"

# Prevent an Agent from accidentally running against another Agent's
# relative paths or sharing one HOME-based lock on multi-Agent hosts.
for pathname in "$REPO" "$DB" "$CONFIG" "$HATCH_RUNTIME" "$LOCK"; do
  case "$pathname" in /*) ;; *) exit 2 ;; esac
done
[ -d "$REPO" ] && [ -f "$DB" ] && [ -f "$CONFIG" ] && [ -f "$HATCH_RUNTIME" ] || exit 2
source "$HATCH_RUNTIME"
type silent >/dev/null 2>&1 || exit 2
type log >/dev/null 2>&1 || exit 2

mkdir -p "$(dirname "$LOCK")"
exec 9>"$LOCK"
if ! flock -n 9; then
  silent "Muse Runtime: concurrent native Hook skipped" '{"status":"busy"}'
  exit 0
fi

# Native wake() is the only actual invocation. It is best_effort unless
# another REAL Host adapter supplies independently verified acceptance.
if ! type wake >/dev/null 2>&1; then
  HOST_CAPABILITY="unavailable"
fi
case "$HOST_CAPABILITY" in
  unavailable|best_effort|reliable_ack) ;;
  *) log "Muse Runtime: invalid native Host capability"; exit 2 ;;
esac

RESULT=""
if ! RESULT="$(cd "$REPO" && timeout 50s python3 -m patrol \
    --config "$CONFIG" --db "$DB" runtime-cycle \
    --owner "$HOOK_OWNER" --host-capability "$HOST_CAPABILITY" 2>/dev/null)"; then
  log "Muse Runtime: bounded runtime-cycle failed (inspect local SQLite diagnostics)"
  silent "Muse Runtime unavailable" '{"status":"degraded"}'
  exit 0
fi

# Never wake from guessed model decisions, invalid JSON or a stale/mismatched
# Host capability. No remote write, ACK or Scheduler grant occurs in Shell.
HANDOFF="$(printf '%s' "$RESULT" | python3 -c '
import json,sys
try:
    document=json.load(sys.stdin)
    h=document.get("host")
    if not isinstance(h,dict) or h.get("should_wake") is not True:
        sys.exit(0)
    mode=h.get("host_capability")
    if mode is None and h.get("status")=="reserved":
        # Backwards-compatible field for old reliable Host handoffs.
        mode="reliable_ack" if h.get("requires_host_receipt") is True else None
    gen=h.get("lease_version")
    ident=h.get("wakeup_id")
    if (h.get("status") not in ("notification_intent","reserved")
        or mode not in ("best_effort","reliable_ack")
        or mode!=sys.argv[1]
        or not isinstance(ident,str) or not 1<=len(ident)<=128
        or type(gen) is not int or gen<1
        or h.get("source_type") not in ("work","social","promise")):
        sys.exit(0)
    if mode=="best_effort" and (
        h.get("status")!="notification_intent"
        or h.get("notification_generation")!=gen
        or h.get("requires_host_receipt") is not False):
        sys.exit(0)
    if mode=="reliable_ack" and (
        h.get("status")!="reserved" or
        h.get("requires_host_receipt") is not True):
        sys.exit(0)
    print(json.dumps(h,ensure_ascii=False,separators=(",",":")))
except (ValueError,TypeError,KeyError):
    pass
' "$HOST_CAPABILITY" 2>/dev/null)" || HANDOFF=""

if [ -z "$HANDOFF" ]; then
  silent "Muse Runtime: no valid Host wake due" '{"status":"idle"}'
  exit 0
fi
if ! type wake >/dev/null 2>&1; then
  silent "Muse Runtime: wake() not supplied by HATCH" '{"status":"host_unavailable"}'
  exit 0
fi
# wake() must be the ACTUAL HATCH primitive. Its shell return code is NOT
# Host accepted and cannot mark any Worker started or Outbox confirmed.
if ! wake "CrewQuay Muse work ready: consume the attached fenced handoff with muse-consume; never fake Host accepted or replay Outbox effects" "$HANDOFF"; then
  log "Muse Runtime: native wake() failed; original intent retained"
  silent "Muse Runtime: Host wake not delivered" '{"status":"wake_failed"}'
fi
