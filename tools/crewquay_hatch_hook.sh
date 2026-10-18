#!/usr/bin/env bash
# MUSE_HATCH_HOOK_ABI=1
# One canonical HATCH 60-second Hook executable for ALL Muse agents.
# The native HATCH registry invokes this existing tracked path; this file
# never creates a timer, grants authority, acknowledges a Host wake, or
# runs the model. Each Agent supplies ITS OWN config, DB and Host runtime.
set -euo pipefail

# Each Agent's installed canonical script can have its own strictly private
# data-only .binding.json. Native HATCH registrations which cannot inject
# CREWQUAY_* use this path. NO old script, old runtime, code eval or legacy
# wakeup behavior is involved in resolving the binding.
BINDING="${BASH_SOURCE[0]}.binding.json"
if [ -z "${CREWQUAY_REPO:-}" ] || [ -z "${CREWQUAY_DB:-}" ] || \
   [ -z "${CREWQUAY_CONFIG:-}" ] || [ -z "${HATCH_HOOK_RUNTIME:-}" ]; then
  BOUND=""
  if ! BOUND="$(python3 - "$BINDING" <<'PY'
import json,os,pathlib,stat,sys
try:
    p=pathlib.Path(sys.argv[1])
    s=p.lstat()
    if (not stat.S_ISREG(s.st_mode) or s.st_uid!=os.geteuid()
        or stat.S_IMODE(s.st_mode)!=0o600 or s.st_size>4096):
        raise ValueError()
    d=json.loads(p.read_text(encoding="utf-8"))
    allowed={"schema_version","agent_id","host_id","repo","db","config","hatch_runtime"}
    if not isinstance(d,dict) or set(d)!=allowed or type(d["schema_version"]) is not int or d["schema_version"]!=1:
        raise ValueError()
    keys=("agent_id","host_id","repo","db","config","hatch_runtime")
    if not all(isinstance(d[k],str) and d[k] and len(d[k])<=1024 and
               not any(c in d[k] for c in ("\\t","\\r","\\n","\\0"))
               for k in keys):
        raise ValueError()
    paths=("repo","db","config","hatch_runtime")
    if not all(pathlib.Path(d[k]).is_absolute() for k in paths):
        raise ValueError()
    c=json.loads(pathlib.Path(d["config"]).read_text(encoding="utf-8"))
    if (not isinstance(c,dict) or c.get("actor_id")!=d["agent_id"] or
        pathlib.Path(c.get("database","")).resolve()!=pathlib.Path(d["db"]).resolve()):
        raise ValueError()
    if not pathlib.Path(d["repo"]).is_dir() or not all(
        pathlib.Path(d[k]).is_file() for k in ("db","config","hatch_runtime")):
        raise ValueError()
    print("\\t".join(d[k] for k in paths))
except (OSError,ValueError,TypeError,KeyError,UnicodeError):
    sys.exit(2)
PY
)"; then
    printf '%s\\n' 'Muse Hook: private Agent binding missing/invalid; runtime not invoked' >&2
    exit 2
  fi
  IFS=

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
\\t' read -r BOUND_REPO BOUND_DB BOUND_CONFIG BOUND_RUNTIME <<< "$BOUND"
  # Never mix one Agent's env variables with another Agent's installed binding.
  for name in CREWQUAY_REPO CREWQUAY_DB CREWQUAY_CONFIG HATCH_HOOK_RUNTIME; do
    case "$name" in
      CREWQUAY_REPO) expected="$BOUND_REPO" ;;
      CREWQUAY_DB) expected="$BOUND_DB" ;;
      CREWQUAY_CONFIG) expected="$BOUND_CONFIG" ;;
      HATCH_HOOK_RUNTIME) expected="$BOUND_RUNTIME" ;;
    esac
    if [ -n "${!name:-}" ] && [ "${!name}" != "$expected" ]; then
      printf '%s\\n' 'Muse Hook: conflicting Agent env and private binding' >&2
      exit 2
    fi
  done
  CREWQUAY_REPO="$BOUND_REPO"
  CREWQUAY_DB="$BOUND_DB"
  CREWQUAY_CONFIG="$BOUND_CONFIG"
  HATCH_HOOK_RUNTIME="$BOUND_RUNTIME"
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
