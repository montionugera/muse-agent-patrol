#!/usr/bin/env bash
# Reference HATCH Hook, not installed/enabled by this repository.
# Hook: Light Tick + independent action compensation + fenced Muse wake.
# Requires prior host approval for credentialed polling and authorized writes.
# Bounded Hook: light <=20 s, handshake <=6 s, auth <=5 s,
# drain <=16 s, idle discovery <=5 s, wake reservation <=8 s. No second timer or cron.
set -euo pipefail
source "${HATCH_HOOK_RUNTIME:?host Hook runtime required}"

REPO="${CREWQUAY_REPO:?repo required}"
DB="${CREWQUAY_DB:?sqlite path required}"
CONFIG="${CREWQUAY_CONFIG:?config required}"
LOCK="${CREWQUAY_HOOK_LOCK:-${HOME}/hooks/state/crewquay-phase3-hook.lock}"
HOOK_OWNER="${CREWQUAY_HOOK_OWNER:-crewquay-hatch-hook}"
mkdir -p "$(dirname "$LOCK")"

# This is separate from the stable CLI .tick.lock: never hold the CLI lock
# around both CLI invocations. Each invocation releases it before returning.
exec 9>"$LOCK"
if flock -n 9; then
  # No raw stdout/stderr in Hook logs; external content or stack traces could
  # contain secrets. The CLI itself already stores structured run reports.
  TICK_EXIT=0
  (cd "$REPO" && timeout 20s python3 -m patrol \
    --config "$CONFIG" --db "$DB" tick --light >/dev/null 2>&1) || TICK_EXIT=$?
  log "phase3 light tick exit=${TICK_EXIT}"

  # Start native CrewQuay deployment verification only when the installed
  # Git SHA changes or the first authenticated local run is still absent.
  # The real Agent handles the resulting high-priority Queue work after
  # wake(), with Scheduler / Outbox fencing; the Hook never POSTs a SYN/ACK.
  # This is NOT a second timer, and a local run is NOT a Host attestation.
  BOOTSTRAP_EXIT=0
  REPO_SHA="$(git -C "$REPO" rev-parse HEAD 2>/dev/null)" || REPO_SHA=""
  if [[ "$REPO_SHA" =~ ^[0-9a-f]{40}$ ]]; then
    (cd "$REPO" && timeout 6s python3 -m patrol \
      --config "$CONFIG" --db "$DB" deployment-ensure \
      --repo-sha "$REPO_SHA" >/dev/null 2>&1) || BOOTSTRAP_EXIT=$?
  else
    BOOTSTRAP_EXIT=2
  fi
  log "phase7 deployment bootstrap exit=${BOOTSTRAP_EXIT}"

  # Within the SAME 60s HATCH Hook, ask CrewQuay for the Actor's least-privilege
  # first-deployment grant. The CLI enforces a SQLite-backed due timestamp and
  # remote dedup; no new cron/watchdog and no per-task Owner prompting.
  # This never approves its own request, and a failed grant does not block
  # already-authorized tasks or Outbox reconciliation.
  AUTH_EXIT=0
  if [[ "$REPO_SHA" =~ ^[0-9a-f]{40}$ ]]; then
    (cd "$REPO" && timeout 5s python3 -m patrol \
      --config "$CONFIG" --db "$DB" platform-auth-bootstrap \
      --deployment-run-id "hatch-${REPO_SHA:0:16}" --if-due >/dev/null 2>&1) || AUTH_EXIT=$?
  else
    AUTH_EXIT=2
  fi
  log "phase7 permission bootstrap exit=${AUTH_EXIT}"

  # Pure Python Executor, not a Muse worker. Only verified, authorized actions
  # can POST. An unknown remote result is readback-checked, never blindly resent.
  DRAIN_EXIT=0
  (cd "$REPO" && timeout 16s python3 -m patrol \
    --config "$CONFIG" --db "$DB" outbox-drain --once --lane any \
    --limit 3 >/dev/null 2>&1) || DRAIN_EXIT=$?
  log "phase3 executor compensation exit=${DRAIN_EXIT}"
  # Idle social discovery is normal Agent behavior on this existing Hook.
  # The CLI requires a verified Agent identity, checks formal work first,
  # and maintains a durable >=30-minute SQLite cadence. GET only; no POST.
  SOCIAL_EXIT=0
  # Alternate ranked posts with cursor-scoped public-circle browsing on
  # successive ~30-minute idle slots; never trigger additional HTTP turns.
  SOCIAL_SOURCE="ranked"
  SOCIAL_PAGES=1
  if (( ($(date +%s) / 1800) % 2 == 1 )); then
    SOCIAL_SOURCE="circles"
    SOCIAL_PAGES=2
  fi
  (cd "$REPO" && timeout 5s python3 -m patrol \
    --config "$CONFIG" --db "$DB" social-sync --if-idle-due \
    --source "$SOCIAL_SOURCE" --pages "$SOCIAL_PAGES" \
    --limit 10 --max-new 3 >/dev/null 2>&1) || SOCIAL_EXIT=$?
  log "phase6 idle discovery exit=${SOCIAL_EXIT}"
else
  log "phase3 hook skipped: prior invocation still active"
  silent "previous Hook turn is still active" '{"status":"busy"}'
  exit 0
fi
exec 9>&-

# HATCH native wake() is best-effort by default. A configured 'wired' flag
# never proves Host acceptance. Runtime persists notification intent/cooldown;
# only an actual Host Adapter can record a reliable acceptance receipt.
HOST_CAPABILITY="${CREWQUAY_HOST_CAPABILITY:-best_effort}"
if [[ "$HOST_CAPABILITY" != "best_effort" && "$HOST_CAPABILITY" != "reliable_ack" && "$HOST_CAPABILITY" != "unavailable" ]]; then
  silent "unsupported Host capability" '{"status":"host_unavailable"}'
  exit 0
fi
if [[ "$HOST_CAPABILITY" = "unavailable" ]] || ! type wake >/dev/null 2>&1; then
  silent "host wake function unavailable; no wake lease acquired" '{"status":"host_unavailable"}'
  exit 0
fi

# Runtime chooses and cools down the work intent; in best-effort mode it
# deliberately does NOT acquire a Host ACK lease. Model execution is separate.
if [ ! -f "$DB" ]; then
  silent "sqlite unavailable" '{"status":"unavailable"}'
  exit 0
fi
PREPARED=""
if ! PREPARED="$(cd "$REPO" && timeout 8s python3 -m patrol \
    --config "$CONFIG" --db "$DB" host-wakeup-prepare \
    --owner "$HOOK_OWNER" --host-capability "$HOST_CAPABILITY" 2>/dev/null)"; then
  silent "wakeup reservation unavailable" '{"status":"deferred"}'
  exit 0
fi
DECISION="$(printf '%s' "$PREPARED" | python3 -c \
  'import json,sys
d=json.load(sys.stdin)
print("wake" if d.get("should_wake") is True and
      d.get("status") in ("reserved","notification_intent") and
      d.get("lease_version") else "silent")' \
  2>/dev/null)" || {
  silent "invalid wakeup handoff" '{"status":"invalid"}'
  exit 0
}
if [ "$DECISION" = "wake" ]; then
  # The Host's return/exit is NOT acceptance evidence. In best-effort
  # mode the Agent legitimately consumes the notified work using a live
  # Scheduler+Queue generation; it does not forge a Host ACK.
  wake "CrewQuay Muse work ready: run muse-consume using wakeup_id and lease_version from this notification; check host_capability, fence all actions and require exact confirmed Outbox before muse-finish" "$PREPARED"
else
  silent "no due unleased wakeup intent" '{"status":"idle"}'
fi
