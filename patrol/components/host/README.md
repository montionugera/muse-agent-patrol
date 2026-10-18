# Host handoff boundary

## Purpose

Let the bounded Muse Runtime return a **durable** Host wake intention
to the single native HATCH 60s Hook. With best-effort HATCH wake() we must
not reserve an ACK-gated Wakeup lease; reliable adapters can reserve one. This is an incremental host integration
of the existing SQLite Wakeup and Phase 6.1 Scheduler facilities, not a
replacement scheduler, additional timer, or second wakeup queue.

## Responsibilities

- `HostHandoff.prepare(owner,now,capability="best_effort"|"reliable_ack"|"unavailable")`
  reconciles work. In best-effort mode it atomically throttles notices in
  `host_wakeup_notifications` without ever recording fake Host acceptance.
  Reliable mode separately reserves a due Wakeup delivery lease.
- Return only opaque IDs, lease generation/expiry and activity IDs. Never
  serialize message content, checkpoint text, access tokens or API secrets.
- `runtime-cycle` owns scheduling, notification advice, Outbox compensation
  and idle discovery. The native Hook calls exactly that CLI and, if advised,
  the real Host `wake()`. Agent-owned deployment is not in this Hook.
- Muse / host must acknowledge **verified delivery** using the existing
  `wakeup-ack --result accepted --evidence RECEIPT_JSON` after an actual
  host acceptance receipt is available. If no receipt is available, the
  leased attempt expires as `unknown` and must be reconciled with proof
  of non-delivery. It is not silently retried or marked accepted.
- After real host acceptance, the worker records `wakeup-execution` receipts for verified start/progress/finish (see wakeup component contract). Missing or stalled receipts generate persistent alerts on the original 60-second Tick but **never** automatically replay an accepted wake. These receipts do not prove successful remote side effects.
- Once awake, the real Muse worker uses existing `schedule-next`,
  `work-claim` (where appropriate), `schedule-assess`,
  `schedule-checkpoint`, `schedule-complete` and generation-fenced
  `stage`. A Hook reservation is **not** an activity time slice.


## Social handoff

`HostHandoff.prepare(owner,now,allow_social=True)` includes low-priority
admitted social activity by default **only** if no Scheduler activity is
running and the highest-priority eligible candidate is social. Reliable ACK
uses the original `wakeup_requests` lease, while best-effort uses a separate
bounded notice with no fabricated Host accepted receipt; real Muse can consume
social and explicitly skip or finish one confirmed authorized action.
No Scheduler time slice is claimed by this call; it neither starts nor
implicitly authorizes a Muse model. Identity-bound discovery uses the
existing transactional 30-minute throttle. Legacy callers can still pass
`allow_social=False` for an explicit work-only operation.

## Safe recovery inspection for previously answered work (#47)

Before replaying any wake, claiming a stale work lease, or attempting a new
remote message, the Agent must use the **original agent's SQLite**:

```bash
python3 -m patrol --config /path/to/private-config.json muse-recovery-report --work-id ORIGINAL_WORK_ID
```

This read-only report joins the exact work ID, Scheduler lease/generation,
Wakeup and Worker execution receipts, and `scheduled_outbox` -> `outbox`
request identities. A `confirmed` Outbox entry with a verified remote ID
indicates a remote effect that **must not be sent again**, but it does not
automatically mark the work `done`. An `unknown` entry must first be
reconciled using its original request identity. An unrelated platform cursor,
similar message text, or `accepted` Host receipt is never completion proof.

The read-only report always returns `safe_to_force_complete=false`; that
field is not a license to bypass generation fencing. If the report shows that
**every exact Scheduler-linked effect is already `confirmed` with a remote ID**,
the original Worker also has a verifiable started/progress receipt, and the
original Host wake intent remains valid, the **same Worker** may invoke:

```bash
python3 -m patrol --config /path/to/private-config.json muse-recover-confirmed \
  --wakeup-id ORIGINAL_WAKEUP_ID --wakeup-generation ORIGINAL_GENERATION \
  --worker ORIGINAL_WORKER_ID
```

This bounded local-only recovery uses normal Scheduler ranking/Queue
reclaim, records a new-generation Worker progress receipt, and invokes
`muse-finish`'s existing atomic completion checks; it **never** invokes a
platform POST or stages a new Outbox entry. A different Worker, incomplete
or unknown Outbox effects, missing receipt, stale intent, or competing higher
priority task must fail closed/defer. A repeated command after completion
is a read-only idempotent acknowledgement. If original links cannot be
proven, report the blocker and request original Agent-side evidence instead
of inferring it from message text or platform cursor.

The original production incident still requires Agent-side evidence-based
acceptance; synthetic SQLite tests do not prove a remote message was sent.

## Non-responsibilities

No model invocation, runtime installation, implicit work authorization,
host-acceptance fabrication, automated social message generation, or
assumption that a running model can be forcefully interrupted. Only the
host can confirm actual wake acceptance and run a model.

## Public boundary

`from patrol.components.host.index import HostHandoff`.

## Allowed dependencies

Only published `scheduling.index` and `wakeup.index` plus the supplied
SQLite capability; no private component imports or network clients.

## Invariants

A reserved wakeup intent is leased exactly once per generation and not
retransmitted after a crash without explicit absence evidence.
Scheduler activity lease owner/generation remain distinct from host wakeup
lease owner/version and from CrewQuay roles/assignments. Existing schema and
all older wakeup/Outbox receipts are retained. Social intents can already be
admitted to the shared durable wakeup queue, but the repository **does not**
install or activate an external Host `wake()` bridge. The reference Hook uses `runtime-cycle` and a capability-aware Host
handoff. Native `wake()` alone is `best_effort`, never reliable acceptance.
It must not consume reliable delivery leases or mark activity accepted.

## Tests

Temporary SQLite tests exercise reservation arbitration, stale-source
retirement, deferred work, no-duplicate delivery and local-only scheduler
hints; no HATCH host or CrewQuay network access.
