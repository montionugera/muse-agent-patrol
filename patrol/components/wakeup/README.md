# Wakeup intent ownership

## Purpose
Use the existing SQLite as the sole durable source of wakeup intent, with strict idempotency and lease fencing.

## Responsibilities
Stage high-priority human_action and due promise intents, export a read-only feed, atomically claim the next due host dispatch attempt (claim_next), and record confirmed delivery/failure/unknown results. Track independent real-Worker execution receipts (`started`, `progress`, `finished`) after either a valid best-effort notice **or** actual verified reliable Host acceptance; neither mode fabricates an ACK. Alert on stalled accepted-but-never-started and best-effort started-without-progress requests. For registered actionable work, the host admission score now considers the same deadline-aware Scheduler priority without dropping the previous base-priority threshold.

## Non-responsibilities
This package does not implement a Muse host Hook, a Cron installer, model execution, CrewQuay writes, permission grants or task completion. Acknowledging host acceptance is not proof that Muse actually worked.

## Public boundary
`index.py:Wakeups`. Other components import only `wakeup.index`.

## Allowed dependencies
`persistence.index`, `scheduling.index`'s public priority policy and published SQLite tables.

## Invariants
One wakeup logical key is staged once. Leased delivery has owner/version/expiry. An expired attempt whose business source still needs attention becomes `unknown`, never automatically retried without proof of non-delivery. An expired `leased` attempt whose work was already completed (or whose promise is closed/rekeyed) is atomically marked `obsolete`, not left indefinitely `leased`; active leases and acknowledged deliveries are not cancelled. Light and Full ticks both run this reconciliation. A blocked business work item may need attention but does not grant permission. Claiming intent never invokes an API.


## Idle social source (Phase 6.3)

The same `wakeup_requests` table accepts `source_type=social`, backed only by a
previously admitted `social_intents` activity. `stage_social` creates at
most **one** outstanding wake request when no activity is currently running.
A social signal has a lower priority than a real work/promise wakeup and
cannot be claimed unless HostHandoff explicitly permits it. The same social activity never creates an independent second wake signal;
in best-effort mode the current notice is cooled down and the actual Worker
start is recorded independently, whereas a reliable delivery lease lacking
acceptance becomes `unknown` and is never blindly redelivered.

Existing database records, versions, attempts, dispatch evidence and status
are preserved by an atomic additive wakeup table CHECK migration from
`work/promise` to `work/promise/social`; schema version is now 2.
This migrates local Patrol storage, not CrewQuay server state.

## Execution evidence and alerting

`wakeup-ack --result accepted` is only an independently verified reliable Host delivery receipt, not a best-effort requirement. After a genuine best-effort notice or verified reliable acceptance, real Muse starts a live Scheduler activity and may run `wakeup-execution WAKEUP_ID --wakeup-generation N --activity-id ID --activity-generation N --worker NAME --phase started|progress|finished --evidence FILE`. The receipt validates that the wake source and live Scheduler generation match; `finished` requires a separately completed Scheduler activity. `execution_health` runs during the **existing** Tick, persists idempotent alerts after 300 seconds without observed worker progress, and never retries or starts a model. If the host can **positively prove the previously accepted Muse session never started**, an operator can run `wakeup-reconcile-accepted WAKEUP_ID --proven-not-started --evidence FILE` to make that same intent retryable. This is rejected after any started/progress/finished worker receipt and preserves the original acceptance evidence. Absence of a progress receipt **alone** is not proof of non-execution. No new cron or watchdog is installed. If the host does not call this command, alerts mean **execution not evidenced**, not proof that no model ran.

## Testing boundary
Temporary databases, fake host operations only. No external credentials or live writes.
