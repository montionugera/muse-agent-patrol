# Phase 6.1 — Cooperative Activity Scheduler

## Purpose
Dispatch *local Muse activities* with deadline-aware weighted fairness and safe-point preemption. A schedule activity is NOT a CrewQuay platform task assignment, an external model invocation, or an Outbox action.

## Responsibilities
- Maintain durable activity states (ready, running, suspended, completed, cancelled), runtime-based weighted virtual fairness and bounded waiting-time aging.
- Derive candidate priority dynamically from task urgency, wait duration, service consumed, and context-switch cost; use a finite time slice and hysteresis for preemption decisions. The waiting credit grows after the initial ramp up to a bounded total of 60 (both aging terms cap at 30, saturating after 45 minutes of waiting), so moderately lower-priority work can compete without any single term dominating preemption; very large priority gaps are intentionally not overcome by aging alone and absolute starvation-freedom is not guaranteed; an imminently due running activity remains protected from non-urgent challengers.
- Persist preemption requests without killing model processes. A live owner voluntarily yields at a checkpoint boundary; suspension and resumed leases increment a fencing generation. Support optional strict v1 Checkpoint envelopes and read-only `schedule-restore` packs that inspect referenced Outbox statuses before an Agent decides to resume.
- Import actionable local `human_action` work by stable work ID, without claiming, completing, or granting access to CrewQuay tasks.
- Register non-`human_action` due promises as one-shot `promise_due:<id>:<next_action_at>` activities on the **same Scheduler**, so an accepted promise wake has runnable work. Advancement/completion changes the generation; `schedule-complete` rejects an unchanged open promise without recorded progress.
- Expired local leases become resumable with explicit reconciliation warning. A linked locally claimed Queue lease is released and fenced atomically at a safe-point yield or Scheduler lease expiry. Neither scheduling nor suspension mutates a remote side effect.

## Non-responsibilities
No model wakeup, OS process cancellation, forced LLM interruption, platform API calls, dynamic permission elevation, automatic approval, deletion of existing Outbox, rollback of remote operations, or claim of token-level model-state recovery.

## Public boundary
`from patrol.components.scheduling.index import Scheduler`: `register`, `sync_work`, `assess`, `dispatch`, `checkpoint`, `complete`, `cancel`, `snapshot`. CLI `schedule-add`, `schedule-sync`, `schedule-assess`, `schedule-next`, `schedule-checkpoint`, `schedule-restore`, `schedule-complete`, `schedule-cancel`, `schedule-status`. Once any Scheduler activity exists, **new business Outbox actions require** `stage --schedule-id --schedule-owner --schedule-generation`; immutable previously staged actions are still retrievable. The system-only ACK/read lane is exempt. `work-claim` returns these Scheduler fields for linked work, including operator-evidenced handoffs of blocked items.

## Owner message liveness / Issue #31

`Scheduler.dispatch` now refreshes pending `human_action` items and effective message scores before selecting a time slice; waking Muse does not depend on an earlier Tick having synced its latest work. Fresh high-priority Owner messages therefore enter the same dynamic priority queue immediately. Bounded work synchronization considers higher priority entries first; it does not redefine CrewQuay roles or force Owner messages above genuine deadline-critical work.

For a specific urgent message, run `schedule-next --owner WORKER --for-work-id WORK_ID`. This is a **selection guard**: it grants only if the target is pending/due and is the highest eligible activity. If another activity is still running, it returns `granted=false` with preemption advice. If the work is blocked, not due, completed, or outscored, the result includes `hint_status`/`requested_activity` and dispatches nothing else. `schedule-next` itself never claims a Queue lease; next use `work-claim WORK_ID --owner WORKER --authorization-evidence ...` and stage only with the returned Scheduler context.

If a previous activity is live and should yield, its **current lease owner** uses `schedule-checkpoint ... --yield` at a real safe point. If that worker has crashed and the lease expired, a later `schedule-next` recovers the old Scheduler and matching Queue lease atomically, fencing both generations before dispatching again. A previously damaged handoff where Scheduler is ready/suspended but the linked Queue row is still `running` is also safely recovered **only once that Queue lease itself has expired**; a live Queue owner is never stolen. A stale generation must not `checkpoint`, `complete` or create new Outbox writes; instead inspect `schedule-status` / `schedule-restore`, check already-staged effects by original logical key, and acquire a fresh lease. Never synthesize a dummy checkpoint, replace an Outbox logical key or bypass `StaleLease`.

## Allowed dependencies
`persistence.index` shared SQLite transaction schema. Scheduling has no private dependency on Queue, Wakeups, Outbox, or Runtime. Runtime composes it through the public index.

## Invariants
At most one running activity per local DB (SQLite partial unique index). Only the current lease owner and generation can checkpoint/finish or start a fenced action. Preemption itself is a **request**, not an involuntary cancellation; a safe-point checkpoint fences the old generation. Fairness is based on actual wall-clock duration of the granted slice, not unverified user-reported model tokens. Restart after lease expiry preserves checkpoint, fences the old Queue and Scheduler lease owners, and reports that already-staged external effects need their own Outbox readback. Checkpoint is a bounded structured progress snapshot, **not** an LLM token or full dialog state; actual worker restoration remains a separate host contract. A withdrawn challenger does not force a stale safe-point yield. Project permissions and work-claim authorization are always separate.

## Versioned restore contract

Old arbitrary JSON Checkpoints remain readable as `checkpoint_format=legacy`. A v1 Checkpoint uses exactly `schema_version=1`, `goal`, `phase`, `completed_steps`, `next_actions`, `decisions`, `resource_refs` (typed IDs, never executable prose), `pending_outbox_ids`, `source_task_version` (nullable), and `saved_at` (UTC epoch). It remains bounded to 32 KiB. `schedule-restore ACTIVITY_ID` returns a **read-only** pack with pending Outbox receipts and a warning to revalidate every external resource under current permissions. It never restarts the model, authorizes work, or replays an external operation. Expired source versions require verification in CrewQuay, not blind continuation.

## Testing boundary
Isolated temporary SQLite tests cover EDF urgency, weighted virtual runtime, aging, hysteresis, no competitor time slice, lease expiry, stale-generation rejection, exact Outbox stage fencing, atomic cooperative handoff, crash checkpoint recovery, idempotency and existing Phase 2–5 regressions. No credentials or live CrewQuay API.
