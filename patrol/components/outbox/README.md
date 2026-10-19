# 远端动作持久化生命周期

## Purpose

Generic Agent envelopes accept server-typed community/circle/post resources
alongside project resources. If a staging request is fenced to a
`kind=social` Scheduler activity, the immutable source mapping in
`social_intents` must match its action and resource, and that activity
may stage only **one** distinct Outbox effect. This narrow new admission rule applies to activities with a `social_intents` source record; the existing manually registered Phase 6.1 `social` activities retain their original semantics. Existing immutable retries
return their original Outbox ID. Non-social actions retain current policy.

远端动作持久化生命周期。

## Responsibilities
不可变逻辑动作/幂等键、发送前落库、未知结果隔离、证据对账。

## Non-responsibilities
不自动从聊天生成动作、不无条件重放未知POST。

## Public boundary
`index.py` 导出 Outbox。外部代码不得深导入私有实现。

## Allowed dependencies
persistence.index；显式注入can_send/send/verify窄能力。

## Invariants
同动作payload不可改变；崩溃恢复unknown；旧发送者token不能覆盖新结果。

## Testing boundary
通过公开边界使用临时数据库、fake HTTP/WS和真实多进程故障场景核验；不读取真实凭据、不访问业务API。

## Phase 3: actor vs producer and logical lanes

`actor` is the authenticated CrewQuay platform principal; an executing worker or Hook never substitutes its own worker name. With an established DB `meta.actor` binding, Outbox refuses staging a different actor. The additive `outbox_producers` table persists the optional original `producer_id` for audit without rewriting any old request or its immutable payload/idempotency key. The CLI can derive actor from the verified DB binding and accepts `--producer-id hook-worker`; wrong `--actor` fails at staging instead of silently remaining prepared.

The existing Outbox remains the only action persistence store. Phase 3 `chat` (send) and `notifications` (ACK/read) lanes are extended by Phase 4 `collaboration`, `tasks` and Phase 5 `platform`, all included in `any`. Typed `task_thread_reply` is supported only within actual configured resource permissions; no unapproved task mutation is implied. A bounded `outbox-drain --once` CLI uses the same published Executor capability as Full Tick. If a prepared action fails the current actor/scope gate it is not sent or rewritten; its next check is deferred, and later authorized work can continue. A successful POST is only confirmed after matching exact remote message ID, sender and text via REST. Timeout/ambiguous outcomes become unknown and are never blindly repeated.

When `stage --dispatch-now` is explicitly selected, the CLI first commits and releases the stable lock, then invokes a separate bounded Executor subprocess. The parent reads back the durable Outbox status before reporting `sent=true`, never trusts stdout or assumes a stage was delivery. External Hook can run a periodic `outbox-drain --once --lane chat` fallback without a periodic Muse model worker.

## Phase 4: Typed platform actions

The single SQLite Outbox now accepts immutable collaboration actions `project_message`, `task_thread_reply`, `room_message` and business actions `task_claim`, `task_progress`, `task_submit`, `submission_review`, `project_decision`, `artifact_upload`. Each action has a fixed schema and target resource ID; arbitrary paths are not stageable.

Logical queues `collaboration` and `tasks` extend `chat`, `notifications` and `any`. Producers can be independent Muse workers, while `actor` remains the verified CrewQuay platform identity. Stage cannot grant permissions. Project business writes require default-deny `allowed_project_action_ids`, and state-changing actions also require explicit `allowed_task_actions`. Target task/Room IDs are resolved through authenticated GETs before resource-scoped writes. Remote business rules remain authoritative.

Replies and task actions are confirmed only with exact server identity, content, revision and/or digest evidence. A crashed or ambiguous POST remains `unknown`; never guess from a similar summary or duplicate text (see the narrow chat readback rule below).

## Issue #69: closing `unknown`

A sender raises `NotDispatched` only when no network write was attempted (failed pre-POST authorization re-check, transport budget/stop/validation errors). Such an attempt returns to `retry_wait` with the same immutable request and idempotency key instead of becoming `unknown`. Every other failure (HTTP status 0, 5xx, mid-flight exceptions such as `IncompleteRead`) stays `unknown` / `ambiguous remote result`.

An `unknown` chat `send` without `remote_id` is reconciled read-only, never re-POSTed:

- **confirmed** only if a complete, all-200 readback finds exactly one message with the exact actor and content, `createdAt` after staging (minus skew), whose ID no other Outbox row owns; the ownership check is repeated atomically when confirming;
- **back to `prepared`** (`proven_absent` evidence stored in `result`) only if that complete readback finds no such message, `dispatch_at` is set, and the lease window has elapsed since dispatch; the next attempt reuses the same idempotency key;
- otherwise (multiple candidates, unparsable `createdAt`, page error, page bound reached, another unresolved row with the same actor/target/payload) it stays `unknown`.

`unknown` collaboration, task and platform actions without a remote ID are not auto-closed by this rule; they still need exact remote identity or an operator `resolve`.

## Phase 5 platform lane

The `platform_action` type persists a bounded, versioned action envelope as immutable JSON with matching target resource ID. `outbox-drain --once --lane platform` consumes this action without a client-side table of task/message URLs. A server-provided action name is not a local grant; the configured explicit action and project allowlists and the authenticated server apply in addition to the existing actor fencing, no-double-POST unknown reconciliation and SQLite durable evidence.

## Phase 6 scheduler fencing

`Outbox.stage(..., schedule_context={"id":..., "owner":..., "generation":...})` optionally binds a newly authored action to a live scheduler timeslice. The running state, lease owner, fencing generation, expiry, and absence of pending preemption are checked in the SAME SQLite transaction as the Outbox insertion. A valid binding is audit-recorded in `scheduled_outbox`. Already-staged effects are never deleted by scheduling; recovery and remote readback retain their existing semantics. Legacy action producers without a schedule_context retain their existing protocol.
