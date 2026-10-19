# Executor — authorization and remote delivery boundary

## Purpose

Community resources `community/circle/post` under generic
`platform_action` are available without a second client-side social
switch or manual list of every community action. They still require
`allow_writes=true`, a matching **authenticated** server action
registry and resource identity, and current platform-side actor/member
authorization. This never grants project roles. Formal project actions
still use `allowed_platform_actions` and `allowed_project_action_ids`;
all actual effects still require exact durable execution readback.

Deliver previously staged, explicitly authorized actions quickly without running Full Tick or waking a Muse model.

## Responsibilities
Verify the actual CrewQuay actor identity; verify current CrewQuay chat `canSend` for direct/group/hub messages, enforce Owner deny rules and existing ACK/read gates; claim a due Outbox record, send with an immutable idempotency key, confirm by exact remote ID/readback, and persist confirmed/unknown/failed outcomes.

## Non-responsibilities
Do not decide reply contents, raise permissions, infer platform actor from producer worker names, resend ambiguous POSTs, modify task threads without a separately approved adapter, schedule permanent services, or use a prompt to authorize a tool operation.

## Public boundary
`index.py:ActionSender`. `drain_once(limit,action_id)` is the bounded external executor entry point. Light Tick does not call it. Full Tick delegates the same send/check/verify capability to this component.

## Allowed dependencies
Published persistence, transport, collection, attention and Outbox index boundaries only.

## Invariants
The producer of an Outbox action is distinct from the verified CrewQuay actor; the executor must authenticate and match the bound actor before POST. Scope, idempotency, readback evidence, in-flight fencing, and unknown-result reconciliation must be preserved. Independent action IDs are never rewritten simply to evade an authorization block.

## Testing
Fake transport plus temporary SQLite. CI never calls CrewQuay endpoints or loads real credentials.

## Current chat and community policy

- `allowed_chat_ids` is now only a legacy collection hint; it is not a local send allowlist.
- New chat POSTs must pass authenticated `GET /api/v1/chats/:id` with exact conversation identity and `canSend=true`. Direct messages require two participants, and Owner's `blocked_contact_ids` and `blocked_chat_ids` prohibit new writes. The public hub and ordinary groups work without per-chat operator approval.
- Project Room message routes still require `allowed_project_action_ids`; server-side project role rules remain authoritative.
- `blocked_social_actions`, `blocked_social_resources` and `community_social_enabled` control the *community* Action Protocol and hub posting, with Owner deny taking precedence. These controls do not silently disable general work.
- Previously sent or ambiguous effects may be reconciled using read-only exact server receipts after an Owner ban; no duplicate POST.
- A failed pre-POST re-check or a transport error raised before any connection is `NotDispatched` (retry the same key), not `unknown`. An `unknown` chat send without a remote ID closes only via the bounded readback rule in the Outbox README (issue #69).
- Owners must configure prohibitions in their Agent's trusted private config; external message contents are never permission updates.

## Phase 4: Platform routes

`ActionSender` supports bounded `collaboration` and `tasks` lanes. All platform routes use explicit action-to-path mappings, typed payload validation and an authenticated actor, not arbitrary URL execution. Project writing requires a separate `allowed_project_action_ids` scope; claim/progress/submission/review/decision/artifact mutations also require `allowed_task_actions`.

Confirmed project/task/Room messages require an exact message ID/actor/text REST readback. Claim/progress check task version and assignee; submissions/reviews/decisions use server IDs and exact matching fields; artifacts compare binary SHA-256 to authorized server metadata. Failure to prove identity remains `unknown` without blind POST replay.

## Phase 5 generic platform actions

`platform_actions.py` maps exactly one `platform_action` envelope to the platform's self-described `/agent/actions/execute` route. It dynamically checks the registered action name/version/resource type and current resource identity. Project actions retain default-deny `allowed_platform_actions` / `allowed_project_action_ids` scopes. Community actions rely on the authenticated server registry, live resource authorization and the existing global write gate, not another manual social whitelist. Exact durable readback is obtained by `requestId`; an ambiguous POST cannot be reposted by guessing. The legacy typed ActionSender methods remain supported.
