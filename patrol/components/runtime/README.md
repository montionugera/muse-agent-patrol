# CLI与有界tick组合

## Purpose

`social-sync` is an explicit, bounded **read-only HTTP GET + local
Scheduler admission** command, separate from event-only Light Tick.
`social-detail` reads an already admitted activity's exact resource and
suggested Action Protocol name. No host/model invocation or remote write.

CLI与有界tick组合。

## Responsibilities
稳定文件锁、预算次序、窄回调连接、能力不足blocked、状态/doctor。

## Non-responsibilities
不安装服务、不部署、不托管LLM、不替成员正式claim/approve。`export-pending` 不唤醒模型；`work-claim`/`work-finish` 只是携带本地租约版本的显式操作，不代替服务端审批。

## Public boundary
`index.py` 导出 main/Tick/instance_lock。外部代码不得深导入私有实现。

## Allowed dependencies
仅兄弟组件index；runtime内部composition/process私有实现。

## Invariants
缓存有已核实 actor 身份时，必须先用本地预算执行到期任务，再进行任何远端身份校验/分页，避免网络 BudgetExceeded 抢走本地 inspect_event 的执行机会；没有核实身份不能默默将消息标记为 done。默认只读；所有旧入口转此边界。全局游标未到期时，独立到期的消息或项目游标也必须唤醒；分页或预算未覆盖全部资源时返回 partial，不得虚报 ok。

## Testing boundary
通过公开边界使用临时数据库、fake HTTP/WS和真实多进程故障场景核验；不读取真实凭据、不访问业务API。


The existing Hook performs idle social discovery by default, without
`CREWQUAY_SOCIAL_IDLE_ENABLED` or `allow_community_actions`. It
requires an already verified Agent identity, checks pending work/promises,
and uses a durable 30-minute cadence for a **read-only** GET.
`host-wakeup-prepare` includes eligible social wakeups by default using
the same durable Wakeup intent table. With native best-effort wake it records
a throttled notification, **not** an ACK-gated delivery lease; verified
reliable Host adapters may reserve a delivery lease. No second timer exists.
Sending community actions still requires the existing global
`allow_writes=true` authority and exact CrewQuay catalog/resource checks.
Existing `--include-social` is accepted only for CLI compatibility.

## Phase 5 event-only Light Tick

`Tick.run(light=True)` uses the server-provided authenticated `/agent/events` feed exclusively for all remote discovery. No chats, Inbox, tasks/projects, old event-waterline probes, WS, or remote writes are executed as part of its normal event collection. The feed persists authorized message snapshots and non-message platform pointers as local inspect work; the same work/attention/score/wakeup components remain. Cursor pagination, backoff and incomplete-source reporting are independent of the Full Tick `events` checkpoint. Full Tick keeps its older resource sync/resync capabilities for completeness and compatibility.

## Validation BUG-1 recovery
Light / Full ticks report malformed collector pages as `degraded` with structured per-source error evidence and unchanged cursor. Unexpected errors escaping collectors result in `failed` (with exception class only, never external exception text) and are recorded in `patrol_runs`, instead of reporting a misleading `partial` when an exception aborted a tick. SQLite `Busy` still propagates to the existing CLI status and exit 75. No live write is enabled.

## Phase 3: separate execution from Muse decisions

`stage --producer-id WORKER --dispatch-now` records an immutable authorized action first, releases `.tick.lock`, then runs a bounded separate `python -m patrol ... outbox-drain --once --id ACTION_ID` process. It does not grant the Muse worker platform authority and never sends an action from Light Tick. The Executor must GET and verify the CrewQuay actor and check target scope, ACK/read preconditions, exact remote readback and existing idempotency. `outbox-drain --once --lane chat|notifications|any --limit 3` is an independent bounded consumer, not another scanning scheduler. A Hook can invoke `--lane any` to compensate **all** due, authorized Outbox types without expanding any project/action grant; Full Tick remains a recovery path until its separate responsibilities have a verified replacement.

The runtime does not install background services or external job schedules. The **single native HATCH 60-second Hook** runs one bounded `runtime-cycle`, which internally composes Light Tick, Outbox compensation, SQLite-throttled social discovery, Scheduler and Host wake advice. The Hook then calls the **actual** host `wake()` only when advised; it does not run those component CLIs serially, initiate deployment/authorization or manufacture acknowledgements. For `best_effort`, the Runtime writes only a bounded notice with no reliable delivery lease or fabricated acceptance. `reliable_ack` requires a separately verifiable real Host receipt before `wakeup-ack accepted`; `unavailable` means no real Host wake path. `HostHandoff.prepare` returns routing advice, never invokes a model or grants a Scheduler time slice. Plain SHA upgrades do not trigger a new handshake; Agent-owned initial Setup and platform SYN/ACK are separate from each Hook tick.

## Phase 6.1 cooperative scheduler

`Tick` composes the `scheduling.index.Scheduler` public boundary and reports local `scheduler` work reconciliation and safe-point advice. It never dispatches models automatically. The `schedule-*` CLI commands own explicit local activity timeslices and checkpoints. A safe-point yield revokes the previous generation without touching `work_items` authorization or already-staged Outbox effects.
