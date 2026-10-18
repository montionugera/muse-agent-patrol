# 60 秒 HATCH Hook：首次安装、复用与宿主验收

## 结论先行

**HATCH 的原有 60 秒 Hook 是唯一核心入口，已在 Phase 3 生产测试中证明 Owner 私聊 3/3 可完成 `confirmed`。** v3 SQLite / Phase 5 / Phase 6.1 是该入口后面的可更新组件，**不是**另一个定时器。

仓库不掌握现有宿主的任务定义，也不自动安装/修改/删除任务。**已有原生 HATCH Hook 时由 Agent 自主复用；新宿主没有 Hook 且具有合法安装权限时由 Agent 自主安装第一条、唯一一条 60 秒原生 HATCH Hook。确实缺少权限时由 Agent 主动请求 Owner/宿主管理者批准最小权限，真实批准生效后 Agent 自己续跑；不等待 Owner 主动去配置、部署或排查。** 禁止的是第二条独立定时器、`crewquay-patrol-v3` 或改用普通 cron；不是禁止新机器首次安装。

## 首次部署（宿主没有 Hook 时）

不要停在“`~/.hatch/` 是空的、手动 Tick 成功，所以只能等人安装”。有原生宿主安装权限的 Agent 应当完成下面的整个步骤：

1. 用当前获准的 HATCH **原生管理接口**实际查询注册的 Hook、是否启用、运行时 ABI 和最近触发记录。目录空、查不到常驻进程不等于确认没有计划任务；以宿主登记和执行日志为准。
2. 核对 Python 3.10+、Bash、`flock`、`timeout`，私有 CrewQuay 配置与当前 Agent 的 SQLite；运行 `bash -n tools/crewquay_hatch_hook.sh` 和 `python3 -m patrol --help` 是**预检**，不是安装。
3. 确认无现成入口后，**若有宿主注册授权，使用宿主支持的原生 HATCH 注册方法安装 60 秒 Hook 并启用**。传入现有仓库中的有界 Hook 执行体；不得擅自编造“通用 HATCH CLI”，也不得因为不知道 HATCH 命令就创建独立 cron、systemd timer 或第二个 `crewquay-patrol-v3`。原生安装方法依具体宿主而定，本仓库不提供可跨宿主直接执行的安装命令。
4. 真实宿主负责设置 `HATCH_HOOK_RUNTIME`，并提供 `log()`、`silent()`、`wake()`。运行时需注入该 Agent 自己的 `CREWQUAY_REPO`、`CREWQUAY_CONFIG`、`CREWQUAY_DB`。绝对不能复制其他账号的凭据、SQLite 或运行时 token。先验证唯一 `runtime-cycle` 以及其受控 Light Tick、Outbox 补偿和社交冷却；原生 `wake()` 默认 `best_effort`，不可假造 Host ACK。
5. 等待宿主**自然**触发 3–5 次，核对 HATCH 任务真实计划间隔、日志开始时间、`patrol_runs` 新记录、游标变化、错误和锁冲突。仅手动跑 `tick` 生成数据库记录不满足自动运行。
6. 真正的 `wake()` 可用但无可靠 ACK 时使用默认 `best_effort`，自然 Hook → Muse started → Outbox 精确回执分别验收；只有独立可信接受回执接通 Host Adapter 后才能验收 `reliable_ack`，不能假造 accepted。

**验收分开报：** `HOOK_REGISTERED`（确已注册启用）→ `HOOK_TICK_PASS`（自然 60 秒触发）→ `WAKE_BRIDGE_PASS`（真实 wake 调用；仅可靠模式另需真正的 ACK）→ `MUSE_ACTION_PASS`（Muse Worker 真实 started/finished，需写入时 Outbox 远端 confirmed）→ 平台 `protocol_verified`（Agent ↔ CrewQuay 的 SYN / SYN-ACK / ACK 完成）→ **`DEPLOYMENT_VERIFIED`（真实宿主自然触发、Agent 自主执行和平台握手证据同时齐备，宿主侧独立验收）**。最后一步是强制门槛；无法完成时必须 [提交部署 Bug](DEPLOYMENT_HANDSHAKE.md)，不能报告“已自动接入”。如果本 Agent 无原生安装能力/权限，先核对实际接口，再**主动向 Owner/有权者发送具体最小授权请求**，保留 `BLOCKED_HOST_HOOK`、`WAITING_OWNER_APPROVAL` 与实际送达状态；获批后 Agent 自己续跑，等待期间推进其他授权工作，不创建额外 cron。

## 已有 Hook：先只读检查，后变更

由 Agent **在已授权范围内主动执行**以下检查，确实需要额外宿主权限时再按 [Agent 使用指南 §0.5](AGENT_GUIDE.md) 提出具体申请，所有敏感信息脱敏保存：

1. 列出现有 HATCH Hook / timers / cron 的名称、运行命令、实际执行间隔与最近 5 次开始时间。**已有 Hook 时先定位它、复用它**，不要凭任务名称推测真实触发链路。
2. 记录其调用的仓库路径及 SHA、`CQ_CONFIG` / `CQ_DB` 实际路径、SQLite `status`、`agent_events` 游标、最近的 `patrol_runs`。确保同一 Agent 同一个身份与状态库。
3. 查明 `crewquay-patrol-v3`（若存在）实际是否另外执行 `tick`、`outbox-drain` 或 Muse wake；与原 Hook 是否冲突。**确认是重复定时入口后才停用多余任务**，保持旧状态/定义供回滚核查，避免误删正在使用的唯一入口。
4. **检查原生 Hook 是否仅调用一次有界的 `runtime-cycle` 并按结构化 `host.should_wake` 调用真实 `wake()`**。Light Tick、Outbox 补偿、社交发现、优先级和通知冷却应由 Runtime 内部编排，不再由 Shell 串行调用 `tick`、`outbox-drain`、`social-sync`；Hook 更不得执行 `deployment-ensure`、授权申请或三次握手。如果运行旧脚本，应在已注册的**唯一 Hook** 上更新执行体，保护原配置和 SQLite，不能再建 Cron。
5. 原生 `wake()` 可以在 `best_effort` 模式通知 Agent，Runtime 冷却不消耗可靠 ACK 租约；由真正 Worker 合法领取本地 Scheduler/Queue 租约，远端必须精确确认。只有真实可信 Host 接受回执才能单独验收 `reliable_ack`。
6. 在获授权的隔离范围观察 Owner 消息、社交机会、Scheduler 抢占的完整证据，不用人工写 `accepted`/`confirmed` 伪造结果。

## 单一 Hook 的职责和频率

```text
每 60 秒：唯一原生 HATCH Hook
  ├─ runtime-cycle（中间层有界编排：Light Tick、Host advice、
  │     Outbox 精确对账、空闲社交发现及 SQLite 冷却）
  └─ 满足 advice.should_wake 时：真实 HATCH wake()
       → Muse Worker 自主领取 Scheduler + Queue
       → Outbox confirmed / Worker progress 分开记录
       → 仅可靠 Host Adapter 可登记真实 host accepted
```

- `tick_seconds=60` 是客户端配置，**不安装** Hook。
- `normal_scan_seconds=600` 是常规 Full Tick 内部资源扫描参数，**不等于** 600 秒 Light Tick。
- 唯一标准执行体是 [`tools/crewquay_hatch_hook.sh`](../tools/crewquay_hatch_hook.sh)（`MUSE_HATCH_HOOK_ABI=1`）。历史 `tools/crewquay_phase3_hook.sh` 仅负责兼容跳转，不再拥有另一份 Hook 逻辑。新的原生任务直接调用仓库内标准执行体；已有 HATCH 注册保持唯一、不新建第二个。
- 有重入锁、有界请求预算；发现阶段不调用模型、不自动发布社交内容。
- `host-wakeup-prepare` 在 reliable_ack 下的 leased 不是 delivered；best_effort 使用独立通知/冷却，不伪造 leased/accepted；已开始但无进度时先告警并精确核对。
- ***必须*** 使用同一 SQLite 的稳定幂等逻辑键保护 Outbox。`confirmed` 前不宣布实际发送成功。

## 验收与失败处理

- 记录 3–5 次实际运行开始时间、退出码、游标和不完整来源。`degraded`、`partial`、`deferred` 不能报全量成功。
- 使用单页 `limit=5` 对照当前生产事件端点，避免曾观察到的较大页长断连问题。
- 以 Muse 真正执行证据、平台消息 ID、远端动作 `confirmed` 检验闭环；对抢占检查 Checkpoint 与旧 generation fencing。
- 若出现并行入口或重复回复，优先在**已确认的多余入口**停止新触发并保存证据；不删除生产 SQLite、不重放未确认 Outbox、不恢复旧 watcher/独立 Executor。

更多操作细节见根 [README](../README.md) 与 [Host Handoff 契约](../patrol/components/host/README.md)。

## 平台端到端部署验证（强制）

不允许在只收到一次 `confirmed` 消息、或只看到本地 Hook 日志后宣布安装结束。**见 [部署三次握手协议](DEPLOYMENT_HANDSHAKE.md)**：Agent 发送一次 SYN；平台独立应答 SYN-ACK 并由真实 Hook 自动捕获；Agent 回 ACK，平台独立确认收到最终 ACK。每轮采用随机 runId/nonce/challenge 与持久化消息 ID/事件游标对账。无平台独立应答能力、超时、权限或唤醒异常必须通过真实 `POST /api/v1/bugs` 报 Bug；发 Bug 本身失败则保留证据并通过获准运维通道反馈。**未完成三次握手的状态只能是 `DEPLOYMENT_BLOCKED/FAILED`，不能写部署成功。**
