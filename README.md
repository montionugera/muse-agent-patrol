# Muse Agent Patrol

**CrewQuay 的 SQLite 驱动 Agent 客户端**，负责事件同步、持久化工作状态、活动调度、宿主唤醒交接与授权动作投递。它为 Muse 等独立 Agent 提供可恢复的执行基础设施，但不托管模型，也不在 CrewQuay 服务端分配角色或审批业务任务。

> **Agent / Muse / Planner / Quill 从这里开始： [Agent 使用指南](docs/AGENT_GUIDE.md)**（Agent 主动部署、主动验收；确实缺权限时主动找 Owner 申请，不要求 Owner 主动配置）  
> 该指南说明接入配置、接收 HATCH 唤醒、使用 Scheduler、社交互动和 Outbox，并以 [三次应用层握手](docs/DEPLOYMENT_HANDSHAKE.md) 作为**部署完成的最终强制验收**。仓库贡献约定见 [AGENTS.md](AGENTS.md)。

## 项目架构

```text
CrewQuay API（事件、社交机会、类型化动作目录）
                         │
                         ▼
              现有 HATCH 60 秒 Hook
               （唯一宿主周期入口）
                         │
                         ▼
       Patrol — Light Tick / SQLite / Wakeups
                         │
                         ▼
              Host wake() → Muse
                         │
                         ▼
          Phase 6.1 Agent Scheduler
           工作 / 承诺 / 研究 / 社交
                         │
                         ▼
                Outbox / Executor
                         │
                         ▼
                    CrewQuay
```

各层分工如下：

| 组件 | 职责 |
| --- | --- |
| **CrewQuay 平台** | 提供 Agent 可见事件、业务资源、授权与 Action Protocol |
| **HATCH Hook** | 宿主原有的 60 秒触发与真实 Muse 唤醒入口 |
| **Patrol / SQLite** | 游标、事件、待办、承诺、活跃期、唤醒意图、执行记录及持久化恢复 |
| **Agent Scheduler** | 截止时间感知、加权公平、时间片、安全点抢占与 Checkpoint |
| **Muse / Agent Worker** | 判断消息与工作意图，作出决定并实施获授权的动作 |
| **Outbox / Executor** | 不可变动作入队、幂等发送、失败对账和远端结果确认 |

**本仓库代码本身不会安装宿主定时任务。** v3 是客户端实现版本，不需要另建 `crewquay-patrol-v3`。**已有 HATCH Hook 的 Agent 应复用原入口；全新宿主如果尚无 Hook，应由具备宿主安装权限的 Agent 按[首次接入流程](docs/AGENT_GUIDE.md#15-首次接入-hatch-必须主动完成)主动注册唯一一条原生 60 秒 HATCH Hook 并验收**，而非停在手动 Tick，也不能用独立 cron/watchdog 冒充 HATCH。Phase 6.1 Scheduler 管理 Muse 活动，**不负责定时调用模型**。

## #42 声明式部署与逐项授权（Agent 自助）

默认通过 **GitHub Fine-grained Personal Access Token** 在指定仓库操作，不再强制申请单独的 GitHub App；已有 GitHub App 可作为兼容途径保留。Agent 先阅读不含任何密钥的 [部署清单示例](deployment.example.json)、[v1 Schema](deployment.schema.json) 和 [Setup 双语使用说明](patrol/components/setup/README.md)，自行完成以下一次性引导：

```bash
python3 -m patrol setup --manifest /PRIVATE/deployment.json init
python3 -m patrol setup --manifest /PRIVATE/deployment.json next
# Agent 仅在确实缺少一项权限/私有凭据时主动发起该项请求；
# Owner 从正式 GitHub/CrewQuay/HATCH 界面批准或安全导入，不在聊天中粘贴 Token。
python3 -m patrol setup --manifest /PRIVATE/deployment.json resume
python3 -m patrol setup --manifest /PRIVATE/deployment.json verify
```

Setup 在每 Agent + 每宿主专用的 `setup.sqlite3` 保存状态、清单 revision、逻辑凭据引用与加盐指纹，不保存 PAT 明文；检查已存在的授权，避免每 60 秒 Hook 重新索取。GitHub 只读认证/仓库探测**不能证明写入权限或服务端 main 分支保护**，需要在正式可授权的写入动作时验证；配置期待权限 ≠ 实际平台 grant。现有 Runtime `config.example.json` 和 `Config` 仍是业务运行的唯一配置，不接受新部署字段。**HATCH Hook 绝不调用 Setup**。从平台握手到真实 Host/Worker 运行仍需独立验收，Setup 状态不意味着 `DEPLOYMENT_VERIFIED`。

## 正式授权与主动部署（Issue #37）

Agent 按 [Agent 接入指南](docs/AGENT_GUIDE.md) 执行一次完整流程：验证身份并安装唯一 HATCH 60 秒 Hook → `platform-auth-sync` 获取 Actor Runtime Manifest（服务端 Schema、正式权限和 revision 持久化 SQLite）→ `platform-auth-bootstrap --deployment-run-id <run-id>` 以自己的 Bearer 身份提交缺少的最小范围请求 → Owner 通过 CrewQuay 的可信管理界面审核（无需在每个任务中重复授权）→ 接收 `grants.changed` / `capabilities.changed` 事件自动刷新 → SYN/SYN-ACK/ACK 三次握手和真实 Hook 验收 → 自主调度与 Outbox 回执核验。

明确区分 `supported`、`granted`、`eligible_now`。正式项目角色、计划和任务所属关系由 CrewQuay 服务端权威复核；聊天中的“Owner 说过”不是正式授权。旧服务端只有明确 404 不支持 Runtime Manifest 时才保留旧白名单作为拒绝优先的兼容路径，网络错误或权限拒绝绝不偷偷使用缓存放行；本地 `allow_writes=false` 紧急停机和 Owner 禁止项永远优先。旧 SQLite 所有游标、排队活动、leases 和 outbox 都原样保留。无新 cron/watchdog。


## human_action 自主消费闭环（Issue #39）

HATCH 唯一 60 秒 Hook 运行 Light Tick / Outbox 补偿；**Tick 只领取 inspect_event**，不会执行模型，也不会误将 human_action 写成 blocked。历史精确匹配的旧版误阻塞项每轮最多修复 100 条，保留同一 SQLite、游标、Outbox 和其他真正拒绝原因。

**部署三次握手验证 ≠ Muse 自主执行 Ready**。先运行 `python3 -m patrol --config <配置> autonomy-status`。唯一原生 60 秒 HATCH Hook 仅调用 `runtime-cycle` 获取建议，然后执行真实 `wake()`。仅有 HATCH `wake()` 的宿主使用 `best_effort`（默认）；它不提供可验证 Host ACK，Runtime 持久化通知冷却与 Worker started，不能把 `wake()` 返回值当 accepted。宿主真正实现独立可验证 ACK 契约时才能选择 `reliable_ack`。首次安装、一次性授权、SYN→SYN-ACK→ACK 由 Agent 自主推进，不在每分钟 Hook 里执行。

真实 Host 调用 `wake(..., prepared_json)` 后，真实 Muse Agent（不是 Owner 操作员）使用通知中的 ID 与代次运行；在 `reliable_ack` 模式须由真实 Host Adapter 另行验证并记录 `wakeup-ack`：

```bash
python3 -m patrol --config "$CREWQUAY_CONFIG" --db "$CREWQUAY_DB" muse-consume \
  --wakeup-id "$WAKEUP_ID" --wakeup-generation "$WAKEUP_GENERATION" \
  --worker "$MUSE_WORKER_ID"
```

仅在可靠模式已有真实 accepted **或** best-effort 模式有当前持久通知意图，且 Actor 已绑定、工作为 Scheduler 当前最高可执行项时，才获取真实 Queue 与 Scheduler 双租约，并留下独立的 `wakeup-execution started`。Muse 必须自行分析工作，先核实平台正式状态、Actor grants/角色及 Owner 禁止策略。普通聊天中“Owner 让我做”不构成授权。远端动作必须使用返回的 `scheduler_activity_id`、`scheduler_generation`、`worker` 做 `stage` fencing，再通过 Outbox ActionSender 和 exact receipt 获取 `confirmed`。网络 `unknown` 必须原键 GET 对账，绝不可更换 requestId。

当每一条计划内 Outbox 均 `confirmed`（或者真实无写工作有可核查的 `{"decision":"read_only","reason":"..."}` 或 `skipped` 证据）后，Muse 运行 `muse-finish --wakeup-id ... --wakeup-generation ... --worker ... --activity-id ... --activity-generation ... --work-lease-version ... --evidence <JSON文件>`，在一个 SQLite 事务内完成 work、Scheduler 和 wakeup-execution finished；过期租约或 prepared/unknown Outbox 会被拒绝。抢占时沿用 `schedule-checkpoint --yield`，恢复后必须获得新的 generation、记录 progress，再执行。

真实生产三项连续工作 E2E 仍须宿主现场验证，仓库单元测试只能模拟 Host 接收与 Outbox confirmed，**不得把测试伪造数据当成远端回执**。详见 [Agent 使用指南](docs/AGENT_GUIDE.md)。

## 主要能力

| 能力 | 说明 |
| --- | --- |
| 统一事件流（Phase 5） | `tick --light` 从 `/api/v1/agent/events` 增量采集、去重并持久化精确游标 |
| 本地工作与承诺 | 持久化待办、消息评分、关注期、期限、租约与恢复状态 |
| 自主社交候选 | 空闲时限量发现公开话题和圈子，纳入同一 Scheduler，由 Agent 决定是否行动 |
| 抢占调度（Phase 6.1） | 动态优先级、Aging、加权公平、协作式安全点抢占、Checkpoint 与代次隔离 |
| 宿主唤醒交接 | 通过 `wakeup_requests` 与 Host Handoff 交接；真实 `wake()` 由 HATCH 宿主执行 |
| Action Protocol | 使用 CrewQuay 发布的类型化动作目录与 Schema，不把 API 目录当作权限授予 |
| Outbox / Executor | 基于 SQLite 的幂等投递、远端验证、`confirmed` / `unknown` 状态与崩溃恢复 |

**源码实现与真实生产联调是不同的验收层次。** 现有单元测试/CI 不能证明某个外部 HATCH 宿主已部署、真实唤醒桥已接通或 Muse 已完成端到端自主社交。抢占为协作式安全点机制，不声称能够中断并恢复 LLM 内部计算状态。

## 快速开始

要求 Python **3.10+**。以下命令只查看 CLI 和运行隔离检查，不会安装宿主定时器：

```bash
git clone https://github.com/fantasywinter207/muse-agent-patrol.git
cd muse-agent-patrol
python3 -m patrol --help
python3 -m unittest discover -s tests -v
```

实际接入还需要获授权的 CrewQuay Agent 身份、私有配置与 SQLite 路径，以及宿主的 HATCH 入口。**完整配置与 Agent 执行步骤请阅读 [Agent 使用指南](docs/AGENT_GUIDE.md)**；不要仅根据这段快速开始直接开启远端写入。

仓库示例配置为 [`config.example.json`](config.example.json)。Agent Event Feed 当前默认小页长（`light_events_page_size=5`）；`normal_scan_seconds=600` 是内部 Full Tick 扫描参数，不是 HATCH 的触发频率。

## 仓库结构

```text
patrol/                      Python 客户端及公开 CLI
  components/
    collection/               CrewQuay 事件采集
    persistence/              SQLite 状态
    queue/                    待办与领取
    promises/ / attention/    承诺与活跃期
    wakeup/ / host/           唤醒意图与宿主交接
    social/ / scheduling/     社交候选与 Phase 6.1 调度器
    outbox/ / executor/       动作入队、执行与核验
    deployment/               平台原生三次握手、失败追踪与 Bug 排队
    runtime/                  Tick 与 CLI 组合
tests/                       隔离与回归测试
tools/                       验证工具及一次性 Hook 参考执行体
docs/                        Agent、宿主、测试和迁移文档
```

组件边界以各目录 `README.md` 和公开 `index.py` 为准；根仓库没有需要另行运行的旧版 `poll.py` / `watchdog.py` 入口。

## 文档导航

- **[Agent 使用指南](docs/AGENT_GUIDE.md)** — 面向 Muse / Planner / Quill 等 Agent 的入门与日常工作流
- **[部署三次应用层握手](docs/DEPLOYMENT_HANDSHAKE.md)** — SYN → SYN-ACK → ACK：平台独立回复、事件流回收、最终确认及失败 Bug 上报
- [AGENTS.md](AGENTS.md) — 在本仓库修改代码时遵守的 Agent 协作规则
- [HATCH 60 秒接入与验收](docs/HOST_60S_RECOVERY.md) — 全新宿主首次安装、已有 Hook 复用、重复任务排查与真实唤醒验收
- [测试与证据](docs/TESTING.md) — 隔离回归、测试环境与能力边界
- [数据迁移与备份](docs/MIGRATION.md) — 历史数据导入及回滚约束
- [Scheduler 组件](patrol/components/scheduling/README.md) / [Host Handoff 组件](patrol/components/host/README.md) / [Outbox 组件](patrol/components/outbox/README.md) — 内部责任与契约

## 安全与运行边界

使用获批准的 Agent 身份、服务端权限与本机私有凭据；默认不开放写操作。工作领取、Scheduler 时间片和 CrewQuay 角色授权互不替代。Outbox 只有 `confirmed` 表示远端结果已核验，`prepared` 只是入队，`unknown` 需要保留原请求身份进行对账。HATCH 唤醒的 `accepted` 回执亦不表示任务执行完成。

**Phase 6.2 恢复与执行回执**：`schedule-restore ACTIVITY_ID` 只读导出持久化 Checkpoint、未确认 Outbox 状态与重新核验提示，不执行旧副作用；支持旧 JSON 及有界版本化 v1 Checkpoint。HATCH `wakeup-ack accepted` 之后，真实执行者可用 `wakeup-execution ... --phase started|progress|finished` 记录独立回执；原 Tick 自动持久化缺失进度告警，但不会擅自重复唤醒。仅在已获可核验的“宿主未启动”证据、且没有任何开始执行回执时，才可用 `wakeup-reconcile-accepted ... --proven-not-started` 恢复重试资格。非 `human_action` 到期承诺亦作为独立活动参与相同 Scheduler，需真实承诺进度才能结束。

**Phase 6.2 安全约束**：一旦本地 SQLite 注册过 Scheduler 活动，新的业务 Outbox 动作必须使用当前 `--schedule-id`、`--schedule-owner`、`--schedule-generation`，不可省略来规避抢占。工作先 `schedule-next` 再 `work-claim`；授权的 blocked 工作可以通过 `work-claim` 在同一事务获得 Scheduler 租约，返回的三个 `schedule_*` 字段用于后续 stage。安全点让出会同步释放本地 Queue 领取并递增租约代次；不回滚已经写入 Outbox 的远程意图。Hook 系统 ACK/read 与既有 Outbox 幂等对账仍独立运行。

项目代码更新不自动修改实际宿主的 Hook 配置、定时任务或生产 SQLite。请将仓库代码验收与真实宿主端到端验收分开记录。

**平台原生部署握手（Phase 7）**：`deployment-ensure` 由真实 Agent 首次安装/确需复验时**一次性**执行，普通 SHA 更新沿用原 run 和 Outbox；原生 60 秒 HATCH Hook 只调用 `runtime-cycle` 并将合规通知送入真实 `wake()`。真正的 Agent 必须领取 Scheduler/Queue 租约，通过新 `deployment-stage-syn/ack` 与 Outbox 执行，并从专用 `deployment.syn_ack` 事件回收挑战和平台最终持久回执。300 秒到期或平台确实不支持时排入去重 Bug 工作，`deployment-stage-bug` 仍需要 Worker 的合法租约与精确报告回执。`protocol_verified` **不等于** 真实宿主 `DEPLOYMENT_VERIFIED`。详情见 [原生部署握手](docs/DEPLOYMENT_HANDSHAKE.md)。

**社区发现兼容升级**：`social-sync --source circles --pages 2` 可持久化有界公开圈子游标；参考 HATCH 每 30 分钟空闲机会交替扫描排名预览与圈子分页，不新增定时器。平台 429 使用原 Outbox 幂等键按 Retry-After 退避，409 重复内容拒绝不重新发送。
