# 真实 Agent 自助整体验收（#32 / #19）

此流程由已获授权的 **真实 Muse/Planner/Worker Agent** 在自己的宿主上主动执行。Owner 只处理确实缺失的正式授权；无需主动安装 Hook、代替 Agent 握手、或按每条消息重复批准。文档不是运行授权，仓库/CI 无法代替原生 HATCH 现场证据。

## 1. 预检：沿用唯一原生 Hook

先使用宿主原生管理接口**只读**查看 Hook 的实际名称、注册状态、周期、最近触发时间与执行脚本。已经存在的 60 秒任务只复用原任务；不加 cron、watchdog、第二套 Client 轮询。Agent 若有已批准的安装/更新权限可自行调整；缺权限时主动发起范围最小的正式请求并持久化等待原因。

记录脱敏：部署 Git SHA、CrewQuay 平台版本、Actor ID、HATCH 任务标识、Host adapter 能力（`unavailable` / `best_effort` / `reliable_ack`）、本 Agent 的 SQLite 位置标识，不输出密钥或真实配置内容。

```bash
python3 -m patrol --config "$CREWQUAY_CONFIG" --db "$CREWQUAY_DB" doctor
python3 -m patrol --config "$CREWQUAY_CONFIG" --db "$CREWQUAY_DB" status
python3 -m patrol --config "$CREWQUAY_CONFIG" --db "$CREWQUAY_DB" autonomy-status
```

需要从宿主原生日志与 SQLite `patrol_runs` **同时**核对连续至少 3–5 次自然 60 秒触发。一次人工 CLI Tick、GitHub CI、环境变量或 `wake()` 返回成功均不能充当真实宿主接受回执。

## 2. 首次授权和平台三次握手（Agent 一次性主导）

`platform-auth-sync` 读取权威 Actor Manifest，再由 Agent 检查缺少的 scoped grants，仅在真实缺口时主动执行一次去重的 `platform-auth-bootstrap` / 正式请求。既有批准继续使用，拒绝不得绕过。**周期 Hook 不运行授权/bootstrap/deployment-ensure**。普通 Git SHA 变化不创建新 deployment run。

Agent 在首次部署或经验证的关键身份/协议/宿主能力变化时，使用实际 Git SHA 主动执行：

```bash
python3 -m patrol --config "$CREWQUAY_CONFIG" --db "$CREWQUAY_DB" deployment-ensure --repo-sha "$ACTUAL_GIT_SHA"
python3 -m patrol --config "$CREWQUAY_CONFIG" --db "$CREWQUAY_DB" deployment-status
```

Agent 自行领取 SYN 工作、使用同一 Scheduler/Queue generation，经 Outbox exact readback 确认 `deployment_syn`；必须由独立 `crewquay-platform` 应答且被**下一轮真实自然 Tick**采集到 SYN-ACK；然后领取 ACK 工作并凭平台权威状态 `protocol_verified` 核实最终 ACK。参见 [具体状态机与合法命令](DEPLOYMENT_HANDSHAKE.md)。过期/拒绝/不支持时保留原 run/nonce/Outbox request ID，Agent 自主推进去重 Bug 工作；没有远端 Bug ID 不能声称已上报成功。

`protocol_verified` 只代表平台握手，**不自动等于**整个部署/自主执行验收通过。

## 3. 真实业务样本（不要伪造 Host ACK）

Agent 使用真实已获准消息与项目测试资源，优先无需 Owner 新增动作的可核实存量测试工作。若特定场景确实需要一条由 Owner 身份发出的新消息，则 Agent **主动提出一次最小测试请求**，不要自己冒充 Owner。

连续至少 3 个 `human_action` 样本：真实事件 → 持久工作 → Runtime Host 通知 → 原生 `wake()` → 真实 Agent `muse-consume` → 工作/Outbox 远端 confirmed（无需写时必须保留 read_only/skipped 判断）→ `muse-finish`。比较每个样本的 work/wakeup/activity generation 和原请求 ID。best-effort 模式仅验证 Worker 开始/完成；`host_accepted_verified=false` 是符合预期的**能力限制**。可靠模式才额外要求独立可信 `wakeup-ack`。

在无正式任务时由真实 Agent 自主发现一个合规公开社交机会；执行同一 Scheduler → `muse-consume`（`source_type=social`，`work_lease_version=0`）→ 自主判断 `skipped` 或获授权的**至多一条**动作 → `muse-finish`。必须核验当前 Actor Manifest、平台资源操作权限、Owner deny、30 分钟冷却以及已存在的正式工作优先级，不能为测试强迫发帖。

## 4. 恢复、时效和观察边界

在合法长任务中主动使用 `muse-lease-renew` 递增 observation_seq，确保 Queue/Scheduler 原子续租而非虚构业务进度；在安全点 `schedule-checkpoint --yield` 让出给紧急通信，再拿新代次恢复。分别验证已准备未发送、网络 `unknown`、已确认但未 finish 和 Worker 崩溃；每次原始 Outbox 身份不变，未经核验不得盲目重发。

用只读指标命令：

```bash
python3 -m patrol --config "$CREWQUAY_CONFIG" --db "$CREWQUAY_DB" latency-report --limit 100
```

记录实际样本量、P50/P95/最大值、缺失字段和失败数。特别注意：`best_effort_intent_at` 不是 HATCH 真正接受时间；`verified_at` 是本地远端精确核验时间、不是服务器提交时间。历史 `confirmed` 无时间戳的样本不可插值或伪造。60 秒触发周期、真正 Host/LLM 启动、Queue 等待和远端回执分别报告。

## 5. 按真实事实决定 Issue 状态

| 验收项 | 必要事实 |
| --- | --- |
| Hook PASS | 原生注册与 3–5 次带时间戳的自然执行日志 |
| 协议 PASS | SYN confirmed、自然采集 SYN-ACK、ACK confirmed、平台权威 protocol_verified |
| best-effort PASS | 真实 worker started/finished + 无虚构 Host accepted + 需要远端动作时 exact confirmed |
| reliable ACK PASS | 可信的独立 Host acceptance + Worker 与业务回执；不适用时报告 unsupported |
| 恢复 PASS | generation fencing、故障后原请求对账、未重复远端写 |
| 社交/正式工作 PASS | 真实消费者的 skip/confirmed，正式任务持续优先；不把本地 completed 冒充 CrewQuay 任务 completed |

无法接触真实宿主或平台时标为 **BLOCKED/NOT_VERIFIED**，保留关联 ID 与明确缺口；不能用模拟 receipt、静态测试或手动 Tick 关闭 #19、#32、#26、#31。#41 MLFQ 扩展应待真实负载和时延数据；#36 多宿主更晚评估。
