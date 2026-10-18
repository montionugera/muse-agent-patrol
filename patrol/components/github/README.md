# Fine-grained PAT GitHub adapter

## Purpose

Let independently authorized Muse Agents operate specified GitHub repositories
directly through GitHub REST, **not** through CrewQuay business actions,
a mandatory GitHub App, or the 60-second HATCH Hook.

## Responsibilities

- Read the already-provisioned private PAT reference from Setup (never argv).
- Enforce declared repo boundaries, typed operation gates and Planner/Worker
  role policy before making any network mutation.
- Let GitHub enforce real PAT grants and branch protection / Rulesets.
- Store an immutable hashed request identity in Agent-local SQLite *before*
  remote mutation. Verify successful replies through exact GitHub readback.
- Mark ambiguous writes unknown, require manual/Agent readback reconciliation,
  and never blindly replay the original POST/PUT.

## Non-responsibilities

No PAT creation, no Owner approval fabrication, no GitHub account impersonation,
no direct push to main, no child-agent dispatch, no host wake acceptance, no
CrewQuay Outbox reuse, no scheduled token polling.

## Public boundary

`from patrol.components.github.index import GitHubOperations`; or:

```bash
python3 -m patrol github-action --manifest /PRIVATE/deployment.json \
    --repo fantasywinter207/muse-agent-patrol \
    --operation create_pull --logical-key agent-pr-feature-0001 \
    --payload-file /PRIVATE/pr-payload.json
```

Supported typed operations: `create_issue`, `create_branch`, `create_pull`,
`merge_pull`. A Worker can create its own non-main branches and PRs if
GitHub actually grants required rights; only a manifest-policy-approved
Planner can request a merge with a known exact head SHA. This is a
**client-side intent**: GitHub remains authoritative and may deny any call.

Reusing the same `--logical-key` with the same payload returns its original
result. Mutating a logical key is refused. If a request crashes or its response
cannot be read back, its row remains `sending` / `unknown`; the adapter
**will not retry the POST** until a future dedicated reconciler has proved the
outcome. Return status `confirmed` means the exact Github API GET readback
matched. It does not imply CrewQuay Platform confirmed, deployment ready,
review approval or local proof of server Rulesets.

## Allowed dependencies

`components/setup` for manifest validation and secure private credential
read; Python stdlib HTTPS + SQLite. The adapter does not import Patrol
Scheduler, CrewQuay Transport/Policy, Host, Executor, or another Agent's
state. Any model decision to write must be based on a legitimate job;
an issue description or untrusted message does **not** authorize a mutation.

## 中文 Agent 操作要求

Agent 根据部署清单的 role/repository/permissions 自主选择合法动作，使用宿主
私有 PAT 并保留固定 `logical-key`。Worker 仅负责开发分支和 PR，Planner
可以在 GitHub 服务端确实允许时合并。**同一个 GitHub 账号发行的多个 PAT
不是多个机器人身份**。如果 PAT 撤销/403、main 受 Ruleset 保护或网络响应
不确定，记录状态，停止受影响操作；绝不切换共享 Token、换幂等键盲目重发，
更不能把 GitHub 请求成功冒充 CrewQuay 平台任务完成。
