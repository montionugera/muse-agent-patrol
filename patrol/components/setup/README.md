# Agent Setup v1 — one-time, resumable onboarding

## Purpose

Give each independent Muse Agent a versioned, non-secret deployment plan and
recoverable *one-item-at-a-time* setup process, separate from the 60s Hook.

## Responsibilities

Validate declared expectations without elevating them to grants; verify only
non-destructive private-credential and repository readback evidence, maintain
per-Agent setup SQLite state, and provide an explicit next actionable step.
Persist credential reference and salted fingerprint, never its value.

## Non-responsibilities

No PAT creation, secret import through chat/CLI arguments, CrewQuay permission
approval, Host Hook registration, Worker-model invocation, scheduler activity
dispatch, remote business mutation, fabricated Host ACK or platform confirmed.

## Public boundary

`from patrol.components.setup.index import Manifest, Setup` and
`python3 -m patrol setup --manifest /PRIVATE/deployment.json init|next|resume|verify|status`.
This API returns advisory status only, not authority to act as Owner.

## Allowed dependencies

Python standard library; `components/config` for read-only existing Runtime
configuration checks. State persistence is isolated from business SQLite;
runtime CLI invokes Setup outside its ordinary Tick lock, and Setup does not
import the scheduler, executor, Host Adapter or CrewQuay mutation clients.

**Ownership:** The real Muse Agent calls this component. The native 60-second HATCH Hook never calls Setup or asks the Owner for anything. Setup lives in its own `components/setup` responsibility root; it does not replace the existing, strictly validated `config.example.json`.

## Manifest / contract (English)

`deployment.example.json` and `deployment.schema.json` describe **requested** Agent identity, host isolation, CrewQuay scopes, expected GitHub fine-grained PAT repository permissions, and the one native HATCH interval. This is declarative JSON, not YAML: the existing Python JSON parser is a stable dependency, while the runtime `Config` deliberately rejects unknown fields. Never put a token, a password or an actual authorization grant inside this manifest.

The manifest's `runtime.state_dir` must contain *both* the Agent ID and host instance. Runtime config and business SQLite must be distinct files inside it. Setup creates only `<state_dir>/setup.sqlite3` and `<state_dir>/secrets/`, separate from the Patrol business DB. Never share a state directory or personal token with another Agent. The setup database stores a version/hash, de-duplicated request state, reason, logical reference, and *salted irreversible credential fingerprint*, **never token contents**.

### From README to active Agent workflow

After creating its own private `runtime.json` and validating actual HATCH privileges, the Agent runs:

```bash
python3 -m patrol setup --manifest /PRIVATE/deployment.json init
python3 -m patrol setup --manifest /PRIVATE/deployment.json next
# Only after a specific missing item is handled through the official host / platform:
python3 -m patrol setup --manifest /PRIVATE/deployment.json resume
python3 -m patrol setup --manifest /PRIVATE/deployment.json verify
python3 -m patrol setup --manifest /PRIVATE/deployment.json status
```

Each `next` checks existing read-only evidence and gives **one** currently unsatisfied step. `prompt_new=true` is limited to a *genuine missing/revoked private credential or repository access*, recorded in SQLite, and not repeated while the same evidence/credential fingerprint remains. Other problems go to the Agent as an actionable verification/repair step, not to Owner as an indiscriminate approval request. `skip-optional --step github_credential` can omit an optional step, but cannot grant permissions or omit required steps. `deny --step NAME` records a restrictive refusal only; it is **not** a CLI to approve Owner grants.

### Secure credential import

`secret://github/pat` maps to the privately provisioned `<state_dir>/secrets/github_pat`. `secret://crewquay/agent` maps to `<state_dir>/secrets/crewquay_agent`. A trusted host secret manager or already authorized Agent host action must place the credential there; Setup does not accept a token argument, read secrets from chat, create a PAT, or claim to offer its own Secret Manager. POSIX: state and `secrets/` mode `0700`, individual files `0600`, no symlinks, same owner. **Windows ACLs cannot be certified by POSIX permissions:** absent a real host adapter the probe returns `HOST_ACL_VERIFICATION_REQUIRED` rather than reading a potentially unprotected token. Missing secure injection is `SECURE_SECRET_INPUT_REQUIRED`.

Use GitHub's official PAT creation settings to limit repositories and Contents/Issues/Pull requests privileges. A PAT issued from a *single GitHub account* never becomes a different GitHub identity just because another Agent holds a different PAT. `github.policy.allow_pr_merge` is a **local intention**, not GitHub permissions: require GitHub **branch protection / rulesets** for service-side main protection. The adapter performs only `GET /user` and `GET /repos/{owner}/{repo}` in credential checks. These show authentication and repository **read** reachability, never write approval. If the intended action fails with 403/404 or a branch ruleset, stop that action and retain evidence; never silently fall back to shared credentials, another Actor, or privileged GitHub App.

### Unverified and unsupported observations

CrewQuay grants are granted only by its official Actor authorization API; local requested scopes are not rights. HATCH registration / natural 3–5 ticks cannot be certified by an ordinary GitHub checkout or Hook environment variable. The platform SYN→SYN-ACK→ACK final state is separate from natural Host wake and real Worker/Outbox completion. These steps report `unverified` until a trusted native/Actor check exists. Therefore even `setup_steps_resolved` or a `verified` PAT file alone **never means** `DEPLOYMENT_VERIFIED`.

If a credential is missing, the Agent should contact Owner with one concise request describing the official target (e.g. PAT limited to one repository, needed Contents write for a worker feature branch/PR), expected scope and risk, **never ask to paste the token in a chat**. If there is no secure import path, report `BLOCKED_SECURE_SECRET_INPUT` through the existing safe owner channel, and continue other pre-authorized work.

## 中文：独立部署状态机

1. **一次性**由 Agent 读取 `deployment.example.json` 清单，检查自己的 Actor、GitHub PAT、Hook 与受保护的 Runtime 配置，部署进度写入 Agent/宿主独立的 `setup.sqlite3`；不要在每 60 秒 Hook 里安装、验证 PAT 或发起授权。
2. `setup next` 每次只返回一项缺失/待验证事项。Agent 能通过现有合法权限处理的就自己做；真正缺少凭据或仓库授权才主动询问 Owner，且只问这一项。Owner 通过 GitHub/CrewQuay/HATCH 的正式界面操作，不在聊天中发送 Token。
3. 宿主安全渠道把 Token 导入私有文件，Agent 自动检测并只读核验。文件存在不等于授权、GitHub GET 成功不等于写权限、配置写着 `allow_pr_merge` 不等于能合并 main。
4. 普通 Git SHA 升级不重置已审批的 CrewQuay grants、部署 run、原 Outbox/SQLite；配置变化只重新核验受影响项。用户明确拒绝后不可使用命令自行标记为 granted。
5. 真正验收还要遵循 [宿主真实执行验收](../../../docs/REAL_HOST_AGENT_ACCEPTANCE.md)：原生 60 秒 Hook、平台三次握手、真实 Muse started/finished、Outbox confirmed 均需独立证据。缺少宿主能力要报告阻塞，不能制造 ACK。

## Boundaries

- `setup/components/manifest.py`: strict public contract / no runtime config mutation
- `setup/components/secrets.py`: exact logical reference → approved private file
- `setup/components/probes.py`: read-only external evidence; no POST/PATCH
- `setup/components/state.py`: one-at-a-time SQLite step and deduplicated prompt state
- `runtime/composition/cli.py`: thin `setup` routing; the separate `runtime-cycle` is untouched

**Remaining integration acceptance:** actual HATCH registration and CrewQuay live grant comparison require native Host/Actor connectors. A noninteractive GitHub CI run can validate safe logic but cannot prove these remote side effects.
