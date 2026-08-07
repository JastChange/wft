# WFT — Linux 节点安装验证与故障诊断

WFT 1.0 面向单个运维人员，在一台 Linux 控制服务器上对 Ubuntu 节点运行经过登记的只读脚本，用于安装验证和故障证据采集。

## 当前状态

分支 `rewrite/node-diagnostics` 已完成 **M2：手动任务执行与权威结果存储**：

- Python 3.12 / Typer CLI 基线；
- 权限为 `0600` 的类型化私有配置；
- Ubuntu 22.04/24.04 节点清单和确定性选择；
- 外部 Git 仓库最新脚本获取、Manifest 门禁和不可变快照；
- 需要操作员明确同意的缓存脚本回退；
- CLI 生成的管理员密码和 Argon2 哈希文件。
- `installation_validation` 与 `fault_diagnosis` 两类手动任务；
- AsyncSSH/SFTP 执行、自动接受并记录 Host Key、节点并发与节点内串行脚本；
- UUIDv7 任务、原子 JSON、逐字节原始输出、任务查询和确认删除；
- 超时后继续、节点故障隔离、无自动重试、Ctrl-C 取消和崩溃残留判定。

只读 Web、派生索引、Webhook、AI 和 Obsidian 导出将在 M3～M6 交付。M2 Web 页面尚未交付，CLI 和权威文件是当前可用界面。

## 安装

```bash
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[test]'
wft --help
```

Python 要求为 `>=3.12,<3.13`。

## M1/M2 命令

```text
wft admin init --auth-file PATH
wft admin reset-password --auth-file PATH
wft config check --config PATH [--json]
wft inventory check --config PATH [--json]
wft inventory select --config PATH [--node NAME] [--group GROUP] [--tag TAG] [--all] [--json]
wft scripts check --config PATH [--json]
wft scripts sync --config PATH [--allow-cached-scripts] [--json]
wft run installation-validation --config PATH SELECTOR SCRIPT_SELECTION [--concurrency N] [--allow-cached-scripts] [--json]
wft run fault-diagnosis --config PATH SELECTOR SCRIPT_SELECTION [--concurrency N] [--allow-cached-scripts] [--json]
wft task list --config PATH [--json]
wft task show TASK_ID --config PATH [--json]
wft task delete TASK_ID --config PATH --reason TEXT
```

`SELECTOR` 是重复的 `--node/--group/--tag` 或单独的 `--all`；`SCRIPT_SELECTION` 是重复的 `--script` 或一个 `--plan`。详细执行和退出码见[任务操作指南](docs/operations/tasks.md)。

详细配置、Manifest 和操作步骤见[配置与脚本操作指南](docs/operations/configuration.md)。示例文件位于 `config/`，其中只有假节点、假地址和假密钥路径。

## 权威文档

- [产品需求](docs/spec/PRODUCT_REQUIREMENTS_v1.0.md)
- [架构](docs/spec/ARCHITECTURE_v1.0.md)
- [权威数据格式](docs/spec/DATA_FORMATS_v1.0.md)
- [验收矩阵](docs/spec/ACCEPTANCE_MATRIX_v1.0.md)
- [需求追踪矩阵](docs/spec/TRACEABILITY_v1.0.md)
- [交付计划](docs/spec/DELIVERY_PLAN_v1.0.md)
- [M1 验收证据](artifacts/acceptance/m1.md)
- [M2 验收证据](artifacts/acceptance/m2.md)
- [领域词汇](CONTEXT.md)
- [架构决策](docs/adr/)

旧规格位于 `docs/archive/legacy-mvp/`，仅供追溯，不再约束实现。

## 安全和运行边界

- 私有配置、脚本 Deploy Key 和节点私钥必须使用绝对路径；配置和 Deploy Key 必须为 `0600`。
- YAML、JSON、日志和 Git 仓库中只保存密钥路径，不保存私钥正文。
- WFT 直接信任配置的脚本仓库 URL 和分支，不验证 commit/tag 签名。
- 每次同步先尝试远端最新提交；远端失败时，只有交互确认或 `--allow-cached-scripts` 才能使用已验证缓存。
- 每个任务只执行一次，不提供 Scheduler、resume、自动恢复或自动重试。
- 权威记录只在 `<data_dir>/tasks/`；SQLite 即使存在也不是权威数据源。
- SSH 使用 `known_hosts=None` 自动接受 Host Key，并把实际算法和 SHA-256 指纹写入节点快照。
- 原始 stdout/stderr 不截断、不解码，容量由操作员负责监控。

## 贡献流程

1. 阅读 `AGENTS.md`、`CONTEXT.md` 和相关 ADR；
2. 从产品需求和验收矩阵定位需求 ID；
3. 使用 TDD 实现；
4. 报告测试证据和规格偏差；
5. 每个里程碑确认后再进入下一阶段。
