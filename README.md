# WFT — Linux 节点安装验证与故障诊断

WFT 1.0 面向单个运维人员，在一台 Linux 控制服务器上对 Ubuntu 节点运行经过登记的只读脚本，用于安装验证和故障证据采集。

## 当前状态

分支 `rewrite/node-diagnostics` 已完成 **M1：CLI、配置、Inventory 与脚本快照**：

- Python 3.12 / Typer CLI 基线；
- 权限为 `0600` 的类型化私有配置；
- Ubuntu 22.04/24.04 节点清单和确定性选择；
- 外部 Git 仓库最新脚本获取、Manifest 门禁和不可变快照；
- 需要操作员明确同意的缓存脚本回退；
- CLI 生成的管理员密码和 Argon2 哈希文件。

任务执行、SSH、权威结果存储、只读 Web、Webhook、AI 和 Obsidian 导出将在 M2～M6 交付。当前版本不能执行诊断任务，也不应作为完整 WFT 1.0 部署。

## 安装

```bash
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[test]'
wft --help
```

Python 要求为 `>=3.12,<3.13`。

## M1 命令

```text
wft admin init --auth-file PATH
wft admin reset-password --auth-file PATH
wft config check --config PATH [--json]
wft inventory check --config PATH [--json]
wft inventory select --config PATH [--node NAME] [--group GROUP] [--tag TAG] [--all] [--json]
wft scripts check --config PATH [--json]
wft scripts sync --config PATH [--allow-cached-scripts] [--json]
```

详细配置、Manifest 和操作步骤见[配置与脚本操作指南](docs/operations/configuration.md)。示例文件位于 `config/`，其中只有假节点、假地址和假密钥路径。

## 权威文档

- [产品需求](docs/spec/PRODUCT_REQUIREMENTS_v1.0.md)
- [架构](docs/spec/ARCHITECTURE_v1.0.md)
- [权威数据格式](docs/spec/DATA_FORMATS_v1.0.md)
- [验收矩阵](docs/spec/ACCEPTANCE_MATRIX_v1.0.md)
- [需求追踪矩阵](docs/spec/TRACEABILITY_v1.0.md)
- [交付计划](docs/spec/DELIVERY_PLAN_v1.0.md)
- [M1 验收证据](artifacts/acceptance/m1.md)
- [领域词汇](CONTEXT.md)
- [架构决策](docs/adr/)

旧规格位于 `docs/archive/legacy-mvp/`，仅供追溯，不再约束实现。

## 安全和运行边界

- 私有配置、脚本 Deploy Key 和节点私钥必须使用绝对路径；配置和 Deploy Key 必须为 `0600`。
- YAML、JSON、日志和 Git 仓库中只保存密钥路径，不保存私钥正文。
- WFT 直接信任配置的脚本仓库 URL 和分支，不验证 commit/tag 签名。
- 每次同步先尝试远端最新提交；远端失败时，只有交互确认或 `--allow-cached-scripts` 才能使用已验证缓存。
- 目前只支持 M1 手动命令，不提供任务执行、Scheduler、自动恢复或自动重试。

## 贡献流程

1. 阅读 `AGENTS.md`、`CONTEXT.md` 和相关 ADR；
2. 从产品需求和验收矩阵定位需求 ID；
3. 使用 TDD 实现；
4. 报告测试证据和规格偏差；
5. 每个里程碑确认后再进入下一阶段。
