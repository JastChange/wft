# WFT — Linux 节点安装验证与故障诊断

WFT 1.0 面向单个运维人员，在一台 Linux 控制服务器上对 Ubuntu 节点运行经过登记的只读脚本，用于：

- 验证新安装操作系统是否符合预期；
- 为故障节点收集完整、可追溯的判断证据；
- 通过只读 Web 页面查看任务、节点、脚本和原始输出；
- 按需发送 Webhook、执行 AI 辅助分析并导出 Obsidian 问题笔记。

## 当前状态

分支 `rewrite/node-diagnostics` 正在进行 1.0 全面重写。当前处于 **M0：需求与架构基线**，业务运行代码仍是归档前的旧 MVP 实现，不代表 1.0 行为。

未经里程碑验收，不应将当前分支部署为 WFT 1.0。

## 权威文档

- [产品需求](docs/spec/PRODUCT_REQUIREMENTS_v1.0.md)
- [架构](docs/spec/ARCHITECTURE_v1.0.md)
- [权威数据格式](docs/spec/DATA_FORMATS_v1.0.md)
- [验收矩阵](docs/spec/ACCEPTANCE_MATRIX_v1.0.md)
- [需求追踪矩阵](docs/spec/TRACEABILITY_v1.0.md)
- [交付计划](docs/spec/DELIVERY_PLAN_v1.0.md)
- [领域词汇](CONTEXT.md)
- [架构决策](docs/adr/)

旧规格位于 `docs/archive/legacy-mvp/`，仅供追溯，不再约束实现。

## 计划技术栈

- Python 3.12
- Typer CLI
- FastAPI + 服务端模板
- AsyncSSH
- JSON/原始文件权威存储
- 可重建 SQLite 查询索引
- Docker Compose

## 计划运行边界

- 只支持 CLI 手动任务，不提供 Scheduler。
- 目标节点为 Ubuntu 22.04/24.04，最多 50 台。
- Web 仅在内网/VPN使用 HTTP，并且只有查看权限。
- 允许 root/sudo，自动接受 SSH Host Key，直接信任配置的 Git 分支。
- 不提供自动恢复、备份、通用审计、HTTPS、内容脱敏或旧数据迁移。

这些是需求方明确确认的 1.0 边界；完整风险说明见产品需求和 ADR。

## 贡献流程

1. 先阅读 `AGENTS.md`、`CONTEXT.md` 和相关 ADR；
2. 从产品需求和验收矩阵定位需求 ID；
3. 使用 TDD 实现；
4. 报告测试证据和任何规格偏差；
5. 每个里程碑确认后再进入下一阶段。
