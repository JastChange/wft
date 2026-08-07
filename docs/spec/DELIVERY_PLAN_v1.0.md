# WFT 1.0 交付计划

> 状态：Approved for Planning
> 分支：`rewrite/node-diagnostics`
> 工作方式：长期 Draft PR，每个里程碑暂停确认。

## M0：需求与架构基线

交付：

- 产品需求、领域词汇和明确不包含项；
- 架构、权威数据格式、状态和退出码；
- ADR 与验收矩阵；
- 旧 MVP 文档归档；
- 新 README；
- M1 可执行实现计划；
- GitHub 总 Issue 和 M1～M7 子 Issue。

关闭门禁：文档内部引用有效、无未决项或冲突、需求方确认、Markdown/示例校验通过。

## M1：CLI、配置、Inventory 与脚本快照

交付：

- Python 3.12/Typer 项目基线；
- 私有配置权限检查和类型化加载；
- 节点清单与选择器；
- Git latest fetch、缓存确认、Manifest/哈希/OS 校验；
- 不可变脚本快照；
- 管理员密码初始化/重置的基础存储；
- 新 CLI 帮助与示例配置。

关闭门禁：AC-001～006，通过代码审查和 CLI 演示。

## M2：任务执行与权威文件存储

交付：

- 新任务领域模型和状态机；
- UUIDv7 标识、节点/脚本快照；
- AsyncSSH/SFTP adapter；
- 自动 Host Key、解释器检查、超时和远端清理；
- 节点并发 1–50、同节点顺序和失败隔离；
- 原始流文件、原子 JSON、任务/节点/脚本终态；
- PASS/FAIL/INCONCLUSIVE、退出码、Ctrl-C 与 abandoned 标记；
- CLI 列表、详情和删除。

关闭门禁：AC-007～017，不包含容量门禁。

## M3：只读 Web 与派生索引

交付：

- 可重建 SQLite 索引；
- 管理员初始化、Argon2 登录和 Session；
- 任务列表/筛选/轮询/详情；
- 问题和分析展示占位读取；
- 受控原始文件下载；
- Docker Compose 同镜像 web/CLI 运行。

关闭门禁：AC-018～020、AC-028 的 Web/Compose 部分。

## M4：单次 Webhook

交付：

- 安装异常和故障诊断完成/中止事件；
- 不含原始输出的通知负载；
- 单次 HTTP 投递和版本化结果文件；
- Web 通知状态展示；
- Webhook Stub。

关闭门禁：AC-021；确认失败后不存在重试行为。

## M5：手动 AI 分析与问题模型

交付：

- `wft analyze <task-id>`；
- OpenAI-compatible adapter 和 AI Stub；
- 输入选择、预算、分块与合并；
- 结构化中文输出和字节范围证据引用；
- 规则摘要回退；
- 版本化分析；
- 待分类问题、同节点归并和来源关系；
- Web 分析与问题展示。

关闭门禁：AC-022～024。

## M6：Obsidian Git 导出

交付：

- `wft export <task-id>`；
- 独立 Vault 工作树检查和 pull；
- 每问题 Markdown、每任务索引；
- managed/free 区块；
- 幂等更新和冲突停止；
- 保证不 commit/push。

关闭门禁：AC-025～026。

## M7：发布硬化与 1.0.0

交付：

- 50 模拟 SSH 节点容量夹具；
- PERF-001～003 报告；
- 完整 Compose、镜像和 CI；
- 示例配置、部署手册、操作手册和发布说明；
- 需求—代码—测试追踪表；
- 签名 Git Tag 和 CI 构建镜像；
- Draft PR 转 Ready 并完成最终审查。

关闭门禁：AC-001～028、PERF-001～003 全部满足；已接受但未测试风险写入发布说明。

## 交付纪律

- 所有功能使用 TDD：失败测试 → 最小实现 → 通过 → 重构。
- 每个提交只完成一个可描述行为，并包含相应测试。
- 任何规格偏差必须先更新需求、ADR 和验收矩阵并确认。
- 每个里程碑提交：变更摘要、测试命令/结果、未完成项、规格偏差、已知风险。
- 未经需求方确认不得进入下一里程碑。

## GitHub Issue 结构

```text
WFT 1.0 节点诊断重写（总 Issue）
├── M0 需求与架构基线
├── M1 CLI、配置、Inventory 与脚本快照
├── M2 任务执行与权威文件存储
├── M3 只读 Web 与派生索引
├── M4 单次 Webhook
├── M5 手动 AI 分析与问题模型
├── M6 Obsidian Git 导出
└── M7 容量验收与 1.0.0 发布
```

Issue 使用 `ready-for-agent` / `ready-for-human` 等标签时，以 `docs/agents/triage-labels.md` 为准。
