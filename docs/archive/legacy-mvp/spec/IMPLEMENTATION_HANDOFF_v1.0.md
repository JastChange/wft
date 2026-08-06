# WFT MVP 实现交接清单 v1.0

## 1. 交接状态

- 规格：Approved for Implementation。
- 仓库：`https://github.com/JastChange/wft.git`，当前为空。
- 实现负责人：@OpenCoder。
- 环境/CI/验收负责人：@GeminiOps。
- 规格偏差裁决：@SpecArchitect。

## 2. 推荐实施阶段

### Phase 0：仓库与契约门禁

@OpenCoder：

- 初始化 Python 包、CLI 骨架和模块目录；
- 导入 Contract-01～12；
- 为每个 schema 建正常/边界/错误样本；
- 建 JSON Schema CI 和基础单测；
- 配置示例只能使用假节点和假凭据引用。

@GeminiOps：

- 建 macOS/Linux CI；
- 锁定 Python 与依赖安装方式；
- 增加 secret scan、依赖审计；
- 准备本地 webhook stub 和基础 SSH 测试容器。

退出条件：空仓库可安装；`wft --help` 成功；12 个契约 CI 全绿。

### Phase 1：清单、主机密钥与脚本注册

@OpenCoder：实现 Inventory、选择器、credential_ref、hostkey onboarding、read_only ScriptRegistry 和完整 SHA-256 校验。

@GeminiOps：准备 5 节点连接检查脚本和安全的 known_hosts 测试流程。

退出条件：AC-001、AC-002、AC-005、AC-008B 通过。

### Phase 2：SSH、状态、存储与恢复

@OpenCoder：实现异步 SSH、限流、重试、ExecutionResult、SQLite WAL、checkpoint、幂等写、stale resume 和 CLI 0/1/2。

@GeminiOps：准备网络故障注入、kill -9、50 SSH 模拟端点和资源采集。

退出条件：AC-003、004、006、007、008、011、014 通过。

### Phase 3：规则与 LLM

@OpenCoder：实现 healthy/基础设施错误/T1/T2 路由、单供应商适配器、schema 校验和规则降级。

@GeminiOps：提供 LLM HTTP stub，覆盖超时、限流、5xx、乱码和非法 JSON。

退出条件：AC-009、010 通过；未配置 LLM 时仍可规则模式运行。

### Phase 4：Obsidian、通知与 scheduler

@OpenCoder：实现 outbox、managed/free 导出、doctor、重建、webhook 去重、本地 scheduler。

@GeminiOps：准备临时 Vault、webhook 收件端、launchd/systemd/Docker 运行示例。

退出条件：AC-012、013、015、016 通过。

### Phase 5：端到端验收

双方执行 AC-017 与 PERF-001～004，生成 `artifacts/acceptance/` 证据包。验收结果回交 @SpecArchitect 对照规格，需求方最终确认。

## 3. 推荐代码边界

```text
src/wft/
├── cli/
├── config/
├── contracts/
├── orchestration/
├── execution/
├── analysis/
├── storage/
├── export/
├── notification/
└── observability/
```

模块只能通过契约对象交接；SQLite 访问集中在 storage；CLI 不直接访问执行或导出内部实现。

## 4. 必须报告的规格偏差

- 任何命令、状态、退出码或错误类型变化；
- 任何 Schema 字段增删改；
- 任何需要新常驻服务或外部中间件的建议；
- 任何非只读脚本支持；
- 任何把 Obsidian 变成权威源的设计；
- 任何需要把真实秘密写进仓库/日志/测试证据的流程；
- 任何 5 节点验收项无法自动或人工复现。

## 5. 禁止事项

- 未经规格更新，不加入 Web UI、PostgreSQL、Redis、Celery、多供应商 LLM、自动修复、费用控制或脱敏。
- 不将 50 模拟端点结果描述为 50 台真实服务器认证。
- 不在主分支直接混入未评审的大范围变更；实现以可审查的阶段性提交/PR 交付。

