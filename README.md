# WFT — 批次式 Linux 节点巡检与知识沉淀工具（MVP）

`wft` 在当前 Mac/Linux 控制机上，通过一条命令或本地定时任务对多台 Linux
节点执行**只读**巡检脚本；自动识别异常，生成规则/AI 摘要，把历史结果保存
在 SQLite，并单向导出到本机 Obsidian Vault；异常批次发送 webhook 通知。

> 规格基线：`SPEC_BASELINE_v1.0.md`（Approved for Implementation）。
> 本文对应实现交接清单 Phase 0–1。

## 当前阶段（Phase 0–1）

- ✅ 12 份数据契约 JSON Schema（`contracts/`，Contract-01～12）
- ✅ 每份契约的正常/边界/错误样本与 CI 校验
- ✅ CLI 骨架（`wft --help`）
- ✅ `wft inventory check`（AC-001）
- ✅ `wft hostkey onboard`（AC-002）
- ✅ 只读 ScriptRegistry + 完整 SHA-256 校验（AC-005 / AC-008B）

未实现命令（`run`/`history`/`scheduler`/`storage`/`export`）在后续阶段交付。

## 安装

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[test]"
wft --help
```

## 快速验证

```bash
wft inventory check --file config/inventory.example.yaml
wft script check --file config/scripts.example.yaml
wft script resolve --file config/scripts.example.yaml --ref disk-usage
pytest
```

## 目录

```text
contracts/      # 12 份 JSON Schema（权威副本）
config/         # 示例配置（仅假节点 / 假凭据引用）
scripts/        # 示例只读巡检脚本
src/wft/        # Python 包
tests/          # 单测与契约样本
```

## 使用边界（MVP 已接受风险）

- 不实现费用预算门禁与敏感信息脱敏（ADR-008）；执行输出按原文存储、导出
  并可能发送给配置的 LLM。
- 只支持 `read_only` 巡检脚本；`risk=mutating` 在注册阶段拒绝。
- 配置只允许凭据引用，禁止把秘密正文写入仓库。
