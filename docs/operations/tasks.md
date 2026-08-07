# M2 手动任务操作指南

## 1. 创建任务

WFT 只支持操作员手动执行。每次命令先获取配置分支的最新脚本快照，生成新的 UUIDv7；不会恢复、复用或自动重试旧任务。远端获取失败时，只有交互确认或 `--allow-cached-scripts` 才能使用已验证缓存。

```bash
wft run installation-validation --config /etc/wft/wft.yaml \
  --group batch --script memory-check --script disk-check --concurrency 10

wft run fault-diagnosis --config /etc/wft/wft.yaml \
  --node node-a --plan memory-triage --json
```

选择规则：

- 重复的 `--node`、`--group`、`--tag` 取并集；也可以单独使用 `--all`；
- 必须且只能选择重复的 `--script` 或一个 `--plan`；
- 并发范围为 1～50，未提供时读取配置的 `default_concurrency`（默认 10）；
- 不支持定时执行、resume、retry 或自动恢复。

## 2. 执行语义

- 不同节点受并发上限约束；同一节点严格按 Manifest/plan 顺序串行执行；
- 通过 SFTP 上传任务自有脚本副本，并在远端校验 SHA-256；
- SSH 自动接受未知 Host Key（`known_hosts=None`），实际算法和 SHA-256 指纹写入节点快照；
- 每个脚本先检查解释器；缺失、超时或执行失败不会阻塞该节点的后续脚本，也不会取消其他节点；
- 超时会请求终止远端进程组，最后总会尽力删除 `/tmp` 下的远端任务目录；
- SSH、脚本和清理都不自动重试，重试只能由操作员创建新任务。

`installation_validation` 的节点结论为：全部检查通过时 `PASS`，检查明确失败时 `FAIL`，连接、解释器、超时或系统证据不足时 `INCONCLUSIVE`。`fault_diagnosis` 的执行状态和问题语义分离：成功采集证据仍为 `COMPLETED`，发现问题通过退出码和后续问题分析表达。

## 3. 权威目录

```text
<data_dir>/tasks/<uuidv7>/
├── task.json
├── snapshot/
│   ├── inventory.json
│   ├── manifest.json
│   └── scripts/<script-id>/<sha256>/source
└── nodes/<node-key>/
    ├── node.json
    ├── result.json
    └── scripts/<script-id>/
        ├── result.json
        ├── stdout.raw
        ├── stderr.raw
        └── execution.log.raw
```

JSON 使用同目录临时文件、`fsync` 和原子替换；读取时拒绝未知 schema 主版本并忽略 `.partial`。stdout/stderr 按原始字节流持续落盘，不做 UTF-8 解码、截断或单次内存聚合。M2 没有 SQLite 权威数据、备份、迁移或容量保留策略；磁盘写入探针失败会拒绝新任务。

## 4. 查询与删除

```bash
wft task list --config /etc/wft/wft.yaml --json
wft task show TASK_ID --config /etc/wft/wft.yaml --json
wft task delete TASK_ID --config /etc/wft/wft.yaml --reason '测试记录已确认无需保留'
```

`list/show` 直接读取权威文件。删除要求非空原因、回显任务 ID 与原因并进行第二次交互确认；确认后删除完整任务目录，不保存 reason、tombstone 或审计事件。M2 Web 尚未交付，也不存在 Web 写操作。

## 5. 中断、残留与退出码

- `0`：任务完成且没有问题，安装验证为 `PASS`；
- `1`：任务完成但发现问题，或安装验证为 `FAIL/INCONCLUSIVE`；
- `2`：配置/系统失败，任务为 `FAILED/CANCELLED`，或执行未完成。

Ctrl-C 保留已独立提交的节点和脚本证据，把未完成节点及任务标记为 `CANCELLED`，不恢复。CLI 的下一次任务或查询命令会把遗留 `RUNNING` 标记为 `FAILED/ABANDONED`，不会重新调度。

M2 发布门禁使用确定性 ENOSPC 注入和回环 AsyncSSH 服务器；没有把真实磁盘填满、kill -9、真实节点认证或超大输出作为发布测试。
