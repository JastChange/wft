# WFT 权威数据格式 v1.0

> 状态：Approved for Planning
> 所有示例字段均为 1.0 必需基线；实现不得私自改变字段语义。

## 1. 目录布局

```text
tasks/<task-id>/
├── task.json
├── snapshot/
│   ├── inventory.json
│   ├── manifest.json
│   └── scripts/<script-id>/<sha256>/source
├── nodes/<node-key>/
│   ├── node.json
│   ├── result.json
│   └── scripts/<script-id>/
│       ├── result.json
│       ├── stdout.raw
│       ├── stderr.raw
│       └── execution.log
├── analyses/<analysis-id>/
│   ├── analysis.json
│   └── chunks/<chunk-id>.json
├── problems/<problem-id>.json
└── notifications/<event-id>.json
```

- `<task-id>`、`<analysis-id>`、`<problem-id>` 和 `<event-id>` 使用 UUIDv7 字符串。
- `<node-key>` 是节点名的稳定路径编码，不直接信任用户输入作为路径。
- JSON 使用 UTF-8、排序键、末尾换行；时间使用 UTC RFC 3339。
- 原始流按字节保存，不要求 UTF-8。

## 2. `task.json`

```json
{
  "schema_version": "1.0",
  "task_id": "0198f000-0000-7000-8000-000000000001",
  "task_type": "installation_validation",
  "status": "COMPLETED",
  "created_at": "2026-08-06T02:00:00Z",
  "started_at": "2026-08-06T02:00:01Z",
  "finished_at": "2026-08-06T02:03:00Z",
  "selector": {
    "node_names": [],
    "groups": ["batch-a"],
    "tags": [],
    "all_enabled": false
  },
  "node_keys": ["node-a-8e4a"],
  "script_snapshot": {
    "repository_url": "ssh://git@example/scripts.git",
    "branch": "main",
    "commit_sha": "0123456789abcdef0123456789abcdef01234567",
    "commit_time": "2026-08-06T01:55:00Z",
    "manifest_sha256": "<64-hex>",
    "used_cached_snapshot": false
  },
  "selection": {
    "script_ids": [],
    "plan_id": "base-installation"
  },
  "concurrency": 10,
  "summary": {
    "nodes_total": 1,
    "nodes_completed": 1,
    "nodes_failed": 0,
    "nodes_cancelled": 0,
    "problems": 0
  },
  "exit_code": 0,
  "failure": null
}
```

约束：

- `task_type`: `installation_validation | fault_diagnosis`；
- `status`: `PENDING | RUNNING | COMPLETED | FAILED | CANCELLED`；
- 终态必须有 `finished_at` 和 `exit_code`；
- `failure` 仅描述任务执行失败，不承载诊断问题。

## 3. 节点快照 `node.json`

```json
{
  "schema_version": "1.0",
  "task_id": "<uuidv7>",
  "node_key": "node-a-8e4a",
  "node_name": "node-a",
  "host": "10.0.0.11",
  "port": 22,
  "username": "root",
  "private_key_path": "/run/secrets/node-a.key",
  "groups": ["batch-a"],
  "tags": ["ubuntu-24.04"],
  "host_key": {
    "algorithm": "ssh-ed25519",
    "fingerprint": "SHA256:...",
    "accepted_automatically": true
  }
}
```

`private_key_path` 是任务创建时的配置快照，不包含私钥正文。Web 默认不显示该路径。

## 4. 节点结果 `result.json`

```json
{
  "schema_version": "1.0",
  "task_id": "<uuidv7>",
  "node_key": "node-a-8e4a",
  "status": "COMPLETED",
  "installation_conclusion": "PASS",
  "started_at": "2026-08-06T02:00:01Z",
  "finished_at": "2026-08-06T02:00:20Z",
  "script_ids": ["memory", "disk"],
  "cleanup": {
    "attempted": true,
    "succeeded": true,
    "error": null
  },
  "failure": null
}
```

- 故障诊断任务的 `installation_conclusion` 必须为 `null`。
- 节点状态使用 `PENDING | RUNNING | COMPLETED | FAILED | CANCELLED`。

## 5. 脚本结果 `scripts/<script-id>/result.json`

```json
{
  "schema_version": "1.0",
  "task_id": "<uuidv7>",
  "node_key": "node-a-8e4a",
  "script_id": "memory",
  "script_sha256": "<64-hex>",
  "interpreter": "bash",
  "status": "COMPLETED",
  "started_at": "2026-08-06T02:00:02Z",
  "finished_at": "2026-08-06T02:00:05Z",
  "exit_code": 0,
  "expected_exit_codes": [0],
  "check_passed": true,
  "stdout": {
    "path": "stdout.raw",
    "size_bytes": 120,
    "sha256": "<64-hex>"
  },
  "stderr": {
    "path": "stderr.raw",
    "size_bytes": 0,
    "sha256": "<64-hex>"
  },
  "execution_log": {
    "path": "execution.log",
    "size_bytes": 240,
    "sha256": "<64-hex>"
  },
  "failure": null
}
```

脚本状态：`PENDING | RUNNING | COMPLETED | TIMEOUT | FAILED | CANCELLED`。`check_passed` 只在 `COMPLETED` 时为布尔值，其余状态为 `null`。

## 6. 分析版本 `analysis.json`

```json
{
  "schema_version": "1.0",
  "analysis_id": "<uuidv7>",
  "task_id": "<uuidv7>",
  "status": "COMPLETED",
  "source": "ai",
  "created_at": "2026-08-06T03:00:00Z",
  "model": "configured-model",
  "endpoint_origin": "https://ai.example.invalid",
  "prompt_version": "1.0",
  "input_sha256": "<64-hex>",
  "input_budget": 120000,
  "language": "zh-CN",
  "summary": "节点存在内存压力迹象。",
  "issues": [
    {
      "title": "内存压力",
      "possible_causes": ["应用工作集增长"],
      "confidence": 0.72,
      "evidence": [
        {
          "node_key": "node-a-8e4a",
          "script_id": "memory",
          "file": "stdout.raw",
          "start_byte": 0,
          "end_byte": 120,
          "quote": "..."
        }
      ],
      "next_checks": ["检查进程 RSS 排名"]
    }
  ],
  "failure": null
}
```

- `source`: `ai | rules`；
- AI 失败时也写一个终态分析版本，`source=rules`，并在 `failure` 中保存 AI 失败事实；
- API Key、完整认证 URL 参数和 Cookie 不得写入分析记录。

## 7. 问题 `problems/<problem-id>.json`

```json
{
  "schema_version": "1.0",
  "problem_id": "<uuidv7>",
  "task_id": "<uuidv7>",
  "node_key": "node-a-8e4a",
  "title": "内存压力",
  "classification": "classified",
  "source_problem_ids": ["<uuidv7>"],
  "source_analysis_id": "<uuidv7>",
  "evidence_refs": [
    {
      "script_id": "memory",
      "file": "stdout.raw",
      "start_byte": 0,
      "end_byte": 120
    }
  ]
}
```

- `classification`: `pending | classified | merged`；
- 问题只能属于一个 `node_key`；禁止跨节点归并；
- 归并后原始问题文件保留并以 `merged` 指向新问题。

## 8. 通知 `notifications/<event-id>.json`

```json
{
  "schema_version": "1.0",
  "event_id": "<uuidv7>",
  "task_id": "<uuidv7>",
  "created_at": "2026-08-06T02:03:01Z",
  "attempted_at": "2026-08-06T02:03:02Z",
  "status": "FAILED",
  "attempt_count": 1,
  "http_status": 503,
  "error": "webhook returned 503",
  "payload_sha256": "<64-hex>"
}
```

`attempt_count` 在 1.0 中只能为 `0` 或 `1`；失败通知不得重新发送。

## 9. 原子性和落盘顺序

1. 创建任务目录和不可变快照；
2. 原子写 `task.json(PENDING)`；
3. 原子写 `task.json(RUNNING)`；
4. 原始流直接写 `.partial`，完成后 fsync + rename；
5. 原子写对应脚本 `result.json`；
6. 全部脚本终态后原子写节点 `result.json`；
7. 全部节点终态后原子写最终 `task.json`；
8. 最后更新派生索引和通知记录。

读取者忽略 `.partial` 文件。未知 schema 主版本、缺少必填 JSON 或哈希不匹配必须显示为损坏记录，不得猜测或静默修复。

## 10. Manifest 最小格式

```yaml
schema_version: "1.0"
scripts:
  - id: memory
    path: scripts/memory.sh
    sha256: <64-hex>
    interpreter: bash
    timeout_seconds: 60
    expected_exit_codes: [0]
    read_only: true
    supported_os: [ubuntu-22.04, ubuntu-24.04]
plans:
  - id: base-installation
    scripts: [memory, disk]
```

脚本路径必须位于仓库根目录内；重复 ID、路径逃逸、哈希不匹配、`read_only != true` 或未支持目标 OS 都必须在创建任务前失败。
