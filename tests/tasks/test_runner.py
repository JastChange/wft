import asyncio
import hashlib
from pathlib import Path

import pytest

from wft.execution.fake import FakeNodeExecutor
from wft.inventory.models import Node, NodeSelector
from wft.scripts.models import Manifest, ScriptDefinition
from wft.scripts.snapshot import ScriptSnapshot
from wft.storage.task_store import TaskStore
from wft.tasks.create import CreateTaskRequest, create_task
from wft.tasks.models import NodeExecutionResult, NodeStatus, TaskRecord, TaskStatus, TaskType
from wft.tasks.runner import run_task


class ObservingTaskStore(TaskStore):
    def __init__(self, data_dir: Path) -> None:
        super().__init__(data_dir)
        self.events: list[tuple[str, str]] = []

    def commit_node_result(self, result: NodeExecutionResult) -> None:
        super().commit_node_result(result)
        self.events.append(("node", result.node_key))

    def update_task(self, task: TaskRecord) -> None:
        super().update_task(task)
        self.events.append(("task", task.status.value))


def _created_task(tmp_path: Path, store: TaskStore, *, nodes_count: int = 4) -> TaskRecord:
    snapshot_root = tmp_path / "snapshot"
    snapshot_root.mkdir()
    definitions: list[ScriptDefinition] = []
    for script_id in ("first", "second"):
        path = snapshot_root / f"{script_id}.sh"
        content = f"#!/bin/sh\necho {script_id}\n".encode()
        path.write_bytes(content)
        definitions.append(
            ScriptDefinition(
                id=script_id,
                path=path.relative_to(snapshot_root),
                sha256=hashlib.sha256(content).hexdigest(),
                interpreter="/bin/sh",
                timeout_seconds=30,
                expected_exit_codes=(0,),
                read_only=True,
                supported_os=("linux",),
            )
        )
    snapshot = ScriptSnapshot(
        path=snapshot_root,
        repository_url="https://example.test/scripts.git",
        branch="main",
        commit_sha="e" * 40,
        commit_time="2026-08-07T04:00:00Z",
        manifest_sha256="f" * 64,
        manifest=Manifest(schema_version="1.0", scripts=tuple(definitions), plans=()),
        used_cache=False,
    )
    key = tmp_path / "id_ed25519"
    key.write_text("fixture key path only")
    nodes = tuple(
        Node(
            name=f"node-{index}",
            host=f"192.0.2.{index + 30}",
            username="root",
            private_key_path=key,
            os="linux",
        )
        for index in range(nodes_count)
    )
    return create_task(
        store,
        CreateTaskRequest(
            task_type=TaskType.FAULT_DIAGNOSIS,
            selector=NodeSelector(all_enabled=True),
            nodes=nodes,
            snapshot=snapshot,
            script_ids=("first", "second"),
            concurrency=2,
        ),
    )


def test_runner_limits_concurrency_and_preserves_snapshot_order(tmp_path: Path) -> None:
    store = ObservingTaskStore(tmp_path / "data")
    task = _created_task(tmp_path, store)
    nodes = [store.load_node(task.task_id, key) for key in task.node_keys]
    executor = FakeNodeExecutor(delays={node.node_name: 0.02 for node in nodes})

    finished = asyncio.run(run_task(store, task.task_id, executor))

    assert finished.status is TaskStatus.COMPLETED
    assert executor.max_active == 2
    assert executor.call_counts == {node.node_name: 1 for node in nodes}
    assert executor.commit_shas == {"e" * 40}
    assert executor.script_orders == {("first", "second")}
    assert all(
        store.load_node_result(task.task_id, key).status is NodeStatus.COMPLETED
        for key in task.node_keys
    )
    final_event = store.events.index(("task", TaskStatus.COMPLETED.value))
    assert sum(event[0] == "node" for event in store.events[:final_event]) == len(nodes)


def test_executor_exception_fails_only_one_node_without_retry(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "data")
    task = _created_task(tmp_path, store, nodes_count=3)
    failed_node = store.load_node(task.task_id, task.node_keys[1])
    executor = FakeNodeExecutor(exceptions={failed_node.node_name: RuntimeError("boom")})

    finished = asyncio.run(run_task(store, task.task_id, executor))

    assert finished.status is TaskStatus.COMPLETED
    assert finished.exit_code == 1
    assert executor.call_counts[failed_node.node_name] == 1
    statuses = [store.load_node_result(task.task_id, key).status for key in task.node_keys]
    assert statuses.count(NodeStatus.FAILED) == 1
    assert statuses.count(NodeStatus.COMPLETED) == 2


def test_cancellation_persists_cancelled_task_and_nodes(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "data")
    task = _created_task(tmp_path, store, nodes_count=3)
    nodes = [store.load_node(task.task_id, key) for key in task.node_keys]
    executor = FakeNodeExecutor(delays={node.node_name: 5 for node in nodes})

    async def cancel_running_task() -> None:
        running = asyncio.create_task(run_task(store, task.task_id, executor))
        await asyncio.sleep(0.03)
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running

    asyncio.run(cancel_running_task())

    cancelled = store.load(task.task_id)
    assert cancelled.status is TaskStatus.CANCELLED
    assert cancelled.exit_code == 2
    results = [store.load_node_result(task.task_id, key) for key in task.node_keys]
    assert all(result.status is NodeStatus.CANCELLED for result in results)
    assert all(count == 1 for count in executor.call_counts.values())
