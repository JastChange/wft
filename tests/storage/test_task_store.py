import hashlib
import json
from pathlib import Path

from wft.inventory.models import Node, NodeSelector
from wft.scripts.models import Manifest, ScriptDefinition
from wft.scripts.snapshot import ScriptSnapshot
from wft.storage.layout import node_key
from wft.storage.task_store import TaskStore
from wft.tasks.create import CreateTaskRequest, create_task
from wft.tasks.models import NodeStatus, TaskStatus, TaskType


def test_publishes_and_loads_authoritative_task_snapshot(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    source = source_root / "check.sh"
    source_bytes = b"#!/bin/sh\nprintf ready\n"
    source.write_bytes(source_bytes)
    digest = hashlib.sha256(source_bytes).hexdigest()
    definition = ScriptDefinition(
        id="check",
        path=Path("check.sh"),
        sha256=digest,
        interpreter="/bin/sh",
        timeout_seconds=20,
        expected_exit_codes=(0,),
        read_only=True,
        supported_os=("linux",),
    )
    snapshot = ScriptSnapshot(
        path=source_root,
        repository_url="https://example.test/scripts.git",
        branch="main",
        commit_sha="c" * 40,
        commit_time="2026-08-07T02:03:04Z",
        manifest_sha256="d" * 64,
        manifest=Manifest(schema_version="1.0", scripts=(definition,), plans=()),
        used_cache=False,
    )
    private_key = tmp_path / "id_ed25519"
    private_key.write_text("THIS PRIVATE KEY BODY MUST STAY OUT")
    nodes = tuple(
        Node(
            name=name,
            host=host,
            username="root",
            private_key_path=private_key,
            os="linux",
        )
        for name, host in (("node-a", "192.0.2.21"), ("node-b", "192.0.2.22"))
    )
    store = TaskStore(tmp_path / "data")

    task = create_task(
        store,
        CreateTaskRequest(
            task_type=TaskType.FAULT_DIAGNOSIS,
            selector=NodeSelector(node_names=("node-a", "node-b")),
            nodes=nodes,
            snapshot=snapshot,
            script_ids=("check",),
            concurrency=2,
        ),
    )

    root = tmp_path / "data" / "tasks" / task.task_id
    expected_source = root / "snapshot" / "scripts" / "check" / digest / "source"
    assert (root / "task.json").is_file()
    assert (root / "snapshot" / "inventory.json").is_file()
    assert (root / "snapshot" / "manifest.json").is_file()
    assert expected_source.read_bytes() == source_bytes
    inventory_text = (root / "snapshot" / "inventory.json").read_text()
    assert str(private_key) in inventory_text
    assert "THIS PRIVATE KEY BODY MUST STAY OUT" not in inventory_text

    manifest_data = json.loads((root / "snapshot" / "manifest.json").read_text())
    assert manifest_data["metadata"]["commit_sha"] == "c" * 40
    assert len(manifest_data["scripts"]) == 1
    for node in nodes:
        key = node_key(node.name)
        assert (root / "nodes" / key / "node.json").is_file()
        assert (root / "nodes" / key / "result.json").is_file()
        assert store.load_node(task.task_id, key).node_name == node.name
        assert store.load_node_result(task.task_id, key).status is NodeStatus.PENDING

    assert store.load(task.task_id).status is TaskStatus.PENDING
    assert list(store.iterate_tasks()) == [task]


def test_iterate_tasks_is_newest_first_and_ignores_staging(tmp_path: Path) -> None:
    store = TaskStore(tmp_path)
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / ".unfinished.partial").mkdir()

    assert list(store.iterate_tasks()) == []
