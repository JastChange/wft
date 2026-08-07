import hashlib
from pathlib import Path

import pytest

from wft.inventory.models import Node, NodeSelector
from wft.scripts.models import DiagnosticPlan, Manifest, ScriptDefinition
from wft.scripts.snapshot import ScriptSnapshot
from wft.storage.task_store import TaskStore
from wft.tasks.create import CreateTaskRequest, create_task
from wft.tasks.models import TaskStatus, TaskType


def _snapshot(tmp_path: Path, *, supported_os: tuple[str, ...] = ("linux",)) -> ScriptSnapshot:
    root = tmp_path / "script-snapshot"
    source = root / "checks" / "memory.sh"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"#!/bin/sh\nfree -m\n")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    script = ScriptDefinition(
        id="memory-check",
        path=Path("checks/memory.sh"),
        sha256=digest,
        interpreter="/bin/sh",
        timeout_seconds=30,
        expected_exit_codes=(0,),
        read_only=True,
        supported_os=supported_os,
    )
    return ScriptSnapshot(
        path=root,
        repository_url="ssh://git@example.test/checks.git",
        branch="main",
        commit_sha="a" * 40,
        commit_time="2026-08-07T01:02:03Z",
        manifest_sha256="b" * 64,
        manifest=Manifest(
            schema_version="1.0",
            scripts=(script,),
            plans=(DiagnosticPlan(id="basic", scripts=(script.id,)),),
        ),
        used_cache=False,
    )


def _nodes(tmp_path: Path) -> tuple[Node, ...]:
    key = tmp_path / "operator-key"
    key.write_text("PRIVATE KEY BODY MUST NOT BE COPIED")
    return (
        Node(
            name="node-a",
            host="192.0.2.10",
            username="root",
            private_key_path=key,
            os="linux",
        ),
        Node(
            name="node-b",
            host="192.0.2.11",
            username="admin",
            private_key_path=key,
            os="linux",
        ),
    )


def test_create_task_uses_new_uuid7_each_time(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "data")
    request = CreateTaskRequest(
        task_type=TaskType.INSTALLATION_VALIDATION,
        selector=NodeSelector(all_enabled=True),
        nodes=_nodes(tmp_path),
        snapshot=_snapshot(tmp_path),
        plan_id="basic",
    )

    first = create_task(store, request)
    second = create_task(store, request)

    assert first.task_id != second.task_id
    assert first.status is TaskStatus.PENDING
    assert second.status is TaskStatus.PENDING


def test_invalid_os_fails_before_task_is_published(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "data")
    request = CreateTaskRequest(
        task_type=TaskType.INSTALLATION_VALIDATION,
        selector=NodeSelector(all_enabled=True),
        nodes=_nodes(tmp_path),
        snapshot=_snapshot(tmp_path, supported_os=("ubuntu",)),
        script_ids=("memory-check",),
    )

    with pytest.raises(ValueError, match="does not support node OS"):
        create_task(store, request)

    assert list(store.iterate_tasks()) == []


def test_unknown_script_fails_before_task_is_published(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "data")
    request = CreateTaskRequest(
        task_type=TaskType.FAULT_DIAGNOSIS,
        selector=NodeSelector(node_names=("node-a",)),
        nodes=_nodes(tmp_path)[:1],
        snapshot=_snapshot(tmp_path),
        script_ids=("not-in-manifest",),
    )

    with pytest.raises(ValueError, match="unknown script"):
        create_task(store, request)

    assert list(store.iterate_tasks()) == []
