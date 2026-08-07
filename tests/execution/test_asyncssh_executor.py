import asyncio
import hashlib
import os
from pathlib import Path

import asyncssh
import pytest

from tests.support.ssh_test_server import RunningServer
from wft.execution.asyncssh_executor import AsyncSSHNodeExecutor
from wft.inventory.models import Node, NodeSelector
from wft.scripts.models import Manifest, ScriptDefinition
from wft.scripts.snapshot import ScriptSnapshot
from wft.storage.layout import TaskLayout
from wft.storage.task_store import TaskStore
from wft.tasks.create import CreateTaskRequest, create_task
from wft.tasks.models import (
    InstallationConclusion,
    NodeStatus,
    ScriptStatus,
    TaskRecord,
    TaskType,
)


def _write_keys(tmp_path: Path) -> tuple[Path, Path, Path]:
    host_private = tmp_path / "host_key"
    client_private = tmp_path / "client_key"
    authorized = tmp_path / "authorized_keys"
    host_key = asyncssh.generate_private_key("ssh-ed25519")
    client_key = asyncssh.generate_private_key("ssh-ed25519")
    host_private.write_bytes(host_key.export_private_key())
    client_private.write_bytes(client_key.export_private_key())
    authorized.write_bytes(client_key.export_public_key())
    host_private.chmod(0o600)
    client_private.chmod(0o600)
    return host_private, client_private, authorized


def _create_task(
    tmp_path: Path,
    store: TaskStore,
    client_key: Path,
    port: int,
    scripts: tuple[tuple[str, bytes, str, int, tuple[int, ...]], ...],
) -> TaskRecord:
    source_root = tmp_path / f"sources-{port}"
    source_root.mkdir()
    definitions: list[ScriptDefinition] = []
    for script_id, content, interpreter, timeout, expected in scripts:
        path = source_root / f"{script_id}.sh"
        path.write_bytes(content)
        definitions.append(
            ScriptDefinition(
                id=script_id,
                path=path.relative_to(source_root),
                sha256=hashlib.sha256(content).hexdigest(),
                interpreter=interpreter,
                timeout_seconds=timeout,
                expected_exit_codes=expected,
                read_only=True,
                supported_os=("linux",),
            )
        )
    snapshot = ScriptSnapshot(
        path=source_root,
        repository_url="https://example.test/scripts.git",
        branch="main",
        commit_sha="1" * 40,
        commit_time="2026-08-07T05:00:00Z",
        manifest_sha256="2" * 64,
        manifest=Manifest(schema_version="1.0", scripts=tuple(definitions), plans=()),
        used_cache=False,
    )
    node = Node(
        name="ssh-node",
        host="127.0.0.1",
        port=port,
        username="operator",
        private_key_path=client_key,
        os="linux",
    )
    return create_task(
        store,
        CreateTaskRequest(
            task_type=TaskType.INSTALLATION_VALIDATION,
            selector=NodeSelector(node_names=(node.name,)),
            nodes=(node,),
            snapshot=snapshot,
            script_ids=tuple(definition.id for definition in definitions),
            concurrency=1,
        ),
    )


def test_executes_and_streams_raw_bytes(tmp_path: Path) -> None:
    async def scenario() -> None:
        host_key, client_key, authorized = _write_keys(tmp_path)
        store = TaskStore(tmp_path / "data")
        async with RunningServer(host_key_path=host_key, authorized_keys=authorized) as server:
            task = _create_task(
                tmp_path,
                store,
                client_key,
                server.port,
                (
                    (
                        "binary",
                        b"#!/bin/sh\nprintf 'prefix\\377suffix'\nprintf 'error\\376' >&2\n",
                        "/bin/sh",
                        10,
                        (0,),
                    ),
                ),
            )
            node = store.load_node(task.task_id, task.node_keys[0])
            snapshot = store.load_snapshot(task.task_id)

            result = await AsyncSSHNodeExecutor(store).execute(node, snapshot)

            assert result.status is NodeStatus.COMPLETED
            assert result.installation_conclusion is InstallationConclusion.PASS
            assert result.host_key is not None
            assert result.host_key.algorithm == "ssh-ed25519"
            assert result.host_key.fingerprint.startswith("SHA256:")
            assert store.load_node(task.task_id, node.node_key).host_key == result.host_key
            script = store.load_script_result(task.task_id, node.node_key, "binary")
            assert script.status is ScriptStatus.COMPLETED
            assert script.check_passed is True
            layout = TaskLayout(store.data_dir, task.task_id)
            script_dir = layout.script_dir(node.node_key, "binary")
            assert (script_dir / script.stdout.path).read_bytes() == b"prefix\xffsuffix"
            assert (script_dir / script.stderr.path).read_bytes() == b"error\xfe"
            assert (script_dir / script.execution_log.path).is_file()
            assert not Path(f"/tmp/wft-{task.task_id}-{node.node_key}").exists()
            assert result.cleanup.succeeded is True

    asyncio.run(scenario())


def test_script_failures_continue_in_order_and_timeout_is_terminated(tmp_path: Path) -> None:
    async def scenario() -> None:
        host_key, client_key, authorized = _write_keys(tmp_path)
        store = TaskStore(tmp_path / "data")
        child_pid_file = Path("/tmp") / f"wft-child-{tmp_path.name}.pid"
        child_pid_file.unlink(missing_ok=True)
        timeout_script = f"trap '' TERM\nsleep 10 &\necho $! > {child_pid_file}\nwait\n".encode()
        async with RunningServer(host_key_path=host_key, authorized_keys=authorized) as server:
            task = _create_task(
                tmp_path,
                store,
                client_key,
                server.port,
                (
                    ("missing", b"echo no\n", "/missing/interpreter", 5, (0,)),
                    ("timeout", timeout_script, "/bin/sh", 1, (0,)),
                    ("unexpected", b"exit 7\n", "/bin/sh", 5, (0,)),
                    ("after", b"echo continued\n", "/bin/sh", 5, (0,)),
                ),
            )
            node = store.load_node(task.task_id, task.node_keys[0])

            result = await AsyncSSHNodeExecutor(store).execute(
                node, store.load_snapshot(task.task_id)
            )

            statuses = [
                store.load_script_result(task.task_id, node.node_key, script_id)
                for script_id in ("missing", "timeout", "unexpected", "after")
            ]
            assert [item.status for item in statuses] == [
                ScriptStatus.FAILED,
                ScriptStatus.TIMEOUT,
                ScriptStatus.COMPLETED,
                ScriptStatus.COMPLETED,
            ]
            assert statuses[2].check_passed is False
            assert statuses[3].check_passed is True
            layout = TaskLayout(store.data_dir, task.task_id)
            missing_log = (
                layout.script_dir(node.node_key, "missing") / statuses[0].execution_log.path
            ).read_text()
            timeout_log = (
                layout.script_dir(node.node_key, "timeout") / statuses[1].execution_log.path
            ).read_text()
            assert "INTERPRETER_NOT_FOUND" in missing_log
            assert "TIMEOUT" in timeout_log
            assert result.script_ids == ("missing", "timeout", "unexpected", "after")
            assert result.installation_conclusion is InstallationConclusion.INCONCLUSIVE
            child_pid = int(child_pid_file.read_text())
            for _ in range(20):
                try:
                    os.kill(child_pid, 0)
                except ProcessLookupError:
                    break
                await asyncio.sleep(0.05)
            else:
                raise AssertionError("timed-out script child process is still running")
            child_pid_file.unlink(missing_ok=True)

    asyncio.run(scenario())


def test_connection_failure_is_one_failed_node_attempt(tmp_path: Path) -> None:
    _host_key, client_key, _authorized = _write_keys(tmp_path)
    store = TaskStore(tmp_path / "data")
    task = _create_task(
        tmp_path,
        store,
        client_key,
        1,
        (("check", b"echo never\n", "/bin/sh", 5, (0,)),),
    )
    node = store.load_node(task.task_id, task.node_keys[0])

    result = asyncio.run(
        AsyncSSHNodeExecutor(store, connect_timeout=0.2).execute(
            node, store.load_snapshot(task.task_id)
        )
    )

    assert result.status is NodeStatus.FAILED
    assert result.installation_conclusion is InstallationConclusion.INCONCLUSIVE
    assert result.failure is not None
    assert result.failure.code == "CONNECTION_FAILED"
    assert "Traceback" in result.failure.message
    assert result.cleanup.attempted is False
    assert tuple(store.iterate_script_results(task.task_id)) == ()


def test_cleanup_failure_does_not_replace_committed_script_result(tmp_path: Path) -> None:
    class CleanupFailingExecutor(AsyncSSHNodeExecutor):
        async def _cleanup_remote(
            self, connection: asyncssh.SSHClientConnection, remote_dir: str
        ) -> None:
            raise RuntimeError("fixture cleanup failure")

    async def scenario() -> None:
        host_key, client_key, authorized = _write_keys(tmp_path)
        store = TaskStore(tmp_path / "data")
        async with RunningServer(host_key_path=host_key, authorized_keys=authorized) as server:
            task = _create_task(
                tmp_path,
                store,
                client_key,
                server.port,
                (("check", b"echo retained\n", "/bin/sh", 5, (0,)),),
            )
            node = store.load_node(task.task_id, task.node_keys[0])

            result = await CleanupFailingExecutor(store).execute(
                node, store.load_snapshot(task.task_id)
            )

            committed = store.load_script_result(task.task_id, node.node_key, "check")
            assert committed.status is ScriptStatus.COMPLETED
            assert result.status is NodeStatus.COMPLETED
            assert result.cleanup.attempted is True
            assert result.cleanup.succeeded is False
            assert result.failure is not None
            assert result.failure.code == "CLEANUP_FAILED"
            assert "Traceback" in result.failure.message
            assert result.cleanup.error is not None
            assert "Traceback" in result.cleanup.error

    asyncio.run(scenario())


def test_cancellation_terminates_remote_process_group_before_returning(tmp_path: Path) -> None:
    async def scenario() -> None:
        host_key, client_key, authorized = _write_keys(tmp_path)
        store = TaskStore(tmp_path / "data")
        child_pid_file = Path("/tmp") / f"wft-cancel-child-{tmp_path.name}.pid"
        child_pid_file.unlink(missing_ok=True)
        script = f"trap '' TERM\nsleep 20 &\necho $! > {child_pid_file}\nwait\n".encode()
        async with RunningServer(host_key_path=host_key, authorized_keys=authorized) as server:
            task = _create_task(
                tmp_path,
                store,
                client_key,
                server.port,
                (("cancel", script, "/bin/sh", 30, (0,)),),
            )
            node = store.load_node(task.task_id, task.node_keys[0])
            execution = asyncio.create_task(
                AsyncSSHNodeExecutor(store).execute(node, store.load_snapshot(task.task_id))
            )
            for _ in range(40):
                if child_pid_file.is_file():
                    break
                await asyncio.sleep(0.05)
            else:
                raise AssertionError("remote child process never started")
            child_pid = int(child_pid_file.read_text())

            execution.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(execution, timeout=6)

            with pytest.raises(ProcessLookupError):
                os.kill(child_pid, 0)
            assert not Path(f"/tmp/wft-{task.task_id}-{node.node_key}").exists()
            child_pid_file.unlink(missing_ok=True)

    asyncio.run(scenario())
