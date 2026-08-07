import asyncio
import errno
from pathlib import Path

import pytest

from tests.tasks.test_runner import _created_task
from wft.execution.fake import FakeNodeExecutor
from wft.storage.atomic import StorageFullError, write_atomic_json
from wft.storage.task_store import TaskStore
from wft.tasks.models import TaskStatus
from wft.tasks.runner import run_task


def test_atomic_replace_maps_disk_full_and_removes_partial(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def disk_full(_source: Path, _destination: Path) -> None:
        raise OSError(errno.ENOSPC, "disk full")

    monkeypatch.setattr("wft.storage.atomic.os.replace", disk_full)

    with pytest.raises(StorageFullError):
        write_atomic_json(tmp_path / "task.json", {"schema_version": "1.0"})
    assert not list(tmp_path.glob("*.partial"))


def test_storage_probe_prevents_publication_without_deleting_history(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    first_root = tmp_path / "first"
    first_root.mkdir()
    store = TaskStore(tmp_path / "data")
    history = _created_task(first_root, store, nodes_count=1)
    second_root = tmp_path / "second"
    second_root.mkdir()

    def disk_full(_fd: int, _data: bytes) -> int:
        raise OSError(errno.ENOSPC, "disk full")

    monkeypatch.setattr("wft.storage.raw_streams.os.write", disk_full)

    with pytest.raises(StorageFullError):
        _created_task(second_root, store, nodes_count=1)
    assert store.load(history.task_id) == history
    assert tuple(store.iterate_tasks()) == (history,)


def test_current_task_becomes_failed_after_one_storage_commit_error(tmp_path: Path) -> None:
    class FailingOnceStore(TaskStore):
        def __init__(self, data_dir: Path) -> None:
            super().__init__(data_dir)
            self.attempts = 0

        def commit_node_result(self, result) -> None:
            self.attempts += 1
            if self.attempts == 1:
                raise StorageFullError(errno.ENOSPC, "fixture full")
            super().commit_node_result(result)

    store = FailingOnceStore(tmp_path / "data")
    task = _created_task(tmp_path, store, nodes_count=1)

    with pytest.raises(StorageFullError):
        asyncio.run(run_task(store, task.task_id, FakeNodeExecutor()))

    assert store.attempts == 1
    assert store.load(task.task_id).status is TaskStatus.FAILED
    assert (store.tasks_dir / task.task_id).is_dir()


def test_executor_storage_full_is_a_task_failure_not_a_node_problem(tmp_path: Path) -> None:
    class StorageFailingExecutor:
        async def execute(self, node, snapshot):
            raise StorageFullError(errno.ENOSPC, "raw stream full")

    store = TaskStore(tmp_path / "data")
    task = _created_task(tmp_path, store, nodes_count=1)

    with pytest.raises(StorageFullError):
        asyncio.run(run_task(store, task.task_id, StorageFailingExecutor()))

    failed = store.load(task.task_id)
    assert failed.status is TaskStatus.FAILED
    assert failed.exit_code == 2


def test_failure_to_persist_running_is_rewritten_as_failed(tmp_path: Path) -> None:
    class StartFailingStore(TaskStore):
        def __init__(self, data_dir: Path) -> None:
            super().__init__(data_dir)
            self.update_attempts = 0

        def update_task(self, task) -> None:
            self.update_attempts += 1
            if self.update_attempts == 1:
                raise StorageFullError(errno.ENOSPC, "cannot persist RUNNING")
            super().update_task(task)

    store = StartFailingStore(tmp_path / "data")
    task = _created_task(tmp_path, store, nodes_count=1)
    executor = FakeNodeExecutor()

    with pytest.raises(StorageFullError):
        asyncio.run(run_task(store, task.task_id, executor))

    assert executor.call_counts == {}
    assert store.update_attempts == 2
    assert store.load(task.task_id).status is TaskStatus.FAILED
