from pathlib import Path

import pytest

from tests.tasks.test_runner import _created_task
from wft.ids import new_uuid7
from wft.storage.task_store import TaskStore


def test_delete_removes_only_authoritative_task_directory(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "data")
    task = _created_task(tmp_path, store, nodes_count=1)
    task_dir = store.tasks_dir / task.task_id

    store.delete(task.task_id)

    assert not task_dir.exists()
    assert list(store.tasks_dir.iterdir()) == []
    with pytest.raises(FileNotFoundError):
        store.delete(task.task_id)


def test_delete_rejects_invalid_task_id(tmp_path: Path) -> None:
    store = TaskStore(tmp_path)

    with pytest.raises(ValueError, match="UUID"):
        store.delete("../outside")


def test_delete_never_follows_task_root_symlink(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "data")
    store.tasks_dir.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    evidence = outside / "keep.txt"
    evidence.write_text("keep")
    task_id = new_uuid7()
    (store.tasks_dir / task_id).symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="symlink"):
        store.delete(task_id)

    assert evidence.read_text() == "keep"
