import asyncio
from contextlib import suppress
from pathlib import Path

from tests.cli.test_m2_commands import CommittingExecutor
from tests.tasks.test_runner import _created_task
from wft.storage.task_store import TaskStore
from wft.tasks.models import NodeStatus, ScriptStatus, TaskStatus
from wft.tasks.runner import run_task


def test_files_remain_authoritative_after_unrelated_sqlite_is_deleted(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "data")
    task = _created_task(tmp_path, store, nodes_count=1)
    finished = asyncio.run(
        run_task(
            store,
            task.task_id,
            CommittingExecutor(store, ScriptStatus.COMPLETED, True),
        )
    )
    node_before = store.load_node_result(task.task_id, task.node_keys[0])
    scripts_before = tuple(store.iterate_script_results(task.task_id))
    raw_before = {
        path.relative_to(store.data_dir): path.read_bytes()
        for path in store.data_dir.rglob("*.raw")
    }
    sqlite = store.data_dir / "index.sqlite"
    sqlite.write_bytes(b"not authoritative")
    sqlite.unlink()

    reopened = TaskStore(store.data_dir)

    assert reopened.load(task.task_id) == finished
    assert reopened.load_node_result(task.task_id, task.node_keys[0]) == node_before
    assert tuple(reopened.iterate_script_results(task.task_id)) == scripts_before
    assert {
        path.relative_to(reopened.data_dir): path.read_bytes()
        for path in reopened.data_dir.rglob("*.raw")
    } == raw_before
    assert node_before.status is NodeStatus.COMPLETED


def test_independent_commits_survive_task_cancellation(tmp_path: Path) -> None:
    async def scenario() -> str:
        store = TaskStore(tmp_path / "data")
        task = _created_task(tmp_path, store, nodes_count=2)
        first_name = store.load_node(task.task_id, task.node_keys[0]).node_name
        first_done = asyncio.Event()

        class SplitExecutor:
            async def execute(self, node, snapshot):
                if node.node_name == first_name:
                    result = await CommittingExecutor(store, ScriptStatus.COMPLETED, True).execute(
                        node, snapshot
                    )
                    first_done.set()
                    return result
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

        running = asyncio.create_task(run_task(store, task.task_id, SplitExecutor()))
        await first_done.wait()
        await asyncio.sleep(0.03)
        running.cancel()
        with suppress(asyncio.CancelledError):
            await running
        return task.task_id

    task_id = asyncio.run(scenario())
    reopened = TaskStore(tmp_path / "data")
    task = reopened.load(task_id)
    first = reopened.load_node_result(task_id, task.node_keys[0])
    second = reopened.load_node_result(task_id, task.node_keys[1])

    assert task.status is TaskStatus.CANCELLED
    assert task.exit_code == 2
    assert first.status is NodeStatus.COMPLETED
    assert tuple(reopened.iterate_script_results(task_id))
    assert second.status is NodeStatus.CANCELLED
    assert not list(reopened.data_dir.rglob("*.partial"))


def test_storage_package_never_imports_sqlite3() -> None:
    storage_root = Path(__file__).parents[2] / "src" / "wft" / "storage"

    assert all("sqlite3" not in path.read_text() for path in storage_root.glob("*.py"))
