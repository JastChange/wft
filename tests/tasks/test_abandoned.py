from pathlib import Path

from tests.tasks.test_runner import _created_task
from wft.clock import format_timestamp, utc_now
from wft.storage.layout import TaskLayout
from wft.storage.task_store import TaskStore
from wft.tasks.models import CleanupResult, NodeExecutionResult, NodeStatus, TaskStatus
from wft.tasks.runner import mark_abandoned_tasks_failed
from wft.tasks.state import transition_task


def test_marks_running_task_failed_without_touching_completed_node(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "data")
    task = _created_task(tmp_path, store, nodes_count=2)
    running = transition_task(task, TaskStatus.RUNNING, format_timestamp(utc_now()))
    store.update_task(running)
    first = store.load_node(task.task_id, task.node_keys[0])
    second = store.load_node(task.task_id, task.node_keys[1])
    completed = NodeExecutionResult(
        task_id=task.task_id,
        node_key=first.node_key,
        status=NodeStatus.COMPLETED,
        started_at="2026-08-07T06:00:00Z",
        finished_at="2026-08-07T06:01:00Z",
        script_ids=("first", "second"),
        cleanup=CleanupResult(attempted=True, succeeded=True),
    )
    unfinished = NodeExecutionResult(
        task_id=task.task_id,
        node_key=second.node_key,
        status=NodeStatus.RUNNING,
        started_at="2026-08-07T06:00:00Z",
        cleanup=CleanupResult(attempted=False, succeeded=False),
    )
    store.commit_node_result(completed)
    store.commit_node_result(unfinished)
    completed_path = (
        TaskLayout(store.data_dir, task.task_id).node_dir(first.node_key) / "result.json"
    )
    before = completed_path.read_bytes()

    assert mark_abandoned_tasks_failed(store) == 1

    assert store.load(task.task_id).status is TaskStatus.FAILED
    assert store.load(task.task_id).exit_code == 2
    assert completed_path.read_bytes() == before
    abandoned = store.load_node_result(task.task_id, second.node_key)
    assert abandoned.status is NodeStatus.FAILED
    assert abandoned.failure is not None
    assert abandoned.failure.code == "ABANDONED"
    assert mark_abandoned_tasks_failed(store) == 0
