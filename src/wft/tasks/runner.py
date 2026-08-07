import asyncio
from contextlib import suppress

from wft.clock import format_timestamp, utc_now
from wft.execution.interface import NodeExecutor
from wft.storage.atomic import StorageFullError
from wft.storage.task_store import TaskStore
from wft.tasks.models import (
    CleanupResult,
    FailureRecord,
    InstallationConclusion,
    NodeExecutionResult,
    NodeSnapshot,
    NodeStatus,
    TaskRecord,
    TaskStatus,
    TaskType,
)
from wft.tasks.state import conclude_installation, finish_task, transition_task


def _failed_node(node: NodeSnapshot, error: Exception) -> NodeExecutionResult:
    now = format_timestamp(utc_now())
    return NodeExecutionResult(
        task_id=node.task_id,
        node_key=node.node_key,
        status=NodeStatus.FAILED,
        installation_conclusion=InstallationConclusion.INCONCLUSIVE,
        started_at=now,
        finished_at=now,
        cleanup=CleanupResult(attempted=False, succeeded=False),
        failure=FailureRecord(code="EXECUTOR_EXCEPTION", message=str(error), phase="execute"),
    )


def _cancelled_node(node: NodeSnapshot, task_type: TaskType) -> NodeExecutionResult:
    now = format_timestamp(utc_now())
    conclusion = (
        InstallationConclusion.INCONCLUSIVE
        if task_type is TaskType.INSTALLATION_VALIDATION
        else None
    )
    return NodeExecutionResult(
        task_id=node.task_id,
        node_key=node.node_key,
        status=NodeStatus.CANCELLED,
        installation_conclusion=conclusion,
        finished_at=now,
        cleanup=CleanupResult(attempted=False, succeeded=False),
        failure=FailureRecord(code="CANCELLED", message="task execution was cancelled"),
    )


def _validate_executor_result(
    node: NodeSnapshot,
    result: NodeExecutionResult,
    expected_scripts: tuple[str, ...],
) -> None:
    if result.task_id != node.task_id or result.node_key != node.node_key:
        raise ValueError("executor returned a result for a different node")
    if result.status in {NodeStatus.PENDING, NodeStatus.RUNNING}:
        raise ValueError("executor returned a nonterminal node result")
    if result.script_ids != expected_scripts[: len(result.script_ids)]:
        raise ValueError("executor returned scripts in an unexpected order")


async def run_task(store: TaskStore, task_id: str, executor: NodeExecutor) -> TaskRecord:
    task = store.load(task_id)
    if task.status is not TaskStatus.PENDING:
        raise ValueError("only a PENDING task can be run")
    snapshot = store.load_snapshot(task_id)
    nodes = tuple(store.load_node(task_id, key) for key in task.node_keys)
    running = transition_task(task, TaskStatus.RUNNING, format_timestamp(utc_now()))
    try:
        store.update_task(running)
    except Exception as exc:
        failed = transition_task(
            task,
            TaskStatus.FAILED,
            format_timestamp(utc_now()),
            failure=FailureRecord(code="START_FAILURE", message=str(exc), phase="runner"),
        )
        with suppress(Exception):
            store.update_task(failed)
        raise
    semaphore = asyncio.Semaphore(running.concurrency)
    results: dict[str, NodeExecutionResult] = {}
    expected_scripts = tuple(script.id for script in snapshot.scripts)

    async def execute_one(node: NodeSnapshot) -> None:
        async with semaphore:
            try:
                result = await executor.execute(node, snapshot)
                _validate_executor_result(node, result, expected_scripts)
            except asyncio.CancelledError:
                raise
            except StorageFullError:
                raise
            except Exception as exc:
                result = _failed_node(node, exc)
                if running.task_type is TaskType.FAULT_DIAGNOSIS:
                    result = NodeExecutionResult.model_validate(
                        {**result.model_dump(mode="json"), "installation_conclusion": None}
                    )
            store.commit_node_result(result)
            results[node.node_key] = result

    jobs = [asyncio.create_task(execute_one(node)) for node in nodes]
    try:
        await asyncio.gather(*jobs)
    except asyncio.CancelledError:
        for job in jobs:
            job.cancel()
        await asyncio.gather(*jobs, return_exceptions=True)
        for node in nodes:
            existing = store.load_node_result(task_id, node.node_key)
            if existing.status in {NodeStatus.PENDING, NodeStatus.RUNNING}:
                store.commit_node_result(_cancelled_node(node, running.task_type))
        cancelled = transition_task(
            store.load(task_id),
            TaskStatus.CANCELLED,
            format_timestamp(utc_now()),
            failure=FailureRecord(code="CANCELLED", message="task execution was cancelled"),
        )
        store.update_task(cancelled)
        raise
    except Exception as exc:
        for job in jobs:
            job.cancel()
        await asyncio.gather(*jobs, return_exceptions=True)
        failed = transition_task(
            store.load(task_id),
            TaskStatus.FAILED,
            format_timestamp(utc_now()),
            failure=FailureRecord(code="RUNNER_FAILURE", message=str(exc), phase="runner"),
        )
        store.update_task(failed)
        raise

    ordered_results = tuple(results[node.node_key] for node in nodes)
    script_results = tuple(store.iterate_script_results(task_id))
    if running.task_type is TaskType.INSTALLATION_VALIDATION:
        adjusted: list[NodeExecutionResult] = []
        for node, result in zip(nodes, ordered_results, strict=True):
            node_scripts = tuple(
                script for script in script_results if script.node_key == node.node_key
            )
            conclusion = conclude_installation(node_scripts)
            adjusted_result = NodeExecutionResult.model_validate(
                {**result.model_dump(mode="json"), "installation_conclusion": conclusion}
            )
            store.commit_node_result(adjusted_result)
            adjusted.append(adjusted_result)
        ordered_results = tuple(adjusted)
    finished = finish_task(
        running,
        ordered_results,
        script_results,
        format_timestamp(utc_now()),
    )
    store.update_task(finished)
    return finished


def mark_abandoned_tasks_failed(store: TaskStore) -> int:
    """Fail RUNNING tasks left behind by a previously interrupted CLI process."""
    changed = 0
    for task in tuple(store.iterate_tasks()):
        if task.status is not TaskStatus.RUNNING:
            continue
        now = format_timestamp(utc_now())
        for key in task.node_keys:
            result = store.load_node_result(task.task_id, key)
            if result.status not in {NodeStatus.PENDING, NodeStatus.RUNNING}:
                continue
            conclusion = (
                InstallationConclusion.INCONCLUSIVE
                if task.task_type is TaskType.INSTALLATION_VALIDATION
                else None
            )
            abandoned = NodeExecutionResult.model_validate(
                {
                    **result.model_dump(mode="json"),
                    "status": NodeStatus.FAILED,
                    "installation_conclusion": conclusion,
                    "finished_at": now,
                    "failure": FailureRecord(
                        code="ABANDONED",
                        message="previous CLI process ended before this node completed",
                        phase="runner",
                    ).model_dump(mode="json"),
                }
            )
            store.commit_node_result(abandoned)
        failed = transition_task(
            task,
            TaskStatus.FAILED,
            now,
            failure=FailureRecord(
                code="ABANDONED",
                message="previous CLI process ended before this task completed",
                phase="runner",
            ),
        )
        store.update_task(failed)
        changed += 1
    return changed
