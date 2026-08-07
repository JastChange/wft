from collections.abc import Mapping, Sequence

from wft.clock import validate_timestamp
from wft.tasks.models import (
    FailureRecord,
    InstallationConclusion,
    NodeExecutionResult,
    NodeStatus,
    ScriptExecutionResult,
    ScriptStatus,
    TaskRecord,
    TaskStatus,
    TaskSummary,
    TaskType,
)

_TERMINAL = {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED}
_TRANSITIONS: Mapping[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset({TaskStatus.RUNNING, TaskStatus.FAILED, TaskStatus.CANCELLED}),
    TaskStatus.RUNNING: frozenset({TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED}),
    TaskStatus.COMPLETED: frozenset(),
    TaskStatus.FAILED: frozenset(),
    TaskStatus.CANCELLED: frozenset(),
}


def _replace_task(task: TaskRecord, **updates: object) -> TaskRecord:
    data = task.model_dump(mode="json")
    data.update(updates)
    return TaskRecord.model_validate(data)


def transition_task(
    task: TaskRecord,
    target: TaskStatus,
    now: str,
    failure: FailureRecord | None = None,
) -> TaskRecord:
    validate_timestamp(now)
    if task.status in _TERMINAL:
        raise ValueError(f"terminal task cannot transition from {task.status}")
    if target not in _TRANSITIONS[task.status]:
        raise ValueError(f"invalid task transition: {task.status} -> {target}")
    if target is TaskStatus.FAILED and failure is None:
        raise ValueError("FAILED task requires a failure record")
    if target is TaskStatus.RUNNING:
        if failure is not None:
            raise ValueError("RUNNING task cannot have a failure record")
        return _replace_task(task, status=target, started_at=now)
    exit_code = 0 if target is TaskStatus.COMPLETED else 2
    return _replace_task(
        task,
        status=target,
        finished_at=now,
        exit_code=exit_code,
        failure=failure,
    )


def conclude_installation(
    script_results: Sequence[ScriptExecutionResult],
) -> InstallationConclusion:
    if not script_results:
        return InstallationConclusion.INCONCLUSIVE
    if any(result.status is not ScriptStatus.COMPLETED for result in script_results):
        return InstallationConclusion.INCONCLUSIVE
    if any(result.check_passed is False for result in script_results):
        return InstallationConclusion.FAIL
    if all(result.check_passed is True for result in script_results):
        return InstallationConclusion.PASS
    return InstallationConclusion.INCONCLUSIVE


def _node_has_problem(result: NodeExecutionResult) -> bool:
    if result.status is not NodeStatus.COMPLETED:
        return True
    if result.failure is not None or (result.cleanup.attempted and not result.cleanup.succeeded):
        return True
    return result.installation_conclusion in {
        InstallationConclusion.FAIL,
        InstallationConclusion.INCONCLUSIVE,
    }


def summarize_nodes(node_results: Sequence[NodeExecutionResult]) -> TaskSummary:
    return TaskSummary(
        nodes_total=len(node_results),
        nodes_completed=sum(result.status is NodeStatus.COMPLETED for result in node_results),
        nodes_failed=sum(result.status is NodeStatus.FAILED for result in node_results),
        nodes_cancelled=sum(result.status is NodeStatus.CANCELLED for result in node_results),
        problems=sum(_node_has_problem(result) for result in node_results),
    )


def task_exit_code(
    task: TaskRecord,
    node_results: Sequence[NodeExecutionResult],
    script_results: Sequence[ScriptExecutionResult],
) -> int:
    if task.status is not TaskStatus.COMPLETED:
        return 2
    if task.summary.problems or any(_node_has_problem(result) for result in node_results):
        return 1
    if any(
        result.status is not ScriptStatus.COMPLETED or result.check_passed is False
        for result in script_results
    ):
        return 1
    return 0


def finish_task(
    task: TaskRecord,
    node_results: Sequence[NodeExecutionResult],
    script_results: Sequence[ScriptExecutionResult],
    now: str,
) -> TaskRecord:
    if task.status is not TaskStatus.RUNNING:
        raise ValueError("only a RUNNING task can finish")
    if task.task_type is TaskType.FAULT_DIAGNOSIS and any(
        result.installation_conclusion is not None for result in node_results
    ):
        raise ValueError("fault diagnosis node results cannot have installation conclusions")
    summary = summarize_nodes(node_results)
    problem_node_keys = {result.node_key for result in node_results if _node_has_problem(result)}
    problem_node_keys.update(
        result.node_key
        for result in script_results
        if result.status is not ScriptStatus.COMPLETED or result.check_passed is False
    )
    summary = TaskSummary.model_validate(
        {**summary.model_dump(mode="json"), "problems": len(problem_node_keys)}
    )
    provisional = _replace_task(
        task,
        status=TaskStatus.COMPLETED,
        finished_at=now,
        summary=summary.model_dump(mode="json"),
        exit_code=0,
    )
    return _replace_task(
        provisional,
        exit_code=task_exit_code(provisional, node_results, script_results),
    )
