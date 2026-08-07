import pytest

from wft.ids import new_uuid7
from wft.tasks.models import (
    CleanupResult,
    FailureRecord,
    InstallationConclusion,
    NodeExecutionResult,
    NodeStatus,
    ScriptExecutionResult,
    ScriptSelection,
    ScriptSnapshotMetadata,
    ScriptStatus,
    SelectorSnapshot,
    StreamReference,
    TaskRecord,
    TaskStatus,
    TaskSummary,
    TaskType,
)
from wft.tasks.state import (
    conclude_installation,
    finish_task,
    summarize_nodes,
    task_exit_code,
    transition_task,
)

NOW = "2026-08-07T03:00:00Z"
LATER = "2026-08-07T03:01:00Z"


def _task(task_type: TaskType = TaskType.INSTALLATION_VALIDATION) -> TaskRecord:
    return TaskRecord(
        task_id=new_uuid7(),
        task_type=task_type,
        status=TaskStatus.PENDING,
        created_at=NOW,
        selector=SelectorSnapshot(all_enabled=True),
        node_keys=("node-a-0123456789ab",),
        script_snapshot=ScriptSnapshotMetadata(
            repository_url="https://example.test/scripts.git",
            branch="main",
            commit_sha="a" * 40,
            commit_time=NOW,
            manifest_sha256="b" * 64,
            used_cached_snapshot=False,
        ),
        selection=ScriptSelection(script_ids=("check",)),
        concurrency=1,
        summary=TaskSummary(
            nodes_total=1,
            nodes_completed=0,
            nodes_failed=0,
            nodes_cancelled=0,
            problems=0,
        ),
    )


def _stream(name: str) -> StreamReference:
    return StreamReference(path=name, size_bytes=0, sha256="0" * 64)


def _script(status: ScriptStatus, check: bool | None = None) -> ScriptExecutionResult:
    terminal = status not in {ScriptStatus.PENDING, ScriptStatus.RUNNING}
    return ScriptExecutionResult(
        task_id=new_uuid7(),
        node_key="node-a-0123456789ab",
        script_id="check",
        script_sha256="c" * 64,
        interpreter="/bin/sh",
        status=status,
        started_at=NOW,
        finished_at=LATER if terminal else None,
        exit_code=0 if status is ScriptStatus.COMPLETED else None,
        expected_exit_codes=(0,),
        check_passed=check,
        stdout=_stream("stdout.raw"),
        stderr=_stream("stderr.raw"),
        execution_log=_stream("execution.log.raw"),
        failure=(
            FailureRecord(code=status.value, message="execution did not complete")
            if terminal and status is not ScriptStatus.COMPLETED
            else None
        ),
    )


def _node(
    status: NodeStatus,
    conclusion: InstallationConclusion | None,
) -> NodeExecutionResult:
    terminal = status not in {NodeStatus.PENDING, NodeStatus.RUNNING}
    return NodeExecutionResult(
        task_id=new_uuid7(),
        node_key="node-a-0123456789ab",
        status=status,
        installation_conclusion=conclusion,
        started_at=NOW,
        finished_at=LATER if terminal else None,
        cleanup=CleanupResult(attempted=True, succeeded=True),
    )


def test_valid_task_transition_chain() -> None:
    running = transition_task(_task(), TaskStatus.RUNNING, NOW)
    completed = transition_task(running, TaskStatus.COMPLETED, LATER)

    assert running.started_at == NOW
    assert completed.finished_at == LATER
    assert completed.exit_code == 0


@pytest.mark.parametrize("initial", [TaskStatus.PENDING, TaskStatus.RUNNING])
def test_pending_or_running_task_can_be_cancelled(initial: TaskStatus) -> None:
    task = _task()
    if initial is TaskStatus.RUNNING:
        task = transition_task(task, TaskStatus.RUNNING, NOW)

    cancelled = transition_task(task, TaskStatus.CANCELLED, LATER)

    assert cancelled.exit_code == 2


def test_running_task_can_fail_with_failure_record() -> None:
    running = transition_task(_task(), TaskStatus.RUNNING, NOW)
    failure = FailureRecord(code="STORE", message="cannot continue")

    failed = transition_task(running, TaskStatus.FAILED, LATER, failure=failure)

    assert failed.failure == failure
    assert failed.exit_code == 2


def test_pending_task_can_fail_before_execution_starts() -> None:
    failed = transition_task(
        _task(),
        TaskStatus.FAILED,
        LATER,
        failure=FailureRecord(code="START_FAILURE", message="cannot persist RUNNING"),
    )

    assert failed.status is TaskStatus.FAILED
    assert failed.started_at is None
    assert failed.exit_code == 2


def test_terminal_task_cannot_transition_again() -> None:
    running = transition_task(_task(), TaskStatus.RUNNING, NOW)
    completed = transition_task(running, TaskStatus.COMPLETED, LATER)

    with pytest.raises(ValueError, match="terminal"):
        transition_task(completed, TaskStatus.FAILED, LATER)


@pytest.mark.parametrize(
    ("checks", "expected"),
    [
        ((_script(ScriptStatus.COMPLETED, True),), InstallationConclusion.PASS),
        (
            (
                _script(ScriptStatus.COMPLETED, True),
                _script(ScriptStatus.COMPLETED, False),
            ),
            InstallationConclusion.FAIL,
        ),
        ((_script(ScriptStatus.TIMEOUT),), InstallationConclusion.INCONCLUSIVE),
        ((_script(ScriptStatus.FAILED),), InstallationConclusion.INCONCLUSIVE),
    ],
)
def test_installation_conclusion(
    checks: tuple[ScriptExecutionResult, ...],
    expected: InstallationConclusion,
) -> None:
    assert conclude_installation(checks) is expected


def test_summary_counts_terminal_nodes_and_problems() -> None:
    summary = summarize_nodes(
        (
            _node(NodeStatus.COMPLETED, InstallationConclusion.PASS),
            _node(NodeStatus.COMPLETED, InstallationConclusion.FAIL),
            _node(NodeStatus.FAILED, InstallationConclusion.INCONCLUSIVE),
            _node(NodeStatus.CANCELLED, None),
        )
    )

    assert summary.nodes_total == 4
    assert summary.nodes_completed == 2
    assert summary.nodes_failed == 1
    assert summary.nodes_cancelled == 1
    assert summary.problems == 3


@pytest.mark.parametrize(
    ("status", "conclusion", "expected"),
    [
        (NodeStatus.COMPLETED, InstallationConclusion.PASS, 0),
        (NodeStatus.COMPLETED, InstallationConclusion.FAIL, 1),
        (NodeStatus.COMPLETED, InstallationConclusion.INCONCLUSIVE, 1),
        (NodeStatus.FAILED, InstallationConclusion.INCONCLUSIVE, 1),
    ],
)
def test_finish_task_sets_summary_and_exit_code(
    status: NodeStatus,
    conclusion: InstallationConclusion,
    expected: int,
) -> None:
    running = transition_task(_task(), TaskStatus.RUNNING, NOW)
    node = _node(status, conclusion)
    script = _script(ScriptStatus.COMPLETED, conclusion is InstallationConclusion.PASS)

    finished = finish_task(running, (node,), (script,), LATER)

    assert finished.status is TaskStatus.COMPLETED
    assert finished.exit_code == expected
    assert task_exit_code(finished, (node,), (script,)) == expected


def test_failed_cancelled_and_nonterminal_tasks_exit_two() -> None:
    pending = _task()
    running = transition_task(pending, TaskStatus.RUNNING, NOW)
    failed = transition_task(
        running,
        TaskStatus.FAILED,
        LATER,
        failure=FailureRecord(code="SYSTEM", message="stopped"),
    )
    cancelled = transition_task(_task(), TaskStatus.CANCELLED, LATER)

    assert task_exit_code(pending, (), ()) == 2
    assert task_exit_code(failed, (), ()) == 2
    assert task_exit_code(cancelled, (), ()) == 2


def test_fault_diagnosis_rejects_installation_conclusion() -> None:
    running = transition_task(_task(TaskType.FAULT_DIAGNOSIS), TaskStatus.RUNNING, NOW)

    with pytest.raises(ValueError, match="fault diagnosis"):
        finish_task(
            running,
            (_node(NodeStatus.COMPLETED, InstallationConclusion.PASS),),
            (_script(ScriptStatus.COMPLETED, True),),
            LATER,
        )


def test_cleanup_failure_is_preserved_as_a_completed_task_problem() -> None:
    running = transition_task(_task(), TaskStatus.RUNNING, NOW)
    node = _node(NodeStatus.COMPLETED, InstallationConclusion.PASS)
    node = NodeExecutionResult.model_validate(
        {
            **node.model_dump(mode="json"),
            "cleanup": CleanupResult(
                attempted=True, succeeded=False, error="remote directory remains"
            ).model_dump(mode="json"),
            "failure": FailureRecord(
                code="CLEANUP_FAILED", message="remote directory remains"
            ).model_dump(mode="json"),
        }
    )

    finished = finish_task(
        running,
        (node,),
        (_script(ScriptStatus.COMPLETED, True),),
        LATER,
    )

    assert finished.status is TaskStatus.COMPLETED
    assert finished.summary.problems == 1
    assert finished.exit_code == 1
