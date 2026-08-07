import asyncio
import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from tests.cli.test_commands import _write_config, _write_script_repository
from tests.tasks.test_runner import _created_task
from wft.cli.app import app
from wft.clock import format_timestamp, utc_now
from wft.storage.task_store import TaskStore
from wft.tasks.models import (
    CleanupResult,
    FailureRecord,
    NodeExecutionResult,
    NodeSnapshot,
    NodeStatus,
    ScriptExecutionResult,
    ScriptStatus,
    TaskScriptSnapshot,
    TaskStatus,
    TaskType,
)
from wft.tasks.state import conclude_installation, transition_task

runner = CliRunner()


class CommittingExecutor:
    def __init__(
        self,
        store: TaskStore,
        status: ScriptStatus,
        check_passed: bool | None,
    ) -> None:
        self.store = store
        self.status = status
        self.check_passed = check_passed

    async def execute(
        self, node: NodeSnapshot, snapshot: TaskScriptSnapshot
    ) -> NodeExecutionResult:
        results: list[ScriptExecutionResult] = []
        for definition in snapshot.scripts:
            streams = []
            for filename in ("stdout.raw", "stderr.raw", "execution.log.raw"):
                streams.append(
                    self.store.raw_writer(
                        node.task_id, node.node_key, definition.id, filename
                    ).finish()
                )
            now = format_timestamp(utc_now())
            completed = self.status is ScriptStatus.COMPLETED
            result = ScriptExecutionResult(
                task_id=node.task_id,
                node_key=node.node_key,
                script_id=definition.id,
                script_sha256=definition.sha256,
                interpreter=definition.interpreter,
                status=self.status,
                started_at=now,
                finished_at=now,
                exit_code=(0 if self.check_passed else 7) if completed else None,
                expected_exit_codes=definition.expected_exit_codes,
                check_passed=self.check_passed,
                stdout=streams[0],
                stderr=streams[1],
                execution_log=streams[2],
                failure=(
                    None
                    if completed
                    else FailureRecord(code=self.status.value, message="fixture failure")
                ),
            )
            self.store.commit_script_result(result)
            results.append(result)
        conclusion = (
            conclude_installation(results)
            if snapshot.task_type is TaskType.INSTALLATION_VALIDATION
            else None
        )
        now = format_timestamp(utc_now())
        return NodeExecutionResult(
            task_id=node.task_id,
            node_key=node.node_key,
            status=NodeStatus.COMPLETED,
            installation_conclusion=conclusion,
            started_at=now,
            finished_at=now,
            script_ids=tuple(result.script_id for result in results),
            cleanup=CleanupResult(attempted=True, succeeded=True),
        )


def _fixture_config(tmp_path: Path) -> Path:
    repository = tmp_path / "repository"
    _write_script_repository(repository)
    return _write_config(tmp_path, repository=repository)


def _run_args(config: Path, command: str = "installation-validation") -> list[str]:
    return [
        "run",
        command,
        "--config",
        str(config),
        "--all",
        "--script",
        "check",
        "--json",
    ]


def test_m2_help_has_no_future_commands() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    for unavailable in ("scheduler", "resume", "retry", "web", "analyze", "export", "index"):
        assert unavailable not in result.stdout.lower()
    assert runner.invoke(app, ["run", "--help"]).exit_code == 0
    assert runner.invoke(app, ["task", "--help"]).exit_code == 0


@pytest.mark.parametrize(
    ("status", "check", "expected"),
    [
        (ScriptStatus.COMPLETED, True, 0),
        (ScriptStatus.COMPLETED, False, 1),
        (ScriptStatus.TIMEOUT, None, 1),
    ],
)
def test_installation_validation_exit_contract(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    status: ScriptStatus,
    check: bool | None,
    expected: int,
) -> None:
    config = _fixture_config(tmp_path)
    monkeypatch.setattr(
        "wft.cli.commands.run.executor_factory",
        lambda store: CommittingExecutor(store, status, check),
    )

    result = runner.invoke(app, _run_args(config))

    assert result.exit_code == expected
    payload = json.loads(result.stdout)
    assert payload["status"] == "COMPLETED"
    assert payload["exit_code"] == expected


def test_fault_diagnosis_unexpected_check_is_completed_exit_one(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = _fixture_config(tmp_path)
    monkeypatch.setattr(
        "wft.cli.commands.run.executor_factory",
        lambda store: CommittingExecutor(store, ScriptStatus.COMPLETED, False),
    )

    result = runner.invoke(app, _run_args(config, "fault-diagnosis"))

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "COMPLETED"
    assert payload["summary"]["problems"] == 2


def test_configuration_failure_exits_two(tmp_path: Path) -> None:
    config = _fixture_config(tmp_path)

    result = runner.invoke(
        app,
        ["run", "fault-diagnosis", "--config", str(config), "--all", "--json"],
    )

    assert result.exit_code == 2


def test_cancellation_exits_two_and_persists_task(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    class CancellingExecutor:
        async def execute(
            self, node: NodeSnapshot, snapshot: TaskScriptSnapshot
        ) -> NodeExecutionResult:
            raise asyncio.CancelledError

    config = _fixture_config(tmp_path)
    monkeypatch.setattr("wft.cli.commands.run.executor_factory", lambda store: CancellingExecutor())

    result = runner.invoke(app, _run_args(config))

    assert result.exit_code == 2
    tasks = tuple(TaskStore(tmp_path / "data").iterate_tasks())
    assert len(tasks) == 1
    assert tasks[0].status.value == "CANCELLED"


def test_list_show_and_delete_use_files_without_sqlite(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = _fixture_config(tmp_path)
    monkeypatch.setattr(
        "wft.cli.commands.run.executor_factory",
        lambda store: CommittingExecutor(store, ScriptStatus.COMPLETED, True),
    )
    created = runner.invoke(app, _run_args(config))
    task_id = json.loads(created.stdout)["task_id"]
    sqlite = tmp_path / "data" / "index.sqlite"
    sqlite.write_text("unrelated")
    sqlite.unlink()

    listed = runner.invoke(app, ["task", "list", "--config", str(config), "--json"])
    shown = runner.invoke(app, ["task", "show", task_id, "--config", str(config), "--json"])

    assert listed.exit_code == 0
    assert json.loads(listed.stdout)["tasks"][0]["task_id"] == task_id
    assert shown.exit_code == 0
    assert json.loads(shown.stdout)["task"]["task_id"] == task_id

    rejected = runner.invoke(
        app,
        ["task", "delete", task_id, "--config", str(config), "--reason", "cleanup"],
        input="n\n",
    )
    assert rejected.exit_code == 1
    assert (tmp_path / "data" / "tasks" / task_id).exists()

    confirmed = runner.invoke(
        app,
        ["task", "delete", task_id, "--config", str(config), "--reason", "cleanup"],
        input="y\n",
    )
    assert confirmed.exit_code == 0
    assert list((tmp_path / "data" / "tasks").iterdir()) == []


def test_non_task_command_marks_abandoned_running_task_failed(tmp_path: Path) -> None:
    config = _fixture_config(tmp_path)
    store = TaskStore(tmp_path / "data")
    fixture_root = tmp_path / "abandoned-fixture"
    fixture_root.mkdir()
    task = _created_task(fixture_root, store, nodes_count=1)
    store.update_task(transition_task(task, TaskStatus.RUNNING, format_timestamp(utc_now())))

    result = runner.invoke(app, ["config", "check", "--config", str(config), "--json"])

    assert result.exit_code == 0
    assert store.load(task.task_id).status is TaskStatus.FAILED


def test_admin_command_marks_standard_data_dir_abandoned_task_failed(
    tmp_path: Path,
) -> None:
    _fixture_config(tmp_path)
    store = TaskStore(tmp_path / "data")
    fixture_root = tmp_path / "admin-abandoned-fixture"
    fixture_root.mkdir()
    task = _created_task(fixture_root, store, nodes_count=1)
    store.update_task(transition_task(task, TaskStatus.RUNNING, format_timestamp(utc_now())))
    auth_file = tmp_path / "data" / "auth" / "admin.json"

    result = runner.invoke(app, ["admin", "init", "--auth-file", str(auth_file)])

    assert result.exit_code == 0
    assert store.load(task.task_id).status is TaskStatus.FAILED
