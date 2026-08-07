import asyncio
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, NoReturn

import typer

from wft.cli.commands.scripts import _prepare
from wft.config.loader import load_config
from wft.execution.asyncssh_executor import AsyncSSHNodeExecutor
from wft.execution.interface import NodeExecutor
from wft.inventory.loader import load_inventory
from wft.inventory.models import NodeSelector
from wft.inventory.selector import select_nodes
from wft.storage.task_store import TaskStore
from wft.tasks.create import CreateTaskRequest, create_task
from wft.tasks.models import TaskType
from wft.tasks.runner import mark_abandoned_tasks_failed, run_task

from ._common import emit, fail

app = typer.Typer(help="Manually execute diagnostic tasks.")
executor_factory: Callable[[TaskStore], NodeExecutor] = AsyncSSHNodeExecutor


def _cancelled(json_output: bool) -> NoReturn:
    if json_output:
        typer.echo('{"status":"CANCELLED"}')
    else:
        typer.echo("Task cancelled.", err=True)
    raise typer.Exit(code=2)


def _execute(
    task_type: TaskType,
    config_path: Path,
    node_names: list[str],
    groups: list[str],
    tags: list[str],
    all_enabled: bool,
    script_ids: list[str],
    plan_id: str | None,
    concurrency: int | None,
    allow_cached_scripts: bool,
    json_output: bool,
) -> None:
    try:
        if not (node_names or groups or tags or all_enabled):
            raise ValueError("at least one selector is required")
        if all_enabled and (node_names or groups or tags):
            raise ValueError("--all cannot be combined with --node, --group, or --tag")
        if bool(script_ids) == bool(plan_id):
            raise ValueError("select repeatable --script or one --plan")
        config = load_config(config_path)
        store = TaskStore(config.data_dir)
        mark_abandoned_tasks_failed(store)
        selector = NodeSelector(
            node_names=tuple(node_names),
            groups=tuple(groups),
            tags=tuple(tags),
            all_enabled=all_enabled,
        )
        nodes = select_nodes(load_inventory(config.inventory_path), selector)
        if not nodes:
            raise ValueError("selector matched no enabled nodes")
        snapshot = _prepare(
            config_path,
            allow_cached_scripts=allow_cached_scripts,
            json_output=json_output,
        )
        task = create_task(
            store,
            CreateTaskRequest(
                task_type=task_type,
                selector=selector,
                nodes=nodes,
                snapshot=snapshot,
                script_ids=tuple(script_ids),
                plan_id=plan_id,
                concurrency=concurrency or config.default_concurrency,
            ),
        )
        finished = asyncio.run(run_task(store, task.task_id, executor_factory(store)))
    except (KeyboardInterrupt, asyncio.CancelledError):
        _cancelled(json_output)
    except (OSError, TypeError, ValueError, RuntimeError) as error:
        fail(str(error), json_output=json_output)
    payload = {
        "exit_code": finished.exit_code,
        "status": finished.status.value,
        "summary": finished.summary.model_dump(mode="json"),
        "task_id": finished.task_id,
        "task_type": finished.task_type.value,
    }
    emit(
        payload,
        json_output=json_output,
        message=(
            f"task {finished.task_id}: {finished.status.value} "
            f"problems={finished.summary.problems} exit={finished.exit_code}"
        ),
    )
    raise typer.Exit(code=finished.exit_code or 0)


def _options_command(task_type: TaskType) -> Callable[..., None]:
    def command(
        config_path: Annotated[
            Path,
            typer.Option("--config", exists=True, dir_okay=False, readable=True),
        ],
        node_names: Annotated[list[str] | None, typer.Option("--node")] = None,
        groups: Annotated[list[str] | None, typer.Option("--group")] = None,
        tags: Annotated[list[str] | None, typer.Option("--tag")] = None,
        all_enabled: Annotated[bool, typer.Option("--all")] = False,
        script_ids: Annotated[list[str] | None, typer.Option("--script")] = None,
        plan_id: Annotated[str | None, typer.Option("--plan")] = None,
        concurrency: Annotated[int | None, typer.Option("--concurrency", min=1, max=50)] = None,
        allow_cached_scripts: Annotated[bool, typer.Option("--allow-cached-scripts")] = False,
        json_output: Annotated[bool, typer.Option("--json")] = False,
    ) -> None:
        _execute(
            task_type,
            config_path,
            node_names or [],
            groups or [],
            tags or [],
            all_enabled,
            script_ids or [],
            plan_id,
            concurrency,
            allow_cached_scripts,
            json_output,
        )

    return command


app.command("installation-validation")(_options_command(TaskType.INSTALLATION_VALIDATION))
app.command("fault-diagnosis")(_options_command(TaskType.FAULT_DIAGNOSIS))
