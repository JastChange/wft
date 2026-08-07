import json
from pathlib import Path
from typing import Any, NoReturn

import typer

from wft.storage.task_store import TaskStore
from wft.tasks.runner import mark_abandoned_tasks_failed


def emit(payload: dict[str, Any], *, json_output: bool, message: str) -> None:
    if json_output:
        typer.echo(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    else:
        typer.echo(message)


def fail(message: str, *, json_output: bool = False) -> NoReturn:
    if json_output:
        typer.echo(
            json.dumps(
                {"error": "configuration or validation failed", "status": "error"},
                sort_keys=True,
            )
        )
    else:
        typer.echo(f"Error: {message}", err=True)
    raise typer.Exit(code=2)


def reconcile_abandoned(data_dir: Path) -> None:
    mark_abandoned_tasks_failed(TaskStore(data_dir))
