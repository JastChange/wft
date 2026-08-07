import json
from pathlib import Path
from typing import Annotated, Any

import typer

from wft.config.loader import load_config
from wft.storage.task_store import TaskStore
from wft.tasks.runner import mark_abandoned_tasks_failed

from ._common import emit, fail

app = typer.Typer(help="Inspect and delete authoritative task records.")


def _store(config_path: Path) -> TaskStore:
    store = TaskStore(load_config(config_path).data_dir)
    mark_abandoned_tasks_failed(store)
    return store


@app.command("list")
def list_tasks(
    config_path: Annotated[
        Path, typer.Option("--config", exists=True, dir_okay=False, readable=True)
    ],
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    try:
        tasks = tuple(_store(config_path).iterate_tasks())
    except (OSError, TypeError, ValueError, RuntimeError) as error:
        fail(str(error), json_output=json_output)
    rows = [
        {
            "created_at": task.created_at,
            "finished_at": task.finished_at,
            "status": task.status.value,
            "summary": task.summary.model_dump(mode="json"),
            "task_id": task.task_id,
            "task_type": task.task_type.value,
        }
        for task in tasks
    ]
    if json_output:
        typer.echo(json.dumps({"tasks": rows}, ensure_ascii=False, sort_keys=True))
    elif not rows:
        typer.echo("No tasks.")
    else:
        for row in rows:
            typer.echo(
                f"{row['task_id']} {row['task_type']} {row['status']} "
                f"created={row['created_at']} finished={row['finished_at']} "
                f"summary={json.dumps(row['summary'], sort_keys=True)}"
            )


@app.command("show")
def show_task(
    task_id: str,
    config_path: Annotated[
        Path, typer.Option("--config", exists=True, dir_okay=False, readable=True)
    ],
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    try:
        store = _store(config_path)
        task = store.load(task_id)
        nodes = [
            {
                "node": store.load_node(task_id, key).model_dump(mode="json"),
                "result": store.load_node_result(task_id, key).model_dump(mode="json"),
            }
            for key in task.node_keys
        ]
        scripts = [
            result.model_dump(mode="json") for result in store.iterate_script_results(task_id)
        ]
    except (OSError, TypeError, ValueError, RuntimeError) as error:
        fail(str(error), json_output=json_output)
    payload: dict[str, Any] = {
        "nodes": nodes,
        "scripts": scripts,
        "task": task.model_dump(mode="json"),
    }
    emit(
        payload,
        json_output=json_output,
        message=json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
    )


@app.command("delete")
def delete_task(
    task_id: str,
    config_path: Annotated[
        Path, typer.Option("--config", exists=True, dir_okay=False, readable=True)
    ],
    reason: Annotated[str, typer.Option("--reason")],
) -> None:
    if not reason.strip():
        fail("deletion reason must not be empty")
    try:
        store = _store(config_path)
        store.load(task_id)
    except (OSError, TypeError, ValueError, RuntimeError) as error:
        fail(str(error))
    typer.echo(f"Delete task {task_id}\nReason: {reason}")
    typer.confirm("Permanently delete this authoritative task?", abort=True)
    try:
        store.delete(task_id)
    except (OSError, ValueError) as error:
        fail(str(error))
    typer.echo(f"Deleted task {task_id}")
