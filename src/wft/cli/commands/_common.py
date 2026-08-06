import json
from typing import Any, NoReturn

import typer


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
