from pathlib import Path
from typing import Annotated

import typer

from wft.auth.password import initialize_password, reset_password

from ._common import fail, reconcile_abandoned

app = typer.Typer(help="Initialize or reset administrator credentials.")


def _show_password(password: str) -> None:
    typer.echo("Generated administrator password (shown once):")
    typer.echo(password)


def _reconcile_auth_data_dir(auth_file: Path) -> None:
    resolved = auth_file.expanduser().resolve()
    if resolved.parent.name == "auth":
        reconcile_abandoned(resolved.parent.parent)


@app.command("init")
def init_admin(
    auth_file: Annotated[Path, typer.Option("--auth-file", dir_okay=False)],
) -> None:
    """Create the initial administrator password hash file."""
    try:
        _reconcile_auth_data_dir(auth_file)
        password = initialize_password(auth_file)
    except OSError as error:
        fail(str(error))
    _show_password(password)


@app.command("reset-password")
def reset_admin_password(
    auth_file: Annotated[Path, typer.Option("--auth-file", dir_okay=False)],
) -> None:
    """Replace the administrator password with a generated value."""
    try:
        _reconcile_auth_data_dir(auth_file)
        password = reset_password(auth_file)
    except OSError as error:
        fail(str(error))
    _show_password(password)
