from pathlib import Path
from typing import Annotated

import typer

from wft.config.loader import load_config

from ._common import emit, fail, reconcile_abandoned

app = typer.Typer(help="Validate the private WFT configuration.")


@app.command("check")
def check_config(
    config_path: Annotated[
        Path,
        typer.Option("--config", exists=True, dir_okay=False, readable=True),
    ],
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Load and validate the private application configuration."""
    try:
        loaded = load_config(config_path)
        reconcile_abandoned(loaded.data_dir)
    except (OSError, TypeError, ValueError) as error:
        fail(str(error), json_output=json_output)
    payload = {
        "default_concurrency": loaded.default_concurrency,
        "script_repository": {
            "branch": loaded.script_repository.branch,
            "url": loaded.script_repository.url,
        },
        "status": "ok",
        "web": {"bind_host": loaded.web.bind_host, "port": loaded.web.port},
    }
    emit(payload, json_output=json_output, message="configuration valid")
