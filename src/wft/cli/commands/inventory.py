from pathlib import Path
from typing import Annotated

import typer

from wft.config.loader import load_config
from wft.inventory.loader import load_inventory
from wft.inventory.models import NodeSelector
from wft.inventory.selector import select_nodes

from ._common import emit, fail, reconcile_abandoned

app = typer.Typer(help="Validate inventory and select diagnostic nodes.")


def _load(config_path: Path):  # type: ignore[no-untyped-def]
    config = load_config(config_path)
    reconcile_abandoned(config.data_dir)
    return load_inventory(config.inventory_path)


@app.command("check")
def check_inventory(
    config_path: Annotated[
        Path,
        typer.Option("--config", exists=True, dir_okay=False, readable=True),
    ],
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Validate all configured nodes."""
    try:
        loaded = _load(config_path)
    except (OSError, TypeError, ValueError) as error:
        fail(str(error), json_output=json_output)
    enabled = sum(node.enabled for node in loaded.nodes)
    payload = {"enabled": enabled, "nodes": len(loaded.nodes), "status": "ok"}
    emit(
        payload,
        json_output=json_output,
        message=f"inventory valid: {len(loaded.nodes)} nodes ({enabled} enabled)",
    )


@app.command("select")
def select_inventory_nodes(
    config_path: Annotated[
        Path,
        typer.Option("--config", exists=True, dir_okay=False, readable=True),
    ],
    node_names: Annotated[list[str] | None, typer.Option("--node")] = None,
    groups: Annotated[list[str] | None, typer.Option("--group")] = None,
    tags: Annotated[list[str] | None, typer.Option("--tag")] = None,
    all_enabled: Annotated[bool, typer.Option("--all")] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Select an ordered, deduplicated set of enabled nodes."""
    if not (node_names or groups or tags or all_enabled):
        fail("at least one node selector is required", json_output=json_output)
    try:
        loaded = _load(config_path)
        requested_names = set(node_names or ())
        known_names = {node.name for node in loaded.nodes}
        unknown_names = sorted(requested_names - known_names)
        if unknown_names:
            raise ValueError(f"unknown node names: {unknown_names}")
        selected = select_nodes(
            loaded,
            NodeSelector(
                node_names=tuple(node_names or ()),
                groups=tuple(groups or ()),
                tags=tuple(tags or ()),
                all_enabled=all_enabled,
            ),
        )
        if not selected:
            raise ValueError("node selector matched no enabled nodes")
    except (OSError, TypeError, ValueError) as error:
        fail(str(error), json_output=json_output)
    names = [node.name for node in selected]
    emit(
        {"node_names": names, "status": "ok"},
        json_output=json_output,
        message="\n".join(names),
    )
