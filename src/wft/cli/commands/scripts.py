from pathlib import Path
from typing import Annotated

import typer

from wft.config.loader import load_config
from wft.scripts.snapshot import (
    CacheDecision,
    ScriptSnapshot,
    SnapshotRequest,
    prepare_snapshot,
)

from ._common import emit, fail, reconcile_abandoned

app = typer.Typer(help="Validate and synchronize diagnostic scripts.")


def _request(config_path: Path) -> SnapshotRequest:
    config = load_config(config_path)
    reconcile_abandoned(config.data_dir)
    return SnapshotRequest(
        repository_url=config.script_repository.url,
        branch=config.script_repository.branch,
        cache_dir=config.data_dir / "script-cache" / "git",
        snapshots_dir=config.data_dir / "script-snapshots",
        deploy_key_path=config.script_repository.deploy_key_path,
    )


def _snapshot_payload(snapshot: ScriptSnapshot) -> dict[str, object]:
    return {
        "branch": snapshot.branch,
        "commit_sha": snapshot.commit_sha,
        "commit_time": snapshot.commit_time,
        "manifest_sha256": snapshot.manifest_sha256,
        "repository_url": snapshot.repository_url,
        "status": "ok",
        "used_cached_snapshot": snapshot.used_cache,
    }


def _prepare(
    config_path: Path,
    *,
    allow_cached_scripts: bool,
    json_output: bool,
) -> ScriptSnapshot:
    request = _request(config_path)

    def decide_cache(candidate: ScriptSnapshot) -> CacheDecision:
        if allow_cached_scripts:
            return CacheDecision.ACCEPT
        typer.echo(
            "Cached script snapshot available: "
            f"commit={candidate.commit_sha} time={candidate.commit_time} "
            f"repository={candidate.repository_url} branch={candidate.branch}",
            err=True,
        )
        if json_output:
            return CacheDecision.REJECT
        return (
            CacheDecision.ACCEPT
            if typer.confirm("Use this cached snapshot?")
            else CacheDecision.REJECT
        )

    return prepare_snapshot(request, decide_cache=decide_cache)


def _run(
    config_path: Path,
    *,
    allow_cached_scripts: bool,
    json_output: bool,
) -> None:
    try:
        snapshot = _prepare(
            config_path,
            allow_cached_scripts=allow_cached_scripts,
            json_output=json_output,
        )
    except (OSError, TypeError, ValueError, RuntimeError) as error:
        fail(str(error), json_output=json_output)
    emit(
        _snapshot_payload(snapshot),
        json_output=json_output,
        message=(
            f"script snapshot ready: {snapshot.commit_sha} "
            f"(cached={str(snapshot.used_cache).lower()})"
        ),
    )


@app.command("check")
def check_scripts(
    config_path: Annotated[
        Path,
        typer.Option("--config", exists=True, dir_okay=False, readable=True),
    ],
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Fetch and validate the latest configured script Manifest."""
    _run(config_path, allow_cached_scripts=False, json_output=json_output)


@app.command("sync")
def sync_scripts(
    config_path: Annotated[
        Path,
        typer.Option("--config", exists=True, dir_okay=False, readable=True),
    ],
    allow_cached_scripts: Annotated[bool, typer.Option("--allow-cached-scripts")] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Publish the latest immutable script snapshot, with explicit cache fallback."""
    _run(
        config_path,
        allow_cached_scripts=allow_cached_scripts,
        json_output=json_output,
    )
