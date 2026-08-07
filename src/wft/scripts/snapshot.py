import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

from .git_source import GitSourceError, checkout, fetch_latest, remove_checkout
from .manifest import load_manifest
from .models import Manifest


class CacheDecision(Enum):
    ACCEPT = "accept"
    REJECT = "reject"


@dataclass(frozen=True)
class SnapshotRequest:
    repository_url: str
    branch: str
    cache_dir: Path
    snapshots_dir: Path
    deploy_key_path: Path | None = None


@dataclass(frozen=True)
class ScriptSnapshot:
    path: Path
    repository_url: str
    branch: str
    commit_sha: str
    commit_time: str
    manifest_sha256: str
    manifest: Manifest
    used_cache: bool


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_snapshot(path: Path, *, used_cache: bool) -> ScriptSnapshot:
    try:
        loaded = json.loads((path / ".wft-snapshot.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"invalid cached snapshot metadata: {path}") from error
    if not isinstance(loaded, dict):
        raise RuntimeError(f"invalid cached snapshot metadata: {path}")
    metadata: dict[str, Any] = loaded
    manifest_path = path / "manifest.yaml"
    manifest_sha256 = _hash(manifest_path)
    if manifest_sha256 != metadata.get("manifest_sha256"):
        raise RuntimeError(f"cached manifest hash mismatch: {path}")
    manifest = load_manifest(manifest_path)
    if path.name != metadata.get("commit_sha"):
        raise RuntimeError(f"cached snapshot directory mismatch: {path}")
    return ScriptSnapshot(
        path=path,
        repository_url=str(metadata["repository_url"]),
        branch=str(metadata["branch"]),
        commit_sha=str(metadata["commit_sha"]),
        commit_time=str(metadata["commit_time"]),
        manifest_sha256=manifest_sha256,
        manifest=manifest,
        used_cache=used_cache,
    )


def _latest_cache(request: SnapshotRequest) -> ScriptSnapshot:
    candidates: list[ScriptSnapshot] = []
    paths = request.snapshots_dir.iterdir() if request.snapshots_dir.exists() else ()
    for path in paths:
        if not path.is_dir() or not (path / ".wft-snapshot.json").is_file():
            continue
        candidate = _load_snapshot(path, used_cache=True)
        if (
            candidate.repository_url == request.repository_url
            and candidate.branch == request.branch
        ):
            candidates.append(candidate)
    if not candidates:
        raise RuntimeError("no validated cached script snapshot")
    return max(candidates, key=lambda candidate: (candidate.commit_time, candidate.commit_sha))


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _publish(request: SnapshotRequest, sha: str, committed_at: str) -> ScriptSnapshot:
    final = request.snapshots_dir / sha
    if final.is_dir():
        existing = _load_snapshot(final, used_cache=False)
        if existing.repository_url != request.repository_url or existing.branch != request.branch:
            raise RuntimeError(f"snapshot identity collision: {sha}")
        return existing

    request.snapshots_dir.mkdir(parents=True, exist_ok=True)
    scratch = Path(tempfile.mkdtemp(prefix="wft-checkout-"))
    checkout_dir = scratch / "checkout"
    staging = request.snapshots_dir / f".{sha}.partial"
    try:
        shutil.rmtree(staging, ignore_errors=True)
        checkout(request.cache_dir, sha, checkout_dir)
        shutil.copytree(checkout_dir, staging, ignore=shutil.ignore_patterns(".git"))
        manifest = load_manifest(staging / "manifest.yaml")
        manifest_sha256 = _hash(staging / "manifest.yaml")
        metadata = {
            "schema_version": "1.0",
            "repository_url": request.repository_url,
            "branch": request.branch,
            "commit_sha": sha,
            "commit_time": committed_at,
            "manifest_sha256": manifest_sha256,
        }
        metadata_path = staging / ".wft-snapshot.json"
        metadata_path.write_text(
            json.dumps(metadata, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8"
        )
        with metadata_path.open("rb") as handle:
            os.fsync(handle.fileno())
        staging.rename(final)
        _fsync_directory(request.snapshots_dir)
        return ScriptSnapshot(
            path=final,
            repository_url=request.repository_url,
            branch=request.branch,
            commit_sha=sha,
            commit_time=committed_at,
            manifest_sha256=manifest_sha256,
            manifest=manifest,
            used_cache=False,
        )
    finally:
        if checkout_dir.exists():
            try:
                remove_checkout(request.cache_dir, checkout_dir)
            except GitSourceError:
                shutil.rmtree(checkout_dir, ignore_errors=True)
        shutil.rmtree(scratch, ignore_errors=True)
        shutil.rmtree(staging, ignore_errors=True)


def prepare_snapshot(
    request: SnapshotRequest,
    decide_cache: Callable[[ScriptSnapshot], CacheDecision],
) -> ScriptSnapshot:
    """Fetch once, validate once, and return one immutable task snapshot."""
    try:
        fetched = fetch_latest(
            request.repository_url,
            request.branch,
            request.cache_dir,
            request.deploy_key_path,
        )
        return _publish(request, fetched.sha, fetched.committed_at)
    except GitSourceError as fetch_error:
        cached = _latest_cache(request)
        if decide_cache(cached) is CacheDecision.ACCEPT:
            return cached
        raise RuntimeError("cached snapshot rejected") from fetch_error
