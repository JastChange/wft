import hashlib
from pathlib import Path

import pytest

from wft.scripts.git_source import GitSourceError, fetch_latest
from wft.scripts.snapshot import CacheDecision, SnapshotRequest, prepare_snapshot

from .conftest import commit_all


def _request(repo: Path, tmp_path: Path) -> SnapshotRequest:
    return SnapshotRequest(
        repository_url=str(repo),
        branch="main",
        cache_dir=tmp_path / "cache",
        snapshots_dir=tmp_path / "snaps",
    )


def test_fetch_failure_requires_explicit_cache_acceptance(
    fixture_repo: Path, tmp_path: Path
) -> None:
    request = _request(fixture_repo, tmp_path)
    first = prepare_snapshot(request, decide_cache=lambda _: CacheDecision.REJECT)
    fixture_repo.rename(fixture_repo.with_suffix(".offline"))

    with pytest.raises(RuntimeError, match="cached snapshot rejected"):
        prepare_snapshot(request, decide_cache=lambda _: CacheDecision.REJECT)

    seen = []
    cached = prepare_snapshot(
        request,
        decide_cache=lambda candidate: seen.append(candidate) or CacheDecision.ACCEPT,
    )
    assert cached.commit_sha == first.commit_sha
    assert cached.used_cache is True
    assert seen == [cached]


def test_new_commit_publishes_new_validated_immutable_snapshot(
    fixture_repo: Path, tmp_path: Path
) -> None:
    request = _request(fixture_repo, tmp_path)
    first = prepare_snapshot(request, decide_cache=lambda _: CacheDecision.REJECT)
    script = fixture_repo / "check.sh"
    script.write_text("#!/bin/sh\nprintf 'updated\\n'\n")
    digest = hashlib.sha256(script.read_bytes()).hexdigest()
    manifest = fixture_repo / "manifest.yaml"
    text = manifest.read_text()
    old_digest = first.manifest.scripts[0].sha256
    manifest.write_text(text.replace(old_digest, digest))
    commit_all(fixture_repo, "update")

    second = prepare_snapshot(request, decide_cache=lambda _: CacheDecision.REJECT)

    assert second.commit_sha != first.commit_sha
    assert second.path != first.path
    assert (first.path / "check.sh").read_text() == "#!/bin/sh\ntrue\n"
    assert not list(request.snapshots_dir.glob("*.partial"))


def test_reusing_latest_commit_returns_same_snapshot(fixture_repo: Path, tmp_path: Path) -> None:
    request = _request(fixture_repo, tmp_path)
    first = prepare_snapshot(request, decide_cache=lambda _: CacheDecision.REJECT)

    second = prepare_snapshot(request, decide_cache=lambda _: CacheDecision.REJECT)

    assert second.path == first.path
    assert second.manifest_sha256 == first.manifest_sha256
    assert second.used_cache is False


def test_rejects_option_like_branch_before_invoking_git(fixture_repo: Path, tmp_path: Path) -> None:
    with pytest.raises(GitSourceError, match="invalid branch"):
        fetch_latest(str(fixture_repo), "--upload-pack=evil", tmp_path / "cache")
