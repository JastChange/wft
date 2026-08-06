import os
import shlex
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path


class GitSourceError(RuntimeError):
    """Raised when a Git source cannot be fetched or checked out."""


@dataclass(frozen=True)
class FetchedCommit:
    sha: str
    committed_at: str


def _git_environment(deploy_key_path: Path | None) -> dict[str, str]:
    environment = os.environ.copy()
    environment["GIT_TERMINAL_PROMPT"] = "0"
    if deploy_key_path is None:
        return environment
    if not deploy_key_path.is_absolute():
        raise GitSourceError("deploy key path must be absolute")
    try:
        mode = stat.S_IMODE(deploy_key_path.stat().st_mode)
    except OSError as error:
        raise GitSourceError(f"cannot read deploy key metadata: {deploy_key_path}") from error
    if mode != 0o600:
        raise GitSourceError(f"deploy key must be mode 0600: {deploy_key_path}")
    quoted_key = shlex.quote(str(deploy_key_path))
    environment["GIT_SSH_COMMAND"] = f"ssh -i {quoted_key} -o IdentitiesOnly=yes -o BatchMode=yes"
    return environment


def _git(
    *args: str,
    cwd: Path | None = None,
    deploy_key_path: Path | None = None,
) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=cwd,
        env=_git_environment(deploy_key_path),
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise GitSourceError(completed.stderr.strip() or "git command failed")
    return completed.stdout.strip()


def _validate_source(repository_url: str, branch: str) -> None:
    if not repository_url or repository_url.startswith("-"):
        raise GitSourceError("invalid repository URL")
    completed = subprocess.run(
        ["git", "check-ref-format", "--branch", branch],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise GitSourceError(f"invalid branch: {branch}")


def fetch_latest(
    repository_url: str,
    branch: str,
    cache_dir: Path,
    deploy_key_path: Path | None = None,
) -> FetchedCommit:
    _validate_source(repository_url, branch)
    if not (cache_dir / ".git").is_dir():
        cache_dir.parent.mkdir(parents=True, exist_ok=True)
        _git(
            "clone",
            "--no-checkout",
            "--",
            repository_url,
            str(cache_dir),
            deploy_key_path=deploy_key_path,
        )
    _git("remote", "set-url", "--", "origin", repository_url, cwd=cache_dir)
    _git(
        "fetch",
        "--prune",
        "origin",
        f"refs/heads/{branch}",
        cwd=cache_dir,
        deploy_key_path=deploy_key_path,
    )
    sha = _git("rev-parse", "FETCH_HEAD^{commit}", cwd=cache_dir)
    committed_at = _git("show", "-s", "--format=%cI", sha, cwd=cache_dir)
    return FetchedCommit(sha=sha, committed_at=committed_at)


def checkout(cache_dir: Path, sha: str, destination: Path) -> None:
    _git("worktree", "add", "--detach", str(destination), sha, cwd=cache_dir)


def remove_checkout(cache_dir: Path, destination: Path) -> None:
    _git("worktree", "remove", "--force", str(destination), cwd=cache_dir)
