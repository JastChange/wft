import hashlib
import subprocess
from pathlib import Path

import pytest


def commit_all(repo: Path, message: str) -> None:
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=WFT Test",
            "-c",
            "user.email=wft@example.invalid",
            "commit",
            "-m",
            message,
        ],
        cwd=repo,
        check=True,
    )


@pytest.fixture
def fixture_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "script-repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-b", "main"], cwd=repo, check=True)
    script = repo / "check.sh"
    script.write_text("#!/bin/sh\ntrue\n")
    digest = hashlib.sha256(script.read_bytes()).hexdigest()
    (repo / "manifest.yaml").write_text(
        "schema_version: '1.0'\nscripts:\n"
        f"- id: check\n  path: check.sh\n  sha256: {digest}\n"
        "  interpreter: sh\n  timeout_seconds: 10\n"
        "  expected_exit_codes: [0]\n  read_only: true\n"
        "  supported_os: [ubuntu-24.04]\nplans: []\n"
    )
    commit_all(repo, "fixture")
    return repo
