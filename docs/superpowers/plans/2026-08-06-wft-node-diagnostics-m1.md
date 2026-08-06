# WFT Node Diagnostics M1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Replace the legacy MVP package with the WFT 1.0 CLI/config/inventory/script-snapshot foundation required by AC-001 through AC-006.

**Architecture:** Typer commands call deep config, inventory, and script-snapshot modules. Pydantic models own validation; Git access and cache prompting sit behind the single `prepare_snapshot()` interface. M1 creates no diagnostic tasks and performs no SSH execution.

**Tech Stack:** Python 3.12, Typer, Pydantic 2, PyYAML, argon2-cffi, pytest, Ruff, mypy, Git CLI.

---

## Locked file map

```text
src/wft/
├── __init__.py                 version only
├── cli/
│   ├── app.py                  Typer root
│   └── commands/
│       ├── admin.py            init/reset password
│       ├── config.py           config check
│       ├── inventory.py        inventory check/select
│       └── scripts.py          script sync/check
├── config/
│   ├── models.py               AppConfig
│   └── loader.py               private YAML loading
├── inventory/
│   ├── models.py               Node and selectors
│   ├── loader.py               YAML loading
│   └── selector.py             deterministic selection
├── scripts/
│   ├── models.py               Manifest and snapshot values
│   ├── manifest.py             validation and hashes
│   ├── git_source.py           Git CLI adapter
│   └── snapshot.py             deep snapshot interface
└── auth/
    └── password.py             Argon2 hash file
```

## Task 1: Cut over the Python package baseline

**Files:**
- Modify: `pyproject.toml`
- Replace: `src/wft/`
- Replace: `tests/`
- Preserve by moving: `tests/http_fault_stubs.py` → `tests/support/http_fault_stubs.py`
- Preserve by moving: `tests/ssh_test_server.py` → `tests/support/ssh_test_server.py`
- Create: `tests/test_package.py`

- [x] **Step 1: Move reusable fixtures, then remove legacy runtime/tests/contracts**

```bash
mkdir -p /tmp/wft-fixtures
cp tests/http_fault_stubs.py /tmp/wft-fixtures/
cp tests/ssh_test_server.py /tmp/wft-fixtures/
rm -rf src/wft tests contracts
mkdir -p src/wft tests/support
cp /tmp/wft-fixtures/http_fault_stubs.py tests/support/
cp /tmp/wft-fixtures/ssh_test_server.py tests/support/
```

- [x] **Step 2: Write the package smoke test**

```python
# tests/test_package.py
from typer.testing import CliRunner

from wft import __version__
from wft.cli.app import app


def test_version_and_help() -> None:
    assert __version__ == "1.0.0"
    result = CliRunner().invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "WFT node diagnostics" in result.stdout
```

- [x] **Step 3: Run the test and confirm RED**

Run: `.venv/bin/pytest tests/test_package.py -q`
Expected: FAIL because the new package and CLI do not exist.

- [x] **Step 4: Replace `pyproject.toml` and add the minimal package**

```toml
[build-system]
requires = ["setuptools>=75"]
build-backend = "setuptools.build_meta"

[project]
name = "wft"
version = "1.0.0"
requires-python = ">=3.12,<3.13"
dependencies = [
  "argon2-cffi>=23.1,<26",
  "pydantic>=2.11,<3",
  "PyYAML>=6,<7",
  "typer>=0.16,<1",
]

[project.optional-dependencies]
test = ["mypy>=1.17,<2", "pytest>=8.4,<9", "ruff>=0.12,<1"]

[project.scripts]
wft = "wft.cli.app:app"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["src"]

[tool.ruff]
target-version = "py312"
line-length = 100

[tool.mypy]
python_version = "3.12"
strict = true
packages = ["wft"]
```

```python
# src/wft/__init__.py
__version__ = "1.0.0"
```

```python
# src/wft/cli/app.py
import typer

app = typer.Typer(help="WFT node diagnostics")


@app.callback()
def root() -> None:
    """WFT node diagnostics."""
```

- [x] **Step 5: Reinstall and confirm GREEN**

Run: `.venv/bin/python -m pip install -e '.[test]' && .venv/bin/pytest tests/test_package.py -q`
Expected: `1 passed`.

- [x] **Step 6: Commit**

```bash
git add pyproject.toml src tests
git commit -m "chore: establish WFT 1.0 package baseline"
```

## Task 2: Type and load the private application config

**Files:**
- Create: `src/wft/config/models.py`
- Create: `src/wft/config/loader.py`
- Create: `tests/config/test_loader.py`
- Create: `config/wft.example.yaml`

- [x] **Step 1: Write failing permission and validation tests**

```python
# tests/config/test_loader.py
from pathlib import Path

import pytest

from wft.config.loader import ConfigFileModeError, load_config


def test_loads_private_absolute_paths(tmp_path: Path) -> None:
    key = tmp_path / "node.key"
    key.write_text("fixture")
    key.chmod(0o600)
    cfg = tmp_path / "wft.yaml"
    cfg.write_text(
        "data_dir: ./data\n"
        "inventory_path: ./inventory.yaml\n"
        "script_repository:\n"
        "  url: ssh://git@example/scripts.git\n"
        "  branch: main\n"
        f"  deploy_key_path: {key}\n"
        "web:\n  bind_host: 10.0.0.5\n  port: 8080\n"
    )
    cfg.chmod(0o600)

    loaded = load_config(cfg)

    assert loaded.data_dir == (tmp_path / "data").resolve()
    assert loaded.script_repository.deploy_key_path == key.resolve()
    assert loaded.web.bind_host == "10.0.0.5"


def test_rejects_group_readable_config(tmp_path: Path) -> None:
    cfg = tmp_path / "wft.yaml"
    cfg.write_text("data_dir: ./data\ninventory_path: ./inventory.yaml\n")
    cfg.chmod(0o640)
    with pytest.raises(ConfigFileModeError):
        load_config(cfg)
```

- [x] **Step 2: Run RED**

Run: `.venv/bin/pytest tests/config/test_loader.py -q`
Expected: import failure.

- [x] **Step 3: Implement typed config and a strict loader**

```python
# src/wft/config/models.py
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class ScriptRepositoryConfig(BaseModel):
    model_config = ConfigDict(frozen=True)
    url: str
    branch: str = "main"
    deploy_key_path: Path


class WebConfig(BaseModel):
    model_config = ConfigDict(frozen=True)
    bind_host: str
    port: int = Field(default=8080, ge=1, le=65535)


class AppConfig(BaseModel):
    model_config = ConfigDict(frozen=True)
    data_dir: Path
    inventory_path: Path
    script_repository: ScriptRepositoryConfig
    web: WebConfig
    default_concurrency: int = Field(default=10, ge=1, le=50)
```

```python
# src/wft/config/loader.py
from pathlib import Path

import yaml

from .models import AppConfig


class ConfigFileModeError(ValueError):
    pass


def _resolve(value: str, base: Path) -> Path:
    path = Path(value).expanduser()
    return (base / path).resolve() if not path.is_absolute() else path.resolve()


def load_config(path: Path) -> AppConfig:
    path = path.resolve()
    if path.stat().st_mode & 0o077:
        raise ConfigFileModeError(f"config must be mode 0600: {path}")
    raw = yaml.safe_load(path.read_text()) or {}
    base = path.parent
    raw["data_dir"] = _resolve(raw["data_dir"], base)
    raw["inventory_path"] = _resolve(raw["inventory_path"], base)
    repo = raw["script_repository"]
    repo["deploy_key_path"] = _resolve(repo["deploy_key_path"], base)
    return AppConfig.model_validate(raw)
```

- [x] **Step 4: Run GREEN and static checks**

Run: `.venv/bin/pytest tests/config/test_loader.py -q && .venv/bin/ruff check src tests && .venv/bin/mypy src`
Expected: all pass.

- [x] **Step 5: Add a fake-value example config and commit**

```yaml
# config/wft.example.yaml
data_dir: /var/lib/wft
inventory_path: /etc/wft/inventory.yaml
default_concurrency: 10
script_repository:
  url: ssh://git@example.invalid/diagnostics.git
  branch: main
  deploy_key_path: /run/secrets/scripts-deploy-key
web:
  bind_host: 10.0.0.5
  port: 8080
```

```bash
git add src/wft/config tests/config config/wft.example.yaml
git commit -m "feat: load private typed configuration"
```

## Task 3: Load and select inventory nodes

**Files:**
- Create: `src/wft/inventory/models.py`
- Create: `src/wft/inventory/loader.py`
- Create: `src/wft/inventory/selector.py`
- Create: `tests/inventory/test_inventory.py`
- Replace: `config/inventory.example.yaml`

- [x] **Step 1: Write selection tests**

```python
# tests/inventory/test_inventory.py
from pathlib import Path

from wft.inventory.loader import load_inventory
from wft.inventory.models import NodeSelector
from wft.inventory.selector import select_nodes


def test_group_tag_and_explicit_selection_is_deduplicated(tmp_path: Path) -> None:
    key = tmp_path / "id_ed25519"
    key.write_text("fixture")
    inventory = tmp_path / "inventory.yaml"
    inventory.write_text(
        "nodes:\n"
        f"- name: node-a\n  host: 10.0.0.1\n  username: root\n  private_key_path: {key}\n"
        "  os: ubuntu-24.04\n  groups: [batch]\n  tags: [memory]\n"
        f"- name: node-b\n  host: 10.0.0.2\n  username: root\n  private_key_path: {key}\n"
        "  os: ubuntu-22.04\n  groups: [batch]\n  tags: [disk]\n"
    )
    loaded = load_inventory(inventory)
    selected = select_nodes(
        loaded,
        NodeSelector(node_names=("node-a",), groups=("batch",), tags=("disk",)),
    )
    assert [node.name for node in selected] == ["node-a", "node-b"]
```

- [x] **Step 2: Run RED**

Run: `.venv/bin/pytest tests/inventory/test_inventory.py -q`
Expected: import failure.

- [x] **Step 3: Implement immutable models and deterministic selection**

```python
# src/wft/inventory/models.py
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class Node(BaseModel):
    model_config = ConfigDict(frozen=True)
    name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
    host: str
    port: int = Field(default=22, ge=1, le=65535)
    username: str
    private_key_path: Path
    os: str
    groups: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    enabled: bool = True


class Inventory(BaseModel):
    model_config = ConfigDict(frozen=True)
    nodes: tuple[Node, ...]


class NodeSelector(BaseModel):
    model_config = ConfigDict(frozen=True)
    node_names: tuple[str, ...] = ()
    groups: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    all_enabled: bool = False
```

```python
# src/wft/inventory/loader.py
from pathlib import Path

import yaml

from .models import Inventory

SUPPORTED_OS = {"ubuntu-22.04", "ubuntu-24.04"}


def load_inventory(path: Path) -> Inventory:
    path = path.resolve()
    raw = yaml.safe_load(path.read_text()) or {}
    nodes = raw.get("nodes", [])
    for node in nodes:
        key_path = Path(node["private_key_path"]).expanduser()
        node["private_key_path"] = (
            (path.parent / key_path).resolve() if not key_path.is_absolute() else key_path.resolve()
        )
        if node.get("os") not in SUPPORTED_OS:
            raise ValueError(f"unsupported target OS: {node.get('os')}")
    names = [node["name"] for node in nodes]
    if len(names) != len(set(names)):
        raise ValueError("duplicate node name")
    return Inventory.model_validate({"nodes": nodes})
```

```python
# src/wft/inventory/selector.py
from .models import Inventory, Node, NodeSelector


def select_nodes(inventory: Inventory, selector: NodeSelector) -> tuple[Node, ...]:
    selected: list[Node] = []
    for node in inventory.nodes:
        matches = selector.all_enabled or node.name in selector.node_names
        matches = matches or bool(set(node.groups) & set(selector.groups))
        matches = matches or bool(set(node.tags) & set(selector.tags))
        if node.enabled and matches:
            selected.append(node)
    return tuple(sorted(selected, key=lambda node: node.name))
```

- [x] **Step 4: Run GREEN, add validation cases, and commit**

Run: `.venv/bin/pytest tests/inventory -q && .venv/bin/ruff check src tests && .venv/bin/mypy src`
Expected: all pass.

```bash
git add src/wft/inventory tests/inventory config/inventory.example.yaml
git commit -m "feat: validate and select diagnostic nodes"
```

## Task 4: Validate the script Manifest

**Files:**
- Create: `src/wft/scripts/models.py`
- Create: `src/wft/scripts/manifest.py`
- Create: `tests/scripts/test_manifest.py`
- Create: `config/manifest.example.yaml`

- [x] **Step 1: Write failing hash, path, and OS tests**

```python
# tests/scripts/test_manifest.py
from pathlib import Path

import pytest

from wft.scripts.manifest import ManifestError, load_manifest


def test_rejects_hash_mismatch(tmp_path: Path) -> None:
    (tmp_path / "check.sh").write_text("#!/bin/sh\ntrue\n")
    (tmp_path / "manifest.yaml").write_text(
        "schema_version: '1.0'\nscripts:\n"
        "- id: check\n  path: check.sh\n  sha256: " + "0" * 64 + "\n"
        "  interpreter: sh\n  timeout_seconds: 10\n"
        "  expected_exit_codes: [0]\n  read_only: true\n"
        "  supported_os: [ubuntu-24.04]\nplans: []\n"
    )
    with pytest.raises(ManifestError, match="sha256"):
        load_manifest(tmp_path / "manifest.yaml")
```

- [x] **Step 2: Run RED**

Run: `.venv/bin/pytest tests/scripts/test_manifest.py -q`
Expected: import failure.

- [x] **Step 3: Implement models and validation**

```python
# src/wft/scripts/models.py
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class ScriptDefinition(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,63}$")
    path: Path
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    interpreter: str
    timeout_seconds: int = Field(ge=1, le=3600)
    expected_exit_codes: tuple[int, ...] = (0,)
    read_only: bool
    supported_os: tuple[str, ...]


class DiagnosticPlan(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: str
    scripts: tuple[str, ...]


class Manifest(BaseModel):
    model_config = ConfigDict(frozen=True)
    schema_version: str
    scripts: tuple[ScriptDefinition, ...]
    plans: tuple[DiagnosticPlan, ...]
```

```python
# src/wft/scripts/manifest.py
import hashlib
from pathlib import Path

import yaml

from .models import Manifest


class ManifestError(ValueError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_manifest(path: Path) -> Manifest:
    path = path.resolve()
    root = path.parent
    manifest = Manifest.model_validate(yaml.safe_load(path.read_text()) or {})
    if manifest.schema_version != "1.0":
        raise ManifestError(f"unsupported manifest schema: {manifest.schema_version}")
    ids = [script.id for script in manifest.scripts]
    if len(ids) != len(set(ids)):
        raise ManifestError("duplicate script id")
    for script in manifest.scripts:
        source = (root / script.path).resolve()
        if not source.is_relative_to(root):
            raise ManifestError(f"script path escapes repository: {script.id}")
        if script.interpreter not in {"bash", "sh", "python3"}:
            raise ManifestError(f"unsupported interpreter: {script.interpreter}")
        if script.read_only is not True:
            raise ManifestError(f"script is not declared read-only: {script.id}")
        if _sha256(source) != script.sha256:
            raise ManifestError(f"sha256 mismatch: {script.id}")
    known = set(ids)
    plan_ids = [plan.id for plan in manifest.plans]
    if len(plan_ids) != len(set(plan_ids)):
        raise ManifestError("duplicate plan id")
    for plan in manifest.plans:
        missing = set(plan.scripts) - known
        if missing:
            raise ManifestError(f"plan {plan.id} references missing scripts: {sorted(missing)}")
    return manifest
```

- [x] **Step 4: Run GREEN and commit**

Run: `.venv/bin/pytest tests/scripts/test_manifest.py -q && .venv/bin/ruff check src tests && .venv/bin/mypy src`
Expected: all pass.

```bash
git add src/wft/scripts tests/scripts config/manifest.example.yaml
git commit -m "feat: validate diagnostic script manifests"
```

## Task 5: Prepare immutable Git script snapshots

**Files:**
- Create: `src/wft/scripts/git_source.py`
- Create: `src/wft/scripts/snapshot.py`
- Create: `tests/scripts/conftest.py`
- Create: `tests/scripts/test_snapshot.py`

- [x] **Step 1: Write local-repository happy path and cache-fallback tests**

```python
# tests/scripts/conftest.py
import hashlib
import subprocess
from pathlib import Path

import pytest


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
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(
        [
            "git", "-c", "user.name=WFT Test", "-c", "user.email=wft@example.invalid",
            "commit", "-m", "fixture",
        ],
        cwd=repo,
        check=True,
    )
    return repo
```

```python
# tests/scripts/test_snapshot.py
from pathlib import Path

import pytest

from wft.scripts.snapshot import CacheDecision, SnapshotRequest, prepare_snapshot


def test_fetch_failure_requires_explicit_cache_acceptance(fixture_repo: Path, tmp_path: Path) -> None:
    first = prepare_snapshot(
        SnapshotRequest(str(fixture_repo), "main", tmp_path / "cache", tmp_path / "snaps"),
        decide_cache=lambda _: CacheDecision.REJECT,
    )
    fixture_repo.rename(fixture_repo.with_suffix(".offline"))
    with pytest.raises(RuntimeError, match="cached snapshot rejected"):
        prepare_snapshot(
            SnapshotRequest(str(fixture_repo), "main", tmp_path / "cache", tmp_path / "snaps"),
            decide_cache=lambda _: CacheDecision.REJECT,
        )
    cached = prepare_snapshot(
        SnapshotRequest(str(fixture_repo), "main", tmp_path / "cache", tmp_path / "snaps"),
        decide_cache=lambda _: CacheDecision.ACCEPT,
    )
    assert cached.commit_sha == first.commit_sha
    assert cached.used_cache is True
```

- [x] **Step 2: Run RED**

Run: `.venv/bin/pytest tests/scripts/test_snapshot.py -q`
Expected: import failure.

- [x] **Step 3: Implement the deep snapshot interface**

```python
# src/wft/scripts/git_source.py
import subprocess
from dataclasses import dataclass
from pathlib import Path


class GitSourceError(RuntimeError):
    pass


@dataclass(frozen=True)
class FetchedCommit:
    sha: str
    committed_at: str


def _git(*args: str, cwd: Path | None = None) -> str:
    completed = subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True, check=False
    )
    if completed.returncode != 0:
        raise GitSourceError(completed.stderr.strip() or "git command failed")
    return completed.stdout.strip()


def fetch_latest(repository_url: str, branch: str, cache_dir: Path) -> FetchedCommit:
    if not (cache_dir / ".git").is_dir():
        cache_dir.parent.mkdir(parents=True, exist_ok=True)
        _git("clone", "--no-checkout", "--", repository_url, str(cache_dir))
    _git("remote", "set-url", "origin", repository_url, cwd=cache_dir)
    _git("fetch", "--prune", "origin", branch, cwd=cache_dir)
    sha = _git("rev-parse", "FETCH_HEAD^{commit}", cwd=cache_dir)
    committed_at = _git("show", "-s", "--format=%cI", sha, cwd=cache_dir)
    return FetchedCommit(sha=sha, committed_at=committed_at)


def checkout(cache_dir: Path, sha: str, destination: Path) -> None:
    _git("worktree", "add", "--detach", str(destination), sha, cwd=cache_dir)


def remove_checkout(cache_dir: Path, destination: Path) -> None:
    _git("worktree", "remove", "--force", str(destination), cwd=cache_dir)
```

```python
# src/wft/scripts/snapshot.py
import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

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
    metadata = json.loads((path / ".wft-snapshot.json").read_text())
    manifest = load_manifest(path / "manifest.yaml")
    return ScriptSnapshot(
        path=path,
        repository_url=metadata["repository_url"],
        branch=metadata["branch"],
        commit_sha=metadata["commit_sha"],
        commit_time=metadata["commit_time"],
        manifest_sha256=metadata["manifest_sha256"],
        manifest=manifest,
        used_cache=used_cache,
    )


def _latest_cache(request: SnapshotRequest) -> ScriptSnapshot:
    candidates: list[ScriptSnapshot] = []
    for path in request.snapshots_dir.iterdir() if request.snapshots_dir.exists() else ():
        if not (path / ".wft-snapshot.json").is_file():
            continue
        candidate = _load_snapshot(path, used_cache=True)
        if (
            candidate.repository_url == request.repository_url
            and candidate.branch == request.branch
        ):
            candidates.append(candidate)
    if not candidates:
        raise RuntimeError("no validated cached script snapshot")
    return max(candidates, key=lambda candidate: candidate.commit_time)


def _publish(request: SnapshotRequest, sha: str, committed_at: str) -> ScriptSnapshot:
    final = request.snapshots_dir / sha
    if final.is_dir():
        return _load_snapshot(final, used_cache=False)
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
            "repository_url": request.repository_url,
            "branch": request.branch,
            "commit_sha": sha,
            "commit_time": committed_at,
            "manifest_sha256": manifest_sha256,
        }
        metadata_path = staging / ".wft-snapshot.json"
        metadata_path.write_text(json.dumps(metadata, sort_keys=True) + "\n")
        with metadata_path.open("rb") as handle:
            os.fsync(handle.fileno())
        staging.rename(final)
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
        fetched = fetch_latest(request.repository_url, request.branch, request.cache_dir)
        return _publish(request, fetched.sha, fetched.committed_at)
    except GitSourceError as fetch_error:
        cached = _latest_cache(request)
        if decide_cache(cached) is CacheDecision.ACCEPT:
            return cached
        raise RuntimeError("cached snapshot rejected") from fetch_error
```

Never execute from the mutable Git cache. M1 acceptance checks atomic publication and immutable snapshot reuse.

- [x] **Step 4: Add Git command injection and path tests, then run GREEN**

Run: `.venv/bin/pytest tests/scripts -q && .venv/bin/ruff check src tests && .venv/bin/mypy src`
Expected: all pass.

- [x] **Step 5: Commit**

```bash
git add src/wft/scripts tests/scripts
git commit -m "feat: prepare immutable Git script snapshots"
```

## Task 6: Store the administrator password hash

**Files:**
- Create: `src/wft/auth/password.py`
- Create: `tests/auth/test_password.py`

- [x] **Step 1: Write the hash-file test**

```python
# tests/auth/test_password.py
import json
from pathlib import Path

from wft.auth.password import initialize_password, verify_password


def test_initializes_hash_without_plaintext(tmp_path: Path) -> None:
    path = tmp_path / "admin.json"
    generated = initialize_password(path)
    saved = json.loads(path.read_text())
    assert generated not in path.read_text()
    assert saved["schema_version"] == "1.0"
    assert verify_password(path, generated) is True
    assert path.stat().st_mode & 0o777 == 0o600
```

- [x] **Step 2: Run RED, implement, and run GREEN**

```python
# src/wft/auth/password.py
import json
import os
import secrets
from pathlib import Path

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError

_hasher = PasswordHasher()


def initialize_password(path: Path) -> str:
    password = secrets.token_urlsafe(24)
    payload = {
        "schema_version": "1.0",
        "username": "admin",
        "password_hash": _hasher.hash(password),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.partial")
    temporary.write_text(json.dumps(payload, sort_keys=True) + "\n")
    temporary.chmod(0o600)
    with temporary.open("rb") as handle:
        os.fsync(handle.fileno())
    temporary.replace(path)
    return password


def verify_password(path: Path, password: str) -> bool:
    payload = json.loads(path.read_text())
    try:
        return _hasher.verify(payload["password_hash"], password)
    except VerifyMismatchError:
        return False
```

Run: `.venv/bin/pytest tests/auth/test_password.py -q`
Expected: pass.

- [x] **Step 3: Commit**

```bash
git add src/wft/auth tests/auth
git commit -m "feat: initialize administrator credentials"
```

## Task 7: Wire the M1 Typer commands

**Files:**
- Modify: `src/wft/cli/app.py`
- Create: `src/wft/cli/commands/admin.py`
- Create: `src/wft/cli/commands/config.py`
- Create: `src/wft/cli/commands/inventory.py`
- Create: `src/wft/cli/commands/scripts.py`
- Create: `tests/cli/test_commands.py`

- [x] **Step 1: Write CLI contract tests**

```python
# tests/cli/test_commands.py
from typer.testing import CliRunner

from wft.cli.app import app

runner = CliRunner()


def test_m1_help_exposes_only_available_commands() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "admin" in result.stdout
    assert "config" in result.stdout
    assert "inventory" in result.stdout
    assert "scripts" in result.stdout
    assert "run" not in result.stdout
```

- [x] **Step 2: Run RED**

Run: `.venv/bin/pytest tests/cli/test_commands.py -q`
Expected: command assertion failure.

- [x] **Step 3: Register command groups and delegate to modules**

Command surface:

```text
wft admin init --auth-file PATH
wft admin reset-password --auth-file PATH
wft config check --config PATH
wft inventory check --config PATH
wft inventory select --config PATH [--node NAME] [--group GROUP] [--tag TAG] [--all]
wft scripts check --config PATH
wft scripts sync --config PATH [--allow-cached-scripts]
```

Each command must return exit 0 on success and exit 2 on configuration/Git/validation failure. Commands print JSON only when `--json` is supplied; secret values and key paths are omitted from JSON output.

- [x] **Step 4: Run all M1 checks**

Run:

```bash
.venv/bin/pytest -q
.venv/bin/ruff format --check src tests
.venv/bin/ruff check src tests
.venv/bin/mypy src
.venv/bin/wft --help
```

Expected: all commands exit 0; no legacy command appears.

- [x] **Step 5: Commit**

```bash
git add src/wft/cli tests/cli
git commit -m "feat: expose M1 diagnostic configuration commands"
```

## Task 8: M1 documentation and acceptance evidence

**Files:**
- Modify: `README.md`
- Create: `docs/operations/configuration.md`
- Create: `artifacts/acceptance/m1.md`

- [x] **Step 1: Document exact install and command examples**

Document Python 3.12 setup, chmod 0600 requirements, fake inventory, read-only Deploy Key, cache fallback, Manifest fields, and the command surface from Task 7. Do not document M2+ commands as available.

- [x] **Step 2: Execute AC-001 through AC-006 and record evidence**

Run the exact commands from `docs/spec/ACCEPTANCE_MATRIX_v1.0.md`, then record command, exit code, and test name in `artifacts/acceptance/m1.md`.

- [x] **Step 3: Verify the milestone**

Run:

```bash
.venv/bin/pytest -q
.venv/bin/ruff format --check src tests
.venv/bin/ruff check src tests
.venv/bin/mypy src
git diff --check
```

Expected: all pass with AC-001 through AC-006 mapped to tests.

- [x] **Step 4: Commit and stop for milestone review**

```bash
git add README.md docs/operations artifacts/acceptance/m1.md
git commit -m "docs: publish M1 configuration and acceptance evidence"
```

Stop. Report M1 changes, fresh verification output, accepted risks, and every spec deviation. Do not begin M2 until the user confirms.
