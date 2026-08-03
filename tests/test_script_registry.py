"""Read-only script registry: SHA-256 identity and version resolution (AC-005, AC-008B)."""
from __future__ import annotations

from pathlib import Path

import pytest

from wft.contracts.errors import WFTScriptRegistryError
from wft.scriptreg.registry import ScriptRegistry, sha256_file

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_YAML = REPO_ROOT / "config" / "scripts.example.yaml"
SCRIPTS_DIR = REPO_ROOT / "scripts"


@pytest.fixture(scope="module")
def registry() -> ScriptRegistry:
    return ScriptRegistry.from_file(SCRIPTS_YAML)


def test_example_registry_loads(registry: ScriptRegistry) -> None:
    assert {s.name for s in registry.scripts} == {"disk-usage", "load-average"}
    assert all(s.risk == "read_only" for s in registry.scripts)


def test_sha256_matches_declared(registry: ScriptRegistry) -> None:
    for script in registry.scripts:
        assert sha256_file(script.path) == script.sha256
        assert len(script.sha256) == 64


def test_resolve_by_name(registry: ScriptRegistry) -> None:
    assert registry.resolve("disk-usage").name == "disk-usage"
    assert registry.resolve("load-average").name == "load-average"


def test_resolve_by_short_hash(registry: ScriptRegistry) -> None:
    script = registry.resolve("disk-usage")
    assert registry.resolve(f"disk-usage@{script.short_hash}").sha256 == script.sha256


def test_resolve_unknown_script_raises(registry: ScriptRegistry) -> None:
    with pytest.raises(WFTScriptRegistryError):
        registry.resolve("no-such-script")


def test_resolve_unknown_version_raises(registry: ScriptRegistry) -> None:
    with pytest.raises(WFTScriptRegistryError):
        registry.resolve("disk-usage@ffffffffffff")


def test_version_exists(registry: ScriptRegistry) -> None:
    sha = registry.resolve("disk-usage").sha256
    assert registry.version_exists(sha)
    assert not registry.version_exists("0" * 64)


def test_ambiguous_bare_name_rejected(tmp_path: Path) -> None:
    # Two file copies inside tmp_path (the registry boundary), same display name.
    file1 = tmp_path / "disk_usage.sh"
    file1.write_text(SCRIPTS_DIR.joinpath("disk_usage.sh").read_text(), encoding="utf-8")
    file2 = tmp_path / "disk_usage2.sh"
    file2.write_text(SCRIPTS_DIR.joinpath("disk_usage.sh").read_text() + "\n# second version\n", encoding="utf-8")
    sha1 = sha256_file(file1)
    sha2 = sha256_file(file2)
    assert sha1 != sha2
    yaml_file = tmp_path / "scripts.yaml"
    yaml_file.write_text(
        "scripts:\n"
        f"  - name: disk-usage\n    path: {file1}\n"
        f"    sha256: {sha1}\n    risk: read_only\n    shell: bash\n    timeout_sec: 30\n    enabled: true\n"
        f"  - name: disk-usage\n    path: {file2}\n"
        f"    sha256: {sha2}\n    risk: read_only\n    shell: bash\n    timeout_sec: 30\n    enabled: true\n",
        encoding="utf-8",
    )
    reg = ScriptRegistry.from_file(yaml_file)
    with pytest.raises(WFTScriptRegistryError):
        reg.resolve("disk-usage")  # ambiguous bare name
    assert reg.resolve(f"disk-usage@{sha1[:12]}").sha256 == sha1
    assert reg.resolve(f"disk-usage@{sha2[:12]}").sha256 == sha2


def test_mutating_script_rejected(tmp_path: Path) -> None:
    script_file = tmp_path / "mutator.sh"
    script_file.write_text("#!/bin/sh\necho mutating\n", encoding="utf-8")
    sha = sha256_file(script_file)
    yaml_file = tmp_path / "scripts.yaml"
    yaml_file.write_text(
        "scripts:\n"
        f"  - name: mutator\n    path: {script_file}\n    sha256: {sha}\n"
        "    risk: mutating\n    shell: bash\n    timeout_sec: 30\n    enabled: true\n",
        encoding="utf-8",
    )
    with pytest.raises(WFTScriptRegistryError):
        ScriptRegistry.from_file(yaml_file)


def test_sha256_mismatch_rejected(tmp_path: Path) -> None:
    script_file = tmp_path / "script.sh"
    script_file.write_text("#!/bin/sh\necho hi\n", encoding="utf-8")
    yaml_file = tmp_path / "scripts.yaml"
    yaml_file.write_text(
        "scripts:\n"
        f"  - name: x\n    path: {script_file}\n    sha256: {'0' * 64}\n"
        "    risk: read_only\n    shell: bash\n    timeout_sec: 30\n    enabled: true\n",
        encoding="utf-8",
    )
    with pytest.raises(WFTScriptRegistryError):
        ScriptRegistry.from_file(yaml_file)


def test_script_path_escape_rejected(tmp_path: Path) -> None:
    # yaml lives in tmp_path/config; the script is outside that boundary.
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    outside = tmp_path / "outside.sh"
    outside.write_text("#!/bin/sh\necho hi\n", encoding="utf-8")
    yaml_file = config_dir / "scripts.yaml"
    yaml_file.write_text(
        "scripts:\n"
        f"  - name: escape\n    path: {outside}\n    sha256: {sha256_file(outside)}\n"
        "    risk: read_only\n    shell: bash\n    timeout_sec: 30\n    enabled: true\n",
        encoding="utf-8",
    )
    # No contracts/ marker under tmp_path, so the boundary is config_dir itself;
    # the script resolving outside of it must be rejected.
    with pytest.raises(WFTScriptRegistryError):
        ScriptRegistry.from_file(yaml_file)
