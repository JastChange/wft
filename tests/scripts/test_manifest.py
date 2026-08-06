import hashlib
from pathlib import Path

import pytest

from wft.scripts.manifest import ManifestError, load_manifest


def _write_manifest(
    root: Path,
    *,
    script_path: str = "check.sh",
    sha256: str | None = None,
    interpreter: str = "sh",
    read_only: bool = True,
    supported_os: str = "ubuntu-24.04",
) -> Path:
    source = root / "check.sh"
    source.write_text("#!/bin/sh\ntrue\n")
    digest = sha256 or hashlib.sha256(source.read_bytes()).hexdigest()
    manifest = root / "manifest.yaml"
    manifest.write_text(
        "schema_version: '1.0'\nscripts:\n"
        f"- id: check\n  path: {script_path}\n  sha256: {digest}\n"
        f"  interpreter: {interpreter}\n  timeout_seconds: 10\n"
        f"  expected_exit_codes: [0]\n  read_only: {str(read_only).lower()}\n"
        f"  supported_os: [{supported_os}]\n"
        "plans:\n- id: baseline\n  scripts: [check]\n"
    )
    return manifest


def test_loads_valid_manifest_and_resolves_script_path(tmp_path: Path) -> None:
    loaded = load_manifest(_write_manifest(tmp_path))

    assert loaded.schema_version == "1.0"
    assert loaded.scripts[0].path == Path("check.sh")
    assert loaded.plans[0].scripts == ("check",)


def test_rejects_hash_mismatch(tmp_path: Path) -> None:
    manifest = _write_manifest(tmp_path, sha256="0" * 64)

    with pytest.raises(ManifestError, match="sha256"):
        load_manifest(manifest)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"script_path": "../check.sh"}, "escapes"),
        ({"interpreter": "ruby"}, "interpreter"),
        ({"read_only": False}, "read-only"),
        ({"supported_os": "debian-12"}, "target OS"),
    ],
)
def test_rejects_manifest_gate_violations(
    tmp_path: Path, changes: dict[str, object], message: str
) -> None:
    manifest = _write_manifest(tmp_path, **changes)  # type: ignore[arg-type]

    with pytest.raises(ManifestError, match=message):
        load_manifest(manifest)


def test_rejects_plan_reference_to_unknown_script(tmp_path: Path) -> None:
    manifest = _write_manifest(tmp_path)
    manifest.write_text(manifest.read_text().replace("scripts: [check]", "scripts: [missing]"))

    with pytest.raises(ManifestError, match="missing scripts"):
        load_manifest(manifest)
