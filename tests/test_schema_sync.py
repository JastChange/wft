"""The bundled package schemas must be byte-identical to the canonical contracts/."""
from __future__ import annotations

from pathlib import Path

from wft.contracts import registry

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_bundled_schemas_are_byte_identical() -> None:
    canonical = REPO_ROOT / "contracts"
    bundled = Path(registry._package_schema_dir())
    canonical_files = {p.name for p in canonical.glob("contract-*.schema.json")}
    bundled_files = {p.name for p in bundled.glob("contract-*.schema.json")}
    assert canonical_files == bundled_files, (
        f"schema file set differs: only in contracts/ {canonical_files - bundled_files}, "
        f"only bundled {bundled_files - canonical_files}"
    )
    for name in sorted(canonical_files):
        assert (bundled / name).read_bytes() == (canonical / name).read_bytes(), (
            f"schema {name} drifted between contracts/ and bundled copy"
        )
