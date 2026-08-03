"""Load the 12 WFT JSON Schemas and build a jsonschema validator for each.

The canonical schemas live in the repository root ``contracts/`` directory.
The same files are bundled as package data under ``src/wft/contracts/schemas/``
so that an installed wheel can still validate. A CI test asserts the two
copies are byte-identical.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.parse import urlparse

import jsonschema
from jsonschema import Draft202012Validator
from referencing import Registry, Resource
from referencing.exceptions import NoSuchResource

# Canonical contract file stems. Each derives the value used in
# ``meta.schema_name`` (see contract validation rules in 数据契约设计_v0.1.md §4).
CONTRACT_KEYS: list[str] = [
    "contract-01-envelope",
    "contract-02-runspec",
    "contract-03-execution-result",
    "contract-04-analysis-result",
    "contract-05-batch-summary",
    "contract-06-persist-ack",
    "contract-07-export-manifest",
    "contract-08-note-frontmatter",
    "contract-09-run-event",
    "contract-10-alert-event",
    "contract-11-inventory",
    "contract-12-script-registry",
]

_SCHEMA_FILENAMES = {key: f"{key}.schema.json" for key in CONTRACT_KEYS}

# Contracts that wrap the meta+payload Envelope. Contract-08 (NoteFrontmatter)
# is intentionally a plain object and is excluded.
ENVELOPE_CONTRACTS: set[str] = {
    key for key in CONTRACT_KEYS if key != "contract-08-note-frontmatter"
}


def _package_schema_dir() -> Path:
    return Path(__file__).resolve().parent / "schemas"


def _repo_schema_dir() -> Path:
    # src/wft/contracts/registry.py -> repo root
    return Path(__file__).resolve().parents[3] / "contracts"


def default_schema_dir() -> Path:
    """Locate the schema directory, honouring an explicit override first."""
    override = os.environ.get("WFT_CONTRACTS_DIR")
    if override:
        path = Path(override)
        if path.is_dir():
            return path
    repo_dir = _repo_schema_dir()
    if repo_dir.is_dir() and any(repo_dir.glob("contract-*.schema.json")):
        return repo_dir
    pkg_dir = _package_schema_dir()
    if pkg_dir.is_dir():
        return pkg_dir
    raise FileNotFoundError(
        "WFT contract schemas not found. Set WFT_CONTRACTS_DIR or run from a "
        "source checkout with a contracts/ directory."
    )


def schema_path(key: str, schema_dir: Path | None = None) -> Path:
    if key not in CONTRACT_KEYS:
        raise ValueError(f"unknown contract key: {key!r}")
    return (schema_dir or default_schema_dir()) / _SCHEMA_FILENAMES[key]


def load_schema(key: str, schema_dir: Path | None = None) -> dict:
    path = schema_path(key, schema_dir)
    return json.loads(path.read_text(encoding="utf-8"))


def _retrieve(uri: str, schema_dir: Path) -> Resource:
    # Relative $refs resolve to a URL that ends in the target file name (e.g.
    # contract-01-envelope.schema.json). Match by basename against the schema dir.
    name = Path(urlparse(uri).path).name
    candidate = schema_dir / name
    if not name or not candidate.is_file():
        raise NoSuchResource(uri)
    return Resource.from_contents(json.loads(candidate.read_text(encoding="utf-8")))


def get_validator(key: str, schema_dir: Path | None = None) -> Draft202012Validator:
    """Return a Draft 2020-12 validator for the given contract key."""
    schema_dir = schema_dir or default_schema_dir()
    schema = load_schema(key, schema_dir)
    registry = Registry(retrieve=lambda uri: _retrieve(uri, schema_dir))
    return Draft202012Validator(
        schema,
        registry=registry,
        format_checker=Draft202012Validator.FORMAT_CHECKER,
    )


def iter_contracts(schema_dir: Path | None = None) -> list[tuple[str, dict]]:
    """Return (key, schema) pairs for every contract."""
    return [(key, load_schema(key, schema_dir)) for key in CONTRACT_KEYS]


def is_valid(key: str, instance: object, schema_dir: Path | None = None) -> bool:
    return get_validator(key, schema_dir).is_valid(instance)


def validate(key: str, instance: object, schema_dir: Path | None = None) -> None:
    """Validate ``instance`` against contract ``key``; raise on failure."""
    get_validator(key, schema_dir).validate(instance)


def validate_with_errors(key, instance, schema_dir=None) -> list[str]:
    errors = sorted(
        get_validator(key, schema_dir).iter_errors(instance),
        key=lambda e: (list(e.path), e.message),
    )
    return [f"{'/'.join(map(str, e.path))}: {e.message}" for e in errors]
