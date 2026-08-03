"""The committed normal/boundary/error samples exercise every contract.

Normal and boundary samples must validate cleanly (schema + semantics). Error
samples must fail the JSON Schema itself, not only the semantic checks.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from wft.contracts import registry
from wft.contracts import validate as cv

SAMPLES = Path(__file__).resolve().parent / "samples"


def _samples_for(key: str, kind: str) -> list[tuple[str, object]]:
    directory = SAMPLES / key / kind
    if not directory.is_dir():
        return []
    return [(p.name, json.loads(p.read_text(encoding="utf-8"))) for p in sorted(directory.glob("*.json"))]


@pytest.mark.parametrize("key", registry.CONTRACT_KEYS)
def test_every_contract_has_all_three_kinds(key: str) -> None:
    for kind in ("normal", "boundary", "error"):
        samples = _samples_for(key, kind)
        assert samples, f"{key} has no {kind} samples"


@pytest.mark.parametrize("key", registry.CONTRACT_KEYS)
def test_normal_and_boundary_samples_validate(key: str) -> None:
    for kind in ("normal", "boundary"):
        for name, instance in _samples_for(key, kind):
            problems = cv.validate_contract_all(key, instance)
            assert not problems, f"{key}/{kind}/{name}: {problems}"


@pytest.mark.parametrize("key", registry.CONTRACT_KEYS)
def test_error_samples_fail_schema(key: str) -> None:
    for name, instance in _samples_for(key, "error"):
        schema_errors = registry.validate_with_errors(key, instance)
        assert schema_errors, f"{key}/error/{name}: expected a JSON Schema violation"
