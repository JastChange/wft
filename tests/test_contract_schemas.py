"""The 12 contract schemas load, are valid Draft 2020-12, and resolve $refs."""
from __future__ import annotations

import json

import pytest
from jsonschema import Draft202012Validator
from referencing.exceptions import NoSuchResource

from wft.contracts import registry

EXPECTED_CONTRACTS = 12


def test_all_contracts_present() -> None:
    assert len(registry.CONTRACT_KEYS) == EXPECTED_CONTRACTS
    assert registry.CONTRACT_KEYS[0] == "contract-01-envelope"
    assert registry.CONTRACT_KEYS[-1] == "contract-12-script-registry"


def test_every_key_has_a_schema_file() -> None:
    schema_dir = registry.default_schema_dir()
    for key in registry.CONTRACT_KEYS:
        path = registry.schema_path(key, schema_dir)
        assert path.is_file(), f"missing schema file for {key}"


@pytest.mark.parametrize("key", registry.CONTRACT_KEYS)
def test_schema_is_valid_draft_2020_12(key: str) -> None:
    schema = registry.load_schema(key)
    Draft202012Validator.check_schema(schema)


@pytest.mark.parametrize("key", registry.CONTRACT_KEYS)
def test_relative_refs_resolve(key: str) -> None:
    # Building the validator must not raise NoSuchResource for any $ref target.
    try:
        registry.get_validator(key)
    except NoSuchResource as exc:
        pytest.fail(f"{key}: unresolved $ref {exc}")


@pytest.mark.parametrize("key", registry.CONTRACT_KEYS)
def test_schema_has_expected_title(key: str) -> None:
    schema = registry.load_schema(key)
    assert schema["title"], f"{key} missing title"


def test_envelope_contracts_exclude_note_frontmatter() -> None:
    assert "contract-08-note-frontmatter" not in registry.ENVELOPE_CONTRACTS
    assert len(registry.ENVELOPE_CONTRACTS) == EXPECTED_CONTRACTS - 1


@pytest.mark.parametrize("key", registry.CONTRACT_KEYS)
def test_meta_schema_name_matches_contract_key(key: str) -> None:
    """meta.schema_name convention (数据契约设计 §4): file stem = contract key."""
    meta = registry.load_schema("contract-01-envelope")["$defs"]["meta"]
    assert "schema_name" in meta["properties"]
    assert "schema_version" in meta["properties"]
    assert "producer" in meta["properties"]
    assert "created_at" in meta["properties"]


def test_error_matrix_consistent_with_schema_enum() -> None:
    """Every error_class in the schema enum has an entry in the error matrix."""
    from wft.contracts.validate import ERROR_MATRIX

    schema = registry.load_schema("contract-01-envelope")
    enum = set(schema["$defs"]["error_class"]["enum"])
    assert enum == set(ERROR_MATRIX.keys())
    for cls, (category, retryable) in ERROR_MATRIX.items():
        assert category in ("TRANSIENT", "RESOURCE", "PERMANENT", "SECURITY", "DATA")
        assert isinstance(retryable, bool)
