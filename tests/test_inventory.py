"""Inventory loading, validation, and selection (AC-001)."""
from __future__ import annotations

from pathlib import Path

import pytest

from wft.contracts.errors import WFTInventoryError, WFTUserError
from wft.inventory.loader import line_for, load_yaml_with_lines
from wft.inventory.selector import require_targets, select_nodes
from wft.inventory.validate import validate_inventory_payload


def node(node_id: str, **kw) -> dict:
    base = {
        "node_id": node_id,
        "host": "10.0.0.1",
        "port": 22,
        "username": "ops",
        "auth": {"method": "key", "credential_ref": f"env://WFT_KEY_{node_id.upper().replace('-', '_')}"},
        "groups": ["web"],
        "tags": ["prod"],
    }
    base.update(kw)
    return base


def payload(*nodes) -> dict:
    return {"nodes": list(nodes)}


# ------------------------------------------------------------------ validation


def test_valid_inventory_has_no_problems() -> None:
    assert validate_inventory_payload(payload(node("node-a"), node("node-b"))) == []


def test_duplicate_node_id_detected() -> None:
    problems = validate_inventory_payload(payload(node("node-a"), node("node-a")))
    assert any("duplicate node_id" in p for p in problems)


def test_bad_credential_scheme_detected() -> None:
    problems = validate_inventory_payload(payload(node("node-a", auth={"method": "key", "credential_ref": "k8s://secret"})))
    assert any("not a recognised reference" in p for p in problems)


def test_inline_secret_body_detected() -> None:
    problems = validate_inventory_payload(
        payload(node("node-a", auth={"method": "key", "credential_ref": "op://item/field -----BEGIN OPENSSH PRIVATE KEY-----"}))
    )
    assert any("inline secret" in p for p in problems)


def test_inline_secret_body_with_whitespace_detected() -> None:
    problems = validate_inventory_payload(
        payload(node("node-a", auth={"method": "key", "credential_ref": "op://item/field hunter2 password"}))
    )
    assert any("inline secret" in p for p in problems)


def test_inline_secret_body_no_scheme_is_scheme_error() -> None:
    problems = validate_inventory_payload(
        payload(node("node-a", auth={"method": "key", "credential_ref": "-----BEGIN OPENSSH PRIVATE KEY-----"}))
    )
    assert any("not a recognised reference" in p for p in problems)


@pytest.mark.parametrize("ref", [
    "env://WFT_KEY_A", "file:///etc/wft/keys/a.key", "vault://wft/nodes/a",
    "op://vault/item/field", "agent://ssh-agent",
])
def test_legitimate_credential_refs_allowed(ref: str) -> None:
    problems = validate_inventory_payload(payload(node("node-a", auth={"method": "key", "credential_ref": ref})))
    assert problems == []


@pytest.mark.parametrize("ref", [
    "env://hunter2",      # lowercase password-looking value, not an UPPER_SNAKE ref
    "env://1BAD",         # cannot start with a digit
    "env://MY.KEY",       # dot is not valid in an env var name
    "env://MY KEY",       # whitespace
])
def test_env_ref_must_be_upper_snake_name(ref: str) -> None:
    problems = validate_inventory_payload(payload(node("node-a", auth={"method": "key", "credential_ref": ref})))
    assert any("environment-variable reference" in p for p in problems)


@pytest.mark.parametrize("ref", ["file://relative/path", "file://etc/wft/key"])
def test_file_ref_must_be_absolute(ref: str) -> None:
    problems = validate_inventory_payload(payload(node("node-a", auth={"method": "key", "credential_ref": ref})))
    assert any("absolute path" in p for p in problems)


@pytest.mark.parametrize("ref", ["vault://", "op://", "agent://"])
def test_empty_reference_body_rejected(ref: str) -> None:
    problems = validate_inventory_payload(payload(node("node-a", auth={"method": "key", "credential_ref": ref})))
    assert problems  # empty body must never be accepted as a reference


def test_missing_bastion_detected() -> None:
    problems = validate_inventory_payload(payload(node("node-a", bastion="node-z")))
    assert any("does not reference an existing node" in p for p in problems)


def test_bastion_cycle_detected() -> None:
    problems = validate_inventory_payload(payload(node("node-a", bastion="node-b"), node("node-b", bastion="node-a")))
    assert any("forms a cycle" in p for p in problems)


# ------------------------------------------------------------------ line tracking


def test_line_numbers_reported(tmp_path: Path) -> None:
    inventory = tmp_path / "inventory.yaml"
    inventory.write_text(
        "nodes:\n"
        "  - node_id: node-a\n"
        "    groups: [web]\n"
        "  - node_id: node-a\n"
        "    groups: [db]\n",
        encoding="utf-8",
    )
    data, line_index = load_yaml_with_lines(inventory)
    assert data["nodes"][1]["node_id"] == "node-a"
    assert line_for(line_index, "nodes", 0, "node_id") == 2
    assert line_for(line_index, "nodes", 1, "node_id") == 4
    problems = validate_inventory_payload(data, line_index=line_index)
    assert any("(line 4)" in p for p in problems)


def test_load_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(WFTInventoryError):
        load_yaml_with_lines(tmp_path / "nope.yaml")


def test_load_non_mapping_raises(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text("- just\n- a\n- list\n", encoding="utf-8")
    with pytest.raises(WFTInventoryError):
        load_yaml_with_lines(bad)


# ------------------------------------------------------------------ selection


def test_no_selector_targets_all_enabled() -> None:
    nodes = [node("node-a"), node("node-b", enabled=False)]
    assert [n["node_id"] for n in select_nodes(nodes)] == ["node-a"]


def test_groups_union() -> None:
    nodes = [
        node("node-a", groups=["web"]),
        node("node-b", groups=["db"]),
        node("node-c", groups=["batch"]),
    ]
    selected = select_nodes(nodes, groups=["web", "batch"])
    assert [n["node_id"] for n in selected] == ["node-a", "node-c"]


def test_tags_union() -> None:
    nodes = [node("node-a", tags=["prod"]), node("node-b", tags=["canary"]), node("node-c", tags=["staging"])]
    selected = select_nodes(nodes, tags=["prod", "canary"])
    assert [n["node_id"] for n in selected] == ["node-a", "node-b"]


def test_group_and_tag_intersect() -> None:
    nodes = [node("node-a", groups=["web"], tags=["prod"]), node("node-b", groups=["db"], tags=["prod"])]
    selected = select_nodes(nodes, groups=["web", "db"], tags=["prod"])
    assert [n["node_id"] for n in selected] == ["node-a", "node-b"]
    selected = select_nodes(nodes, groups=["web"], tags=["prod"])
    assert [n["node_id"] for n in selected] == ["node-a"]


def test_disabled_node_never_selected() -> None:
    nodes = [node("node-a", enabled=False)]
    assert select_nodes(nodes) == []
    assert select_nodes(nodes, groups=["web"]) == []


def test_require_targets_raises_on_empty() -> None:
    require_targets([node("node-a")])  # no raise
    with pytest.raises(WFTUserError):
        require_targets([])
