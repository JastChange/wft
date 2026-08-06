from pathlib import Path

import pytest

from wft.inventory.loader import load_inventory
from wft.inventory.models import NodeSelector
from wft.inventory.selector import select_nodes


def _write_inventory(path: Path, key: str) -> None:
    path.write_text(
        "nodes:\n"
        f"- name: node-b\n  host: 10.0.0.2\n  username: root\n  private_key_path: {key}\n"
        "  os: ubuntu-22.04\n  groups: [batch]\n  tags: [disk]\n"
        f"- name: node-a\n  host: 10.0.0.1\n  username: root\n  private_key_path: {key}\n"
        "  os: ubuntu-24.04\n  groups: [batch]\n  tags: [memory]\n"
    )


def test_group_tag_and_explicit_selection_is_deduplicated(tmp_path: Path) -> None:
    key = tmp_path / "id_ed25519"
    key.write_text("fixture")
    inventory = tmp_path / "inventory.yaml"
    _write_inventory(inventory, str(key))

    loaded = load_inventory(inventory)
    selected = select_nodes(
        loaded,
        NodeSelector(node_names=("node-a",), groups=("batch",), tags=("disk",)),
    )

    assert [node.name for node in selected] == ["node-a", "node-b"]


def test_all_enabled_excludes_disabled_nodes(tmp_path: Path) -> None:
    key = tmp_path / "id_ed25519"
    key.write_text("fixture")
    inventory = tmp_path / "inventory.yaml"
    _write_inventory(inventory, str(key))
    inventory.write_text(
        inventory.read_text().replace("- name: node-b", "- name: node-b\n  enabled: false")
    )

    selected = select_nodes(load_inventory(inventory), NodeSelector(all_enabled=True))

    assert [node.name for node in selected] == ["node-a"]


def test_rejects_relative_private_key_path(tmp_path: Path) -> None:
    inventory = tmp_path / "inventory.yaml"
    _write_inventory(inventory, "./id_ed25519")

    with pytest.raises(ValueError, match="absolute"):
        load_inventory(inventory)


def test_rejects_duplicate_node_name(tmp_path: Path) -> None:
    key = tmp_path / "id_ed25519"
    key.write_text("fixture")
    inventory = tmp_path / "inventory.yaml"
    _write_inventory(inventory, str(key))
    inventory.write_text(inventory.read_text().replace("name: node-b", "name: node-a"))

    with pytest.raises(ValueError, match="duplicate node name"):
        load_inventory(inventory)


def test_rejects_unsupported_target_os(tmp_path: Path) -> None:
    key = tmp_path / "id_ed25519"
    key.write_text("fixture")
    inventory = tmp_path / "inventory.yaml"
    _write_inventory(inventory, str(key))
    inventory.write_text(inventory.read_text().replace("ubuntu-22.04", "debian-12"))

    with pytest.raises(ValueError, match="unsupported target OS"):
        load_inventory(inventory)
