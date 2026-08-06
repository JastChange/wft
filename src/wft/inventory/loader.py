from pathlib import Path
from typing import Any

import yaml

from .models import Inventory

SUPPORTED_OS = {"ubuntu-22.04", "ubuntu-24.04"}


def load_inventory(path: Path) -> Inventory:
    path = path.expanduser().resolve()
    loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(loaded, dict):
        raise TypeError("inventory root must be a mapping")
    raw: dict[str, Any] = loaded
    nodes = raw.get("nodes", [])
    if not isinstance(nodes, list):
        raise TypeError("inventory nodes must be a list")

    for node in nodes:
        if not isinstance(node, dict):
            raise TypeError("inventory node must be a mapping")
        key_path = Path(node["private_key_path"])
        if not key_path.is_absolute():
            raise ValueError(f"private key path must be absolute: {node.get('name', '<unknown>')}")
        node["private_key_path"] = key_path.resolve()
        if node.get("os") not in SUPPORTED_OS:
            raise ValueError(f"unsupported target OS: {node.get('os')}")

    names = [node["name"] for node in nodes]
    if len(names) != len(set(names)):
        raise ValueError("duplicate node name")
    return Inventory.model_validate(raw)
