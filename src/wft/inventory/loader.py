"""Load inventory YAML, capturing the source line of every node field.

Line numbers let ``wft inventory check`` report errors against the exact
location in the user's file (AC-001).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from wft.contracts.errors import WFTInventoryError


class LineLoader(yaml.SafeLoader):
    """SafeLoader that records the line where each value starts."""

    def construct_mapping(self, node, deep=False):
        mapping = super().construct_mapping(node, deep=deep)
        mapping["__line__"] = node.start_mark.line + 1
        return mapping


def load_yaml_with_lines(path: Path) -> tuple[dict, dict]:
    """Return (data, line_index) where line_index maps path tuples to line numbers."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise WFTInventoryError(f"cannot read {path}: {exc}") from exc
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise WFTInventoryError(f"{path} is not valid YAML: {exc}") from exc
    if data is None:
        data = {}
    if not isinstance(data, dict):
        raise WFTInventoryError(f"{path} must contain a mapping at top level")
    line_index = _index_lines(path)
    return data, line_index


def _index_lines(path: Path) -> dict:
    loader = LineLoader(path.read_text(encoding="utf-8"))
    index: dict = {}
    try:
        node = loader.get_single_node()
        if node is None:
            return index

        def walk(node: Any, keypath: tuple):
            if node.start_mark is not None:
                index[keypath] = node.start_mark.line + 1
            if isinstance(node, yaml.MappingNode):
                for key_node, value_node in node.value:
                    if isinstance(key_node, yaml.ScalarNode):
                        walk(value_node, keypath + (key_node.value,))
            elif isinstance(node, yaml.SequenceNode):
                for i, child in enumerate(node.value):
                    walk(child, keypath + (i,))

        walk(node, ())
    finally:
        loader.dispose()
    return index


def line_for(line_index: dict, *keys) -> int | None:
    """Return the line number for a path like ("nodes", 2, "node_id"), if known."""
    return line_index.get(tuple(keys))
