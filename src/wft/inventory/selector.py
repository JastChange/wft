"""Node selection by group and tag (状态与命令契约_v0.1.md §7.1).

Selection rule: repeatable ``--group`` / ``--tag``; multiple values of the
same kind form a union, different kinds intersect.
"""
from __future__ import annotations

from wft.contracts.errors import WFTUserError


def select_nodes(nodes: list[dict], *, groups: list[str] | None = None, tags: list[str] | None = None) -> list[dict]:
    groups = list(groups or [])
    tags = list(tags or [])
    if not groups and not tags:
        # No selector: every enabled node is targeted.
        return [n for n in nodes if n.get("enabled", True)]

    selected = [n for n in nodes if n.get("enabled", True)]

    def matches_any(node: dict, kind: str, values: list[str]) -> bool:
        if not values:
            return True
        node_values = set(node.get(kind) or [])
        return bool(node_values & set(values))

    if groups:
        selected = [n for n in selected if matches_any(n, "groups", groups)]
    if tags:
        selected = [n for n in selected if matches_any(n, "tags", tags)]
    return selected


def require_targets(nodes: list[dict]) -> None:
    if not nodes:
        raise WFTUserError("target node set is empty; refusing to create a Run (exit 2)")
