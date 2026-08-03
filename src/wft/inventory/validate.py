"""Inventory validation: Contract-11 JSON Schema plus semantic checks (AC-001).

Semantic checks layered on top of the schema:
- ``node_id`` must be globally unique;
- every ``auth.credential_ref`` must use a recognised reference scheme and
  never embed a secret body;
- ``bastion`` references must point to an existing node and must not form a
  cycle;
- ``nodes`` must be within the 1..200 contract bound.
"""
from __future__ import annotations

from pathlib import Path

from wft.contracts import registry
from wft.contracts.errors import WFTInventoryError

_CREDENTIAL_SCHEMES = ("env://", "vault://", "op://", "agent://", "file://")


def validate_inventory_payload(payload: dict, *, line_index: dict | None = None) -> list[str]:
    """Return all violations (schema + semantic) with file line hints.

    ``payload`` is the content of the user's inventory YAML (the Contract-11
    payload, i.e. a mapping with a ``nodes`` key).
    """
    problems: list[str] = []
    line_index = line_index or {}

    schema_problems = registry.validate_with_errors("contract-11-inventory", _as_envelope(payload))
    for problem in schema_problems:
        problems.append(f"contract-11: {problem}")

    nodes = payload.get("nodes")
    if not isinstance(nodes, list):
        return problems

    seen_ids: dict[str, int] = {}
    for i, node in enumerate(nodes):
        if not isinstance(node, dict):
            continue
        node_id = node.get("node_id")
        if isinstance(node_id, str):
            if node_id in seen_ids:
                line = line_index.get(("nodes", i, "node_id"))
                problems.append(
                    f"duplicate node_id {node_id!r} at nodes[{i}]"
                    + (f" (line {line})" if line else "")
                    + f"; first seen at nodes[{seen_ids[node_id]}]"
                )
            else:
                seen_ids[node_id] = i

        auth = node.get("auth")
        if isinstance(auth, dict) and isinstance(auth.get("credential_ref"), str):
            ref = auth["credential_ref"]
            if not any(ref.startswith(s) for s in _CREDENTIAL_SCHEMES):
                line = line_index.get(("nodes", i, "auth", "credential_ref"))
                problems.append(
                    f"nodes[{i}] auth.credential_ref {ref!r} is not a recognised "
                    f"reference (allowed schemes: {', '.join(_CREDENTIAL_SCHEMES)})"
                    + (f" at line {line}" if line else "")
                )
            elif _looks_like_secret_body(ref):
                line = line_index.get(("nodes", i, "auth", "credential_ref"))
                problems.append(
                    f"nodes[{i}] auth.credential_ref looks like an inline secret "
                    "body; use a credential reference only"
                    + (f" at line {line}" if line else "")
                )

    _check_bastions(nodes, problems)

    return problems


def _as_envelope(payload: dict) -> dict:
    return {
        "meta": {
            "schema_name": "contract-11-inventory",
            "schema_version": "1.0.0",
            "producer": "wft.inventory",
            "created_at": "1970-01-01T00:00:00+00:00",
        },
        "payload": payload,
    }


_SECRET_BODY_MARKERS = ("-----BEGIN", "ssh-rsa ", "ssh-ed25519 ", "ecdsa-sha2-")
_REFERENCE_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    "_-./:@%~?&="
)


def _looks_like_secret_body(ref: str) -> bool:
    body = ref.split("://", 1)[1] if "://" in ref else ref
    # PEM blocks, inline ssh keys, and whitespace are unambiguous secret bodies.
    if any(m in body for m in _SECRET_BODY_MARKERS):
        return True
    if " " in body or "\n" in body or "\t" in body:
        return True
    # A reference is a path/name (env var, vault path, file path, op ref). Any
    # character outside that set (e.g. a pasted password or base64 blob) is suspect.
    return not body or not all(c in _REFERENCE_CHARS for c in body)


def _check_bastions(nodes: list, problems: list[str]) -> None:
    by_id = {n.get("node_id"): i for i, n in enumerate(nodes) if isinstance(n, dict) and n.get("node_id")}
    for i, node in enumerate(nodes):
        if not isinstance(node, dict):
            continue
        bastion = node.get("bastion")
        if bastion is None:
            continue
        if bastion not in by_id:
            problems.append(
                f"nodes[{i}] bastion {bastion!r} does not reference an existing node"
            )
    # cycle detection
    def find_cycle(start: str) -> list[str]:
        seen: set[str] = set()
        path: list[str] = []
        cur = start
        while cur in by_id and cur not in seen:
            seen.add(cur)
            path.append(cur)
            node = nodes[by_id[cur]]
            nxt = node.get("bastion") if isinstance(node, dict) else None
            if nxt is None:
                return []
            cur = nxt
        if cur in seen:
            idx = path.index(cur)
            return path[idx:] + [cur]
        return []

    for node in nodes:
        if isinstance(node, dict) and node.get("node_id"):
            cycle = find_cycle(node["node_id"])
            if cycle:
                problems.append("bastion chain forms a cycle: " + " -> ".join(cycle))


def check_inventory_file(path: Path, payload: dict | None = None, *, line_index: dict | None = None) -> list[str]:
    """Validate an inventory file, returning violations (empty when valid)."""
    if payload is None:
        from .loader import load_yaml_with_lines

        payload, line_index = load_yaml_with_lines(path)
    problems = validate_inventory_payload(payload, line_index=line_index)
    return [f"{path}: {p}" for p in problems]


def ensure_inventory_valid(payload: dict, *, source: str = "inventory", line_index: dict | None = None) -> None:
    problems = validate_inventory_payload(payload, line_index=line_index)
    if problems:
        raise WFTInventoryError(f"{source}: " + "; ".join(problems[:10]))
