"""``wft hostkey onboard`` — discover and confirm host fingerprints (AC-002)."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from wft.contracts.errors import WFTError, WFTInventoryError
from wft.inventory.loader import load_yaml_with_lines
from wft.inventory.validate import ensure_inventory_valid
from wft.observability.audit import AuditLog
from wft.security import hostkey

from .common import EXIT_CONFIG, EXIT_OK, emit_json, envelope

_HAS_PROMPT = sys.stdin.isatty() and sys.stdout.isatty()


def add_parser(subparsers) -> None:
    parser = subparsers.add_parser("hostkey", help="manage host key fingerprints")
    sub = parser.add_subparsers(dest="hostkey_action", required=True)
    onboard = sub.add_parser("onboard", help="discover and confirm host fingerprints")
    onboard.add_argument("--inventory", required=True, help="path to inventory YAML")
    onboard.add_argument("--node", action="append", default=[], help="only onboard these node_ids (repeatable)")
    onboard.add_argument("--accept", action="append", default=[], help="pre-reviewed fingerprint (node_id:algo:fingerprint), repeatable")
    onboard.add_argument("--known-hosts", help="known_hosts file to update (default: config or data/known_hosts)")
    onboard.add_argument("--timeout", type=float, default=5.0, help="ssh-keyscan timeout seconds")
    onboard.add_argument("--json", action="store_true", help="machine-readable output")
    onboard.add_argument("--yes", action="store_true", help="accept all discovered fingerprints (explicit, audited)")
    onboard.set_defaults(handler=handle_onboard)


def _load_inventory(path: Path) -> tuple[dict, dict]:
    payload, line_index = load_yaml_with_lines(path)
    ensure_inventory_valid(payload, source=str(path), line_index=line_index)
    return payload, line_index


def _nodes_for(payload: dict, node_filter: list[str]) -> list[dict]:
    nodes = payload["nodes"]
    if node_filter:
        wanted = set(node_filter)
        nodes = [n for n in nodes if n.get("node_id") in wanted]
        missing = wanted - {n.get("node_id") for n in payload["nodes"]}
        if missing:
            raise WFTInventoryError(f"node_id not found in inventory: {', '.join(sorted(missing))}")
    return [n for n in nodes if n.get("enabled", True)]


def handle_onboard(args: argparse.Namespace) -> int:
    known_hosts_path = Path(args.known_hosts) if args.known_hosts else Path("data") / "known_hosts"
    audit = AuditLog(Path("logs") / "audit.jsonl")
    accepted: list[hostkey.DiscoveredKey] = []
    rejected: list[dict] = []

    try:
        payload, _ = _load_inventory(Path(args.inventory))
        nodes = _nodes_for(payload, args.node)
        pre_reviewed = _parse_accept(args.accept)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_CONFIG

    if not nodes:
        print("error: no target nodes to onboard", file=sys.stderr)
        return EXIT_CONFIG

    for node in nodes:
        node_id = node["node_id"]
        host = node["host"]
        port = int(node["port"])
        try:
            keys = hostkey.discover(host, port, node_id, timeout_sec=args.timeout)
        except WFTError as exc:
            print(f"warning: {node_id}: {exc}", file=sys.stderr)
            rejected.append({"node_id": node_id, "reason": str(exc)})
            continue
        if not keys:
            print(f"warning: {node_id}: no keys returned for {host}:{port}", file=sys.stderr)
            rejected.append({"node_id": node_id, "reason": "no keys returned"})
            continue

        for key in keys:
            print(key.display())
            confirmed = _decide(key, pre_reviewed, args)
            if confirmed:
                accepted.append(key)
            else:
                rejected.append({"node_id": key.node_id, "reason": "not confirmed"})

    written = hostkey.commit_confirmed(known_hosts_path, accepted)
    for key in accepted:
        audit.append(
            actor=_actor(),
            action="hostkey.onboard.confirm",
            subject=key.node_id,
            detail={"host": key.host, "port": key.port, "algorithm": key.algorithm,
                    "fingerprint": key.fingerprint, "known_hosts": str(known_hosts_path)},
        )
    for rej in rejected:
        audit.append(
            actor=_actor(),
            action="hostkey.onboard.reject",
            subject=rej.get("node_id", ""),
            detail={"reason": rej.get("reason", "")},
        )

    result = {
        "confirmed": [k.node_id for k in accepted],
        "written": written,
        "rejected": rejected,
        "known_hosts": str(known_hosts_path),
    }
    if args.json:
        emit_json(envelope("contract-01-envelope", result, stage="security"))
    else:
        print(f"confirmed {len(accepted)} fingerprint(s); wrote {len(written)} to {known_hosts_path}")
        for rej in rejected:
            print(f"  not written: {rej['node_id']} ({rej['reason']})")
    return EXIT_OK


def _actor() -> str:
    import getpass

    return getpass.getuser()


def _parse_accept(accept: list[str]) -> dict[tuple[str, str], str]:
    """Return {(node_id, algorithm): fingerprint} from pre-reviewed accepts."""
    parsed: dict[tuple[str, str], str] = {}
    for item in accept:
        parts = item.split(":")
        if len(parts) < 3:
            raise WFTInventoryError(f"--accept entry {item!r} must be node_id:algorithm:fingerprint")
        node_id, algo = parts[0], parts[1]
        fingerprint = ":".join(parts[2:])
        parsed[(node_id, algo)] = fingerprint
    return parsed


def _decide(key: hostkey.DiscoveredKey, pre_reviewed: dict, args: argparse.Namespace) -> bool:
    reviewed = pre_reviewed.get((key.node_id, key.algorithm))
    if reviewed is not None:
        return reviewed == key.fingerprint
    if args.yes:
        return True
    if not _HAS_PROMPT:
        print(
            f"  [{key.node_id}] not in --accept and no TTY; refusing to auto-accept",
            file=sys.stderr,
        )
        return False
    answer = input(f"  confirm {key.algorithm} fingerprint for {key.node_id}? [y/N] ").strip().lower()
    return answer in ("y", "yes")
