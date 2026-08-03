"""``wft inventory check`` — validate a node inventory file (AC-001)."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from wft.inventory.loader import load_yaml_with_lines
from wft.inventory.validate import check_inventory_file

from .common import EXIT_CONFIG, EXIT_OK, emit_json, envelope, print_errors


def add_parser(subparsers) -> None:
    parser = subparsers.add_parser("inventory", help="inspect node inventory files")
    sub = parser.add_subparsers(dest="inventory_action", required=True)
    check = sub.add_parser("check", help="validate an inventory file")
    check.add_argument("--file", required=True, help="path to inventory YAML")
    check.add_argument("--json", action="store_true", help="machine-readable output")
    check.set_defaults(handler=handle_check)


def handle_check(args: argparse.Namespace) -> int:
    path = Path(args.file)
    try:
        payload, line_index = load_yaml_with_lines(path)
    except Exception as exc:  # parse / IO errors
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_CONFIG

    problems = check_inventory_file(path, payload, line_index=line_index)
    if problems:
        print_errors(problems)
        if args.json:
            emit_json(envelope("contract-01-envelope", {
                "file": str(path),
                "valid": False,
                "errors": problems,
            }))
        return EXIT_CONFIG

    count = len(payload.get("nodes", []))
    if args.json:
        emit_json(envelope("contract-01-envelope", {
            "file": str(path),
            "valid": True,
            "node_count": count,
        }))
    else:
        print(f"OK: {path} is valid with {count} node(s)")
    return EXIT_OK
