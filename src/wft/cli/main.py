"""WFT command-line entry point.

MVP command tree (状态与命令契约_v0.1.md §7). Phase 1 implements
``inventory check``, ``hostkey onboard`` and ``script check|resolve``;
remaining commands are wired as stubs that fail cleanly with exit code 2.
"""
from __future__ import annotations

import argparse
import sys

from wft import __version__

from . import hostkey_cmd, inventory_cmd, script_cmd
from .common import EXIT_CONFIG, EXIT_OK, emit_json, envelope

NOT_IMPLEMENTED = ("run", "history", "scheduler", "storage", "export")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wft",
        description="Batch Linux node inspection and knowledge-sedimentation tool (MVP)",
    )
    parser.add_argument("--version", action="version", version=f"wft {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name in NOT_IMPLEMENTED:
        sp = subparsers.add_parser(name, help=f"({name} arrives in a later phase)")
        sp.add_argument("--json", action="store_true")
        sp.set_defaults(handler=_not_implemented)

    inventory_cmd.add_parser(subparsers)
    hostkey_cmd.add_parser(subparsers)
    script_cmd.add_parser(subparsers)
    return parser


def _not_implemented(args: argparse.Namespace) -> int:
    if args.json:
        emit_json(envelope("contract-01-envelope", {
            "command": args.command,
            "available": False,
            "reason": "not implemented in this phase",
        }))
    else:
        print(f"wft {args.command}: not implemented in this phase (Phase 1 ships "
              "inventory/hostkey/script commands only)", file=sys.stderr)
    return EXIT_CONFIG


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return EXIT_OK
    return int(handler(args))


if __name__ == "__main__":
    sys.exit(main())
