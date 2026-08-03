"""``wft script`` — inspect the read-only script registry (AC-005 / AC-008B).

This is a Phase 1 support command for verifying the registry before SSH
execution (Phase 2) is available.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from wft.scriptreg.registry import ScriptRegistry

from .common import EXIT_CONFIG, EXIT_OK, emit_json, envelope


def add_parser(subparsers) -> None:
    parser = subparsers.add_parser("script", help="inspect the read-only script registry")
    sub = parser.add_subparsers(dest="script_action", required=True)
    check = sub.add_parser("check", help="validate scripts.yaml and verify SHA-256 hashes")
    check.add_argument("--file", required=True, help="path to scripts.yaml")
    check.add_argument("--json", action="store_true", help="machine-readable output")
    check.set_defaults(handler=handle_check)
    resolve = sub.add_parser("resolve", help="resolve a script ref to its exact version")
    resolve.add_argument("--file", required=True, help="path to scripts.yaml")
    resolve.add_argument("--ref", required=True, help="script ref (name or name@sha256-prefix)")
    resolve.add_argument("--json", action="store_true", help="machine-readable output")
    resolve.set_defaults(handler=handle_resolve)


def handle_check(args: argparse.Namespace) -> int:
    try:
        registry = ScriptRegistry.from_file(Path(args.file))
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_CONFIG
    scripts = [{"name": s.name, "sha256": s.sha256, "risk": s.risk, "display_ref": s.display_ref}
               for s in registry.scripts]
    if args.json:
        emit_json(envelope("contract-12-script-registry", {
            "file": args.file,
            "valid": True,
            "scripts": scripts,
        }))
    else:
        print(f"OK: {args.file} — {len(scripts)} read_only script(s)")
        for s in scripts:
            print(f"  {s['display_ref']}  risk={s['risk']}")
    return EXIT_OK


def handle_resolve(args: argparse.Namespace) -> int:
    try:
        registry = ScriptRegistry.from_file(Path(args.file))
        script = registry.resolve(args.ref)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_CONFIG
    if args.json:
        emit_json(envelope("contract-12-script-registry", {
            "ref": args.ref,
            "script": {"name": script.name, "sha256": script.sha256, "path": str(script.path),
                       "risk": script.risk, "shell": script.shell, "timeout_sec": script.timeout_sec},
        }))
    else:
        print(f"resolved {args.ref} -> {script.display_ref} at {script.path}")
    return EXIT_OK
