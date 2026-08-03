"""``wft run`` — execute a registered read-only script on selected nodes.

Exit codes follow 命令契约 §7: 0 when the Run is SUCCESS, 1 for partial/failed/
degraded/cancelled, 2 for argument/config/idempotency/storage failures.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import sys
from pathlib import Path

from wft.cli.common import EXIT_BUSINESS, EXIT_CONFIG, EXIT_OK, emit_json, envelope
from wft.config.loader import load_config
from wft.contracts.errors import WFTError
from wft.idgen import new_run_id
from wft.inventory.loader import load_yaml_with_lines
from wft.inventory.selector import require_targets, select_nodes
from wft.inventory.validate import ensure_inventory_valid
from wft.scriptreg.registry import ScriptRegistry
from wft.storage.db import Database
from wft.storage.store import Store

from ..orchestration.events import now_iso
from ..orchestration.run import RunOutcome, build_run_spec, create_run, execute_run


def add_parser(subparsers) -> None:
    parser = subparsers.add_parser(
        "run", help="execute a read-only script on selected nodes"
    )
    parser.add_argument("--inventory", required=True, help="path to inventory YAML")
    parser.add_argument(
        "--script", required=True, help="script ref (name or name@<sha256 prefix>)"
    )
    parser.add_argument("--scripts", help="path to the script registry YAML")
    parser.add_argument("--config", help="path to wft.yaml")
    parser.add_argument(
        "--known-hosts", help="path to known_hosts (default from config data_dir)"
    )
    parser.add_argument(
        "--group", action="append", default=[], help="select nodes by group (repeatable)"
    )
    parser.add_argument(
        "--tag", action="append", default=[], help="select nodes by tag (repeatable)"
    )
    parser.add_argument(
        "--idempotency-key",
        help="re-run guard: identical params reuse the original Run (exit 2 on mismatch)",
    )
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    parser.set_defaults(handler=handle_run)


def handle_run(args: argparse.Namespace) -> int:
    try:
        return _run(args)
    except WFTError as exc:
        return _config_error(args, str(exc))
    except Exception as exc:  # orchestration/storage failure => exit 2
        return _config_error(args, str(exc))


def _config_error(args: argparse.Namespace, message: str) -> int:
    print(f"error: {message}", file=sys.stderr)
    if args.json:
        emit_json(
            envelope("contract-01-envelope", {
                "command": "run",
                "ok": False,
                "error": message,
            })
        )
    return EXIT_CONFIG


def _run(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)

    inventory_path = Path(args.inventory)
    payload, line_index = load_yaml_with_lines(inventory_path)
    ensure_inventory_valid(payload, source=str(inventory_path), line_index=line_index)
    selected = select_nodes(payload.get("nodes", []), groups=args.group, tags=args.tag)
    require_targets(selected)

    scripts_path = Path(args.scripts) if args.scripts else (
        cfg.scripts_path if cfg.scripts_path else None
    )
    if scripts_path is None:
        raise WFTError(
            "no script registry configured; pass --scripts or set scripts_path in wft.yaml"
        )
    script = ScriptRegistry.from_file(scripts_path).resolve(args.script)
    if not script.enabled:
        raise WFTError(f"script {script.display_ref!r} is disabled")

    run_id = new_run_id()
    requested_at = now_iso()
    run_spec = build_run_spec(
        run_id=run_id,
        trigger_type="manual",
        actor="cli",
        requested_at=requested_at,
        inventory_ref=str(inventory_path),
        selector={"groups": list(args.group), "tags": list(args.tag)},
        script=script,
        limits=_default_limits(script),
        config_snapshot_hash=_config_snapshot_hash(inventory_path, script.path),
        idempotency_key=args.idempotency_key,
    )

    store = Store(Database(cfg.data_dir / "wft.db"))
    node_ids = [n["node_id"] for n in selected]
    run_id, created = create_run(store, run_spec, node_ids)
    if not created:
        return _reused_run(args, store, run_id)

    known_hosts_path = Path(args.known_hosts) if args.known_hosts else cfg.resolved_known_hosts
    outcome = asyncio.run(
        execute_run(
            store,
            run_id=run_id,
            run_spec=run_spec,
            nodes=selected,
            script=script,
            known_hosts_path=known_hosts_path,
        )
    )
    _report(args, outcome)
    return outcome.exit_code


def _default_limits(script) -> dict:
    return {
        "global_concurrency": 50,
        "per_node_concurrency": 1,
        "connect_rate_per_sec": 20,
        "connect_timeout_sec": 10,
        "exec_timeout_sec": script.timeout_sec,
    }


def _config_snapshot_hash(inventory_path: Path, script_path: Path) -> str:
    h = hashlib.sha256()
    for path in (inventory_path, script_path):
        h.update(path.resolve().read_bytes())
    return h.hexdigest()


def _reused_run(args: argparse.Namespace, store: Store, run_id: str) -> int:
    """AC-011: identical idempotency_key + params returns the original Run."""
    run = store.get_run(run_id) or {}
    status = run.get("status")
    # The stored BatchSummary.exit_code is authoritative (a SUCCESS Run can be a
    # failed batch => exit 1); fall back only when the Run has no summary yet.
    summary = store.get_batch_summary(run_id)
    if summary is not None:
        exit_code = summary["exit_code"]
    else:
        exit_code = EXIT_OK if status == "SUCCESS" else EXIT_BUSINESS
    if args.json:
        emit_json(
            envelope("contract-01-envelope", {
                "command": "run",
                "ok": True,
                "run_id": run_id,
                "reused": True,
                "run_status": status,
                "batch_status": run.get("batch_status"),
            })
        )
    else:
        print(f"run {run_id}: reused existing run (status={status})")
    return exit_code


def _report(args: argparse.Namespace, outcome: RunOutcome) -> None:
    if args.json:
        emit_json(outcome.summary)
    else:
        print(f"run {outcome.run_id}: {outcome.run_status} ({outcome.batch_status})")
        print(
            f"  targeted={outcome.counts['targeted']} "
            f"succeeded={outcome.counts['succeeded']} "
            f"failed={outcome.counts['failed']}"
        )
