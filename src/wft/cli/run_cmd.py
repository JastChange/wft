"""``wft run`` — execute a registered read-only script on selected nodes.

Exit codes follow 命令契约 §7: 0 when the Run is SUCCESS, 1 for partial/failed/
degraded/cancelled, 2 for argument/config/idempotency/storage failures.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
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
    parser.add_argument(
        "--resume",
        metavar="RUN_ID",
        help="resume a stale RUNNING run (mutually exclusive with new-run args)",
    )
    parser.add_argument("--inventory", help="path to inventory YAML (new-run)")
    parser.add_argument(
        "--script", help="script ref (name or name@<sha256 prefix>) (new-run)"
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
        if args.resume:
            offenders = _new_run_args(args)
            if offenders:
                raise WFTError(
                    "--resume is mutually exclusive with new-run args "
                    f"({', '.join(offenders)})"
                )
            return _resume(args)
        missing = [flag for flag, value in (
            ("--inventory", args.inventory), ("--script", args.script)
        ) if not value]
        if missing:
            raise WFTError(f"missing required args for a new run: {', '.join(missing)}")
        return _run(args)
    except WFTError as exc:
        return _config_error(args, str(exc))
    except Exception as exc:  # orchestration/storage failure => exit 2
        return _config_error(args, str(exc))


def _new_run_args(args: argparse.Namespace) -> list[str]:
    """Args that configure a *new* run and are therefore forbidden with --resume."""
    offenders = []
    if args.inventory:
        offenders.append("--inventory")
    if args.script:
        offenders.append("--script")
    if args.group:
        offenders.append("--group")
    if args.tag:
        offenders.append("--tag")
    if args.idempotency_key:
        offenders.append("--idempotency-key")
    return offenders


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
    _reject_bastion(selected)

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


def _resume(args: argparse.Namespace) -> int:
    """Resume a stale RUNNING run exactly as originally targeted.

    Nothing here creates a run: the original RunSpec (script name + full SHA,
    inventory_ref, limits) and the original node_tasks are the only source of
    targets, so a resume can never mint a new run_id, run_created event or node.
    Every pre-check (run exists / RUNNING / lease stale) and every rebuild step
    (inventory and script unrecoverable) exits 2 before the recovery transaction
    is even attempted, so a refused resume has no state side effects. The
    authoritative stale gate is the ``resume_run`` CAS: a live owner or CAS
    conflict inside ``execute_run`` also exits 2 with no state changed.
    """
    cfg = load_config(args.config)
    store = Store(Database(cfg.data_dir / "wft.db"))
    run_id = args.resume
    run = store.get_run(run_id)
    if run is None:
        raise WFTError(f"run {run_id}: no such run")
    if run["status"] != "RUNNING":
        raise WFTError(
            f"run {run_id}: status is {run['status']}, only RUNNING runs can be resumed"
        )
    if not store.is_resume_eligible(run):
        raise WFTError(
            f"run {run_id}: lease not yet stale (heartbeat must be older than the "
            "lease window and the lease must have expired); a live owner holds it"
        )

    run_spec = json.loads(run["run_spec_json"])
    payload = run_spec["payload"]
    script_name = payload["script"]["name"]
    script_sha = payload["script"]["sha256"]
    if len(script_sha) != 64 or any(c not in "0123456789abcdef" for c in script_sha):
        raise WFTError(f"run {run_id}: corrupt recorded script sha256 {script_sha!r}")

    scripts_path = Path(args.scripts) if args.scripts else (
        cfg.scripts_path if cfg.scripts_path else None
    )
    if scripts_path is None:
        raise WFTError(
            f"run {run_id}: no script registry configured to resolve "
            f"{script_name}@{script_sha[:12]}; pass --scripts or set scripts_path in wft.yaml"
        )
    script = ScriptRegistry.from_file(scripts_path).resolve(f"{script_name}@{script_sha}")
    # Strict match: the resolved version must be byte-identical to the one the
    # original run executed (a same-prefix version is not acceptable on resume).
    if script.sha256 != script_sha:
        raise WFTError(
            f"run {run_id}: resolved script {script.display_ref!r} does not match "
            f"the recorded {script_sha[:12]}"
        )
    if not script.enabled:
        raise WFTError(f"run {run_id}: script {script.display_ref!r} is disabled")

    inventory_path = Path(payload["inventory_ref"])
    inv_payload, line_index = load_yaml_with_lines(inventory_path)
    ensure_inventory_valid(inv_payload, source=str(inventory_path), line_index=line_index)
    by_id = {node["node_id"]: node for node in inv_payload.get("nodes", [])}
    target_ids = [task["node_id"] for task in store.get_node_tasks(run_id)]
    missing = [node_id for node_id in target_ids if node_id not in by_id]
    if missing:
        raise WFTError(
            f"run {run_id}: inventory {inventory_path} no longer defines targeted "
            f"nodes: {', '.join(missing)}"
        )
    selected = [by_id[node_id] for node_id in target_ids]
    _reject_bastion(selected)

    known_hosts_path = Path(args.known_hosts) if args.known_hosts else cfg.resolved_known_hosts
    outcome = asyncio.run(
        execute_run(
            store,
            run_id=run_id,
            run_spec=run_spec,
            nodes=selected,
            script=script,
            known_hosts_path=known_hosts_path,
            resume=True,
        )
    )
    _report(args, outcome)
    return outcome.exit_code


def _reject_bastion(nodes: list[dict]) -> None:
    """Fail fast (exit 2) before Run creation when a target uses a bastion.

    Bastion routing is a declared Phase 2 deviation: silently ignoring the field
    would execute against the wrong path, so any selected bastion node is refused
    rather than ignored (approved deviation: bastion deferred).
    """
    for node in nodes:
        if node.get("bastion"):
            raise WFTError(
                f"node {node['node_id']!r} routes through bastion "
                f"{node['bastion']!r}, which is not yet supported; "
                "refusing to create a Run (exit 2)"
            )


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
    if outcome.lease_lost:
        # The DB Run stays RUNNING for a resumer and this process cannot form
        # an authoritative summary, so report the contract status RUNNING with
        # exit 2 (owner loss / no final trusted result), never an invented
        # status such as INTERRUPTED.
        if args.json:
            emit_json(
                envelope("contract-01-envelope", {
                    "command": "run",
                    "ok": False,
                    "run_id": outcome.run_id,
                    "run_status": "RUNNING",
                    "error": "lease lost; run left RUNNING for resume",
                })
            )
        else:
            print(
                f"run {outcome.run_id}: RUNNING — lease lost; "
                "run left RUNNING for resume"
            )
        return
    if args.json:
        emit_json(outcome.summary)
    else:
        print(f"run {outcome.run_id}: {outcome.run_status} ({outcome.batch_status})")
        print(
            f"  targeted={outcome.counts['targeted']} "
            f"succeeded={outcome.counts['succeeded']} "
            f"failed={outcome.counts['failed']}"
        )
