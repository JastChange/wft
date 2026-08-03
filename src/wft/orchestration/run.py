"""Single-node vertical-slice batch executor (Gate B).

Implements the Contract-02 -> dispatch -> Contract-03 commit -> Contract-05
finalize path for the approved asyncssh/SQLite plan: a Run is created (with
idempotency), each selected node executes the script with matrix-bounded
retries, the ExecutionResult is committed atomically, and a final BatchSummary
is persisted. Multi-node concurrency, throttle application and stale-run
resume land in later Gate B commits after the slice is reported.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone

from wft.contracts.errors import WFTContractError
from wft.contracts.validate import validate_contract
from wft.execution.result import build_execution_result, classify_exit
from wft.execution.retry import backoff_seconds, should_retry
from wft.execution.ssh import execute_script
from wft.idgen import new_uuid7
from wft.scriptreg.registry import Script
from wft.storage.store import Store

from .events import build_event, now_iso


def build_run_spec(
    *,
    run_id: str,
    trigger_type: str,
    actor: str,
    requested_at: str,
    inventory_ref: str,
    selector: dict,
    script: Script,
    limits: dict,
    config_snapshot_hash: str,
    idempotency_key: str | None = None,
) -> dict:
    """Build and validate a Contract-02 RunSpec envelope."""
    trigger: dict = {
        "type": trigger_type,
        "actor": actor,
        "requested_at": requested_at,
    }
    if idempotency_key is not None:
        trigger["idempotency_key"] = idempotency_key
    envelope = {
        "meta": {
            "schema_name": "contract-02-runspec",
            "schema_version": "1.0.0",
            "producer": "wft.orchestration",
            "created_at": requested_at,
            "run_id": run_id,
            "stage": "trigger",
        },
        "payload": {
            "run_id": run_id,
            "trigger": trigger,
            "inventory_ref": inventory_ref,
            "selector": selector,
            "script": {
                "name": script.name,
                "sha256": script.sha256,
                "risk": script.risk,
            },
            "limits": limits,
            "config_snapshot_hash": config_snapshot_hash,
        },
    }
    validate_contract("contract-02-runspec", envelope)
    return envelope


def create_run(store: Store, run_spec: dict, node_ids: list[str]) -> tuple[str, bool]:
    """Persist the Run, its node tasks and the ``run_created`` event.

    Returns ``(run_id, created)``. ``created=False`` means an identical Run
    (same idempotency_key, same parameters) already exists and is returned;
    nothing is re-inserted for it (AC-011).
    """
    validate_contract("contract-02-runspec", run_spec)
    run_id, created = store.create_run(run_spec["payload"])
    if created:
        store.insert_node_tasks(run_id, node_ids)
        store.insert_run_event(
            run_id,
            build_event(
                run_id,
                "run_created",
                "run created",
                data={"targeted": len(node_ids)},
            ),
        )
    return run_id, created


@dataclass
class RunOutcome:
    run_id: str
    run_status: str
    batch_status: str
    counts: dict
    error_counts: dict
    exit_code: int
    duration_ms: int
    summary: dict


async def execute_run(
    store: Store,
    *,
    run_id: str,
    run_spec: dict,
    nodes: list[dict],
    script: Script,
    known_hosts_path,
    lease_owner: str = "cli",
) -> RunOutcome:
    """Run ``script`` on ``nodes`` and finalize with a Contract-05 summary."""
    limits = run_spec["payload"]["limits"]
    started_at = now_iso()
    loop = asyncio.get_running_loop()
    start = loop.time()

    store.start_run(run_id, lease_owner=lease_owner)
    store.insert_run_event(
        run_id,
        build_event(
            run_id,
            "run_started",
            "run started",
            data={"targeted": len(nodes)},
        ),
    )

    counts = {
        "targeted": len(nodes),
        "succeeded": 0,
        "failed": 0,
        "unknown": 0,
        "cancelled": 0,
        "skipped": 0,
    }
    error_counts: dict[str, int] = {}
    degraded = False
    any_succeeded = False
    any_failed = False

    for node in nodes:
        node_id = node["node_id"]
        store.set_node_task(run_id, node_id, "RUNNING")
        store.insert_run_event(
            run_id,
            build_event(
                run_id,
                "node_started",
                f"node {node_id} started",
                severity="debug",
                node_id=node_id,
            ),
        )
        result, degraded_node, attempts = await _execute_node(
            store,
            run_id,
            node,
            script,
            known_hosts_path,
            limits,
        )
        payload = result["payload"]
        status = payload["status"]
        degraded = degraded or degraded_node
        if status == "SUCCEEDED":
            counts["succeeded"] += 1
            any_succeeded = True
        else:
            counts["failed"] += 1
            any_failed = True
            error_class = (payload.get("error") or {}).get("class", "unknown")
            error_counts[error_class] = error_counts.get(error_class, 0) + 1

        store.commit_execution_result(
            run_id,
            node_id,
            result=result,
            checkpoint_status=status,
            outbox_event=None,
            node_event=build_event(
                run_id,
                "node_finished",
                f"node {node_id} {status.lower()}",
                severity="warning" if status != "SUCCEEDED" else "info",
                node_id=node_id,
                execution_uid=payload["execution_uid"],
                data={"status": status},
            ),
        )
        if attempts:
            store.record_attempts(
                run_id, node_id, payload["execution_uid"], attempts
            )

    finished_at = now_iso()
    duration_ms = int((loop.time() - start) * 1000)
    run_status, batch_status = _batch_status(degraded, any_succeeded, any_failed)
    exit_code = _run_exit_code(run_status, batch_status)

    final_event_type, final_message = {
        "SUCCESS": ("run_completed", "run completed"),
        "DEGRADED": ("run_degraded", "run degraded"),
        "FAILED": ("run_failed", "run failed"),
    }[run_status]
    summary = build_batch_summary(
        run_id=run_id,
        run_status=run_status,
        batch_status=batch_status,
        counts=counts,
        error_counts=error_counts,
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=duration_ms,
        exit_code=exit_code,
    )
    store.finalize_run(
        run_id,
        run_status=run_status,
        batch_status=batch_status,
        summary=summary,
        outbox_event=None,
        final_event=build_event(run_id, final_event_type, final_message),
    )
    return RunOutcome(
        run_id=run_id,
        run_status=run_status,
        batch_status=batch_status,
        counts=counts,
        error_counts=error_counts,
        exit_code=exit_code,
        duration_ms=duration_ms,
        summary=summary,
    )


async def _execute_node(
    store: Store,
    run_id: str,
    node: dict,
    script: Script,
    known_hosts_path,
    limits: dict,
) -> tuple[dict, bool, list[dict]]:
    """Run one node with matrix-bounded retries.

    Returns ``(result_envelope, degraded, attempts)`` where ``attempts`` is the
    per-attempt log for the ``attempts`` table (empty for a single clean run).
    """
    node_id = node["node_id"]
    execution_uid = new_uuid7()
    connect_timeout_sec = limits["connect_timeout_sec"]
    exec_timeout_sec = limits["exec_timeout_sec"]
    attempt_count = 0
    attempts: list[dict] = []

    while True:
        attempt_count += 1
        attempt_started = now_iso()
        outcome = await execute_script(
            node=node,
            script=script,
            known_hosts_path=known_hosts_path,
            connect_timeout_sec=connect_timeout_sec,
            exec_timeout_sec=exec_timeout_sec,
        )
        attempt_finished = now_iso()

        if outcome.error is not None:
            error = outcome.error
            attempts.append(
                _attempt_record(attempt_count, "FAILED", error, attempt_started, attempt_finished)
            )
            if should_retry(error["class"], attempt_count):
                await asyncio.sleep(backoff_seconds(error["class"], attempt_count))
                continue
            status, final_error = "FAILED", error
        else:
            status, final_error = classify_exit(outcome.exit_code, script.expected_exit_codes)
            attempts.append(
                _attempt_record(attempt_count, status, final_error, attempt_started, attempt_finished)
            )

        result, degraded = build_execution_result(
            run_id=run_id,
            execution_uid=execution_uid,
            node_id=node_id,
            script=script,
            status=status,
            attempt_count=attempt_count,
            started_at=attempt_started,
            finished_at=attempt_finished,
            duration_ms=outcome.duration_ms,
            exit_code=outcome.exit_code,
            stdout_bytes=outcome.stdout,
            stderr_bytes=outcome.stderr,
            error=final_error,
            blobs=store.blobs,
            extra_flags=("retried",) if attempt_count > 1 else (),
        )
        return result, degraded, attempts


def _attempt_record(
    attempt_seq: int,
    status: str,
    error: dict | None,
    started_at: str,
    finished_at: str,
) -> dict:
    return {
        "attempt_id": new_uuid7(),
        "attempt_seq": attempt_seq,
        "status": status,
        "error_class": (error or {}).get("class"),
        "error_category": (error or {}).get("category"),
        "error_message": (error or {}).get("message"),
        "retryable": bool((error or {}).get("retryable")),
        "started_at": started_at,
        "finished_at": finished_at,
    }


def _batch_status(degraded: bool, any_succeeded: bool, any_failed: bool) -> tuple[str, str]:
    """Map trustworthy node outcomes to (run_status, batch_status) per 命令契约 §7.

    ``run_status`` records whether the batch completed with trustworthy results.
    Node-level FAILED outcomes (script non-zero, retries exhausted) are still
    trustworthy, so an all-failed batch is ``SUCCESS``/``failed`` and a mixed
    one is ``SUCCESS``/``partial``. Non-critical degradation (binary output,
    etc.) is ``DEGRADED``. ``FAILED``/exit 2 is reserved for a batch with no
    trustworthy result at all.
    """
    if degraded and any_failed:
        return "DEGRADED", "partial"
    if degraded:
        return "DEGRADED", "success"
    if any_failed and any_succeeded:
        return "SUCCESS", "partial"
    if any_failed:
        return "SUCCESS", "failed"
    return "SUCCESS", "success"


def _run_exit_code(run_status: str, batch_status: str) -> int:
    """CLI exit code per 命令契约 §7: 0=success, 1=partial/failed/degraded, 2=no trusted result."""
    if run_status == "SUCCESS" and batch_status == "success":
        return 0
    if run_status == "FAILED":
        return 2
    return 1


def build_batch_summary(
    *,
    run_id: str,
    run_status: str,
    batch_status: str,
    counts: dict,
    error_counts: dict,
    started_at: str,
    finished_at: str,
    duration_ms: int,
    exit_code: int,
) -> dict:
    """Build and validate a final Contract-05 BatchSummary envelope."""
    payload = {
        "run_id": run_id,
        "run_status": run_status,
        "batch_status": batch_status,
        "summary_revision": 1,
        "final": True,
        "counts": counts,
        "error_counts": error_counts,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_ms": duration_ms,
        "exit_code": exit_code,
    }
    envelope = {
        "meta": {
            "schema_name": "contract-05-batch-summary",
            "schema_version": "1.0.0",
            "producer": "wft.orchestration",
            "created_at": finished_at,
            "run_id": run_id,
            "stage": "orchestration",
        },
        "payload": payload,
    }
    validate_contract("contract-05-batch-summary", envelope)
    return envelope
