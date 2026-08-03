"""Batch executor (Gate B): concurrent dispatch + lease heartbeat + stale resume.

Implements the Contract-02 -> dispatch -> Contract-03 commit -> Contract-05
finalize path for the approved asyncssh/SQLite plan: a Run is created (with
idempotency), nodes execute the script concurrently bounded by the Contract-02
``global_concurrency`` semaphore and paced by ``connect_rate_per_sec``, each
ExecutionResult is committed atomically under the run lease, and a final
BatchSummary is persisted. A heartbeat renews the run lease; on loss the run
cancels in-flight nodes and is left RUNNING for a future resumer.

``execute_run(..., resume=True)`` resumes a stale RUNNING run: the single
``resume_run`` recovery transaction claims the lease and flips the leftover
RUNNING checkpoints to UNKNOWN (per-node Contract-09 ``checkpoint_updated``),
only PENDING/UNKNOWN nodes are re-dispatched (terminal nodes never re-run),
``execution_uid`` stays stable from the first PENDING->RUNNING and attempts
continue from ``max(attempt_seq)+1``, and the finalize aggregates every node
(committed terminal + newly dispatched) into the BatchSummary.
"""
from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone

from wft.contracts.errors import WFTError, WFTLeaseLostError
from wft.contracts.validate import validate_contract
from wft.execution.result import build_execution_result, classify_exit
from wft.execution.retry import backoff_seconds, should_retry
from wft.execution.ssh import execute_script
from wft.execution.throttle import Throttle
from wft.idgen import new_uuid7
from wft.scriptreg.registry import Script
from wft.storage.store import Store

from .events import build_event, build_outbox_event, now_iso

HEARTBEAT_INTERVAL_SEC = 10


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
    payload = run_spec["payload"]
    run_id, created = store.create_run(
        payload,
        node_ids=node_ids,
        audit_event=build_event(
            payload["run_id"],
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
    batch_status: str | None
    counts: dict
    error_counts: dict
    exit_code: int
    duration_ms: int
    summary: dict
    lease_lost: bool = False
    blocked: bool = False


@dataclass
class _NodeOutcome:
    node_id: str
    status: str
    degraded: bool
    secondary_errors: tuple[dict, ...]
    error_counts: dict


async def execute_run(
    store: Store,
    *,
    run_id: str,
    run_spec: dict,
    nodes: list[dict],
    script: Script,
    known_hosts_path,
    lease_owner: str | None = None,
    resume: bool = False,
) -> RunOutcome:
    """Run ``script`` on ``nodes`` concurrently and finalize with a Contract-05 summary.

    Nodes are dispatched concurrently bounded by the Contract-02
    ``global_concurrency`` semaphore and paced by ``connect_rate_per_sec``;
    per-node concurrency is 1 (one logical execution per node). A heartbeat
    renews the run lease every ``HEARTBEAT_INTERVAL_SEC``; if it fails the run
    stops dispatching, cancels in-flight nodes and returns ``run_status
    RUNNING`` with exit 2 without finalizing (the DB Run stays RUNNING for a
    resumer; no authoritative summary exists). Every commit is fenced by
    ``lease_owner`` so an owner who lost the lease can never write execution/
    checkpoint/outbox/event against a reclaimed run.

    With ``resume=True`` the run is already RUNNING: the recovery transaction
    (``store.resume_run``) claims a stale lease and flips leftover RUNNING
    checkpoints to UNKNOWN, only PENDING/UNKNOWN nodes are dispatched (terminal
    nodes never re-run), and the finalize aggregates every original node --
    committed terminal and newly dispatched -- into the Contract-05 summary.
    """
    # The lease owner must be unguessable per execution so a separate CLI
    # process cannot renew/resume this run's heartbeat with a shared "cli" tag.
    lease_owner = lease_owner or new_uuid7()
    limits = run_spec["payload"]["limits"]

    if resume:
        # Single recovery transaction: stale 3-condition CAS + new owner + the
        # leftover RUNNING checkpoints -> UNKNOWN with per-node checkpoint_updated
        # events. None means the CAS refused (not RUNNING, not yet stale, or a
        # live owner won the race); no state changed and nothing may proceed.
        recovered = store.resume_run(
            run_id,
            lease_owner=lease_owner,
            run_audit_event=build_event(
                run_id,
                "checkpoint_updated",
                f"run resumed by owner {lease_owner[:8]}",
                severity="warning",
                data={"resume_count_bump": True},
            ),
            node_checkpoint_factory=lambda nid, uid: build_event(
                run_id,
                "checkpoint_updated",
                f"node {nid} checkpoint -> UNKNOWN (resume)",
                severity="warning",
                node_id=nid,
                execution_uid=uid,
                data={"status": "UNKNOWN"},
            ),
        )
        if recovered is None:
            raise WFTError(
                f"run {run_id}: not resumable (must be RUNNING with a heartbeat "
                "and lease stale past the lease window; a live owner or CAS "
                "conflict refuses the resume)"
            )
        run_row = store.get_run(run_id) or {}
        started_at = run_row.get("started_at") or now_iso()
    else:
        started_at = now_iso()
        if not store.start_run(
            run_id,
            lease_owner=lease_owner,
            audit_event=build_event(
                run_id,
                "run_started",
                "run started",
                data={"targeted": len(nodes)},
            ),
        ):
            raise WFTError(
                f"run {run_id}: could not start (expected QUEUED; "
                "a RUNNING lease must go through resume, not a plain overwrite)"
            )

    throttle = Throttle(
        global_concurrency=limits["global_concurrency"],
        connect_rate_per_sec=limits["connect_rate_per_sec"],
    )
    lease_lost = asyncio.Event()
    # A node whose persisted attempts consumed the Contract-03 cap but whose last
    # attempt has no outcome (interrupted by the crash) is "blocked": it stays
    # UNKNOWN and the Run stays RUNNING (exit 2) because neither a further SSH
    # attempt nor a fabricated Contract-03 result is valid.
    blocked = asyncio.Event()
    blocked_nodes: list[str] = []
    node_tasks: list[asyncio.Task] = []
    task_by_id = {t["node_id"]: t for t in store.get_node_tasks(run_id)}
    # Only writable checkpoints are dispatched: PENDING (first execution) and
    # UNKNOWN (recovered by resume). Terminal nodes never re-run.
    dispatch = [
        (node, task_by_id[node["node_id"]])
        for node in nodes
        if task_by_id.get(node["node_id"], {}).get("status") in ("PENDING", "UNKNOWN")
    ]

    async def heartbeat() -> None:
        # A heartbeat must tick even if tests monkeypatch asyncio.sleep into a
        # no-yield coroutine: waiting on a real loop timer suspends the task, so
        # it never busy-loops and always remains cancellable.
        loop = asyncio.get_running_loop()
        wake = asyncio.Event()
        timer = loop.call_later(HEARTBEAT_INTERVAL_SEC, wake.set)
        try:
            while True:
                await wake.wait()
                wake.clear()
                try:
                    ok = store.renew_lease(run_id, lease_owner)
                except Exception:
                    ok = False
                if not ok:
                    # Lease lost: stop dispatching and cancel in-flight nodes.
                    # The run is left RUNNING for a future resume; no finalize.
                    lease_lost.set()
                    for task in node_tasks:
                        task.cancel()
                    return
                timer = loop.call_later(HEARTBEAT_INTERVAL_SEC, wake.set)
        except asyncio.CancelledError:
            pass
        finally:
            timer.cancel()

    async def run_one(node: dict, task: dict) -> _NodeOutcome | None:
        node_id = node["node_id"]
        # execution_uid is stable for the whole logical execution: minted at the
        # first PENDING->RUNNING and persisted on the checkpoint; a resumed
        # (UNKNOWN) node reuses the same uid (crash recovery never invents a new
        # one and never resets attempt_count).
        resumed_node = task["status"] == "UNKNOWN"
        execution_uid = task["execution_uid"] or new_uuid7()
        async with throttle.global_semaphore:
            if lease_lost.is_set():
                return None
            try:
                # The whole checkpoint -> execute -> commit boundary is one
                # fenced critical section: a WFTLeaseLostError from any fenced
                # write (checkpoint, attempt, or commit) closes the run
                # immediately -- set the lease-lost flag, cancel siblings,
                # return no outcome.
                if resumed_node and _resume_disposition(store, execution_uid) == "blocked":
                    # The last persisted attempt consumed the Contract-03 cap but
                    # has no outcome (interrupted by the crash): no SSH attempt
                    # may run and no Contract-03 result may be fabricated. Audit
                    # the reason and leave the checkpoint UNKNOWN; the Run stays
                    # RUNNING (exit 2) for human review.
                    store.record_node_blocked(
                        run_id,
                        node_id,
                        execution_uid,
                        lease_owner=lease_owner,
                        event=build_event(
                            run_id,
                            "checkpoint_updated",
                            f"node {node_id} indeterminate: attempt cap consumed "
                            "by an interrupted attempt with no outcome",
                            severity="warning",
                            node_id=node_id,
                            execution_uid=execution_uid,
                            data={
                                "status": "UNKNOWN",
                                "reason": "attempt_cap_consumed_no_outcome",
                            },
                        ),
                    )
                    blocked_nodes.append(node_id)
                    blocked.set()
                    return None
                if not store.set_node_task(
                    run_id,
                    node_id,
                    "RUNNING",
                    execution_uid=execution_uid,
                    lease_owner=lease_owner,
                    event=build_event(
                        run_id,
                        "node_started",
                        f"node {node_id} started",
                        severity="debug",
                        node_id=node_id,
                        execution_uid=execution_uid,
                    ),
                ):
                    raise WFTError(
                        f"node {node_id}: cannot transition to RUNNING "
                        "(terminal node task cannot be rewritten)"
                    )
                result, degraded_node, secondary_errors, attempts = await _execute_node(
                    store,
                    run_id,
                    node,
                    script,
                    known_hosts_path,
                    limits,
                    execution_uid=execution_uid,
                    connect_limiter=throttle.connect_limiter,
                    lease_owner=lease_owner,
                    resumed=resumed_node,
                )
                if lease_lost.is_set():
                    return None
                payload = result["payload"]
                status = payload["status"]
                # Per-node per-class-once aggregation (错误矩阵_v0.1.md): the
                # primary error counts once, then any secondary blob/decode
                # errors that could not fit the single Contract-03 error slot.
                error_counts: dict[str, int] = {}
                counted: set[str] = set()
                error = payload.get("error")
                if error is not None:
                    error_counts[error["class"]] = 1
                    counted.add(error["class"])
                for sec in secondary_errors:
                    if sec["class"] not in counted:
                        error_counts[sec["class"]] = 1
                        counted.add(sec["class"])
                store.commit_execution_result(
                    run_id,
                    node_id,
                    result=result,
                    checkpoint_status=status,
                    lease_owner=lease_owner,
                    outbox_event=build_outbox_event(
                        object_type="execution_result",
                        object_id=payload["execution_uid"],
                        event_type="execution_result.completed",
                        payload=payload,
                    ),
                    node_event=build_event(
                        run_id,
                        "node_finished",
                        f"node {node_id} {status.lower()}",
                        severity="warning" if status != "SUCCEEDED" else "info",
                        node_id=node_id,
                        execution_uid=payload["execution_uid"],
                        data={
                            "status": status,
                            # Blob/decode errors that could not fit the single
                            # Contract-03 error slot, persisted so per-node
                            # detail is auditable even if the process dies
                            # before the batch summary is committed.
                            "secondary_errors": [dict(e) for e in secondary_errors],
                        },
                    ),
                    attempts=attempts,
                )
            except asyncio.CancelledError:
                raise
            except WFTLeaseLostError:
                # A resumer took the lease during this node's checkpoint/execute/
                # commit window: stop dispatching, cancel in-flight nodes and
                # leave the Run RUNNING for the resumer (nothing committed).
                lease_lost.set()
                for task in node_tasks:
                    if task is not asyncio.current_task():
                        task.cancel()
                return None
            return _NodeOutcome(
                node_id=node_id,
                status=status,
                degraded=degraded_node,
                secondary_errors=secondary_errors,
                error_counts=error_counts,
            )

    for node, task in dispatch:
        node_tasks.append(asyncio.create_task(run_one(node, task)))
    hb_task = asyncio.create_task(heartbeat())
    try:
        results = await asyncio.gather(*node_tasks, return_exceptions=True)
    finally:
        hb_task.cancel()
        with suppress(asyncio.CancelledError):
            await hb_task

    for result in results:
        if isinstance(result, BaseException) and not isinstance(
            result, (asyncio.CancelledError, WFTLeaseLostError)
        ):
            raise result

    finished_at = now_iso()
    duration_ms = _iso_ms_delta(started_at, finished_at)
    completed = [r for r in results if isinstance(r, _NodeOutcome)]
    if resume:
        # The BatchSummary must count every original node: already-terminal
        # nodes (committed before the crash) are reconstructed from their
        # persisted checkpoint + node_finished evidence and summed with the
        # outcomes of the nodes dispatched by this resume.
        finished_data = store.get_node_finished_data_map(run_id)
        terminal = [
            t for t in task_by_id.values()
            if t["status"] in ("SUCCEEDED", "FAILED", "CANCELLED", "SKIPPED")
        ]
        completed += [
            _outcome_from_terminal(t, finished_data.get(t["node_id"], {}))
            for t in terminal
        ]
    # A blocked node has no trustworthy outcome: it counts as UNKNOWN so the
    # reported totals still cover every original node.
    completed += [
        _NodeOutcome(
            node_id=nid, status="UNKNOWN", degraded=False,
            secondary_errors=(), error_counts={},
        )
        for nid in blocked_nodes
    ]
    counts, error_counts, degraded, any_succeeded, any_failed = _aggregate(
        nodes, completed
    )
    if lease_lost.is_set() or blocked.is_set():
        # No authoritative final summary: either the lease was lost (a resumer
        # took over) or a node outcome is indeterminate (blocked). The DB Run
        # stays RUNNING -- report that contract state with exit 2 (owner loss or
        # no final trusted result), never an invented status or a fabricated
        # per-node result.
        return RunOutcome(
            run_id=run_id,
            run_status="RUNNING",
            batch_status=None,
            counts=counts,
            error_counts=error_counts,
            exit_code=2,
            duration_ms=duration_ms,
            summary={},
            lease_lost=lease_lost.is_set(),
            blocked=blocked.is_set(),
        )

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
    try:
        store.finalize_run(
            run_id,
            run_status=run_status,
            batch_status=batch_status,
            summary=summary,
            lease_owner=lease_owner,
            outbox_event=build_outbox_event(
                object_type="batch_summary",
                object_id=run_id,
                event_type="batch_summary.final",
                payload=summary["payload"],
            ),
            final_event=build_event(run_id, final_event_type, final_message),
        )
    except WFTLeaseLostError:
        # A resumer took the lease in the finalize window: abandon, leave the
        # DB Run RUNNING and report that state with exit 2 (no final summary).
        return RunOutcome(
            run_id=run_id,
            run_status="RUNNING",
            batch_status=None,
            counts=counts,
            error_counts=error_counts,
            exit_code=2,
            duration_ms=duration_ms,
            summary={},
            lease_lost=True,
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


def _aggregate(
    nodes: list[dict], completed: list[_NodeOutcome]
) -> tuple[dict, dict, bool, bool, bool]:
    """Order-independent aggregation: per-node outcomes are summed by node_id.

    Node completion order can never change the result -- each node contributes
    exactly one status and its per-class-once error counts.
    """
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
    for outcome in sorted(completed, key=lambda o: o.node_id):
        if outcome.status == "SUCCEEDED":
            counts["succeeded"] += 1
            any_succeeded = True
        elif outcome.status == "FAILED":
            counts["failed"] += 1
            any_failed = True
        elif outcome.status == "CANCELLED":
            counts["cancelled"] += 1
        elif outcome.status == "SKIPPED":
            counts["skipped"] += 1
        else:
            counts["unknown"] += 1
        degraded = degraded or outcome.degraded
        for cls, count in outcome.error_counts.items():
            error_counts[cls] = error_counts.get(cls, 0) + count
    return counts, error_counts, degraded, any_succeeded, any_failed


# Error classes that make a node outcome "degraded" (binary output / blob
# fallback): the batch still ran with trustworthy per-node results.
_DEGRADING_CLASSES = frozenset({"output_decode_failed", "blob_write_failed"})


def _outcome_from_terminal(task: dict, finished_data: dict) -> _NodeOutcome:
    """Reconstruct a terminal node's aggregation evidence for a resume finalize.

    Nodes committed before a crash are never re-dispatched; their contribution
    to the BatchSummary is rebuilt from the persisted checkpoint (primary
    ``error_class``) plus the ``node_finished`` event's ``secondary_errors``
    (per-class-once, matching ``_aggregate``'s order-independent counting).
    """
    primary = task["error_class"]
    secondary = finished_data.get("secondary_errors") or []
    error_counts: dict[str, int] = {}
    counted: set[str] = set()
    if primary:
        error_counts[primary] = 1
        counted.add(primary)
    for sec in secondary:
        cls = sec.get("class")
        if cls and cls not in counted:
            error_counts[cls] = 1
            counted.add(cls)
    degraded = (primary in _DEGRADING_CLASSES) or any(
        s.get("class") in _DEGRADING_CLASSES for s in secondary
    )
    return _NodeOutcome(
        node_id=task["node_id"],
        status=task["status"],
        degraded=degraded,
        secondary_errors=tuple(secondary),
        error_counts=error_counts,
    )


def _iso_ms_delta(start_iso: str, end_iso: str) -> int:
    """Return the UTC wall-clock delta between two ISO timestamps in ms."""
    start = datetime.fromisoformat(start_iso)
    end = datetime.fromisoformat(end_iso)
    return max(0, int((end - start).total_seconds() * 1000))


def _resume_disposition(store: Store, execution_uid: str) -> str:
    """Decide how a resumed node's persisted attempts dispose.

    Returns one of:

    ``"proceed"``
        A fresh attempt at ``max(attempt_seq)+1`` may run.
    ``"reconstruct"``
        The Contract-03 cap or the error matrix says no further SSH attempt is
        allowed: the terminal FAILED result is rebuilt from the last completed
        attempt's persisted error (never a 4th attempt, never a reset to 0).
    ``"blocked"``
        The last attempt has no outcome (interrupted before finish, or finished
        without a persisted error) and the cap is consumed: neither SSH nor a
        fabricated result is valid, so the node stays UNKNOWN and the Run stays
        RUNNING (exit 2) for human review.
    """
    base_seq = store.get_attempt_max_seq(execution_uid)
    if base_seq == 0:
        return "proceed"
    last = store.get_last_attempt(execution_uid)
    if last["status"] != "FAILED" or last["error_class"] is None:
        # Outcome unknown (attempt left RUNNING by a crash) or completed without
        # a persisted error: only re-run when the cap allows a further attempt.
        return "blocked" if base_seq >= 3 else "proceed"
    if base_seq >= 3 or not should_retry(last["error_class"], base_seq):
        # The error matrix's retry budget is consumed across the crash boundary:
        # reconstruct the terminal FAILED from the last attempt, never a further
        # SSH attempt.
        return "reconstruct"
    return "proceed"


async def _execute_node(
    store: Store,
    run_id: str,
    node: dict,
    script: Script,
    known_hosts_path,
    limits: dict,
    *,
    execution_uid: str,
    connect_limiter=None,
    lease_owner: str | None = None,
    resumed: bool = False,
) -> tuple[dict, bool, tuple[dict, ...], list[dict]]:
    """Run one node with matrix-bounded retries across crash boundaries.

    Returns ``(result_envelope, degraded, secondary_errors, attempts)`` where
    ``attempts`` is the per-attempt log for the ``attempts`` table and
    ``secondary_errors`` are blob/decode errors that could not fit the single
    Contract-03 error slot. ``execution_uid`` is fixed by the caller (minted at
    the first PENDING->RUNNING and reused by a resume), so retries and resumed
    executions share one logical execution. The Contract-03 result spans the
    whole logical execution (first attempt start -> final attempt finish),
    while each attempt keeps its own ``started_at``/``finished_at``. Every
    attempt start/end is durably persisted under the ``lease_owner`` fence;
    the attempt sequence continues from ``max(attempt_seq)+1`` so a crash never
    resets ``attempt_count`` to 0 or duplicates an ``attempt_id``. The
    ``_resume_disposition`` of a resumed node's persisted attempts decides the
    path: a completed non-retryable or budget-exhausted attempt is
    reconstructed to a terminal FAILED result (never a further SSH attempt),
    while an interrupted attempt whose outcome is unknown only re-runs when the
    cap allows. ``connect_limiter`` paces every connection attempt (a retry
    reconnects, so each attempt waits).
    """
    node_id = node["node_id"]
    connect_timeout_sec = limits["connect_timeout_sec"]
    exec_timeout_sec = limits["exec_timeout_sec"]
    base_seq = store.get_attempt_max_seq(execution_uid)
    if resumed and _resume_disposition(store, execution_uid) == "reconstruct":
        # The error matrix's retry budget is already consumed across the crash
        # boundary: rebuild the terminal FAILED result from the last completed
        # attempt instead of running a further SSH attempt.
        return _synthesize_exhausted(store, run_id, node, script, execution_uid)
    attempts: list[dict] = []
    local_attempts = 0
    if base_seq > 0:
        # A resumed execution spans the original first attempt start, not the
        # resume time: Contract-03 ``started_at``/``duration_ms`` cover the whole
        # logical run across the crash boundary.
        logical_started_at = store.get_first_attempt_started(execution_uid) or now_iso()
    else:
        logical_started_at = now_iso()

    while True:
        seq = base_seq + local_attempts + 1
        if seq > 3:
            # Unreachable when disposition is "proceed": ``should_retry`` caps
            # the increments and the exhausted case is handled by "reconstruct"
            # above. Guard against a 4th SSH attempt if the invariants ever break.
            raise WFTError(
                f"node {node_id}: attempt_seq {seq} exceeds the Contract-03 cap; "
                "refusing to run a further SSH attempt"
            )
        if connect_limiter is not None:
            await connect_limiter.wait()
        local_attempts += 1
        attempt_id = new_uuid7()
        attempt_started = now_iso()
        store.record_attempt(
            run_id, node_id, execution_uid,
            attempt_id=attempt_id, attempt_seq=seq, status="RUNNING",
            started_at=attempt_started, lease_owner=lease_owner,
        )
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
                _attempt_record(attempt_id, seq, "FAILED", error, attempt_started, attempt_finished)
            )
            store.record_attempt(
                run_id, node_id, execution_uid,
                attempt_id=attempt_id, attempt_seq=seq, status="FAILED",
                started_at=attempt_started, finished_at=attempt_finished,
                error=error, lease_owner=lease_owner,
            )
            if should_retry(error["class"], seq):
                await asyncio.sleep(backoff_seconds(error["class"], seq))
                continue
            status, final_error = "FAILED", error
        else:
            status, final_error = classify_exit(outcome.exit_code, script.expected_exit_codes)
            attempts.append(
                _attempt_record(attempt_id, seq, status, final_error, attempt_started, attempt_finished)
            )
            store.record_attempt(
                run_id, node_id, execution_uid,
                attempt_id=attempt_id, attempt_seq=seq, status=status,
                started_at=attempt_started, finished_at=attempt_finished,
                error=final_error, lease_owner=lease_owner,
            )

        logical_finished_at = now_iso()
        extra_flags: list[str] = []
        if resumed:
            extra_flags.append("resumed")
        if seq > 1:
            # ``retried`` reflects the cumulative attempt_count across the whole
            # logical execution (a resumed node's persisted attempts included),
            # not just the attempts run by this process.
            extra_flags.append("retried")
        result, degraded, secondary_errors = build_execution_result(
            run_id=run_id,
            execution_uid=execution_uid,
            node_id=node_id,
            script=script,
            status=status,
            attempt_count=seq,
            started_at=logical_started_at,
            finished_at=logical_finished_at,
            duration_ms=_iso_ms_delta(logical_started_at, logical_finished_at),
            exit_code=outcome.exit_code,
            stdout_bytes=outcome.stdout,
            stderr_bytes=outcome.stderr,
            stdout_total=outcome.stdout_total,
            stderr_total=outcome.stderr_total,
            stdout_valid_utf8=outcome.stdout_valid_utf8,
            stderr_valid_utf8=outcome.stderr_valid_utf8,
            error=final_error,
            blobs=store.blobs,
            extra_flags=tuple(extra_flags),
        )
        return result, degraded, secondary_errors, attempts


def _synthesize_exhausted(
    store: Store,
    run_id: str,
    node: dict,
    script: Script,
    execution_uid: str,
) -> tuple[dict, bool, tuple[dict, ...], list[dict]]:
    """Reconstruct a terminal FAILED result when persisted attempts used up the cap.

    When a crash/resume boundary has already consumed the Contract-03 budget
    (``attempt_count`` capped at 3) there is no trustworthy SSH output to
    report, only the last persisted attempt's error. The result is FAILED with
    that error, empty streams, and the cumulative attempt_count -- never a 4th
    attempt and never a reset to 0.
    """
    last = store.get_last_attempt(execution_uid)
    error = {
        "class": last["error_class"],
        "category": last["error_category"],
        "message": last["error_message"],
        "retryable": bool(last["retryable"]),
    }
    # The reconstructed result spans the whole logical execution: from the first
    # persisted attempt's start (not the resume time) to the last attempt's
    # finish, so ``duration_ms`` stays truthful across the crash boundary.
    started_at = store.get_first_attempt_started(execution_uid) or last["started_at"]
    finished_at = last["finished_at"] or started_at
    extra_flags = ("resumed", "retried") if last["attempt_seq"] > 1 else ("resumed",)
    result, _, secondary = build_execution_result(
        run_id=run_id,
        execution_uid=execution_uid,
        node_id=node["node_id"],
        script=script,
        status="FAILED",
        attempt_count=last["attempt_seq"],
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=_iso_ms_delta(started_at, finished_at),
        exit_code=None,
        stdout_bytes=b"",
        stderr_bytes=b"",
        error=error,
        blobs=store.blobs,
        extra_flags=extra_flags,
    )
    degraded = last["error_class"] in _DEGRADING_CLASSES
    return result, degraded, secondary, []


def _attempt_record(
    attempt_id: str,
    attempt_seq: int,
    status: str,
    error: dict | None,
    started_at: str,
    finished_at: str,
) -> dict:
    return {
        "attempt_id": attempt_id,
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

    ``batch_status`` aggregates node outcomes independently: success+failed is
    ``partial``, only failed is ``failed``, otherwise ``success``. ``run_status``
    then records whether the batch ran with trustworthy results: non-critical
    degradation (binary output, etc.) is ``DEGRADED``, otherwise ``SUCCESS``.
    ``FAILED``/exit 2 is reserved for a batch with no trustworthy result at all,
    which this function never produces from node outcomes.
    """
    if any_succeeded and any_failed:
        batch_status = "partial"
    elif any_failed:
        batch_status = "failed"
    else:
        batch_status = "success"
    return ("DEGRADED" if degraded else "SUCCESS"), batch_status


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
