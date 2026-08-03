"""Persistence facade over SQLite.

Atomicity boundaries (Phase 2 binding): the business object, the node
checkpoint, a real outbox row and the run event for a node result are written
in a single DB transaction -- never a bare event-id list. ``synchronous=FULL``
holds for every write (kill -9 gate), and blob writes land before their DB
references are committed so no dangling DB reference can exist.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from wft.contracts.errors import (
    WFTError,
    WFTIdempotencyConflict,
    WFTLeaseLostError,
    WFTStorageError,
)
from wft.contracts.validate import validate_contract

from .blobs import BlobStore
from .db import Database
from .schema import SCHEMA_VERSION


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _j(obj: object) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=True, separators=(",", ":"))


def _param_json(spec_or_json: str | dict) -> str:
    """Serialize the idempotency identity of a RunSpec payload.

    The generated ``run_id`` and the timing metadata ``trigger.requested_at`` are
    not parameters: a re-submission minting a fresh run_id at a later time still
    matches when every execution-affecting field is identical (AC-011).
    """
    spec = json.loads(spec_or_json) if isinstance(spec_or_json, str) else spec_or_json
    trigger = spec.get("trigger")
    if isinstance(trigger, dict):
        spec = {
            **spec,
            "trigger": {k: v for k, v in trigger.items() if k != "requested_at"},
        }
    return _j({k: v for k, v in spec.items() if k != "run_id"})


class Store:
    def __init__(self, database: Database, blob_dir: str | Path | None = None):
        self.database = database
        blob_dir = blob_dir or (Path(database.path).parent / "blobs")
        self.blobs = BlobStore(blob_dir)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self.database.connect_migrated()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")
        finally:
            conn.close()

    # --------------------------------------------------------------- runs

    def create_run(
        self,
        run_spec: dict,
        *,
        audit_event: dict | None = None,
        node_ids: list[str] | None = None,
    ) -> tuple[str, bool]:
        """Persist a validated Contract-02 payload; enforce idempotency.

        Returns ``(run_id, created)``. ``created=False`` means an identical
        Run (same idempotency_key and same parameters) already exists and is
        returned (no audit event is written for a reuse). Same key with
        different parameters raises :class:`WFTIdempotencyConflict`. When a new
        Run is created, ``audit_event`` (a Contract-09 envelope) and the
        initial ``node_ids`` task rows are written in the SAME transaction as
        the Run row, so the run_created audit and its targeted node checkpoints
        can never land without the Run (or vice versa).
        """
        run_id = run_spec["run_id"]
        trigger = run_spec.get("trigger") or {}
        idem = trigger.get("idempotency_key")
        spec_json = _j(run_spec)
        now = now_iso()
        with self.transaction() as conn:
            if idem:
                existing = conn.execute(
                    "SELECT run_id, run_spec_json FROM runs WHERE idempotency_key=?",
                    (idem,),
                ).fetchone()
                if existing is not None:
                    # The parameter identity excludes the generated run_id, so a
                    # re-submission minting a fresh run_id still matches (AC-011).
                    if _param_json(existing["run_spec_json"]) == _param_json(spec_json):
                        return existing["run_id"], False
                    raise WFTIdempotencyConflict(
                        f"idempotency_key {idem!r} already used with different parameters"
                    )
            conn.execute(
                "INSERT INTO runs (run_id, run_spec_json, status, batch_status, "
                "heartbeat_at, lease_owner, lease_expires_at, idempotency_key, "
                "created_at, updated_at, started_at, finished_at) "
                "VALUES (?, ?, 'QUEUED', NULL, ?, NULL, NULL, ?, ?, ?, NULL, NULL)",
                (run_id, spec_json, now, idem, now, now),
            )
            if audit_event is not None:
                self._insert_event(conn, run_id, audit_event)
            for node_id in node_ids or ():
                conn.execute(
                    "INSERT OR IGNORE INTO node_tasks "
                    "(run_id, node_id, status, attempt_count, started_at, finished_at) "
                    "VALUES (?, ?, 'PENDING', 0, NULL, NULL)",
                    (run_id, node_id),
                )
            return run_id, True

    def get_run(self, run_id: str) -> dict | None:
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM runs WHERE run_id=?", (run_id,)
            ).fetchone()
            return dict(row) if row else None

    def find_run_by_idempotency_key(self, key: str) -> dict | None:
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM runs WHERE idempotency_key=?", (key,)
            ).fetchone()
            return dict(row) if row else None

    def insert_node_tasks(self, run_id: str, node_ids: list[str]) -> None:
        now = now_iso()
        with self.transaction() as conn:
            for node_id in node_ids:
                conn.execute(
                    "INSERT OR IGNORE INTO node_tasks "
                    "(run_id, node_id, status, attempt_count, started_at, finished_at) "
                    "VALUES (?, ?, 'PENDING', 0, NULL, NULL)",
                    (run_id, node_id),
                )

    def get_node_tasks(self, run_id: str) -> list[dict]:
        with self.transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM node_tasks WHERE run_id=? ORDER BY node_id", (run_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_node_task(self, run_id: str, node_id: str) -> dict | None:
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM node_tasks WHERE run_id=? AND node_id=?",
                (run_id, node_id),
            ).fetchone()
            return dict(row) if row else None

    # ------------------------------------------------------ run lifecycle

    def start_run(
        self, run_id: str, *, lease_owner: str, audit_event: dict | None = None
    ) -> bool:
        """Transition QUEUED -> RUNNING, taking the recovery lease atomically.

        Only a QUEUED run may be started. A live RUNNING run owned by another
        worker must go through ``acquire_resume_lock`` (heartbeat + lease CAS),
        never a plain overwrite; False means the transition did not apply.
        When the transition applies, ``audit_event`` (a Contract-09 envelope) is
        written in the same transaction as the status update, so the run_started
        audit can never land without the state change or vice versa.
        """
        now = now_iso()
        lease_expires = _add_seconds(now, LEASE_SECONDS)
        with self.transaction() as conn:
            cur = conn.execute(
                "UPDATE runs SET status='RUNNING', batch_status=NULL, "
                "heartbeat_at=?, lease_owner=?, lease_expires_at=?, "
                "started_at=COALESCE(started_at, ?), updated_at=? "
                "WHERE run_id=? AND status='QUEUED'",
                (now, lease_owner, lease_expires, now, now, run_id),
            )
            if cur.rowcount == 1 and audit_event is not None:
                self._insert_event(conn, run_id, audit_event)
            return cur.rowcount == 1

    def renew_lease(self, run_id: str, lease_owner: str) -> bool:
        """CAS heartbeat/lease renewal; False means the lease was lost."""
        now = now_iso()
        lease_expires = _add_seconds(now, LEASE_SECONDS)
        with self.transaction() as conn:
            cur = conn.execute(
                "UPDATE runs SET heartbeat_at=?, lease_expires_at=?, updated_at=? "
                "WHERE run_id=? AND lease_owner=? AND lease_expires_at > ?",
                (now, lease_expires, now, run_id, lease_owner, now),
            )
            return cur.rowcount == 1

    def is_resume_eligible(self, run: dict) -> bool:
        """True when a RUNNING run may be resumed (heartbeat stale, lease expired).

        The heartbeat is renewed every ``HEARTBEAT_SECONDS`` but the stale gate
        is the full ``LEASE_SECONDS``: a worker only reclaims a Run whose lease
        has actually lapsed, never one that is merely slow to heartbeat. This is
        a read-only pre-check; the authoritative gate is the ``resume_run`` CAS.
        """
        if run.get("status") != "RUNNING":
            return False
        now = now_iso()
        stale_before = _add_seconds(now, -LEASE_SECONDS)
        heartbeat = run.get("heartbeat_at")
        lease = run.get("lease_expires_at")
        heartbeat_stale = heartbeat is None or heartbeat < stale_before
        lease_expired = lease is None or lease < now
        return heartbeat_stale and lease_expired

    def resume_run(
        self,
        run_id: str,
        *,
        lease_owner: str,
        run_audit_event: dict,
        node_checkpoint_factory,
    ) -> list[dict] | None:
        """Single recovery transaction: stale CAS + new owner + RUNNING->UNKNOWN.

        The stale three-condition CAS (Run=RUNNING, heartbeat stale, lease
        expired) must pass before anything is written; when it fails the whole
        transaction returns None and no state changes (no new owner, no
        ``resume_count`` bump, no events). On success the run gets the new
        ``lease_owner``/heartbeat/expiry, ``resume_count`` is incremented, the
        run-level ``run_audit_event`` (a Contract-09 envelope) lands, every
        original RUNNING checkpoint is flipped to UNKNOWN -- preserving its
        ``execution_uid`` -- and each flip writes its own Contract-09
        ``checkpoint_updated`` event via ``node_checkpoint_factory``, all in the
        SAME transaction. PENDING checkpoints are preserved untouched and
        terminal (SUCCEEDED/FAILED/CANCELLED/SKIPPED) checkpoints are never
        touched. Returns the recovered ``[{node_id, execution_uid}]`` rows, or
        None when the CAS did not apply.
        """
        now = now_iso()
        stale_before = _add_seconds(now, -LEASE_SECONDS)
        lease_expires = _add_seconds(now, LEASE_SECONDS)
        recovered: list[dict] = []
        with self.transaction() as conn:
            cur = conn.execute(
                "UPDATE runs SET heartbeat_at=?, lease_owner=?, lease_expires_at=?, "
                "resume_count=resume_count+1, updated_at=? "
                "WHERE run_id=? AND status='RUNNING' "
                "AND (heartbeat_at IS NULL OR heartbeat_at < ?) "
                "AND (lease_expires_at IS NULL OR lease_expires_at < ?)",
                (now, lease_owner, lease_expires, now, run_id, stale_before, now),
            )
            if cur.rowcount != 1:
                return None
            self._insert_event(conn, run_id, run_audit_event)
            rows = conn.execute(
                "SELECT node_id, execution_uid FROM node_tasks "
                "WHERE run_id=? AND status='RUNNING' ORDER BY node_id",
                (run_id,),
            ).fetchall()
            for row in rows:
                conn.execute(
                    "UPDATE node_tasks SET status='UNKNOWN', updated_at=? "
                    "WHERE run_id=? AND node_id=?",
                    (now, run_id, row["node_id"]),
                )
                event = node_checkpoint_factory(row["node_id"], row["execution_uid"])
                self._insert_event(conn, run_id, event)
                recovered.append(
                    {"node_id": row["node_id"], "execution_uid": row["execution_uid"]}
                )
        return recovered

    def record_node_blocked(
        self,
        run_id: str,
        node_id: str,
        execution_uid: str,
        *,
        event: dict,
        lease_owner: str | None = None,
    ) -> None:
        """Persist a checkpoint_updated audit for an indeterminate resumed node.

        A resumed node whose persisted attempts consumed the Contract-03 cap but
        whose last attempt has no outcome (interrupted) cannot be re-dispatched
        and must not fabricate a Contract-03 result. This writes the audit event
        under the lease fence and leaves the node checkpoint UNKNOWN untouched;
        the Run stays RUNNING for human review.
        """
        with self.transaction() as conn:
            self._assert_lease_owner(conn, run_id, lease_owner)
            self._insert_event(conn, run_id, event)

    def release_lease(self, run_id: str, lease_owner: str) -> None:
        now = now_iso()
        with self.transaction() as conn:
            conn.execute(
                "UPDATE runs SET lease_owner=NULL, lease_expires_at=NULL, updated_at=? "
                "WHERE run_id=? AND lease_owner=?",
                (now, run_id, lease_owner),
            )

    def set_node_task(
        self,
        run_id: str,
        node_id: str,
        status: str,
        *,
        execution_uid: str | None = None,
        error_class: str | None = None,
        event: dict | None = None,
        lease_owner: str | None = None,
    ) -> bool:
        """Transition a node task under CAS; False when the rewrite is illegal.

        Terminal statuses may never be rewritten and RUNNING may only follow a
        writable non-terminal state (PENDING/RUNNING/UNKNOWN), so a finished
        node cannot be silently re-flagged and a resumed node (UNKNOWN, from
        ``resume_run``) can be re-dispatched.
        When ``lease_owner`` is given, the checkpoint is fenced: the update only
        applies while the run is RUNNING, its current lease owner matches and
        the lease is unexpired. A checkpoint attempted by an owner who lost the
        lease (to a resumer, or after expiry) raises :class:`WFTLeaseLostError`
        so the stale owner can never advance a node checkpoint. When the
        transition applies, ``event`` (a Contract-09 envelope) is written in
        the same transaction as the checkpoint update, so a node state change
        and its audit can never land on only one side.
        """
        now = now_iso()
        fence = (
            " AND EXISTS (SELECT 1 FROM runs WHERE run_id=? AND status='RUNNING' "
            "AND lease_owner=? AND lease_expires_at > ?)"
        )
        with self.transaction() as conn:
            if lease_owner is not None:
                self._assert_lease_owner(conn, run_id, lease_owner)
            if status in ("RUNNING",):
                # A resumed node is re-dispatched from UNKNOWN (its checkpoint was
                # flipped by ``resume_run``) and keeps the original execution_uid,
                # so UNKNOWN is writable again; terminal statuses never are.
                cur = conn.execute(
                    "UPDATE node_tasks SET status=?, execution_uid=?, "
                    "started_at=COALESCE(started_at, ?), updated_at=? "
                    "WHERE run_id=? AND node_id=? AND status IN ('PENDING','RUNNING','UNKNOWN')"
                    + (fence if lease_owner is not None else ""),
                    (status, execution_uid, now, now, run_id, node_id)
                    + (() if lease_owner is None else (run_id, lease_owner, now)),
                )
            else:
                # A terminal status may only be set from a writable
                # (PENDING/RUNNING/UNKNOWN) task; a finished node can never be
                # re-flagged, even to the same status with a different
                # execution_uid.
                cur = conn.execute(
                    "UPDATE node_tasks SET status=?, execution_uid=?, error_class=?, "
                    "finished_at=COALESCE(finished_at, ?), updated_at=? "
                    "WHERE run_id=? AND node_id=? AND status IN ('PENDING','RUNNING','UNKNOWN')"
                    + (fence if lease_owner is not None else ""),
                    (status, execution_uid, error_class, now, now, run_id, node_id)
                    + (() if lease_owner is None else (run_id, lease_owner, now)),
                )
            if cur.rowcount == 1 and event is not None:
                self._insert_event(conn, run_id, event)
            return cur.rowcount == 1

    # ------------------------------------------------- attempts (durable)

    def record_attempt(
        self,
        run_id: str,
        node_id: str,
        execution_uid: str,
        *,
        attempt_id: str,
        attempt_seq: int,
        status: str,
        started_at: str,
        finished_at: str | None = None,
        error: dict | None = None,
        lease_owner: str | None = None,
    ) -> None:
        """Durably persist one SSH attempt (start or final) under the lease fence.

        Each attempt is written as it happens: ``status='RUNNING'`` with
        ``finished_at=NULL`` before the SSH call, then updated to its final
        status/error/``finished_at`` after it. A kill -9 therefore leaves a
        durable per-attempt record and a resume continues from
        ``max(attempt_seq)+1``. The upsert keys on ``(execution_uid,
        attempt_id)`` so start and final writes share one row and no attempt_id
        is ever duplicated. ``lease_owner`` fences the write: an owner who lost
        the lease (a resumer took it) raises :class:`WFTLeaseLostError` instead
        of corrupting the resumer's attempt sequence.

        In the SAME fenced transaction the node checkpoint's ``attempt_count``
        is CAS-updated to mirror this attempt: a START write bumps it
        monotonically (``attempt_count < seq``, raising
        :class:`WFTLeaseLostError` when the checkpoint is not RUNNING with this
        ``execution_uid``), an END write is idempotent (``attempt_count <=
        seq``, never decreases). So the checkpoint count is always the highest
        persisted attempt_seq -- a resumed node's cumulative count -- and never
        a per-process reset.
        """
        now = now_iso()
        with self.transaction() as conn:
            self._assert_lease_owner(conn, run_id, lease_owner)
            conn.execute(
                "INSERT INTO attempts (execution_uid, attempt_id, attempt_seq, status, "
                "error_class, error_category, error_message, retryable, started_at, "
                "finished_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(execution_uid, attempt_id) DO UPDATE SET "
                "status=excluded.status, error_class=excluded.error_class, "
                "error_category=excluded.error_category, "
                "error_message=excluded.error_message, retryable=excluded.retryable, "
                "finished_at=excluded.finished_at",
                (
                    execution_uid, attempt_id, attempt_seq, status,
                    (error or {}).get("class"), (error or {}).get("category"),
                    (error or {}).get("message"),
                    int(bool((error or {}).get("retryable"))),
                    started_at, finished_at,
                ),
            )
            if status == "RUNNING":
                cur = conn.execute(
                    "UPDATE node_tasks SET attempt_count=?, updated_at=? "
                    "WHERE run_id=? AND node_id=? AND status='RUNNING' "
                    "AND execution_uid=? AND attempt_count < ?",
                    (attempt_seq, now, run_id, node_id, execution_uid, attempt_seq),
                )
                if cur.rowcount != 1:
                    raise WFTLeaseLostError(
                        f"run {run_id}: node {node_id} checkpoint is not RUNNING "
                        f"with execution_uid {execution_uid}; refusing to record "
                        f"attempt {attempt_seq}"
                    )
            else:
                conn.execute(
                    "UPDATE node_tasks SET attempt_count=?, updated_at=? "
                    "WHERE run_id=? AND node_id=? AND status='RUNNING' "
                    "AND execution_uid=? AND attempt_count <= ?",
                    (attempt_seq, now, run_id, node_id, execution_uid, attempt_seq),
                )

    def get_attempt_max_seq(self, execution_uid: str) -> int:
        """Return the highest persisted ``attempt_seq`` for an execution (0 when none)."""
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(attempt_seq), 0) AS m "
                "FROM attempts WHERE execution_uid=?",
                (execution_uid,),
            ).fetchone()
            return int(row["m"])

    def get_last_attempt(self, execution_uid: str) -> dict | None:
        """Return the highest-seq attempt row for an execution, or None."""
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM attempts WHERE execution_uid=? "
                "ORDER BY attempt_seq DESC LIMIT 1",
                (execution_uid,),
            ).fetchone()
            return dict(row) if row else None

    def get_first_attempt_started(self, execution_uid: str) -> str | None:
        """Return the started_at of the lowest-seq attempt, or None."""
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT started_at FROM attempts WHERE execution_uid=? "
                "ORDER BY attempt_seq ASC LIMIT 1",
                (execution_uid,),
            ).fetchone()
            return row["started_at"] if row else None

    def get_node_finished_data_map(self, run_id: str) -> dict[str, dict]:
        """Map node_id -> the node_finished event data for a run.

        The node_finished event data carries per-node aggregation evidence
        (``secondary_errors``: blob/decode errors that could not fit the single
        Contract-03 error slot). A resume finalize reconstructs already-terminal
        nodes' aggregate contribution from these events without re-running them.
        """
        with self.transaction() as conn:
            rows = conn.execute(
                "SELECT node_id, data_json FROM run_events "
                "WHERE run_id=? AND event_type='node_finished'",
                (run_id,),
            ).fetchall()
        return {r["node_id"]: json.loads(r["data_json"]) for r in rows}

    # --------------------------------------------------- node result commit

    def commit_execution_result(
        self,
        run_id: str,
        node_id: str,
        *,
        result: dict,
        checkpoint_status: str,
        outbox_event: dict,
        node_event: dict,
        attempts: list[dict] | None = None,
        lease_owner: str | None = None,
    ) -> dict:
        """Atomically persist ExecutionResult + final attempt + checkpoint + outbox + event.

        ``result`` is a Contract-03 envelope; ``attempts`` are the per-attempt
        rows for this execution; ``outbox_event`` and ``node_event`` are the
        real outbox row and the Contract-09 event. When ``lease_owner`` is
        given the write is fenced: the whole transaction refuses to apply once
        the run's current lease owner is no longer ``lease_owner`` (a resumer
        took over), raising :class:`WFTLeaseLostError` so an ex-owner can never
        commit execution/checkpoint/outbox/event against a reclaimed run. The
        write is immutable on ``execution_uid`` (AC-011): a replay with
        identical content returns the original ack, a replay with different
        content raises :class:`WFTIdempotencyConflict`.
        """
        payload = result["payload"]
        execution_uid = payload["execution_uid"]
        now = now_iso()
        ack_event_ids: list[str] = []
        with self.transaction() as conn:
            self._assert_lease_owner(conn, run_id, lease_owner)
            existing = conn.execute(
                "SELECT result_json, created_at FROM executions WHERE execution_uid=?",
                (execution_uid,),
            ).fetchone()
            if existing is not None:
                # AC-011 replay: identical content returns the original ack
                # (reusing the original committed_at so the envelope is
                # byte-identical); different content is a conflict.
                if existing["result_json"] != _j(result):
                    raise WFTIdempotencyConflict(
                        f"execution_uid {execution_uid} already persisted "
                        "with different content"
                    )
                ack_event_ids = self._outbox_event_ids(
                    conn, "execution_result", execution_uid
                )
                return self._persist_ack(
                    "execution_result", execution_uid,
                    existing["created_at"], ack_event_ids,
                )
            conn.execute(
                "INSERT INTO executions "
                "(run_id, node_id, execution_uid, script_sha256, status, attempt_count, "
                "started_at, finished_at, duration_ms, exit_code, stdout_json, "
                "stderr_json, error_json, result_json, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id, node_id, execution_uid, payload["script_sha256"],
                    payload["status"], payload["attempt_count"],
                    payload["started_at"], payload["finished_at"],
                    payload["duration_ms"], payload.get("exit_code"),
                    _j(payload["stdout"]), _j(payload["stderr"]),
                    _j(payload["error"]) if payload.get("error") is not None else None,
                    _j(result), now,
                ),
            )
            for attempt in attempts or ():
                # Attempts are already durably persisted as they happen
                # (``record_attempt``), so a replayed/retried commit must not
                # collide on (execution_uid, attempt_id): OR IGNORE keeps the
                # original per-attempt row and only adds rows not yet persisted.
                conn.execute(
                    "INSERT OR IGNORE INTO attempts "
                    "(execution_uid, attempt_id, attempt_seq, status, error_class, "
                    "error_category, error_message, retryable, started_at, finished_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        execution_uid,
                        attempt["attempt_id"],
                        attempt["attempt_seq"],
                        attempt["status"],
                        attempt.get("error_class"),
                        attempt.get("error_category"),
                        attempt.get("error_message"),
                        int(bool(attempt.get("retryable"))),
                        attempt["started_at"],
                        attempt["finished_at"],
                    ),
                )
            if outbox_event:
                conn.execute(
                    "INSERT INTO outbox (event_id, object_type, object_id, event_type, "
                    "payload_json, status, attempts, created_at) "
                    "VALUES (?, ?, ?, ?, ?, 'pending', 0, ?)",
                    (
                        outbox_event["event_id"], outbox_event["object_type"],
                        outbox_event["object_id"], outbox_event["event_type"],
                        _j(outbox_event["payload"]), now,
                    ),
                )
                ack_event_ids.append(outbox_event["event_id"])
            self._insert_event(conn, run_id, node_event)
            cur = conn.execute(
                "UPDATE node_tasks SET status=?, execution_uid=?, error_class=?, "
                "finished_at=?, updated_at=? WHERE run_id=? AND node_id=? "
                "AND status IN ('PENDING','RUNNING','UNKNOWN')",
                (
                    checkpoint_status, execution_uid,
                    (payload.get("error") or {}).get("class"),
                    now, now, run_id, node_id,
                ),
            )
            if cur.rowcount != 1:
                # A terminal node task may not acquire a new execution; raising
                # inside the transaction rolls back the whole commit (AC-011).
                raise WFTStorageError(
                    f"node {node_id}: cannot commit execution_result "
                    f"{execution_uid} — node task is not in a writable "
                    "(PENDING/RUNNING) state"
                )
        return self._persist_ack("execution_result", execution_uid, now, ack_event_ids)

    def finalize_run(
        self,
        run_id: str,
        *,
        run_status: str,
        batch_status: str,
        summary: dict,
        outbox_event: dict,
        final_event: dict,
        lease_owner: str | None = None,
    ) -> dict:
        """Atomically persist the final BatchSummary + run terminal state.

        The Run UPDATE is the single terminal transition: only when it changes
        exactly one row are the summary/outbox/event written. When
        ``lease_owner`` is given the finalize is fenced: once the run's current
        lease owner is no longer ``lease_owner`` (a resumer took over) the
        whole transaction refuses, raising :class:`WFTLeaseLostError`, so an
        ex-owner can never finalize a reclaimed run. A repeated finalize of an
        already-terminal Run is an idempotent replay (identical summary returns
        the original ack; different content raises conflict).
        """
        now = now_iso()
        ack_event_ids: list[str] = []
        with self.transaction() as conn:
            self._assert_lease_owner(conn, run_id, lease_owner)
            cur = conn.execute(
                "UPDATE runs SET status=?, batch_status=?, finished_at=?, updated_at=? "
                "WHERE run_id=? AND status NOT IN ('SUCCESS','DEGRADED','FAILED','CANCELLED')",
                (run_status, batch_status, now, now, run_id),
            )
            if cur.rowcount != 1:
                # Already terminal: nothing may be rewritten. A replay must
                # reproduce the stored summary (with the original committed_at)
                # or it is a conflict.
                existing = conn.execute(
                    "SELECT summary_json, created_at FROM batch_summaries "
                    "WHERE run_id=?",
                    (run_id,),
                ).fetchone()
                if existing is None:
                    raise WFTStorageError(
                        f"run {run_id} is already terminal but has no stored summary"
                    )
                if existing["summary_json"] != _j(summary["payload"]):
                    raise WFTIdempotencyConflict(
                        f"run {run_id} is already terminal; "
                        "cannot finalize with a different summary"
                    )
                ack_event_ids = self._outbox_event_ids(conn, "batch_summary", run_id)
                return self._persist_ack(
                    "batch_summary", run_id,
                    existing["created_at"], ack_event_ids,
                )
            conn.execute(
                "INSERT INTO batch_summaries "
                "(run_id, summary_revision, summary_json, final, created_at) "
                "VALUES (?, ?, ?, 1, ?)",
                (run_id, summary["payload"]["summary_revision"], _j(summary["payload"]), now),
            )
            if outbox_event:
                conn.execute(
                    "INSERT INTO outbox (event_id, object_type, object_id, event_type, "
                    "payload_json, status, attempts, created_at) "
                    "VALUES (?, ?, ?, ?, ?, 'pending', 0, ?)",
                    (
                        outbox_event["event_id"], outbox_event["object_type"],
                        outbox_event["object_id"], outbox_event["event_type"],
                        _j(outbox_event["payload"]), now,
                    ),
                )
                ack_event_ids.append(outbox_event["event_id"])
            self._insert_event(conn, run_id, final_event)
        return self._persist_ack("batch_summary", run_id, now, ack_event_ids)

    # ------------------------------------------------------------ helpers

    def _assert_lease_owner(
        self, conn: sqlite3.Connection, run_id: str, lease_owner: str | None
    ) -> None:
        """Fence a committing transaction by the run's current active lease.

        Raising inside the caller's ``transaction()`` rolls the whole commit
        back, so once a resumer has taken the lease or the lease has lapsed
        this owner can land neither business state nor its paired event. A
        lease only counts while the run is RUNNING, ``lease_owner`` matches the
        current owner and ``lease_expires_at`` is still in the future.
        ``lease_owner=None`` disables the fence (single-owner paths such as
        tests and direct store use).
        """
        if lease_owner is None:
            return
        row = conn.execute(
            "SELECT status, lease_owner, lease_expires_at FROM runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        now = now_iso()
        expired = (
            row is None
            or row["lease_expires_at"] is None
            or row["lease_expires_at"] <= now
        )
        if (
            row is None
            or row["status"] != "RUNNING"
            or row["lease_owner"] != lease_owner
            or expired
        ):
            raise WFTLeaseLostError(
                f"run {run_id}: lease no longer held/active for {lease_owner!r}; "
                "refusing to commit"
            )

    def _insert_event(self, conn: sqlite3.Connection, run_id: str, event: dict) -> None:
        payload = event["payload"]
        conn.execute(
            "INSERT INTO run_events (run_id, event_id, event_type, severity, "
            "occurred_at, message, node_id, execution_uid, data_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                run_id, payload["event_id"], payload["event_type"],
                payload["severity"], payload["occurred_at"], payload["message"],
                payload.get("node_id"), payload.get("execution_uid"),
                _j(payload.get("data") or {}),
            ),
        )

    def _persist_ack(
        self, object_type: str, object_id: str, committed_at: str, event_ids: list[str]
    ) -> dict:
        """Build and validate a Contract-06 PersistAck envelope (not a bare payload)."""
        envelope = {
            "meta": {
                "schema_name": "contract-06-persist-ack",
                "schema_version": "1.0.0",
                "producer": "wft.storage",
                "created_at": committed_at,
                "stage": "persistence",
            },
            "payload": {
                "object_type": object_type,
                "object_id": object_id,
                "committed_at": committed_at,
                "storage_version": SCHEMA_VERSION,
                "outbox_event_ids": event_ids,
            },
        }
        validate_contract("contract-06-persist-ack", envelope)
        return envelope

    def find_orphan_blobs(self) -> list[str]:
        """Return blob names stored on disk that no execution references.

        A blob write lands before the DB commit (so a commit never references an
        incomplete blob); if that DB commit then fails, the blob legitimately
        remains but is orphaned. This scan makes such orphans identifiable by
        comparing the on-disk blob set against every ``blob_ref`` in the
        executions table.
        """
        referenced: set[str] = set()
        with self.transaction() as conn:
            rows = conn.execute("SELECT stdout_json, stderr_json FROM executions").fetchall()
        for row in rows:
            for col in ("stdout_json", "stderr_json"):
                stream = json.loads(row[col])
                ref = stream.get("blob_ref")
                if ref is not None:
                    referenced.add(ref)
        return [sha for sha in self.blobs.list() if sha not in referenced]

    def get_batch_summary(self, run_id: str) -> dict | None:
        """Return the stored Contract-05 payload for a Run, or None."""
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT summary_json FROM batch_summaries WHERE run_id=?",
                (run_id,),
            ).fetchone()
            return json.loads(row["summary_json"]) if row else None

    def _outbox_event_ids(
        self, conn: sqlite3.Connection, object_type: str, object_id: str
    ) -> list[str]:
        return [
            r["event_id"]
            for r in conn.execute(
                "SELECT event_id FROM outbox WHERE object_type=? AND object_id=?",
                (object_type, object_id),
            ).fetchall()
        ]


HEARTBEAT_SECONDS = 10
LEASE_SECONDS = 60


def _add_seconds(iso: str, seconds: int) -> str:
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.fromtimestamp(dt.timestamp() + seconds, timezone.utc).isoformat()
