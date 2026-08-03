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

from wft.contracts.errors import WFTError, WFTIdempotencyConflict, WFTStorageError

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

    def create_run(self, run_spec: dict) -> tuple[str, bool]:
        """Persist a validated Contract-02 payload; enforce idempotency.

        Returns ``(run_id, created)``. ``created=False`` means an identical
        Run (same idempotency_key and same parameters) already exists and is
        returned. Same key with different parameters raises
        :class:`WFTIdempotencyConflict`.
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

    def start_run(self, run_id: str, *, lease_owner: str) -> bool:
        """Transition QUEUED -> RUNNING, taking the recovery lease atomically."""
        now = now_iso()
        lease_expires = _add_seconds(now, LEASE_SECONDS)
        with self.transaction() as conn:
            cur = conn.execute(
                "UPDATE runs SET status='RUNNING', batch_status=NULL, "
                "heartbeat_at=?, lease_owner=?, lease_expires_at=?, "
                "started_at=COALESCE(started_at, ?), updated_at=? "
                "WHERE run_id=? AND status IN ('QUEUED','RUNNING')",
                (now, lease_owner, lease_expires, now, now, run_id),
            )
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

    def acquire_resume_lock(self, run_id: str, lease_owner: str) -> bool:
        """Single-CAS resume: Run=RUNNING, heartbeat stale, lease expired."""
        now = now_iso()
        stale_before = _add_seconds(now, -HEARTBEAT_SECONDS)
        lease_expires = _add_seconds(now, LEASE_SECONDS)
        with self.transaction() as conn:
            cur = conn.execute(
                "UPDATE runs SET heartbeat_at=?, lease_owner=?, lease_expires_at=?, "
                "resume_count=resume_count+1, updated_at=? "
                "WHERE run_id=? AND status='RUNNING' "
                "AND (heartbeat_at IS NULL OR heartbeat_at < ?) "
                "AND (lease_expires_at IS NULL OR lease_expires_at < ?)",
                (now, lease_owner, lease_expires, now, run_id, stale_before, now),
            )
            return cur.rowcount == 1

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
    ) -> None:
        now = now_iso()
        with self.transaction() as conn:
            if status in ("RUNNING",):
                conn.execute(
                    "UPDATE node_tasks SET status=?, execution_uid=?, "
                    "started_at=COALESCE(started_at, ?), updated_at=? "
                    "WHERE run_id=? AND node_id=?",
                    (status, execution_uid, now, now, run_id, node_id),
                )
            else:
                conn.execute(
                    "UPDATE node_tasks SET status=?, execution_uid=?, error_class=?, "
                    "finished_at=COALESCE(finished_at, ?), updated_at=? "
                    "WHERE run_id=? AND node_id=?",
                    (status, execution_uid, error_class, now, now, run_id, node_id),
                )

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
    ) -> dict:
        """Atomically persist ExecutionResult + checkpoint + outbox + event.

        ``result`` is a Contract-03 payload; ``outbox_event`` and ``node_event``
        are the real outbox row payload and the Contract-09 payload data. The
        write is idempotent on ``execution_uid`` (AC-011).
        """
        payload = result["payload"]
        execution_uid = payload["execution_uid"]
        now = now_iso()
        ack_event_ids: list[str] = []
        with self.transaction() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO executions "
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
            conn.execute(
                "UPDATE node_tasks SET status=?, execution_uid=?, error_class=?, "
                "finished_at=?, updated_at=? WHERE run_id=? AND node_id=?",
                (
                    checkpoint_status, execution_uid,
                    (payload.get("error") or {}).get("class"),
                    now, now, run_id, node_id,
                ),
            )
        return self._persist_ack("execution_result", execution_uid, now, ack_event_ids)

    def record_attempts(
        self, run_id: str, node_id: str, execution_uid: str, attempts: list[dict]
    ) -> None:
        """Append per-attempt rows for an execution (attempt_id unique per try)."""
        with self.transaction() as conn:
            for attempt in attempts:
                conn.execute(
                    "INSERT OR REPLACE INTO attempts "
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

    def insert_run_event(self, run_id: str, event: dict) -> dict:
        now = now_iso()
        event_id = event["payload"]["event_id"]
        with self.transaction() as conn:
            self._insert_event(conn, run_id, event)
        return self._persist_ack("run_event", event_id, now, [])

    def finalize_run(
        self,
        run_id: str,
        *,
        run_status: str,
        batch_status: str,
        summary: dict,
        outbox_event: dict,
        final_event: dict,
    ) -> dict:
        """Atomically persist the final BatchSummary + run terminal state."""
        now = now_iso()
        ack_event_ids: list[str] = []
        with self.transaction() as conn:
            revision = summary["payload"]["summary_revision"]
            conn.execute(
                "INSERT OR REPLACE INTO batch_summaries "
                "(run_id, summary_revision, summary_json, final, created_at) "
                "VALUES (?, ?, ?, 1, ?)",
                (run_id, revision, _j(summary["payload"]), now),
            )
            conn.execute(
                "UPDATE runs SET status=?, batch_status=?, finished_at=?, updated_at=? "
                "WHERE run_id=? AND status NOT IN ('SUCCESS','DEGRADED','FAILED','CANCELLED')",
                (run_status, batch_status, now, now, run_id),
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
        return {
            "object_type": object_type,
            "object_id": object_id,
            "committed_at": committed_at,
            "storage_version": SCHEMA_VERSION,
            "outbox_event_ids": event_ids,
        }


HEARTBEAT_SECONDS = 10
LEASE_SECONDS = 60


def _add_seconds(iso: str, seconds: int) -> str:
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.fromtimestamp(dt.timestamp() + seconds, timezone.utc).isoformat()
