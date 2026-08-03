"""Storage layer: atomic run/node lifecycle with the Phase 2 durability gate.

The vertical-slice binding is that a node result's business object, node
checkpoint, real outbox row and run event commit in ONE transaction
(``commit_execution_result``), never as a bare event-id list.
"""
from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

import pytest

from wft.contracts.errors import WFTIdempotencyConflict, WFTStorageError
from wft.storage.blobs import BlobStore
from wft.storage.db import Database
from wft.storage.schema import SCHEMA_VERSION, migrate
from wft.storage.store import Store

import wft.storage.schema as schema
from wft.orchestration.events import build_event


@pytest.fixture()
def store(tmp_path: Path) -> Store:
    return Store(Database(tmp_path / "wft.db"), blob_dir=tmp_path / "blobs")


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _run_spec(run_id: str, *, idempotency_key: str | None = None, param: str = "a") -> dict:
    spec = {
        "run_id": run_id,
        "trigger": {
            "type": "manual",
            "actor": "operator",
            "requested_at": "2026-08-03T10:00:00+00:00",
        },
        "inventory_ref": "config/inventory.example.yaml",
        "selector": {"groups": [], "tags": []},
        "script": {"name": "disk-usage", "sha256": _sha("script"), "risk": "read_only"},
        "limits": {
            "global_concurrency": 50,
            "per_node_concurrency": 1,
            "connect_rate_per_sec": 20,
            "connect_timeout_sec": 10,
            "exec_timeout_sec": 30,
        },
        "config_snapshot_hash": _sha("config"),
    }
    if idempotency_key:
        spec["trigger"]["idempotency_key"] = idempotency_key
    spec["trigger"]["param"] = param  # extra field to distinguish variants
    return spec


def _stream(text: str, *, truncated: bool = False, encoding: str = "utf-8") -> dict:
    return {
        "inline": text,
        "bytes": len(text.encode()),
        "truncated": truncated,
        "sha256": _sha(text),
        "encoding": encoding,
    }


def _result(run_id: str, node_id: str, execution_uid: str, *, status: str = "SUCCEEDED") -> dict:
    payload = {
        "execution_uid": execution_uid,
        "node_id": node_id,
        "script_sha256": _sha("script"),
        "status": status,
        "attempt_count": 1,
        "started_at": "2026-08-03T10:00:01+00:00",
        "finished_at": "2026-08-03T10:00:02+00:00",
        "duration_ms": 1000,
        "exit_code": 0,
        "stdout": _stream("ok"),
        "stderr": _stream(""),
        "flags": [],
    }
    if status == "FAILED":
        payload["error"] = {
            "class": "exec_nonzero",
            "category": "PERMANENT",
            "message": "script exited 1",
            "retryable": False,
        }
    return {
        "meta": {
            "schema_name": "contract-03-execution-result",
            "schema_version": "1.1.0",
            "producer": "wft.execution",
            "created_at": "2026-08-03T10:00:02+00:00",
            "run_id": run_id,
        },
        "payload": payload,
    }


# --------------------------------------------------------------- migration


def test_migrate_is_idempotent(tmp_path: Path) -> None:
    conn = Database(tmp_path / "wft.db").connect()
    migrate(conn)
    migrate(conn)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    conn.close()


def test_migrate_from_empty_creates_schema(tmp_path: Path) -> None:
    conn = Database(tmp_path / "wft.db").connect()
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 0
    migrate(conn)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    for table in ("runs", "node_tasks", "executions", "attempts",
                  "run_events", "batch_summaries", "outbox"):
        assert conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone() is not None, table
    conn.close()


def test_migrate_rejects_newer_version(tmp_path: Path) -> None:
    conn = Database(tmp_path / "wft.db").connect()
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION + 1}")
    with pytest.raises(WFTStorageError, match="newer"):
        migrate(conn)
    # Refusal is read-only: the version is untouched.
    assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION + 1
    conn.close()


def test_migrate_steps_forward_from_old_version(tmp_path: Path, monkeypatch) -> None:
    """A database created by an older release upgrades step by step."""
    monkeypatch.setattr(schema, "SCHEMA_VERSION", 3)
    monkeypatch.setattr(schema, "_MIGRATIONS", {
        1: ("CREATE TABLE t1(x INTEGER)",),
        2: ("CREATE TABLE t2(x INTEGER)",),
        3: ("CREATE TABLE t3(x INTEGER)",),
    })
    db = Database(tmp_path / "wft.db")
    conn = db.connect()
    conn.execute("CREATE TABLE t1(x INTEGER)")
    conn.execute("PRAGMA user_version = 1")
    conn.close()

    conn = db.connect()
    migrate(conn)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 3
    for table in ("t2", "t3"):
        assert conn.execute(
            "SELECT name FROM sqlite_master WHERE name=?", (table,)
        ).fetchone() is not None, table
    conn.close()


def test_migrate_mid_step_failure_advances_nothing(tmp_path: Path, monkeypatch) -> None:
    """A step is all-or-nothing: a mid-step failure rolls back its DDL and
    the user_version bump, so neither schema nor version moves forward."""
    monkeypatch.setattr(schema, "_MIGRATIONS", {
        1: (
            "CREATE TABLE t_ok(x INTEGER)",
            "CREATE TABLE t_broken(x INTEGER THIS IS NOT SQL)",
        ),
    })
    conn = Database(tmp_path / "wft.db").connect()
    with pytest.raises(sqlite3.OperationalError):
        migrate(conn)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 0
    # The earlier statement of the same step was rolled back with the failure.
    assert conn.execute("SELECT name FROM sqlite_master WHERE name='t_ok'").fetchone() is None
    conn.close()


def test_migrate_second_step_failure_rolls_back_earlier_steps(tmp_path: Path, monkeypatch) -> None:
    """The whole migrate() is all-or-nothing across steps: when a later step
    fails, an earlier step's already-applied DDL and every version bump are
    rolled back too, so nothing advances past the pre-call version."""
    monkeypatch.setattr(schema, "SCHEMA_VERSION", 2)
    monkeypatch.setattr(schema, "_MIGRATIONS", {
        1: ("CREATE TABLE t1(x INTEGER)",),
        2: ("CREATE TABLE t2(x INTEGER THIS IS NOT SQL)",),
    })
    conn = Database(tmp_path / "wft.db").connect()
    with pytest.raises(sqlite3.OperationalError):
        migrate(conn)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 0
    for table in ("t1", "t2"):
        assert conn.execute(
            "SELECT name FROM sqlite_master WHERE name=?", (table,)
        ).fetchone() is None, table
    conn.close()


def test_migrate_multi_step_success_sets_final_version(tmp_path: Path, monkeypatch) -> None:
    """All pending steps apply in one transaction and land on SCHEMA_VERSION."""
    monkeypatch.setattr(schema, "SCHEMA_VERSION", 3)
    monkeypatch.setattr(schema, "_MIGRATIONS", {
        1: ("CREATE TABLE t1(x INTEGER)",),
        2: ("CREATE TABLE t2(x INTEGER)",),
        3: ("CREATE TABLE t3(x INTEGER)",),
    })
    conn = Database(tmp_path / "wft.db").connect()
    migrate(conn)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 3
    for table in ("t1", "t2", "t3"):
        assert conn.execute(
            "SELECT name FROM sqlite_master WHERE name=?", (table,)
        ).fetchone() is not None, table
    conn.close()


# ----------------------------------------------------------- run creation


def test_create_run_and_get(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    run_id, created = store.create_run(_run_spec(rid))
    assert run_id == rid
    assert created is True
    row = store.get_run(rid)
    assert row["status"] == "QUEUED"
    assert row["idempotency_key"] is None


def test_idempotent_same_params_returns_existing(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    key = "idem-key-123456"
    first, created1 = store.create_run(_run_spec(rid, idempotency_key=key))
    second, created2 = store.create_run(_run_spec(rid, idempotency_key=key))
    assert first == second
    assert created1 is True
    assert created2 is False


def test_idempotent_conflict_on_different_params(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    key = "idem-key-123456"
    store.create_run(_run_spec(rid, idempotency_key=key, param="a"))
    with pytest.raises(WFTIdempotencyConflict):
        store.create_run(_run_spec(rid, idempotency_key=key, param="b"))


def test_find_by_idempotency_key(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    key = "idem-key-123456"
    store.create_run(_run_spec(rid, idempotency_key=key))
    assert store.find_run_by_idempotency_key(key)["run_id"] == rid
    assert store.find_run_by_idempotency_key("missing") is None


# ---------------------------------------------------------- run lifecycle


def test_start_run_and_lease(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    assert store.start_run(rid, lease_owner="worker-1") is True
    row = store.get_run(rid)
    assert row["status"] == "RUNNING"
    assert row["lease_owner"] == "worker-1"
    # Only QUEUED -> RUNNING is allowed; a different owner cannot take the live
    # lease with a plain start (resume must go through acquire_resume_lock).
    assert store.start_run(rid, lease_owner="worker-2") is False
    assert store.get_run(rid)["lease_owner"] == "worker-1"


def test_renew_lease_lost_returns_false(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    store.start_run(rid, lease_owner="worker-1")
    assert store.renew_lease(rid, lease_owner="worker-1") is True
    assert store.renew_lease(rid, lease_owner="intruder") is False


def test_acquire_resume_lock_requires_stale_heartbeat(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    store.start_run(rid, lease_owner="worker-1")
    # Heartbeat is fresh; resume must not succeed.
    assert store.acquire_resume_lock(rid, lease_owner="worker-2") is False


def test_acquire_resume_lock_stale_gate_is_lease_seconds(store: Store) -> None:
    """Heartbeat renews every 10s but the stale gate is the full 60s lease."""
    from datetime import datetime, timedelta, timezone

    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    store.start_run(rid, lease_owner="worker-1")
    now = datetime.now(timezone.utc)

    def _age(seconds_ago: int) -> None:
        past = (now - timedelta(seconds=seconds_ago)).isoformat()
        conn = store.database.connect()
        conn.execute(
            "UPDATE runs SET heartbeat_at=?, lease_expires_at=? WHERE run_id=?",
            (past, past, rid),
        )
        conn.commit()
        conn.close()

    # 30s-old heartbeat is within the 60s gate: still owned by worker-1.
    _age(30)
    assert store.acquire_resume_lock(rid, lease_owner="worker-2") is False
    # 90s-old heartbeat + lapsed lease: resumable via the CAS.
    _age(90)
    assert store.acquire_resume_lock(rid, lease_owner="worker-2") is True
    assert store.get_run(rid)["lease_owner"] == "worker-2"


# ----------------------------------------------------- audit (same transaction)


def _event_types(store: Store, run_id: str) -> list[str]:
    with store.database.connect() as conn:
        rows = conn.execute(
            "SELECT event_type FROM run_events WHERE run_id=? ORDER BY occurred_at",
            (run_id,),
        ).fetchall()
        return [dict(r)["event_type"] for r in rows]


def test_create_run_audit_persisted_in_same_tx(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid), audit_event=build_event(rid, "run_created", "run created"))
    assert _event_types(store, rid) == ["run_created"]


def test_start_run_audit_persisted_in_same_tx(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    assert store.start_run(
        rid, lease_owner="worker-1",
        audit_event=build_event(rid, "run_started", "run started"),
    ) is True
    assert _event_types(store, rid) == ["run_started"]


def test_resume_audit_persisted_in_same_tx(tmp_path: Path) -> None:
    from datetime import datetime, timedelta, timezone

    store = Store(Database(tmp_path / "wft.db"), blob_dir=tmp_path / "blobs")
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    store.start_run(rid, lease_owner="worker-1")
    past = (datetime.now(timezone.utc) - timedelta(seconds=90)).isoformat()
    conn = store.database.connect()
    conn.execute(
        "UPDATE runs SET heartbeat_at=?, lease_expires_at=? WHERE run_id=?",
        (past, past, rid),
    )
    conn.commit()
    conn.close()
    assert store.acquire_resume_lock(
        rid, lease_owner="worker-2",
        audit_event=build_event(rid, "checkpoint_updated", "run resumed by worker-2"),
    ) is True
    assert _event_types(store, rid) == ["checkpoint_updated"]


def test_start_run_audit_failure_rolls_back_state(tmp_path: Path, monkeypatch) -> None:
    """Fault injection: an audit-event write failing rolls back the status
    update, so a run can never be RUNNING without its audit (or vice versa)."""
    store = Store(Database(tmp_path / "wft.db"), blob_dir=tmp_path / "blobs")
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))

    def _boom(conn, run_id, event):
        raise WFTStorageError("event write failed")

    monkeypatch.setattr(store, "_insert_event", _boom)
    with pytest.raises(WFTStorageError, match="event write failed"):
        store.start_run(
            rid, lease_owner="worker-1",
            audit_event=build_event(rid, "run_started", "run started"),
        )
    # The QUEUED->RUNNING update was rolled back with the event write.
    assert store.get_run(rid)["status"] == "QUEUED"
    assert _event_types(store, rid) == []


def test_create_run_audit_failure_rolls_back_run(tmp_path: Path, monkeypatch) -> None:
    store = Store(Database(tmp_path / "wft.db"), blob_dir=tmp_path / "blobs")

    def _boom(conn, run_id, event):
        raise WFTStorageError("event write failed")

    monkeypatch.setattr(store, "_insert_event", _boom)
    with pytest.raises(WFTStorageError, match="event write failed"):
        store.create_run(
            _run_spec("01HX0" + "A" * 21),
            audit_event=build_event("01HX0" + "A" * 21, "run_created", "run created"),
        )
    with store.database.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM run_events").fetchone()[0] == 0


def test_start_run_state_failure_writes_no_audit(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    # First transition applies and audits.
    assert store.start_run(
        rid, lease_owner="worker-1",
        audit_event=build_event(rid, "run_started", "run started"),
    ) is True
    # The second cannot apply (RUNNING, not QUEUED) and must not audit.
    assert store.start_run(
        rid, lease_owner="worker-2",
        audit_event=build_event(rid, "run_started", "run started"),
    ) is False
    assert _event_types(store, rid) == ["run_started"]


# ------------------------------------------- create_run + node_tasks + audit


def test_create_run_with_nodes_same_tx_consistent(store: Store) -> None:
    """run + initial node_tasks + run_created land in one transaction."""
    rid = "01HX0" + "A" * 21
    run_id, created = store.create_run(
        _run_spec(rid),
        audit_event=build_event(
            rid, "run_created", "run created", data={"targeted": 2}
        ),
        node_ids=["node-a", "node-b"],
    )
    assert run_id == rid
    assert created is True
    assert _event_types(store, rid) == ["run_created"]
    assert [t["status"] for t in store.get_node_tasks(rid)] == ["PENDING", "PENDING"]


def test_create_run_with_nodes_audit_failure_rolls_back_all(tmp_path: Path, monkeypatch) -> None:
    """Fault injection: an audit-event write failing rolls back the Run row and
    its node task rows, so no targeted=N run can exist without checkpoints."""
    store = Store(Database(tmp_path / "wft.db"), blob_dir=tmp_path / "blobs")

    def _boom(conn, run_id, event):
        raise WFTStorageError("event write failed")

    monkeypatch.setattr(store, "_insert_event", _boom)
    with pytest.raises(WFTStorageError, match="event write failed"):
        store.create_run(
            _run_spec("01HX0" + "A" * 21),
            audit_event=build_event(
                "01HX0" + "A" * 21, "run_created", "run created", data={"targeted": 1}
            ),
            node_ids=["node-a"],
        )
    with store.database.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM node_tasks").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM run_events").fetchone()[0] == 0


def test_create_run_with_nodes_state_failure_rolls_back_all(tmp_path: Path) -> None:
    """Fault injection: a node-task write failing rolls back the Run row and
    the run_created event, so checkpoints can never trail a run's audit."""
    store = Store(Database(tmp_path / "wft.db"), blob_dir=tmp_path / "blobs")
    # A trigger aborting any node_tasks INSERT simulates the write failing at
    # the SQL layer (sqlite3.Connection is immutable, so the class can't be
    # monkeypatched); the whole create_run transaction must roll back.
    with store.database.connect_migrated() as conn:
        conn.execute(
            "CREATE TRIGGER boom_node_tasks BEFORE INSERT ON node_tasks "
            "BEGIN SELECT RAISE(ABORT, 'node task write failed'); END"
        )
    with pytest.raises(sqlite3.DatabaseError, match="node task write failed"):
        store.create_run(
            _run_spec("01HX0" + "A" * 21),
            audit_event=build_event(
                "01HX0" + "A" * 21, "run_created", "run created", data={"targeted": 1}
            ),
            node_ids=["node-a"],
        )
    with store.database.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM node_tasks").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM run_events").fetchone()[0] == 0


# ------------------------------------------- checkpoint->RUNNING + node_started


def test_set_node_task_running_event_same_tx(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    store.insert_node_tasks(rid, ["node-a"])
    assert store.set_node_task(
        rid, "node-a", "RUNNING",
        event=build_event(rid, "node_started", "node started", node_id="node-a"),
    ) is True
    assert store.get_node_task(rid, "node-a")["status"] == "RUNNING"
    assert _event_types(store, rid) == ["node_started"]


def test_set_node_task_running_event_failure_rolls_back_checkpoint(tmp_path: Path, monkeypatch) -> None:
    """Fault injection: an audit-event write failing rolls back the PENDING ->
    RUNNING checkpoint, so a node can never be RUNNING without node_started."""
    store = Store(Database(tmp_path / "wft.db"), blob_dir=tmp_path / "blobs")
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    store.insert_node_tasks(rid, ["node-a"])

    def _boom(conn, run_id, event):
        raise WFTStorageError("event write failed")

    monkeypatch.setattr(store, "_insert_event", _boom)
    with pytest.raises(WFTStorageError, match="event write failed"):
        store.set_node_task(
            rid, "node-a", "RUNNING",
            event=build_event(rid, "node_started", "node started", node_id="node-a"),
        )
    assert store.get_node_task(rid, "node-a")["status"] == "PENDING"
    assert _event_types(store, rid) == []


def test_set_node_task_state_failure_writes_no_event(store: Store) -> None:
    """The rowcount guard: a checkpoint that cannot apply must not audit."""
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    store.insert_node_tasks(rid, ["node-a"])
    assert store.set_node_task(rid, "node-a", "RUNNING") is True
    assert store.set_node_task(rid, "node-a", "SUCCEEDED", execution_uid="u1") is True
    # A terminal checkpoint cannot be rewritten; the node_started event must not land.
    assert store.set_node_task(
        rid, "node-a", "RUNNING",
        event=build_event(rid, "node_started", "node started", node_id="node-a"),
    ) is False
    assert _event_types(store, rid) == []


# ------------------------------------------------------- node result commit


def test_commit_execution_result_atomic(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    store.insert_node_tasks(rid, ["node-a"])
    execution_uid = "0190a2b3-c4d5-46e7-8890-1234567890ab"
    ack = store.commit_execution_result(
        rid,
        "node-a",
        result=_result(rid, "node-a", execution_uid),
        checkpoint_status="SUCCEEDED",
        outbox_event={
            "event_id": "0190a2b3-c4d5-46e7-8890-1234567890ac",
            "object_type": "execution_result",
            "object_id": execution_uid,
            "event_type": "execution_result.completed",
            "payload": {"execution_uid": execution_uid},
        },
        node_event={
            "payload": {
                "event_id": "0190a2b3-c4d5-46e7-8890-1234567890ad",
                "event_type": "node_finished",
                "severity": "info",
                "occurred_at": "2026-08-03T10:00:02+00:00",
                "message": "node-a finished SUCCEEDED",
                "node_id": "node-a",
                "execution_uid": execution_uid,
                "data": {},
            }
        },
        attempts=[
            {
                "attempt_id": "0190a2b3-c4d5-46e7-8890-1234567890ae",
                "attempt_seq": 1,
                "status": "SUCCEEDED",
                "error_class": None,
                "error_category": None,
                "error_message": None,
                "retryable": False,
                "started_at": "2026-08-03T10:00:01+00:00",
                "finished_at": "2026-08-03T10:00:02+00:00",
            }
        ],
    )
    assert ack["meta"]["schema_name"] == "contract-06-persist-ack"
    assert ack["payload"]["object_type"] == "execution_result"
    assert ack["payload"]["object_id"] == execution_uid
    assert ack["payload"]["outbox_event_ids"] == ["0190a2b3-c4d5-46e7-8890-1234567890ac"]

    conn = store.database.connect()
    assert conn.execute("SELECT 1 FROM executions WHERE execution_uid=?", (execution_uid,)).fetchone()
    outbox = conn.execute("SELECT status FROM outbox WHERE event_id=?", ("0190a2b3-c4d5-46e7-8890-1234567890ac",)).fetchone()
    assert outbox["status"] == "pending"
    assert conn.execute("SELECT 1 FROM run_events WHERE event_id=?", ("0190a2b3-c4d5-46e7-8890-1234567890ad",)).fetchone()
    assert conn.execute("SELECT 1 FROM attempts WHERE attempt_id=?", ("0190a2b3-c4d5-46e7-8890-1234567890ae",)).fetchone()
    task = conn.execute("SELECT status, execution_uid FROM node_tasks WHERE run_id=? AND node_id=?", (rid, "node-a")).fetchone()
    assert task["status"] == "SUCCEEDED"
    assert task["execution_uid"] == execution_uid
    conn.close()


def test_finalize_run(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    ack = store.finalize_run(
        rid,
        run_status="SUCCESS",
        batch_status="success",
        summary={
            "payload": {
                "summary_revision": 1,
                "run_status": "SUCCESS",
                "batch_status": "success",
                "final": True,
                "counts": {
                    "targeted": 1, "succeeded": 1, "failed": 0,
                    "unknown": 0, "cancelled": 0, "skipped": 0,
                },
                "error_counts": {},
                "started_at": "2026-08-03T10:00:01+00:00",
                "finished_at": "2026-08-03T10:00:02+00:00",
                "duration_ms": 1000,
                "exit_code": 0,
            }
        },
        outbox_event={
            "event_id": "0190a2b3-c4d5-46e7-8890-1234567890ac",
            "object_type": "batch_summary",
            "object_id": rid,
            "event_type": "batch_summary.final",
            "payload": {"run_id": rid},
        },
        final_event={
            "payload": {
                "event_id": "0190a2b3-c4d5-46e7-8890-1234567890ad",
                "event_type": "run_completed",
                "severity": "info",
                "occurred_at": "2026-08-03T10:00:02+00:00",
                "message": "run completed SUCCESS",
                "data": {},
            }
        },
    )
    assert ack["meta"]["schema_name"] == "contract-06-persist-ack"
    assert ack["payload"]["object_type"] == "batch_summary"
    assert ack["payload"]["object_id"] == rid
    row = store.get_run(rid)
    assert row["status"] == "SUCCESS"
    assert row["batch_status"] == "success"
    conn = store.database.connect()
    assert conn.execute("SELECT 1 FROM batch_summaries WHERE run_id=?", (rid,)).fetchone()
    assert conn.execute("SELECT 1 FROM outbox WHERE event_id=?", ("0190a2b3-c4d5-46e7-8890-1234567890ac",)).fetchone()
    conn.close()


def _summary(run_id: str, *, exit_code: int = 0) -> dict:
    return {
        "payload": {
            "summary_revision": 1,
            "run_status": "SUCCESS" if exit_code == 0 else "SUCCESS",
            "batch_status": "success" if exit_code == 0 else "failed",
            "final": True,
            "counts": {
                "targeted": 1, "succeeded": 1 if exit_code == 0 else 0,
                "failed": 0 if exit_code == 0 else 1,
                "unknown": 0, "cancelled": 0, "skipped": 0,
            },
            "error_counts": {},
            "started_at": "2026-08-03T10:00:01+00:00",
            "finished_at": "2026-08-03T10:00:02+00:00",
            "duration_ms": 1000,
            "exit_code": exit_code,
        }
    }


def _outbox_event(object_type: str, object_id: str, event_id: str) -> dict:
    return {
        "event_id": event_id,
        "object_type": object_type,
        "object_id": object_id,
        "event_type": "x.final" if object_type == "batch_summary" else "x.completed",
        "payload": {"object_id": object_id},
    }


def _run_event(event_id: str, *, event_type: str = "run_completed") -> dict:
    return {
        "payload": {
            "event_id": event_id,
            "event_type": event_type,
            "severity": "info",
            "occurred_at": "2026-08-03T10:00:02+00:00",
            "message": "done",
            "data": {},
        }
    }


def test_commit_execution_result_replay_returns_original(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    store.insert_node_tasks(rid, ["node-a"])
    uid = "0190a2b3-c4d5-46e7-8890-1234567890ab"
    args = dict(
        run_id=rid,
        node_id="node-a",
        result=_result(rid, "node-a", uid),
        checkpoint_status="SUCCEEDED",
        outbox_event=_outbox_event("execution_result", uid, "0190a2b3-c4d5-46e7-8890-1234567890ac"),
        node_event=_run_event("0190a2b3-c4d5-46e7-8890-1234567890ad", event_type="node_finished"),
    )
    first = store.commit_execution_result(**args)
    replay = store.commit_execution_result(**args)
    # Byte-identical Contract-06 envelope: committed_at comes from the stored
    # executions.created_at, so the replay is exactly the original ack.
    assert replay == first
    assert replay["payload"]["object_id"] == first["payload"]["object_id"]
    assert replay["payload"]["outbox_event_ids"] == ["0190a2b3-c4d5-46e7-8890-1234567890ac"]
    conn = store.database.connect()
    assert conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 1
    conn.close()


def test_commit_execution_result_conflict_on_different_content(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    store.insert_node_tasks(rid, ["node-a"])
    uid = "0190a2b3-c4d5-46e7-8890-1234567890ab"
    store.commit_execution_result(
        rid,
        "node-a",
        result=_result(rid, "node-a", uid, status="SUCCEEDED"),
        checkpoint_status="SUCCEEDED",
        outbox_event=_outbox_event("execution_result", uid, "0190a2b3-c4d5-46e7-8890-1234567890ac"),
        node_event=_run_event("0190a2b3-c4d5-46e7-8890-1234567890ad", event_type="node_finished"),
    )
    with pytest.raises(WFTIdempotencyConflict):
        store.commit_execution_result(
            rid,
            "node-a",
            result=_result(rid, "node-a", uid, status="FAILED"),
            checkpoint_status="FAILED",
            outbox_event=_outbox_event("execution_result", uid, "0190a2b3-c4d5-46e7-8890-1234567890ac"),
            node_event=_run_event("0190a2b3-c4d5-46e7-8890-1234567890ad", event_type="node_finished"),
        )


def test_finalize_run_terminal_replay_returns_ack(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    args = dict(
        run_id=rid,
        run_status="SUCCESS",
        batch_status="success",
        summary=_summary(rid),
        outbox_event=_outbox_event("batch_summary", rid, "0190a2b3-c4d5-46e7-8890-1234567890ac"),
        final_event=_run_event("0190a2b3-c4d5-46e7-8890-1234567890ad"),
    )
    first = store.finalize_run(**args)
    replay = store.finalize_run(**args)
    # Byte-identical Contract-06 envelope: committed_at comes from the stored
    # batch_summaries.created_at.
    assert replay == first
    assert replay["payload"]["object_id"] == first["payload"]["object_id"]
    assert store.get_run(rid)["status"] == "SUCCESS"
    conn = store.database.connect()
    assert conn.execute("SELECT COUNT(*) FROM batch_summaries").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 1
    conn.close()


def test_finalize_run_terminal_conflict_on_different_summary(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    store.finalize_run(
        run_id=rid,
        run_status="SUCCESS",
        batch_status="success",
        summary=_summary(rid, exit_code=0),
        outbox_event=_outbox_event("batch_summary", rid, "0190a2b3-c4d5-46e7-8890-1234567890ac"),
        final_event=_run_event("0190a2b3-c4d5-46e7-8890-1234567890ad"),
    )
    with pytest.raises(WFTIdempotencyConflict):
        store.finalize_run(
            run_id=rid,
            run_status="SUCCESS",
            batch_status="failed",
            summary=_summary(rid, exit_code=1),
            outbox_event=_outbox_event("batch_summary", rid, "0190a2b3-c4d5-46e7-8890-1234567890ac"),
            final_event=_run_event("0190a2b3-c4d5-46e7-8890-1234567890ad"),
        )


def test_set_node_task_forbids_terminal_rewrite(store: Store) -> None:
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    store.insert_node_tasks(rid, ["node-a"])
    assert store.set_node_task(rid, "node-a", "RUNNING") is True
    assert store.set_node_task(rid, "node-a", "SUCCEEDED", execution_uid="u1") is True
    # Terminal node task cannot be rewritten, not even to the same status
    # with a different execution_uid.
    assert store.set_node_task(rid, "node-a", "FAILED") is False
    assert store.set_node_task(rid, "node-a", "SUCCEEDED", execution_uid="u2") is False
    assert store.get_node_task(rid, "node-a")["status"] == "SUCCEEDED"
    assert store.get_node_task(rid, "node-a")["execution_uid"] == "u1"


def test_commit_execution_result_terminal_node_rolls_back(store: Store) -> None:
    """A terminal node cannot acquire a new execution; the whole commit rolls back."""
    rid = "01HX0" + "A" * 21
    store.create_run(_run_spec(rid))
    store.insert_node_tasks(rid, ["node-a"])
    uid = "0190a2b3-c4d5-46e7-8890-1234567890ab"
    args = dict(
        run_id=rid,
        node_id="node-a",
        result=_result(rid, "node-a", uid, status="SUCCEEDED"),
        checkpoint_status="SUCCEEDED",
        outbox_event=_outbox_event("execution_result", uid, "0190a2b3-c4d5-46e7-8890-1234567890ac"),
        node_event=_run_event("0190a2b3-c4d5-46e7-8890-1234567890ad", event_type="node_finished"),
    )
    store.commit_execution_result(**args)
    # A second execution on the same terminal node is not a replay (different
    # execution_uid) and must roll back everything, not append new rows.
    uid2 = "0190a2b3-c4d5-46e7-8890-1234567890bb"
    with pytest.raises(WFTStorageError):
        store.commit_execution_result(
            rid,
            "node-a",
            result=_result(rid, "node-a", uid2, status="SUCCEEDED"),
            checkpoint_status="SUCCEEDED",
            outbox_event=_outbox_event("execution_result", uid2, "0190a2b3-c4d5-46e7-8890-1234567890bc"),
            node_event=_run_event("0190a2b3-c4d5-46e7-8890-1234567890bd", event_type="node_finished"),
        )
    conn = store.database.connect()
    assert conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM run_events").fetchone()[0] == 1
    conn.close()


# ------------------------------------------------------------------- blobs


def test_blob_write_read_contains(tmp_path: Path) -> None:
    blob = BlobStore(tmp_path / "blobs")
    data = b"hello world" * 1000
    sha = blob.write(data)
    assert len(sha) == 64
    assert blob.contains(sha)
    assert blob.read(sha) == data


def test_blob_write_is_content_addressed(tmp_path: Path) -> None:
    blob = BlobStore(tmp_path / "blobs")
    assert blob.write(b"same bytes") == blob.write(b"same bytes")
    assert len(list((tmp_path / "blobs").iterdir())) == 1


def test_blob_write_atomic_no_partial_on_failure(tmp_path: Path) -> None:
    blob = BlobStore(tmp_path / "blobs")
    sha = blob.write(b"data")
    dest = tmp_path / "blobs" / sha
    # No temp files left behind.
    assert not list(tmp_path.rglob(".blob.*"))
    assert dest.is_file()


def test_blob_lookups_reject_path_escape(tmp_path: Path) -> None:
    blob = BlobStore(tmp_path / "blobs")
    for bad in ("../secret", "..%2Fsecret", "abc", "A" * 64,
                "0" * 63, "0" * 65, "deadbeef/../../x"):
        with pytest.raises(ValueError):
            blob.read(bad)
        with pytest.raises(ValueError):
            blob.contains(bad)


def test_blob_list_excludes_temps_and_junk(tmp_path: Path) -> None:
    blob = BlobStore(tmp_path / "blobs")
    blob.write(b"a")
    blob.write(b"b")
    (tmp_path / "blobs" / ".blob.leftover").write_bytes(b"partial")
    (tmp_path / "blobs" / "not-a-blob").write_bytes(b"junk")
    # A 64-hex-named DIRECTORY is not a completed blob and must not be listed.
    (tmp_path / "blobs" / ("c" * 64)).mkdir()
    names = blob.list()
    assert len(names) == 2
    assert all(len(n) == 64 for n in names)


def _blob_result(run_id: str, node_id: str, execution_uid: str, blob_ref: str, size: int) -> dict:
    res = _result(run_id, node_id, execution_uid)
    res["payload"]["stdout"] = {
        "blob_ref": blob_ref,
        "bytes": size,
        "truncated": False,
        "sha256": blob_ref,
        "encoding": "utf-8",
    }
    return res


def test_find_orphan_blobs_identifies_unreferenced(tmp_path: Path) -> None:
    """A blob written before a DB commit that then lands is referenced; one that
    never gets committed (crash after blob write, before commit) stays
    identifiable as an orphan."""
    store = Store(Database(tmp_path / "wft.db"), blob_dir=tmp_path / "blobs")
    rid = "01HX0" + "A" * 21
    uid = "0190a2b3-c4d5-46e7-8890-1234567890ab"
    store.create_run(_run_spec(rid))
    store.insert_node_tasks(rid, ["node-a"])
    kept = store.blobs.write(b"x" * (300 * 1024))
    orphan = store.blobs.write(b"y" * 1000)  # never referenced by a commit
    store.commit_execution_result(
        rid,
        "node-a",
        result=_blob_result(rid, "node-a", uid, kept, 300 * 1024),
        checkpoint_status="SUCCEEDED",
        outbox_event=_outbox_event("execution_result", uid, "0190a2b3-c4d5-46e7-8890-1234567890ac"),
        node_event=_run_event("0190a2b3-c4d5-46e7-8890-1234567890ad", event_type="node_finished"),
    )
    # The committed blob is complete on disk and not an orphan.
    assert store.blobs.read(kept) == b"x" * (300 * 1024)
    assert store.find_orphan_blobs() == [orphan]
