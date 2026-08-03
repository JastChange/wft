"""Contract-09 RunEvent builder for the orchestration layer.

Every run-lifecycle event (created/started/node_started/node_finished/completed/
degraded/failed) is produced here so callers never hand-assemble envelopes that
could drift from the contract.
"""
from __future__ import annotations

from datetime import datetime, timezone

from wft.contracts.validate import validate_contract
from wft.idgen import new_uuid7


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_event(
    run_id: str,
    event_type: str,
    message: str,
    *,
    severity: str = "info",
    node_id: str | None = None,
    execution_uid: str | None = None,
    data: dict | None = None,
    occurred_at: str | None = None,
) -> dict:
    """Build and validate a Contract-09 RunEvent envelope."""
    occurred_at = occurred_at or now_iso()
    payload: dict = {
        "event_id": new_uuid7(),
        "run_id": run_id,
        "event_type": event_type,
        "severity": severity,
        "occurred_at": occurred_at,
        "message": message,
        "data": data or {},
    }
    if node_id is not None:
        payload["node_id"] = node_id
    if execution_uid is not None:
        payload["execution_uid"] = execution_uid
    envelope = {
        "meta": {
            "schema_name": "contract-09-run-event",
            "schema_version": "1.0.0",
            "producer": "wft.orchestration",
            "created_at": occurred_at,
            "run_id": run_id,
            "stage": "orchestration",
        },
        "payload": payload,
    }
    validate_contract("contract-09-run-event", envelope)
    return envelope
