"""Shared CLI helpers: exit codes, JSON output, envelope wrapping."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

EXIT_OK = 0          # command succeeded, Run=SUCCESS and batch_status=success
EXIT_BUSINESS = 1    # command finished but with node failures / degradation / cancel
EXIT_CONFIG = 2      # argument/config error or orchestration failure


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def envelope(schema_name: str, payload: dict, *, run_id=None, stage=None, producer="wft.cli", trace=None) -> dict:
    meta: dict = {
        "schema_name": schema_name,
        "schema_version": "1.0.0",
        "producer": producer,
        "created_at": now_iso(),
    }
    if run_id is not None:
        meta["run_id"] = run_id
    if stage is not None:
        meta["stage"] = stage
    if trace is not None:
        meta["trace"] = trace
    return {"meta": meta, "payload": payload}


def emit_json(obj: dict) -> None:
    json.dump(obj, sys.stdout, ensure_ascii=True, indent=2)
    sys.stdout.write("\n")


def print_errors(errors: list[str]) -> None:
    for err in errors:
        print(f"error: {err}", file=sys.stderr)
