"""Generate normal/boundary/error sample instances for every contract.

Run once from the repo root:
    python tests/samples/_generate.py

The emitted JSON files are committed and exercised by the contract CI. Error
samples are designed to fail the JSON Schema (not only semantic checks).
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SAMPLES = ROOT / "tests" / "samples"

ULID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
UUID1 = "f47ac10b-58cc-4372-a567-0e02b2c3d479"
UUID2 = "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d"
SHA = "a" * 64
SHA2 = "b" * 64
TS = "2026-08-03T10:00:00+00:00"


def meta(key: str, **kw) -> dict:
    base = {
        "schema_name": key,
        "schema_version": "1.0.0",
        "producer": "test",
        "created_at": TS,
    }
    base.update(kw)
    return base


def envelope(key: str, payload: dict, **kw) -> dict:
    return {"meta": meta(key, **kw), "payload": payload}


def write(key: str, kind: str, name: str, instance: dict) -> None:
    path = SAMPLES / key / kind / f"{name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(instance, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def samples() -> None:
    # ---------------------------------------------------------------- contract-01
    k = "contract-01-envelope"
    write(k, "normal", "normal-01", envelope(
        k, {"note": "anything"}, run_id=ULID, stage="trigger"))
    write(k, "boundary", "boundary-01", {"meta": meta(k), "payload": {}})
    write(k, "error", "error-01", {"meta": meta(k, schema_version="1"), "payload": {}})
    write(k, "error", "error-02", envelope(k, {}, run_id="NOT-A-ULID"))

    # ---------------------------------------------------------------- contract-02
    k = "contract-02-runspec"
    runspec_payload = {
        "run_id": ULID,
        "trigger": {"type": "manual", "actor": "tester", "requested_at": TS},
        "inventory_ref": "config/inventory.example.yaml",
        "selector": {"groups": ["web"], "tags": []},
        "script": {"name": "disk-usage", "sha256": SHA, "risk": "read_only"},
        "limits": {"global_concurrency": 50, "per_node_concurrency": 1,
                   "connect_rate_per_sec": 20, "connect_timeout_sec": 10,
                   "exec_timeout_sec": 300},
        "config_snapshot_hash": SHA2,
    }
    write(k, "normal", "normal-01", envelope(k, runspec_payload, run_id=ULID, stage="trigger"))
    boundary = dict(runspec_payload)
    boundary["limits"] = {"global_concurrency": 200, "per_node_concurrency": 1,
                          "per_bastion_concurrency": 200, "connect_rate_per_sec": 200,
                          "connect_timeout_sec": 120, "exec_timeout_sec": 86400}
    boundary["selector"] = {"groups": [], "tags": []}
    write(k, "boundary", "boundary-01", envelope(k, boundary, run_id=ULID, stage="trigger"))
    err1 = dict(runspec_payload)
    err1["script"] = {"name": "mutator", "sha256": SHA, "risk": "mutating"}
    write(k, "error", "error-01", envelope(k, err1, run_id=ULID, stage="trigger"))
    err2 = dict(runspec_payload)
    del err2["limits"]
    write(k, "error", "error-02", envelope(k, err2, run_id=ULID, stage="trigger"))

    # ---------------------------------------------------------------- contract-03
    k = "contract-03-execution-result"
    stream = lambda **kw: {"bytes": 12, "truncated": False, "sha256": SHA,
                           "encoding": "utf-8", **kw}
    ok_payload = {
        "execution_uid": UUID1, "node_id": "node-a", "script_sha256": SHA,
        "status": "SUCCEEDED", "attempt_count": 1,
        "started_at": TS, "finished_at": "2026-08-03T10:00:05+00:00",
        "duration_ms": 5000, "exit_code": 0,
        "stdout": stream(inline="disk usage ok"), "stderr": stream(inline=""),
        "flags": [],
    }
    v110 = {"schema_version": "1.1.0"}
    write(k, "normal", "normal-01", envelope(k, ok_payload, run_id=ULID, stage="execution", **v110))
    bound = dict(ok_payload)
    bound["stdout"] = stream(blob_ref="blobs/abc.bin", bytes=1048576, truncated=True)
    bound["flags"] = ["truncated", "output_overflow"]
    write(k, "boundary", "boundary-01", envelope(k, bound, run_id=ULID, stage="execution", **v110))
    # Declared non-zero success code: schema-valid since Contract-03 1.1.0.
    nonzero = dict(ok_payload)
    nonzero["exit_code"] = 42
    write(k, "boundary", "boundary-02", envelope(k, nonzero, run_id=ULID, stage="execution", **v110))
    failed = dict(ok_payload)
    failed["status"] = "FAILED"
    failed["exit_code"] = None
    failed["error"] = {"class": "bogus_class", "category": "TRANSIENT",
                       "message": "not in error matrix", "retryable": True}
    write(k, "error", "error-01", envelope(k, failed, run_id=ULID, stage="execution", **v110))
    # FAILED but no error -> allOf violation
    write(k, "error", "error-02", envelope(k, failed_no_err(), run_id=ULID, stage="execution", **v110))
    # SUCCEEDED with null exit_code -> the 1.1.0 then-block requires an integer.
    null_exit = dict(ok_payload)
    null_exit["exit_code"] = None
    write(k, "error", "error-03", envelope(k, null_exit, run_id=ULID, stage="execution", **v110))


def failed_no_err() -> dict:
    return {
        "execution_uid": UUID2, "node_id": "node-b", "script_sha256": SHA,
        "status": "FAILED", "attempt_count": 2,
        "started_at": TS, "finished_at": "2026-08-03T10:00:06+00:00",
        "duration_ms": 6000, "exit_code": None,
        "stdout": {"bytes": 0, "truncated": False, "sha256": SHA, "inline": ""},
        "stderr": {"bytes": 0, "truncated": False, "sha256": SHA, "inline": ""},
        "flags": [],
    }


def samples_rest() -> None:
    # ---------------------------------------------------------------- contract-04
    k = "contract-04-analysis-result"
    anomaly = {"category": "disk", "severity": "warning",
               "evidence": "/dev/sda1 at 92%", "suggestion": "review retention"}
    llm = {
        "analysis_id": UUID1, "execution_uid": UUID1,
        "source": "llm", "tier": "T1", "status": "analyzed",
        "summary": "磁盘使用率超阈值，建议清理", "anomalies": [anomaly],
        "model": {"provider": "deepseek", "name": "deepseek-v4-pro", "prompt_version": "1.0.0"},
        "fallback": {"used": False},
    }
    write(k, "normal", "normal-01", envelope(k, llm, run_id=ULID, stage="analysis"))
    rules = {
        "analysis_id": UUID2, "execution_uid": UUID2,
        "source": "rules", "tier": "none", "status": "fallback",
        "summary": "规则摘要：连接失败", "anomalies": [anomaly],
        "fallback": {"used": True, "reason": "llm_unavailable"},
    }
    write(k, "boundary", "boundary-01", envelope(k, rules, run_id=ULID, stage="analysis"))
    err = dict(llm)
    del err["model"]
    write(k, "error", "error-01", envelope(k, err, run_id=ULID, stage="analysis"))

    # ---------------------------------------------------------------- contract-05
    k = "contract-05-batch-summary"
    ok = {
        "run_id": ULID, "run_status": "SUCCESS", "batch_status": "success",
        "summary_revision": 1, "final": True,
        "counts": {"targeted": 2, "succeeded": 2, "failed": 0, "unknown": 0,
                   "cancelled": 0, "skipped": 0},
        "error_counts": {}, "top_anomalies": [],
        "started_at": TS, "finished_at": "2026-08-03T10:01:00+00:00",
        "duration_ms": 60000, "report_path": "reports/run.json", "exit_code": 0,
    }
    write(k, "normal", "normal-01", envelope(k, ok, run_id=ULID, stage="orchestration"))
    partial = dict(ok)
    partial["run_status"] = "DEGRADED"
    partial["batch_status"] = "partial"
    partial["counts"] = {"targeted": 5, "succeeded": 4, "failed": 1, "unknown": 0,
                         "cancelled": 0, "skipped": 0}
    partial["error_counts"] = {"conn_refused": 1}
    partial["top_anomalies"] = ["node-b: connection refused"]
    partial["exit_code"] = 1
    write(k, "boundary", "boundary-01", envelope(k, partial, run_id=ULID, stage="orchestration"))
    bad = dict(ok)
    bad["counts"] = {"targeted": 2, "succeeded": -1, "failed": 0, "unknown": 0,
                     "cancelled": 0, "skipped": 0}
    write(k, "error", "error-01", envelope(k, bad, run_id=ULID, stage="orchestration"))

    # ---------------------------------------------------------------- contract-06
    k = "contract-06-persist-ack"
    ok = {"object_type": "execution_result", "object_id": UUID1,
          "committed_at": TS, "storage_version": 1, "outbox_event_ids": [UUID1]}
    write(k, "normal", "normal-01", envelope(k, ok, run_id=ULID, stage="persistence"))
    b = {"object_type": "run_event", "object_id": UUID2, "committed_at": TS,
         "storage_version": 1, "outbox_event_ids": []}
    write(k, "boundary", "boundary-01", envelope(k, b, run_id=ULID, stage="persistence"))
    e = dict(ok)
    e["object_type"] = "bogus"
    write(k, "error", "error-01", envelope(k, e, run_id=ULID, stage="persistence"))

    # ---------------------------------------------------------------- contract-07
    k = "contract-07-export-manifest"
    ok = {
        "export_id": UUID1, "run_id": ULID, "status": "exported",
        "started_at": TS, "finished_at": "2026-08-03T10:01:05+00:00",
        "files": [{"path": "vault/runs/2026-08-03.md", "note_type": "run",
                   "sha256": SHA, "bytes": 120, "status": "written"}],
    }
    write(k, "normal", "normal-01", envelope(k, ok, run_id=ULID, stage="export"))
    partial = dict(ok)
    partial["status"] = "partial"
    partial["files"] = [
        {"path": "a.md", "note_type": "run", "sha256": SHA, "bytes": 10, "status": "written"},
        {"path": "b.md", "note_type": "node", "sha256": SHA2, "bytes": 20,
         "status": "conflict", "error_class": "export_failed"},
    ]
    write(k, "boundary", "boundary-01", envelope(k, partial, run_id=ULID, stage="export"))
    bad = dict(ok)
    bad["files"] = [{"path": "a.md", "note_type": "run", "sha256": SHA, "status": "written"}]
    write(k, "error", "error-01", envelope(k, bad, run_id=ULID, stage="export"))

    # ---------------------------------------------------------------- contract-08
    k = "contract-08-note-frontmatter"
    ok = {"managed_by": "wft", "wf_schema": "note-frontmatter/1.0.0",
          "note_type": "run", "run_id": ULID, "created_at": TS,
          "status": "success", "tags": ["wft", "disk"]}
    write(k, "normal", "normal-01", ok)
    node_note = dict(ok)
    node_note["note_type"] = "node"
    node_note["node_id"] = "node-a"
    node_note["script_name"] = "disk-usage"
    node_note["script_sha256"] = SHA
    node_note["status"] = "partial"
    node_note["managed_hash"] = SHA2
    write(k, "boundary", "boundary-01", node_note)
    bad = dict(ok)
    bad["note_type"] = "node"  # missing required node_id -> allOf violation
    write(k, "error", "error-01", bad)

    # ---------------------------------------------------------------- contract-09
    k = "contract-09-run-event"
    ok = {"event_id": UUID1, "run_id": ULID, "event_type": "run_created",
          "severity": "info", "occurred_at": TS, "message": "run created", "data": {}}
    write(k, "normal", "normal-01", envelope(k, ok, run_id=ULID, stage="orchestration"))
    b = dict(ok)
    b.update({"event_type": "node_finished", "severity": "warning", "node_id": "node-a",
              "execution_uid": UUID1, "message": "node-a finished"})
    write(k, "boundary", "boundary-01", envelope(k, b, run_id=ULID, stage="execution"))
    e = dict(ok)
    e["event_type"] = "something_else"
    write(k, "error", "error-01", envelope(k, e, run_id=ULID, stage="orchestration"))

    # ---------------------------------------------------------------- contract-10
    k = "contract-10-alert-event"
    ok = {"alert_id": UUID1, "run_id": ULID, "rule": "batch_partial",
          "severity": "warning", "title": "batch partial",
          "summary": "1/5 nodes failed", "counts": {"targeted": 5, "succeeded": 4, "failed": 1},
          "report_path": "reports/run.json", "dedupe_key": "batch_partial:01ARZ3NDEKTSV4RRFFQ69G5FAV",
          "created_at": TS}
    write(k, "normal", "normal-01", envelope(k, ok, run_id=ULID, stage="notification"))
    b = dict(ok)
    b.update({"rule": "security_event", "severity": "critical",
              "title": "host key mismatch"})
    del b["counts"]
    b["dedupe_key"] = "security:01ARZ3NDEKTSV4RRFFQ69G5FAV"
    write(k, "boundary", "boundary-01", envelope(k, b, run_id=ULID, stage="notification"))
    e = dict(ok)
    e["rule"] = "mystery"
    write(k, "error", "error-01", envelope(k, e, run_id=ULID, stage="notification"))

    # ---------------------------------------------------------------- contract-11
    k = "contract-11-inventory"
    node = lambda nid, **kw: {
        "node_id": nid, "host": "10.0.0.11", "port": 22, "username": "ops",
        "auth": {"method": "key", "credential_ref": "env://WFT_KEY"},
        "groups": ["web"], "tags": ["prod"], **kw}
    ok_payload = {"nodes": [node("node-a"), node("node-b", tags=["staging"])]}
    write(k, "normal", "normal-01", envelope(k, ok_payload, stage="trigger"))
    b_payload = {"nodes": [
        node("node-a", groups=[], tags=[]),
        node("node-b", port=2222, bastion="node-a"),
    ]}
    write(k, "boundary", "boundary-01", envelope(k, b_payload, stage="trigger"))
    bad_payload = {"nodes": [{
        "node_id": "node-a", "host": "10.0.0.11", "port": 22, "username": "ops",
        "auth": {"method": "key", "credential_ref": "correct-horse-battery-staple"},
        "groups": ["web"], "tags": []}]}
    write(k, "error", "error-01", envelope(k, bad_payload, stage="trigger"))
    no_auth = node("node-a")
    del no_auth["auth"]
    write(k, "error", "error-02", envelope(k, {"nodes": [no_auth]}, stage="trigger"))

    # ---------------------------------------------------------------- contract-12
    k = "contract-12-script-registry"
    script = lambda n, sha: {
        "name": n, "path": "scripts/disk_usage.sh", "sha256": sha,
        "risk": "read_only", "shell": "bash", "timeout_sec": 30, "enabled": True}
    ok_payload = {"scripts": [script("disk-usage", SHA)]}
    write(k, "normal", "normal-01", envelope(k, ok_payload, stage="trigger"))
    b_payload = {"scripts": [script("disk-usage", SHA), script("disk-usage", SHA2)]}
    write(k, "boundary", "boundary-01", envelope(k, b_payload, stage="trigger"))
    mut = {"scripts": [script("mutator", SHA) | {"risk": "mutating"}]}
    write(k, "error", "error-01", envelope(k, mut, stage="trigger"))


if __name__ == "__main__":
    samples()
    samples_rest()
    count = len(list(SAMPLES.glob("**/*.json")))
    print(f"wrote {count} sample files under {SAMPLES}")
