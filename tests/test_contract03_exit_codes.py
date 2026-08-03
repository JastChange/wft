"""Contract-03 1.1.0: integer success exit codes and the cross-contract rule.

Gate A acceptance (per @SpecArchitect):
- The Contract-03 schema accepts an integer success code, not only 0.
- The cross-contract semantic check (validate_execution_result) proves the
  default-0 success, a declared non-zero success, and an undeclared non-zero
  rejection, using expected_exit_codes from the script registry (Contract-12).
- The Script model explicitly applies the expected_exit_codes default of [0].
"""
from __future__ import annotations

from pathlib import Path

import pytest

from wft.contracts import validate as cv
from wft.contracts.errors import WFTScriptRegistryError
from wft.scriptreg.registry import ScriptRegistry, sha256_file

KEY = "contract-03-execution-result"

ULID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
UUID = "f47ac10b-58cc-4372-a567-0e02b2c3d479"
SHA = "a" * 64
TS = "2026-08-03T10:00:00+00:00"


def _meta(schema_version: str = "1.1.0") -> dict:
    return {
        "schema_name": KEY,
        "schema_version": schema_version,
        "producer": "test",
        "created_at": TS,
    }


def _stream(inline: str = "", blob_ref: str | None = None, *, truncated: bool = False) -> dict:
    stream = {"bytes": len(inline), "truncated": truncated, "sha256": SHA, "encoding": "utf-8"}
    if blob_ref is not None:
        stream["blob_ref"] = blob_ref
        stream["bytes"] = 12
    else:
        stream["inline"] = inline
    return stream


def _payload(**overrides) -> dict:
    payload = {
        "execution_uid": UUID,
        "node_id": "node-a",
        "script_sha256": SHA,
        "status": "SUCCEEDED",
        "attempt_count": 1,
        "started_at": TS,
        "finished_at": "2026-08-03T10:00:05+00:00",
        "duration_ms": 5000,
        "exit_code": 0,
        "stdout": _stream("ok"),
        "stderr": _stream(""),
        "flags": [],
    }
    payload.update(overrides)
    return payload


def _envelope(payload: dict) -> dict:
    return {"meta": _meta(), "payload": payload}


def _envelope_v(schema_version: str, payload: dict) -> dict:
    return {"meta": _meta(schema_version=schema_version), "payload": payload}


def _failed_payload() -> dict:
    return _payload(
        status="FAILED",
        exit_code=None,
        error={"class": "conn_refused", "category": "TRANSIENT",
               "message": "connection refused", "retryable": True},
    )


# --------------------------------------------------------------------------- schema

def test_schema_accepts_exit_code_zero() -> None:
    problems = cv.validate_contract_all(KEY, _envelope(_payload(exit_code=0)))
    assert problems == []


def test_schema_accepts_declared_nonzero_exit_code() -> None:
    # Contract-03 1.1.0 accepts any integer 0..255 at the schema level; the
    # declared-code rule lives in the cross-contract semantic check.
    problems = cv.validate_contract_all(KEY, _envelope(_payload(exit_code=42)))
    assert problems == []


def test_schema_rejects_null_success_exit_code() -> None:
    # The 1.1.0 then-block requires an integer when status is SUCCEEDED.
    problems = cv.validate_contract_all(KEY, _envelope(_payload(exit_code=None)))
    assert problems, "SUCCEEDED with null exit_code must fail the schema"


def test_schema_rejects_out_of_range_success_exit_code() -> None:
    problems = cv.validate_contract_all(KEY, _envelope(_payload(exit_code=256)))
    assert problems, "SUCCEEDED with exit_code=256 must fail the schema"


# ------------------------------------------------------------------- cross-contract rule

def test_default_zero_is_success() -> None:
    # A result with exit_code=0 validates against a script whose
    # expected_exit_codes defaults to (0,).
    assert cv.validate_execution_result(_envelope(_payload(exit_code=0))) == []


def test_undeclared_nonzero_is_rejected() -> None:
    problems = cv.validate_execution_result(
        _envelope(_payload(exit_code=42)), expected_exit_codes=(0,)
    )
    assert any("expected_exit_codes" in p and "42" in p for p in problems)


def test_declared_nonzero_is_accepted() -> None:
    assert cv.validate_execution_result(
        _envelope(_payload(exit_code=42)), expected_exit_codes=(0, 42)
    ) == []


def test_failed_result_bypasses_exit_code_rule() -> None:
    # Only SUCCEEDED results are checked against expected_exit_codes.
    assert cv.validate_execution_result(_envelope(_failed_payload())) == []


def test_schema_errors_survive_cross_contract_validation() -> None:
    # validate_execution_result still reports schema violations.
    problems = cv.validate_execution_result(
        _envelope(_payload(exit_code=None)), expected_exit_codes=(0,)
    )
    assert problems


# ---------------------------------------------------------------- version binding

def test_v100_code_zero_is_compatible() -> None:
    # A 1.0.x producer may still emit SUCCEEDED exit_code=0 (compatible read).
    assert cv.validate_execution_result(_envelope_v("1.0.0", _payload(exit_code=0))) == []


def test_v100_code_42_is_rejected() -> None:
    # 1.0.x never allowed a non-zero success code, even if the script declares it.
    problems = cv.validate_execution_result(
        _envelope_v("1.0.0", _payload(exit_code=42)), expected_exit_codes=(0, 42)
    )
    assert any("schema_version" in p and "1.1.0" in p for p in problems)


def test_v110_declared_42_accepted() -> None:
    assert cv.validate_execution_result(
        _envelope_v("1.1.0", _payload(exit_code=42)), expected_exit_codes=(0, 42)
    ) == []


def test_v120_declared_42_compatible() -> None:
    # A higher compatible MINOR keeps the 1.1.0 behaviour.
    assert cv.validate_execution_result(
        _envelope_v("1.2.0", _payload(exit_code=42)), expected_exit_codes=(0, 42)
    ) == []


def test_v200_unsupported_major_rejected() -> None:
    # Unsupported major must be rejected outright, even for exit_code=0.
    problems = cv.validate_execution_result(_envelope_v("2.0.0", _payload(exit_code=0)))
    assert any("major" in p and "2" in p for p in problems)


@pytest.mark.parametrize(
    "status",
    ["FAILED", "UNKNOWN", "CANCELLED", "SKIPPED"],
)
def test_v200_unsupported_major_rejected_for_all_statuses(status: str) -> None:
    # The major gate runs before the SUCCEEDED branch, so no status may bypass it.
    payload = _payload(status=status, exit_code=0)
    if status == "FAILED":
        payload = _failed_payload()
        payload["status"] = "FAILED"
    problems = cv.validate_execution_result(_envelope_v("2.0.0", payload))
    assert any("major" in p and "2" in p for p in problems), f"status={status} must reject major 2"


def test_v110_declared_42_still_needs_declaration() -> None:
    # Version >= 1.1.0 makes 42 legal, but the script must still declare it.
    problems = cv.validate_execution_result(
        _envelope_v("1.2.0", _payload(exit_code=42)), expected_exit_codes=(0,)
    )
    assert any("expected_exit_codes" in p for p in problems)


# ----------------------------------------------------------------------- Script model

def _write_registry(tmp_path: Path, scripts: list[dict]) -> Path:
    script_files: list[str] = []
    for i, entry in enumerate(scripts):
        script_file = tmp_path / f"script{i}.sh"
        script_file.write_text("#!/bin/sh\necho hi\n", encoding="utf-8")
        sha = sha256_file(script_file)
        entry.setdefault("path", str(script_file))
        entry.setdefault("sha256", sha)
        entry.setdefault("risk", "read_only")
        entry.setdefault("shell", "bash")
        entry.setdefault("timeout_sec", 30)
        entry.setdefault("enabled", True)
        script_files.append(_yaml_entry(entry))
    yaml_file = tmp_path / "scripts.yaml"
    yaml_file.write_text("scripts:\n" + "".join(script_files), encoding="utf-8")
    return yaml_file


def _yaml_entry(entry: dict) -> str:
    lines = [f"  - name: {entry['name']}\n"]
    for field, value in entry.items():
        if field == "name" or value is None:
            continue
        if field == "expected_exit_codes":
            lines.append(f"    expected_exit_codes: {value}\n")
        elif isinstance(value, bool):
            lines.append(f"    {field}: {str(value).lower()}\n")
        elif isinstance(value, str):
            lines.append(f"    {field}: {value}\n")
        else:
            lines.append(f"    {field}: {value}\n")
    return "".join(lines)


def test_script_model_explicit_default_is_zero(tmp_path: Path) -> None:
    yaml_file = _write_registry(tmp_path, [{"name": "ok"}])
    registry = ScriptRegistry.from_file(yaml_file)
    assert registry.resolve("ok").expected_exit_codes == (0,)


def test_script_model_reads_declared_codes(tmp_path: Path) -> None:
    yaml_file = _write_registry(tmp_path, [{"name": "ok", "expected_exit_codes": [0, 42]}])
    registry = ScriptRegistry.from_file(yaml_file)
    assert registry.resolve("ok").expected_exit_codes == (0, 42)


@pytest.mark.parametrize(
    "codes",
    [[], [0, 0], [256], [0, 1, 1], "0", [True], [None]],
)
def test_script_model_rejects_bad_expected_exit_codes(tmp_path: Path, codes) -> None:
    yaml_file = _write_registry(tmp_path, [{"name": "ok", "expected_exit_codes": codes}])
    with pytest.raises(WFTScriptRegistryError):
        ScriptRegistry.from_file(yaml_file)


def test_declared_codes_drive_cross_contract_validation(tmp_path: Path) -> None:
    yaml_file = _write_registry(tmp_path, [{"name": "ok", "expected_exit_codes": [0, 42]}])
    registry = ScriptRegistry.from_file(yaml_file)
    codes = registry.resolve("ok").expected_exit_codes
    assert cv.validate_execution_result(_envelope(_payload(exit_code=42)), expected_exit_codes=codes) == []
    problems = cv.validate_execution_result(_envelope(_payload(exit_code=7)), expected_exit_codes=codes)
    assert problems
