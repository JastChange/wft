"""Contract-03 result building: output bounds, binary degradation, flags."""
from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from wft.execution.result import (
    INLINE_STDERR_CAP,
    INLINE_STDOUT_CAP,
    STREAM_HARD_CAP,
    build_execution_result,
    build_stream,
    classify_exit,
)
from wft.scriptreg.registry import Script
from wft.storage.blobs import BlobStore

RUN_ID = "01HX0" + "A" * 21
EXEC_UID = "0190a2b3-c4d5-46e7-8890-1234567890ab"
NODE_ID = "node-a"


def _script(expected=(0,)) -> Script:
    return Script(
        name="disk-usage",
        path=Path("/nonexistent"),
        sha256="a" * 64,
        risk="read_only",
        shell="bash",
        timeout_sec=30,
        enabled=True,
        expected_exit_codes=expected,
    )


@pytest.fixture()
def blobs(tmp_path: Path) -> BlobStore:
    return BlobStore(tmp_path / "blobs")


def _build(blobs: BlobStore, **overrides) -> tuple[dict, bool]:
    kwargs = {
        "run_id": RUN_ID,
        "execution_uid": EXEC_UID,
        "node_id": NODE_ID,
        "script": _script(),
        "status": "SUCCEEDED",
        "attempt_count": 1,
        "started_at": "2026-08-03T10:00:01+00:00",
        "finished_at": "2026-08-03T10:00:02+00:00",
        "duration_ms": 1000,
        "exit_code": 0,
        "stdout_bytes": b"ok\n",
        "stderr_bytes": b"",
        "error": None,
        "blobs": blobs,
    }
    kwargs.update(overrides)
    return build_execution_result(**kwargs)


# ------------------------------------------------------------------ streams


def test_small_stdout_inlined(blobs: BlobStore) -> None:
    stream, flags, degraded = build_stream("stdout", b"hello", blobs)
    assert stream["inline"] == "hello"
    assert stream["bytes"] == 5
    assert stream["truncated"] is False
    assert stream["encoding"] == "utf-8"
    assert flags == []
    assert degraded is False


def test_stdout_overflow_goes_to_blob(blobs: BlobStore) -> None:
    big = b"x" * (INLINE_STDOUT_CAP + 1024)
    stream, flags, degraded = build_stream("stdout", big, blobs)
    assert "inline" not in stream
    assert blobs.contains(stream["blob_ref"])
    assert stream["bytes"] == len(big)
    assert stream["truncated"] is False
    assert flags == []
    assert degraded is False


def test_stderr_cap_is_smaller(blobs: BlobStore) -> None:
    # Between stdout and stderr caps: inlined for stdout, spilled for stderr.
    size = INLINE_STDERR_CAP + 1024
    assert size < INLINE_STDOUT_CAP
    out_stream, _, _ = build_stream("stdout", b"y" * size, blobs)
    err_stream, _, _ = build_stream("stderr", b"y" * size, blobs)
    assert "inline" in out_stream
    assert "blob_ref" in err_stream


def test_hard_cap_truncates_tail(blobs: BlobStore) -> None:
    data = b"Z" * (STREAM_HARD_CAP + 5000)
    stream, flags, degraded = build_stream("stdout", data, blobs)
    assert stream["bytes"] == STREAM_HARD_CAP
    assert stream["truncated"] is True
    assert "truncated" in flags
    assert "output_overflow" in flags
    assert degraded is False


def test_pre_capped_tail_reports_total_overflow(blobs: BlobStore) -> None:
    # The SSH layer already capped the tail to the hard cap; the original total
    # (which exceeds the cap) must still mark the stream truncated.
    tail = b"Z" * STREAM_HARD_CAP
    stream, flags, degraded = build_stream(
        "stdout", tail, blobs, total_bytes=STREAM_HARD_CAP + 5000
    )
    assert stream["bytes"] == STREAM_HARD_CAP
    assert stream["truncated"] is True
    assert "truncated" in flags
    assert "output_overflow" in flags
    assert degraded is False


def test_non_utf8_binary_blob_degrades(blobs: BlobStore) -> None:
    data = b"\xff\xfe binary \x00 bytes"
    stream, flags, degraded = build_stream("stdout", data, blobs)
    assert stream["encoding"] == "binary"
    assert "blob_ref" in stream
    assert blobs.read(stream["blob_ref"]) == data
    assert degraded is True


# ------------------------------------------------------------ classify_exit


def test_classify_exit_expected_succeeds() -> None:
    assert classify_exit(0, (0,)) == ("SUCCEEDED", None)


def test_classify_exit_expected_nonzero_succeeds() -> None:
    status, err = classify_exit(3, (0, 3))
    assert status == "SUCCEEDED"
    assert err is None


def test_classify_exit_unexpected_fails() -> None:
    status, err = classify_exit(1, (0,))
    assert status == "FAILED"
    assert err["class"] == "exec_nonzero"
    assert err["category"] == "PERMANENT"
    assert err["retryable"] is False


# -------------------------------------------------------------- full result


def test_success_result_valid(blobs: BlobStore) -> None:
    envelope, degraded = _build(blobs)
    assert envelope["payload"]["status"] == "SUCCEEDED"
    assert envelope["payload"]["stdout"]["inline"] == "ok\n"
    assert envelope["payload"]["flags"] == []
    assert degraded is False


def test_nonzero_expected_success_valid(blobs: BlobStore) -> None:
    env, degraded = _build(blobs, script=_script(expected=(0, 3)), exit_code=3)
    assert env["payload"]["status"] == "SUCCEEDED"
    assert env["payload"]["exit_code"] == 3
    assert degraded is False


def test_failure_result_has_error(blobs: BlobStore) -> None:
    from wft.execution.errors import error_dict

    env, degraded = _build(
        blobs,
        status="FAILED",
        exit_code=1,
        error=error_dict("exec_nonzero", "script exited 1"),
    )
    assert env["payload"]["error"]["class"] == "exec_nonzero"
    assert env["payload"]["stdout"]["inline"] == "ok\n"


def test_overflow_flag_propagates_to_result(blobs: BlobStore) -> None:
    env, degraded = _build(blobs, stdout_bytes=b"q" * (STREAM_HARD_CAP + 1))
    assert "output_overflow" in env["payload"]["flags"]
    assert "truncated" in env["payload"]["flags"]
    assert degraded is False


def test_binary_output_degrades_run(blobs: BlobStore) -> None:
    env, degraded = _build(blobs, stderr_bytes=b"\x00\x01\xff")
    assert env["payload"]["stderr"]["encoding"] == "binary"
    assert degraded is True


def test_extra_flags_included(blobs: BlobStore) -> None:
    env, _ = _build(blobs, extra_flags=("slow", "retried"))
    assert "slow" in env["payload"]["flags"]
    assert "retried" in env["payload"]["flags"]


def test_unknown_flag_rejected(blobs: BlobStore) -> None:
    with pytest.raises(ValueError):
        _build(blobs, extra_flags=("nonsense",))
