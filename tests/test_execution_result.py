"""Contract-03 result building: output bounds, binary degradation, flags."""
from __future__ import annotations

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


def _build(blobs: BlobStore, **overrides) -> tuple[dict, bool, tuple[dict, ...]]:
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


class _WriteFails(BlobStore):
    def write(self, data: bytes) -> str:
        raise OSError("disk full")


class _CountingBlob(BlobStore):
    """A blob store that fails its first ``failures`` writes then succeeds."""

    def __init__(self, blob_dir: Path, *, failures: int) -> None:
        super().__init__(blob_dir)
        self.failures = failures
        self.calls = 0

    def write(self, data: bytes) -> str:
        self.calls += 1
        if self.calls <= self.failures:
            raise OSError("transient blob failure")
        return super().write(data)


# ------------------------------------------------------------------ streams


def test_small_stdout_inlined(blobs: BlobStore) -> None:
    stream, flags, degraded, err = build_stream("stdout", b"hello", blobs)
    assert stream["inline"] == "hello"
    assert stream["bytes"] == 5
    assert stream["truncated"] is False
    assert stream["encoding"] == "utf-8"
    assert flags == []
    assert degraded is False
    assert err is None


def test_stdout_overflow_goes_to_blob(blobs: BlobStore) -> None:
    big = b"x" * (INLINE_STDOUT_CAP + 1024)
    stream, flags, degraded, err = build_stream("stdout", big, blobs)
    assert "inline" not in stream
    assert blobs.contains(stream["blob_ref"])
    assert stream["bytes"] == len(big)
    assert stream["truncated"] is False
    assert flags == []
    assert degraded is False
    assert err is None


def test_stderr_cap_is_smaller(blobs: BlobStore) -> None:
    # Between stdout and stderr caps: inlined for stdout, spilled for stderr.
    size = INLINE_STDERR_CAP + 1024
    assert size < INLINE_STDOUT_CAP
    out_stream, _, _, _ = build_stream("stdout", b"y" * size, blobs)
    err_stream, _, _, _ = build_stream("stderr", b"y" * size, blobs)
    assert "inline" in out_stream
    assert "blob_ref" in err_stream


def test_hard_cap_truncates_tail(blobs: BlobStore) -> None:
    data = b"Z" * (STREAM_HARD_CAP + 5000)
    stream, flags, degraded, err = build_stream("stdout", data, blobs)
    assert stream["bytes"] == STREAM_HARD_CAP
    assert stream["truncated"] is True
    assert "truncated" in flags
    assert "output_overflow" in flags
    assert degraded is False
    assert err is None


def test_pre_capped_tail_reports_total_overflow(blobs: BlobStore) -> None:
    # The SSH layer already capped the tail to the hard cap; the original total
    # (which exceeds the cap) must still mark the stream truncated.
    tail = b"Z" * STREAM_HARD_CAP
    stream, flags, degraded, err = build_stream(
        "stdout", tail, blobs, total_bytes=STREAM_HARD_CAP + 5000
    )
    assert stream["bytes"] == STREAM_HARD_CAP
    assert stream["truncated"] is True
    assert "truncated" in flags
    assert "output_overflow" in flags
    assert degraded is False
    assert err is None


def test_non_utf8_binary_blob_degrades(blobs: BlobStore) -> None:
    data = b"\xff\xfe binary \x00 bytes"
    stream, _flags, degraded, err = build_stream("stdout", data, blobs)
    assert stream["encoding"] == "binary"
    assert "blob_ref" in stream
    assert blobs.read(stream["blob_ref"]) == data
    assert degraded is True
    assert err is None


def test_blob_write_failure_utf8_falls_back_to_inline(blobs: BlobStore) -> None:
    # Over-cap UTF-8: a blob write failure must truncate to the inline cap and
    # degrade the run instead of failing it (错误矩阵_v0.1.md blob_write_failed).
    data = b"y" * (INLINE_STDOUT_CAP + 1024)
    stream, flags, degraded, err = build_stream("stdout", data, _WriteFails(blobs.blob_dir))
    assert stream is not None
    assert stream["inline"] == "y" * INLINE_STDOUT_CAP
    assert stream["truncated"] is True
    assert "output_overflow" in flags
    assert degraded is True
    assert err["class"] == "blob_write_failed"
    assert err["category"] == "RESOURCE"
    assert err["retryable"] is True


def test_blob_write_failure_binary_is_unrecoverable(blobs: BlobStore) -> None:
    # Binary output cannot be inlined, so a blob write failure must signal the
    # caller to mark the result FAILED.
    stream, _flags, degraded, err = build_stream(
        "stdout", b"\xff\x00\x01", _WriteFails(blobs.blob_dir)
    )
    assert stream is None
    assert degraded is True
    assert err["class"] == "blob_write_failed"


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
    envelope, degraded, secondary = _build(blobs)
    assert secondary == ()
    assert envelope["payload"]["status"] == "SUCCEEDED"
    assert envelope["payload"]["stdout"]["inline"] == "ok\n"
    assert envelope["payload"]["flags"] == []
    assert degraded is False


def test_nonzero_expected_success_valid(blobs: BlobStore) -> None:
    env, degraded, _ = _build(blobs, script=_script(expected=(0, 3)), exit_code=3)
    assert env["payload"]["status"] == "SUCCEEDED"
    assert env["payload"]["exit_code"] == 3
    assert degraded is False


def test_failure_result_has_error(blobs: BlobStore) -> None:
    from wft.execution.errors import error_dict

    env, _degraded, _ = _build(
        blobs,
        status="FAILED",
        exit_code=1,
        error=error_dict("exec_nonzero", "script exited 1"),
    )
    assert env["payload"]["error"]["class"] == "exec_nonzero"
    assert env["payload"]["stdout"]["inline"] == "ok\n"


def test_overflow_flag_propagates_to_result(blobs: BlobStore) -> None:
    env, degraded, _ = _build(blobs, stdout_bytes=b"q" * (STREAM_HARD_CAP + 1))
    assert "output_overflow" in env["payload"]["flags"]
    assert "truncated" in env["payload"]["flags"]
    assert degraded is False


def test_binary_output_degrades_run(blobs: BlobStore) -> None:
    env, degraded, _ = _build(blobs, stderr_bytes=b"\x00\x01\xff")
    assert env["payload"]["stderr"]["encoding"] == "binary"
    assert degraded is True


def test_binary_output_attaches_decode_error(blobs: BlobStore) -> None:
    # A SUCCEEDED result with binary output carries structured output_decode_failed
    # evidence so batch aggregation can count it (错误矩阵_v0.1.md).
    env, degraded, _ = _build(blobs, stderr_bytes=b"\x00\x01\xff")
    assert env["payload"]["status"] == "SUCCEEDED"
    assert env["payload"]["error"]["class"] == "output_decode_failed"
    assert env["payload"]["error"]["category"] == "DATA"
    assert env["payload"]["error"]["retryable"] is False
    assert degraded is True


def test_binary_output_keeps_primary_error(blobs: BlobStore) -> None:
    from wft.execution.errors import error_dict

    env, degraded, _ = _build(
        blobs,
        status="FAILED",
        exit_code=1,
        error=error_dict("exec_nonzero", "script exited 1"),
        stderr_bytes=b"\x00\x01\xff",
    )
    assert env["payload"]["status"] == "FAILED"
    assert env["payload"]["error"]["class"] == "exec_nonzero"
    assert env["payload"]["stderr"]["encoding"] == "binary"
    assert degraded is True


def test_hard_blob_failure_marks_result_failed(blobs: BlobStore) -> None:
    # A stream that cannot be inlined (binary) failing to persist is FAILED with
    # blob_write_failed regardless of the exit status.
    env, degraded, _ = _build(_WriteFails(blobs.blob_dir), stderr_bytes=b"\x00\x01\xff")
    assert env["payload"]["status"] == "FAILED"
    assert env["payload"]["error"]["class"] == "blob_write_failed"
    assert degraded is True


def test_extra_flags_included(blobs: BlobStore) -> None:
    env, _, _ = _build(blobs, extra_flags=("slow", "retried"))
    assert "slow" in env["payload"]["flags"]
    assert "retried" in env["payload"]["flags"]


def test_unknown_flag_rejected(blobs: BlobStore) -> None:
    with pytest.raises(ValueError):
        _build(blobs, extra_flags=("nonsense",))


# --------------------------------------------------- blob write retry (matrix)


def test_blob_write_retries_without_degrading(blobs: BlobStore) -> None:
    # 错误矩阵_v0.1.md: blob_write_failed is RESOURCE with up to 2 retries
    # (3 write attempts). The first two fail, the third succeeds: no fallback,
    # no degradation, and the SSH attempt_count is untouched (this is internal).
    data = b"y" * (INLINE_STDOUT_CAP + 1024)
    store = _CountingBlob(blobs.blob_dir, failures=2)
    stream, _flags, degraded, err = build_stream("stdout", data, store, valid_utf8=True)
    assert store.calls == 3
    assert err is None
    assert degraded is False
    assert "blob_ref" in stream
    assert stream["encoding"] == "utf-8"
    assert blobs.contains(stream["blob_ref"])


def test_blob_write_all_attempts_fail_falls_back(blobs: BlobStore) -> None:
    # Three failed writes exhaust the retry bound, then the UTF-8 inline
    # fallback engages with the truncated/output_overflow flags.
    data = b"y" * (INLINE_STDOUT_CAP + 1024)
    store = _CountingBlob(blobs.blob_dir, failures=3)
    stream, flags, degraded, err = build_stream("stdout", data, store, valid_utf8=True)
    assert store.calls == 3
    assert stream is not None
    assert stream["inline"] == "y" * INLINE_STDOUT_CAP
    assert "output_overflow" in flags
    assert degraded is True
    assert err["class"] == "blob_write_failed"
    assert err["category"] == "RESOURCE"
    assert err["retryable"] is True


def test_binary_blob_write_retries_then_succeeds(blobs: BlobStore) -> None:
    # Binary is always degraded, but a transient blob failure still retries
    # internally instead of failing the result.
    data = b"\xff\x00 binary \x01"
    store = _CountingBlob(blobs.blob_dir, failures=2)
    stream, _flags, degraded, err = build_stream("stdout", data, store)
    assert store.calls == 3
    assert err is None
    assert stream is not None
    assert stream["encoding"] == "binary"
    assert degraded is True


def test_binary_blob_write_all_fail_is_unrecoverable(blobs: BlobStore) -> None:
    data = b"\xff\x00 binary \x01"
    store = _CountingBlob(blobs.blob_dir, failures=3)
    stream, _flags, _degraded, err = build_stream("stdout", data, store)
    assert store.calls == 3
    assert stream is None
    assert err["class"] == "blob_write_failed"


# --------------------------------------------------------- UTF-8 tail alignment


def test_truncated_utf8_tail_is_not_misdetected_binary(blobs: BlobStore) -> None:
    # A 1 MiB cut can land mid-character; the saved tail must stay classified as
    # UTF-8 (never binary/DEGRADED) and be aligned to a character boundary.
    size = 400000  # '你' is 3 UTF-8 bytes -> 1.2 MiB total
    data = ("你".encode() * size)
    tail = data[-STREAM_HARD_CAP:]  # starts with a continuation byte
    stream, flags, degraded, err = build_stream(
        "stdout", tail, blobs, total_bytes=len(data), valid_utf8=True
    )
    assert err is None
    assert stream["encoding"] == "utf-8"
    assert degraded is False
    assert "truncated" in flags
    assert "output_overflow" in flags
    assert STREAM_HARD_CAP - 3 <= stream["bytes"] <= STREAM_HARD_CAP
    blob = blobs.read(stream["blob_ref"])
    blob.decode("utf-8")  # must not raise
    assert blob == data[-len(blob):]


def test_utf8_blob_fallback_inline_aligns_boundary(blobs: BlobStore) -> None:
    # Both the hard-cap cut and the inline fallback cut can land mid-character;
    # the fallback must still decode without raising and stay a suffix of data.
    size = 400000
    data = "你".encode() * size
    tail = data[-STREAM_HARD_CAP:]
    stream, _flags, degraded, err = build_stream(
        "stdout", tail, _WriteFails(blobs.blob_dir), total_bytes=len(data), valid_utf8=True
    )
    assert err["class"] == "blob_write_failed"
    assert stream is not None
    assert stream["encoding"] == "utf-8"
    assert stream["truncated"] is True
    assert degraded is True
    saved = stream["inline"].encode("utf-8")
    assert saved == data[-len(saved):]


# ------------------------------------------------ secondary error preservation


def test_utf8_blob_failure_not_misattributed(blobs: BlobStore) -> None:
    # UTF-8 over-cap blob failure must surface blob_write_failed, not be
    # overwritten by output_decode_failed (regression for the SUCCEEDED +
    # output_decode_failed misattribution).
    env, degraded, secondary = _build(
        _WriteFails(blobs.blob_dir),
        stdout_bytes=b"y" * (INLINE_STDOUT_CAP + 1024),
    )
    assert env["payload"]["status"] == "SUCCEEDED"
    assert env["payload"]["error"]["class"] == "blob_write_failed"
    assert degraded is True
    assert secondary == ()


def test_binary_decode_error_secondary_with_primary(blobs: BlobStore) -> None:
    from wft.execution.errors import error_dict

    env, degraded, secondary = _build(
        blobs,
        status="FAILED",
        exit_code=1,
        error=error_dict("exec_nonzero", "script exited 1"),
        stderr_bytes=b"\x00\x01\xff",
    )
    assert env["payload"]["error"]["class"] == "exec_nonzero"
    assert degraded is True
    classes = [e["class"] for e in secondary]
    assert classes == ["output_decode_failed"]


def test_blob_failure_secondary_with_primary(blobs: BlobStore) -> None:
    from wft.execution.errors import error_dict

    # A primary exec_nonzero keeps its slot; the blob_write_failed from the
    # UTF-8 fallback is returned as secondary so aggregation does not lose it.
    env, degraded, secondary = _build(
        _WriteFails(blobs.blob_dir),
        status="FAILED",
        exit_code=1,
        error=error_dict("exec_nonzero", "script exited 1"),
        stdout_bytes=b"y" * (INLINE_STDOUT_CAP + 1024),
    )
    assert env["payload"]["error"]["class"] == "exec_nonzero"
    assert degraded is True
    classes = [e["class"] for e in secondary]
    assert classes == ["blob_write_failed"]
