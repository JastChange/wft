"""Contract-03 ExecutionResult builder with output bounds (数据契约 §3, §4).

Stream bounds: stdout is inlined up to 256 KiB and stderr up to 64 KiB;
larger UTF-8 output spills to a content-addressed blob. Anything beyond the
1 MiB per-stream hard cap is truncated (tail preserved, head dropped) and the
stream flags ``truncated`` + ``output_overflow``. Non-UTF-8 bytes always go to
a binary blob and degrade the run (per the approved plan).
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from wft.contracts.errors import WFTContractError
from wft.contracts.validate import validate_execution_result
from wft.scriptreg.registry import Script
from wft.storage.blobs import BlobStore

from .errors import error_dict

INLINE_STDOUT_CAP = 256 * 1024
INLINE_STDERR_CAP = 64 * 1024
STREAM_HARD_CAP = 1024 * 1024

RESULT_SCHEMA_VERSION = "1.1.0"
FLAG_VALUES = frozenset({"truncated", "slow", "retried", "resumed", "output_overflow"})

# Storage-layer blob writes retry internally (错误矩阵_v0.1.md: ``blob_write_failed``
# is RESOURCE/retryable, max 2 retries). These attempts are internal to a single
# SSH attempt and must never bump ``attempt_count``.
_BLOB_WRITE_ATTEMPTS = 3


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _inline_empty() -> dict:
    """An empty UTF-8 stream used when a hard blob failure dropped the output."""
    return {
        "inline": "",
        "bytes": 0,
        "truncated": False,
        "sha256": _sha(b""),
        "encoding": "utf-8",
    }


def align_utf8_tail(data: bytes) -> bytes:
    """Drop up to 3 leading UTF-8 continuation bytes from ``data``.

    A fixed-cap cut (1 MiB tail or the inline fallback) can land mid-character;
    the saved fragment must start on a character boundary so it stays decodable
    as UTF-8 and is never misclassified as binary. ``bytes``/``sha256`` then
    describe exactly the saved fragment, per the approved output contract.
    """
    i = 0
    while i < len(data) and i < 3 and 0x80 <= data[i] <= 0xBF:
        i += 1
    return data[i:] if i else data


def _write_blob(blobs: BlobStore, content: bytes, *, context: str) -> tuple[str | None, dict | None]:
    """Persist ``content`` with up to 3 attempts; return ``(blob_ref, error)``.

    Retries are internal to the result build (错误矩阵 RESOURCE retry bound); a
    successful retry does not degrade and does not create a new SSH attempt.
    """
    last: Exception | None = None
    for _ in range(_BLOB_WRITE_ATTEMPTS):
        try:
            return blobs.write(content), None
        except OSError as exc:
            last = exc
    return None, error_dict("blob_write_failed", f"{context}: {last}")


def build_stream(
    name: str,
    data: bytes,
    blobs: BlobStore,
    total_bytes: int | None = None,
    valid_utf8: bool | None = None,
) -> tuple[dict | None, list[str], bool, dict | None]:
    """Return ``(stream, flags, degraded, error)`` for a stdout/stderr stream.

    ``stream`` is ``None`` only when a stream that cannot be inlined (binary)
    failed to persist to a blob — the caller must then mark the whole result
    FAILED (错误矩阵_v0.1.md: ``blob_write_failed``). For over-cap UTF-8 the
    fallback truncates to the inline cap and degrades instead of failing.

    ``degraded`` is True for non-UTF-8 (binary) content or when a blob write
    failed and the stream had to fall back. ``error`` is a ``blob_write_failed``
    error dict when a blob write failed, else ``None``. ``valid_utf8`` overrides
    auto-detection with the SSH layer's full-stream assessment: a 1 MiB cut can
    land mid-character, so the retained tail alone cannot prove binary-ness.
    When the SSH layer already capped the tail (``total_bytes`` > the data
    length), ``truncated`` is decided from the original total.
    """
    inline_cap = INLINE_STDOUT_CAP if name == "stdout" else INLINE_STDERR_CAP
    flags: list[str] = []
    if valid_utf8 is None:
        try:
            data.decode("utf-8")
            encoding = "utf-8"
        except UnicodeDecodeError:
            encoding = "binary"
    else:
        encoding = "utf-8" if valid_utf8 else "binary"

    content = data if len(data) <= STREAM_HARD_CAP else data[-STREAM_HARD_CAP:]
    total = len(data) if total_bytes is None else total_bytes
    truncated = total > STREAM_HARD_CAP
    if truncated:
        flags.extend(["truncated", "output_overflow"])
    if encoding == "utf-8":
        # Align the tail to a UTF-8 boundary so inline/blob bytes stay decodable.
        content = align_utf8_tail(content)

    if encoding == "binary":
        blob_ref, write_err = _write_blob(
            blobs, content, context=f"cannot persist binary {name} to blob"
        )
        if write_err is not None:
            return None, flags, True, write_err
        return (
            {
                "blob_ref": blob_ref,
                "bytes": len(content),
                "truncated": truncated,
                "sha256": _sha(content),
                "encoding": "binary",
            },
            flags,
            True,
            None,
        )
    if len(content) <= inline_cap:
        return (
            {
                "inline": content.decode("utf-8"),
                "bytes": len(content),
                "truncated": truncated,
                "sha256": _sha(content),
                "encoding": "utf-8",
            },
            flags,
            False,
            None,
        )
    blob_ref, write_err = _write_blob(
        blobs, content, context=f"blob write failed; {name} truncated to {inline_cap} bytes inline"
    )
    if write_err is not None:
        fallback = align_utf8_tail(content[-inline_cap:])
        if "truncated" not in flags:
            flags.extend(["truncated", "output_overflow"])
        return (
            {
                "inline": fallback.decode("utf-8"),
                "bytes": len(fallback),
                "truncated": True,
                "sha256": _sha(fallback),
                "encoding": "utf-8",
            },
            flags,
            True,
            write_err,
        )
    return (
        {
            "blob_ref": blob_ref,
            "bytes": len(content),
            "truncated": truncated,
            "sha256": _sha(content),
            "encoding": "utf-8",
        },
        flags,
        False,
        None,
    )


def classify_exit(exit_code: int, expected_exit_codes: tuple[int, ...]) -> tuple[str, dict | None]:
    """Map an exit code to (status, error). Non-expected codes become FAILED."""
    if exit_code in expected_exit_codes:
        return "SUCCEEDED", None
    return (
        "FAILED",
        error_dict(
            "exec_nonzero",
            f"script exited {exit_code}, expected {sorted(expected_exit_codes)}",
        ),
    )


def build_execution_result(
    *,
    run_id: str,
    execution_uid: str,
    node_id: str,
    script: Script,
    status: str,
    attempt_count: int,
    started_at: str,
    finished_at: str,
    duration_ms: int,
    exit_code: int | None,
    stdout_bytes: bytes,
    stderr_bytes: bytes,
    error: dict | None,
    blobs: BlobStore,
    extra_flags: tuple[str, ...] = (),
    produced_at: str | None = None,
    stdout_total: int | None = None,
    stderr_total: int | None = None,
    stdout_valid_utf8: bool | None = None,
    stderr_valid_utf8: bool | None = None,
) -> tuple[dict, bool, tuple[dict, ...]]:
    """Build and validate a Contract-03 envelope.

    Returns ``(envelope, degraded, secondary_errors)``. ``degraded`` is True
    when either stream held non-UTF-8 bytes or a blob write fell back. Secondary
    blob/decode errors that cannot fit the single Contract-03 ``error`` slot
    (e.g. beside an ``exec_nonzero`` primary) are returned so the caller can
    keep them in aggregate evidence instead of dropping them. Same-class errors
    from different streams all survive (identity-based, not class-filtered) so
    per-stream detail is never lost. Raises :class:`WFTContractError` if the
    built envelope fails Contract-03 validation.
    """
    stdout_stream, stdout_flags, stdout_degraded, stdout_err = build_stream(
        "stdout",
        stdout_bytes,
        blobs,
        total_bytes=stdout_total,
        valid_utf8=stdout_valid_utf8,
    )
    stderr_stream, stderr_flags, stderr_degraded, stderr_err = build_stream(
        "stderr",
        stderr_bytes,
        blobs,
        total_bytes=stderr_total,
        valid_utf8=stderr_valid_utf8,
    )
    flags = sorted(set([*stdout_flags, *stderr_flags, *extra_flags]))
    unknown = set(flags) - FLAG_VALUES
    if unknown:
        raise ValueError(f"unknown result flags: {sorted(unknown)}")

    degraded = stdout_degraded or stderr_degraded
    binary = (
        (stdout_valid_utf8 is False)
        or (stderr_valid_utf8 is False)
        or (stdout_stream is not None and stdout_stream.get("encoding") == "binary")
        or (stderr_stream is not None and stderr_stream.get("encoding") == "binary")
    )
    stream_errors = [e for e in (stdout_err, stderr_err) if e is not None]
    decode_err = (
        error_dict(
            "output_decode_failed", "script output is not valid UTF-8 (binary content)"
        )
        if binary
        else None
    )

    if stdout_stream is None or stderr_stream is None:
        # A stream that cannot be inlined (binary) failed to persist to a blob:
        # the output is lost, so the result must be FAILED (错误矩阵_v0.1.md).
        status = "FAILED"
        degraded = True
        if error is None:
            error = stream_errors[0] if stream_errors else decode_err
        if stdout_stream is None:
            stdout_stream = _inline_empty()
        if stderr_stream is None:
            stderr_stream = _inline_empty()
    elif error is None:
        # No caller-supplied primary error: promote the strongest stream-level
        # evidence (blob_write_failed before output_decode_failed) so batch
        # aggregation counts the real class.
        error = (stream_errors[0] if stream_errors else None) or decode_err

    # Every stream-level and decode error that is not the primary error itself
    # survives as structured evidence. The filter is identity-based, not
    # class-based: when both stdout and stderr failed a blob write they share
    # blob_write_failed yet each stream's message must be preserved.
    secondary: list[dict] = []
    for extra in [*stream_errors, decode_err]:
        if extra is not None and extra is not error:
            secondary.append(extra)

    payload: dict = {
        "execution_uid": execution_uid,
        "node_id": node_id,
        "script_sha256": script.sha256,
        "status": status,
        "attempt_count": attempt_count,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_ms": duration_ms,
        "exit_code": exit_code,
        "stdout": stdout_stream,
        "stderr": stderr_stream,
        "flags": flags,
    }
    if error is not None:
        payload["error"] = error
    envelope = {
        "meta": {
            "schema_name": "contract-03-execution-result",
            "schema_version": RESULT_SCHEMA_VERSION,
            "producer": "wft.execution",
            "created_at": produced_at or now_iso(),
            "run_id": run_id,
        },
        "payload": payload,
    }
    problems = validate_execution_result(
        envelope, expected_exit_codes=script.expected_exit_codes
    )
    if problems:
        raise WFTContractError("contract-03-execution-result invalid: " + "; ".join(problems))
    return envelope, degraded, tuple(secondary)
