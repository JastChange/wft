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


def build_stream(
    name: str,
    data: bytes,
    blobs: BlobStore,
    total_bytes: int | None = None,
) -> tuple[dict | None, list[str], bool, dict | None]:
    """Return ``(stream, flags, degraded, error)`` for a stdout/stderr stream.

    ``stream`` is ``None`` only when a stream that cannot be inlined (binary)
    failed to persist to a blob — the caller must then mark the whole result
    FAILED (错误矩阵_v0.1.md: ``blob_write_failed``). For over-cap UTF-8 the
    fallback truncates to the inline cap and degrades instead of failing.

    ``degraded`` is True for non-UTF-8 (binary) content or when a blob write
    failed and the stream had to fall back. ``error`` is a ``blob_write_failed``
    error dict when a blob write failed, else ``None``. When the SSH layer
    already capped the tail (``total_bytes`` > the data length), ``truncated``
    is decided from the original total instead of the capped ``data``.
    """
    inline_cap = INLINE_STDOUT_CAP if name == "stdout" else INLINE_STDERR_CAP
    flags: list[str] = []
    try:
        data.decode("utf-8")
        encoding = "utf-8"
    except UnicodeDecodeError:
        encoding = "binary"

    content = data if len(data) <= STREAM_HARD_CAP else data[-STREAM_HARD_CAP:]
    total = len(data) if total_bytes is None else total_bytes
    truncated = total > STREAM_HARD_CAP
    if truncated:
        flags.extend(["truncated", "output_overflow"])

    if encoding == "binary":
        try:
            blob_ref = blobs.write(content)
        except OSError as exc:
            return None, flags, True, error_dict(
                "blob_write_failed",
                f"cannot persist binary {name} to blob: {exc}",
            )
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
    try:
        blob_ref = blobs.write(content)
    except OSError as exc:
        fallback = content[-inline_cap:]
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
            error_dict(
                "blob_write_failed",
                f"blob write failed; {name} truncated to {inline_cap} bytes inline: {exc}",
            ),
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
) -> tuple[dict, bool]:
    """Build and validate a Contract-03 envelope; return ``(envelope, degraded)``.

    ``degraded`` is True when either stream held non-UTF-8 bytes; callers should
    then treat the run as DEGRADED. Raises :class:`WFTContractError` if the
    built envelope fails Contract-03 validation (a developer bug).
    """
    stdout_stream, stdout_flags, stdout_degraded, stdout_err = build_stream(
        "stdout", stdout_bytes, blobs, total_bytes=stdout_total
    )
    stderr_stream, stderr_flags, stderr_degraded, stderr_err = build_stream(
        "stderr", stderr_bytes, blobs, total_bytes=stderr_total
    )
    flags = sorted(set([*stdout_flags, *stderr_flags, *extra_flags]))
    unknown = set(flags) - FLAG_VALUES
    if unknown:
        raise ValueError(f"unknown result flags: {sorted(unknown)}")

    degraded = stdout_degraded or stderr_degraded
    if stdout_stream is None or stderr_stream is None:
        # A stream that cannot be inlined (binary) failed to persist to a blob:
        # the output is lost, so the result must be FAILED (错误矩阵_v0.1.md).
        status = "FAILED"
        degraded = True
        if error is None:
            error = stdout_err or stderr_err
        if stdout_stream is None:
            stdout_stream = _inline_empty()
        if stderr_stream is None:
            stderr_stream = _inline_empty()
    elif degraded and error is None:
        # Structured evidence for binary output: degrade the result and attach
        # an output_decode_failed error so batch aggregation can count it.
        error = error_dict(
            "output_decode_failed", "script output is not valid UTF-8 (binary content)"
        )

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
    return envelope, stdout_degraded or stderr_degraded
