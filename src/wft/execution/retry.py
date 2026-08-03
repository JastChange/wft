"""Retry bounds from the error matrix (错误矩阵_v0.1.md).

Per attempt a fresh ``attempt_id`` is minted, while ``execution_uid`` stays
stable across retries so Contract-03 idempotency (AC-011) holds at the node
level. The Contract-03 schema caps ``attempt_count`` at 3.
"""
from __future__ import annotations

from wft.contracts.validate import ERROR_MATRIX

NO_RETRY_BEFORE = {"PERMANENT", "SECURITY", "DATA"}


def max_attempts(error_class: str) -> int:
    """Return the maximum attempt_count (1-based) for an error class."""
    category, _ = ERROR_MATRIX[error_class]
    if category == "TRANSIENT":
        return 3  # up to 2 retries
    if category == "RESOURCE" and error_class == "exec_timeout":
        return 2  # exactly 1 retry
    return 1  # PERMANENT / SECURITY / DATA: no retry


def should_retry(error_class: str, attempt_count: int) -> bool:
    """Return True when a node may still be retried after this attempt."""
    return attempt_count < max_attempts(error_class)


def backoff_seconds(error_class: str, retry_number: int) -> float:
    """Short exponential backoff between retries (1s, 2s, 4s for TRANSIENT).

    Storage-layer retries (``db_busy``/``db_write_failed``) use a much shorter
    fixed backoff; the SSH layer never sees those classes.
    """
    if error_class in ("db_busy", "db_write_failed"):
        return 0.1 * retry_number
    return float(2 ** (retry_number - 1))
