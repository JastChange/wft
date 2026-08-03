"""Retry bounds follow the error matrix (错误矩阵_v0.1.md)."""
from __future__ import annotations

import pytest

from wft.execution.retry import backoff_seconds, max_attempts, should_retry


def test_transient_max_three_attempts() -> None:
    assert max_attempts("conn_timeout") == 3
    assert max_attempts("upload_failed") == 3
    assert should_retry("conn_timeout", 1) is True
    assert should_retry("conn_timeout", 2) is True
    assert should_retry("conn_timeout", 3) is False


def test_exec_timeout_one_retry() -> None:
    assert max_attempts("exec_timeout") == 2
    assert should_retry("exec_timeout", 1) is True
    assert should_retry("exec_timeout", 2) is False


def test_permanent_security_data_no_retry() -> None:
    for cls in ("auth_failed", "exec_nonzero", "host_key_unknown", "host_key_mismatch",
                "script_integrity_failed", "output_decode_failed", "cancelled"):
        assert max_attempts(cls) == 1, cls
        assert should_retry(cls, 1) is False, cls


def test_attempt_count_within_schema_bound() -> None:
    from wft.contracts.validate import ERROR_MATRIX

    for cls in ERROR_MATRIX:
        assert max_attempts(cls) <= 3


def test_transient_backoff_grows() -> None:
    # 1s then 4s (no jitter) per the approved SSH retry plan.
    assert backoff_seconds("conn_timeout", 1, jitter=False) == 1.0
    assert backoff_seconds("conn_timeout", 2, jitter=False) == 4.0
    assert backoff_seconds("conn_timeout", 3, jitter=False) == 4.0


def test_transient_backoff_jitter_bounds() -> None:
    # Jitter scales each base by [0.5, 1.5] so retries do not pile up in lockstep.
    for retry in (1, 2, 3):
        base = 1.0 if retry <= 1 else 4.0
        value = backoff_seconds("conn_timeout", retry, jitter=True)
        assert 0.5 * base <= value <= 1.5 * base


def test_storage_backoff_is_short() -> None:
    assert backoff_seconds("db_busy", 1) == 0.1
    assert backoff_seconds("db_write_failed", 2) == 0.2
