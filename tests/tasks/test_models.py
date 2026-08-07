from uuid import UUID

import pytest
from pydantic import ValidationError

from wft.ids import new_uuid7
from wft.tasks.models import (
    FailureRecord,
    ScriptExecutionResult,
    ScriptStatus,
    StreamReference,
)


def _empty_stream(name: str) -> StreamReference:
    return StreamReference(path=name, size_bytes=0, sha256="0" * 64)


def _script_fields() -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "task_id": new_uuid7(),
        "node_key": "node-a-01234567",
        "script_id": "memory",
        "script_sha256": "0" * 64,
        "interpreter": "bash",
        "started_at": "2026-08-07T00:00:00Z",
        "finished_at": "2026-08-07T00:00:01Z",
        "expected_exit_codes": (0,),
        "stdout": _empty_stream("stdout.raw"),
        "stderr": _empty_stream("stderr.raw"),
        "execution_log": _empty_stream("execution.log"),
    }


def test_new_id_is_unique_uuid7() -> None:
    values = [new_uuid7() for _ in range(100)]

    assert len(values) == len(set(values))
    assert all(UUID(value).version == 7 for value in values)


def test_stream_reference_requires_safe_path_sha_and_size() -> None:
    reference = StreamReference(path="stdout.raw", size_bytes=0, sha256="0" * 64)

    assert reference.path == "stdout.raw"
    with pytest.raises(ValidationError):
        StreamReference(path="../secret", size_bytes=-1, sha256="bad")


def test_completed_script_requires_exit_code_and_check_result() -> None:
    result = ScriptExecutionResult(
        **_script_fields(),
        status=ScriptStatus.COMPLETED,
        exit_code=0,
        check_passed=True,
        failure=None,
    )

    assert result.check_passed is True
    assert result.exit_code == 0


def test_noncompleted_script_cannot_claim_check_passed() -> None:
    with pytest.raises(ValidationError, match="check_passed"):
        ScriptExecutionResult(
            **_script_fields(),
            status=ScriptStatus.TIMEOUT,
            exit_code=None,
            check_passed=True,
            failure=FailureRecord(code="TIMEOUT", message="deadline exceeded"),
        )


def test_timestamps_must_be_timezone_aware() -> None:
    fields = _script_fields()
    fields["started_at"] = "2026-08-07T00:00:00"

    with pytest.raises(ValidationError, match="timezone"):
        ScriptExecutionResult(
            **fields,
            status=ScriptStatus.FAILED,
            exit_code=None,
            check_passed=None,
            failure=FailureRecord(code="SYSTEM", message="failed"),
        )
