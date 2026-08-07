from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from wft.clock import validate_timestamp


class TaskType(StrEnum):
    INSTALLATION_VALIDATION = "installation_validation"
    FAULT_DIAGNOSIS = "fault_diagnosis"


class TaskStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class NodeStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ScriptStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    TIMEOUT = "TIMEOUT"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class InstallationConclusion(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    INCONCLUSIVE = "INCONCLUSIVE"


class FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


def _validate_uuid7(value: str) -> str:
    parsed = UUID(value)
    if parsed.version != 7:
        raise ValueError("task_id must be UUIDv7")
    return str(parsed)


class FailureRecord(FrozenModel):
    code: str = Field(min_length=1, max_length=64)
    message: str = Field(min_length=1)
    phase: str | None = None


class StreamReference(FrozenModel):
    path: str
    size_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("path")
    @classmethod
    def path_is_local_filename(cls, value: str) -> str:
        parsed = PurePosixPath(value)
        if parsed.is_absolute() or ".." in parsed.parts or len(parsed.parts) != 1:
            raise ValueError("stream path must be one local filename")
        return value


class HostKeyRecord(FrozenModel):
    algorithm: str
    fingerprint: str
    accepted_automatically: bool = True


class SelectorSnapshot(FrozenModel):
    node_names: tuple[str, ...] = ()
    groups: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    all_enabled: bool = False


class ScriptSelection(FrozenModel):
    script_ids: tuple[str, ...] = ()
    plan_id: str | None = None

    @model_validator(mode="after")
    def exactly_one_selection_mode(self) -> Self:
        if bool(self.script_ids) == bool(self.plan_id):
            raise ValueError("select script_ids or plan_id, but not both")
        return self


class ScriptSnapshotMetadata(FrozenModel):
    repository_url: str
    branch: str
    commit_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    commit_time: str
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    used_cached_snapshot: bool

    _commit_time = field_validator("commit_time")(validate_timestamp)


class TaskScriptDefinition(FrozenModel):
    id: str
    source_path: Path
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    interpreter: str
    timeout_seconds: int = Field(ge=1, le=3600)
    expected_exit_codes: tuple[int, ...]
    supported_os: tuple[str, ...]


class TaskScriptSnapshot(FrozenModel):
    schema_version: str = "1.0"
    task_id: str
    task_type: TaskType
    metadata: ScriptSnapshotMetadata
    scripts: tuple[TaskScriptDefinition, ...]

    _task_id = field_validator("task_id")(_validate_uuid7)


class TaskInventorySnapshot(FrozenModel):
    schema_version: str = "1.0"
    task_id: str
    nodes: tuple["NodeSnapshot", ...]

    _task_id = field_validator("task_id")(_validate_uuid7)


class NodeSnapshot(FrozenModel):
    schema_version: str = "1.0"
    task_id: str
    node_key: str
    node_name: str
    host: str
    port: int = Field(ge=1, le=65535)
    username: str
    private_key_path: Path
    os: str
    groups: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    host_key: HostKeyRecord | None = None

    _task_id = field_validator("task_id")(_validate_uuid7)


class ScriptExecutionResult(FrozenModel):
    schema_version: str = "1.0"
    task_id: str
    node_key: str
    script_id: str
    script_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    interpreter: str
    status: ScriptStatus
    started_at: str | None = None
    finished_at: str | None = None
    exit_code: int | None = Field(default=None, ge=0, le=255)
    expected_exit_codes: tuple[int, ...]
    check_passed: bool | None
    stdout: StreamReference
    stderr: StreamReference
    execution_log: StreamReference
    failure: FailureRecord | None = None

    _task_id = field_validator("task_id")(_validate_uuid7)
    _started_at = field_validator("started_at")(
        lambda value: validate_timestamp(value) if value else value
    )
    _finished_at = field_validator("finished_at")(
        lambda value: validate_timestamp(value) if value else value
    )

    @model_validator(mode="after")
    def status_fields_are_consistent(self) -> Self:
        if self.status is ScriptStatus.COMPLETED:
            if self.exit_code is None or self.check_passed is None:
                raise ValueError("COMPLETED script requires exit_code and check_passed")
        elif self.check_passed is not None:
            raise ValueError("check_passed must be null unless script is COMPLETED")
        if self.status not in {ScriptStatus.PENDING, ScriptStatus.RUNNING} and not self.finished_at:
            raise ValueError("terminal script requires finished_at")
        return self


class CleanupResult(FrozenModel):
    attempted: bool
    succeeded: bool
    error: str | None = None


class NodeExecutionResult(FrozenModel):
    schema_version: str = "1.0"
    task_id: str
    node_key: str
    status: NodeStatus
    installation_conclusion: InstallationConclusion | None = None
    started_at: str | None = None
    finished_at: str | None = None
    script_ids: tuple[str, ...] = ()
    cleanup: CleanupResult
    failure: FailureRecord | None = None
    host_key: HostKeyRecord | None = None

    _task_id = field_validator("task_id")(_validate_uuid7)
    _started_at = field_validator("started_at")(
        lambda value: validate_timestamp(value) if value else value
    )
    _finished_at = field_validator("finished_at")(
        lambda value: validate_timestamp(value) if value else value
    )

    @model_validator(mode="after")
    def terminal_node_has_finished_at(self) -> Self:
        if self.status not in {NodeStatus.PENDING, NodeStatus.RUNNING} and not self.finished_at:
            raise ValueError("terminal node requires finished_at")
        return self


class TaskSummary(FrozenModel):
    nodes_total: int = Field(ge=0)
    nodes_completed: int = Field(ge=0)
    nodes_failed: int = Field(ge=0)
    nodes_cancelled: int = Field(ge=0)
    problems: int = Field(ge=0)


class TaskRecord(FrozenModel):
    schema_version: str = "1.0"
    task_id: str
    task_type: TaskType
    status: TaskStatus
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    selector: SelectorSnapshot
    node_keys: tuple[str, ...]
    script_snapshot: ScriptSnapshotMetadata
    selection: ScriptSelection
    concurrency: int = Field(ge=1, le=50)
    summary: TaskSummary
    exit_code: int | None = Field(default=None, ge=0, le=2)
    failure: FailureRecord | None = None

    _task_id = field_validator("task_id")(_validate_uuid7)
    _created_at = field_validator("created_at")(validate_timestamp)
    _started_at = field_validator("started_at")(
        lambda value: validate_timestamp(value) if value else value
    )
    _finished_at = field_validator("finished_at")(
        lambda value: validate_timestamp(value) if value else value
    )

    @model_validator(mode="after")
    def terminal_task_has_outcome(self) -> Self:
        terminal = self.status in {
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        }
        if terminal and (self.finished_at is None or self.exit_code is None):
            raise ValueError("terminal task requires finished_at and exit_code")
        if not terminal and self.finished_at is not None:
            raise ValueError("nonterminal task cannot have finished_at")
        return self
