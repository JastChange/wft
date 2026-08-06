from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class ScriptDefinition(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,63}$")
    path: Path
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    interpreter: str
    timeout_seconds: int = Field(ge=1, le=3600)
    expected_exit_codes: tuple[int, ...] = (0,)
    read_only: bool
    supported_os: tuple[str, ...]


class DiagnosticPlan(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,63}$")
    scripts: tuple[str, ...]


class Manifest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: str
    scripts: tuple[ScriptDefinition, ...]
    plans: tuple[DiagnosticPlan, ...]
