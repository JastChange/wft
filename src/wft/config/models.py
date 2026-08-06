from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class ScriptRepositoryConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    url: str
    branch: str = "main"
    deploy_key_path: Path


class WebConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    bind_host: str
    port: int = Field(default=8080, ge=1, le=65535)


class AppConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    data_dir: Path
    inventory_path: Path
    script_repository: ScriptRepositoryConfig
    web: WebConfig
    default_concurrency: int = Field(default=10, ge=1, le=50)
