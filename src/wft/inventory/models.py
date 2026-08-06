from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class Node(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
    host: str
    port: int = Field(default=22, ge=1, le=65535)
    username: str
    private_key_path: Path
    os: str
    groups: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    enabled: bool = True


class Inventory(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    nodes: tuple[Node, ...]


class NodeSelector(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    node_names: tuple[str, ...] = ()
    groups: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    all_enabled: bool = False
