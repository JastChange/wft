import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

_UNSAFE_COMPONENT = re.compile(r"[^a-z0-9._-]+")
_NODE_KEY = re.compile(r"^[a-z0-9][a-z0-9._-]{0,79}-[0-9a-f]{12}$")
_SCRIPT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def node_key(node_name: str) -> str:
    """Return a stable filesystem-safe key without exposing a raw node name."""
    if not node_name:
        raise ValueError("node name must not be empty")
    slug = _UNSAFE_COMPONENT.sub("-", node_name.lower()).strip("-._")[:48]
    if not slug:
        slug = "node"
    digest = hashlib.sha256(node_name.encode("utf-8")).hexdigest()[:12]
    return f"{slug}-{digest}"


def validate_node_key(value: str) -> str:
    if not _NODE_KEY.fullmatch(value):
        raise ValueError("invalid node key")
    return value


def validate_script_id(value: str) -> str:
    if not _SCRIPT_ID.fullmatch(value):
        raise ValueError("invalid script ID")
    return value


def validate_task_id(value: str) -> str:
    try:
        parsed = UUID(value)
    except ValueError as exc:
        raise ValueError("invalid task UUID") from exc
    if parsed.version != 7 or str(parsed) != value:
        raise ValueError("task ID must be a canonical UUIDv7")
    return value


@dataclass(frozen=True)
class TaskLayout:
    data_dir: Path
    task_id: str

    def __post_init__(self) -> None:
        validate_task_id(self.task_id)

    @property
    def tasks_dir(self) -> Path:
        return self.data_dir / "tasks"

    @property
    def task_dir(self) -> Path:
        return self.tasks_dir / self.task_id

    @property
    def task_json(self) -> Path:
        return self.task_dir / "task.json"

    @property
    def snapshot_dir(self) -> Path:
        return self.task_dir / "snapshot"

    @property
    def inventory_json(self) -> Path:
        return self.snapshot_dir / "inventory.json"

    @property
    def scripts_json(self) -> Path:
        return self.snapshot_dir / "scripts.json"

    @property
    def nodes_dir(self) -> Path:
        return self.task_dir / "nodes"

    def node_dir(self, key: str) -> Path:
        return self.nodes_dir / validate_node_key(key)

    def node_json(self, key: str) -> Path:
        return self.node_dir(key) / "node.json"

    def scripts_dir(self, key: str) -> Path:
        return self.node_dir(key) / "scripts"

    def script_dir(self, key: str, script_id: str) -> Path:
        return self.scripts_dir(key) / validate_script_id(script_id)

    def script_result_json(self, key: str, script_id: str) -> Path:
        return self.script_dir(key, script_id) / "result.json"

    def raw_path(self, key: str, script_id: str, filename: str) -> Path:
        if filename not in {"stdout.raw", "stderr.raw", "execution.log.raw"}:
            raise ValueError("invalid raw stream filename")
        return self.script_dir(key, script_id) / filename
