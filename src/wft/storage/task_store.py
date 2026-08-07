import hashlib
import os
import secrets
import shutil
from collections.abc import Iterator
from pathlib import Path

from wft.inventory.models import Node
from wft.scripts.models import ScriptDefinition
from wft.scripts.snapshot import ScriptSnapshot
from wft.storage.atomic import fsync_directory, read_versioned_json, write_atomic_json
from wft.storage.layout import TaskLayout, node_key, validate_task_id
from wft.storage.raw_streams import RawWriter, storage_write_probe
from wft.tasks.models import (
    CleanupResult,
    NodeExecutionResult,
    NodeSnapshot,
    NodeStatus,
    ScriptExecutionResult,
    TaskInventorySnapshot,
    TaskRecord,
    TaskScriptDefinition,
    TaskScriptSnapshot,
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _selected_scripts(task: TaskRecord, snapshot: ScriptSnapshot) -> tuple[ScriptDefinition, ...]:
    by_id = {script.id: script for script in snapshot.manifest.scripts}
    if task.selection.plan_id:
        plans = {plan.id: plan for plan in snapshot.manifest.plans}
        try:
            selected_ids = plans[task.selection.plan_id].scripts
        except KeyError as exc:
            raise ValueError(f"unknown plan: {task.selection.plan_id}") from exc
    else:
        selected_ids = task.selection.script_ids
    try:
        return tuple(by_id[script_id] for script_id in selected_ids)
    except KeyError as exc:
        raise ValueError(f"unknown script: {exc.args[0]}") from exc


def _validated_source(snapshot: ScriptSnapshot, definition: ScriptDefinition) -> Path:
    root = snapshot.path.resolve()
    source = (root / definition.path).resolve()
    try:
        source.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"script source escapes snapshot: {definition.id}") from exc
    if not source.is_file():
        raise ValueError(f"script source is missing: {definition.id}")
    if _sha256(source) != definition.sha256:
        raise ValueError(f"script source hash mismatch: {definition.id}")
    return source


def _copy_fsynced(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with source.open("rb") as input_stream, os.fdopen(descriptor, "wb") as output_stream:
            shutil.copyfileobj(input_stream, output_stream)
            output_stream.flush()
            os.fsync(output_stream.fileno())
    except BaseException:
        destination.unlink(missing_ok=True)
        raise
    fsync_directory(destination.parent)


class TaskStore:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir

    @property
    def tasks_dir(self) -> Path:
        return self.data_dir / "tasks"

    def create(
        self,
        task: TaskRecord,
        nodes: tuple[Node, ...],
        snapshot: ScriptSnapshot,
    ) -> None:
        storage_write_probe(self.data_dir)
        selected = _selected_scripts(task, snapshot)
        sources = tuple(
            (definition, _validated_source(snapshot, definition)) for definition in selected
        )
        self.tasks_dir.mkdir(parents=True, exist_ok=True)
        final_layout = TaskLayout(self.data_dir, task.task_id)
        if final_layout.task_dir.exists():
            raise FileExistsError(f"task already exists: {task.task_id}")
        staging = self.tasks_dir / f".{task.task_id}.{secrets.token_hex(8)}.partial"
        try:
            staging.mkdir(mode=0o700)
            node_snapshots = tuple(
                NodeSnapshot(
                    task_id=task.task_id,
                    node_key=node_key(node.name),
                    node_name=node.name,
                    host=node.host,
                    port=node.port,
                    username=node.username,
                    private_key_path=node.private_key_path,
                    os=node.os,
                    groups=node.groups,
                    tags=node.tags,
                )
                for node in nodes
            )
            script_definitions: list[TaskScriptDefinition] = []
            for definition, source in sources:
                relative = Path("snapshot/scripts") / definition.id / definition.sha256 / "source"
                _copy_fsynced(source, staging / relative)
                script_definitions.append(
                    TaskScriptDefinition(
                        id=definition.id,
                        source_path=final_layout.task_dir / relative,
                        sha256=definition.sha256,
                        interpreter=definition.interpreter,
                        timeout_seconds=definition.timeout_seconds,
                        expected_exit_codes=definition.expected_exit_codes,
                        supported_os=definition.supported_os,
                    )
                )
            task_snapshot = TaskScriptSnapshot(
                task_id=task.task_id,
                task_type=task.task_type,
                metadata=task.script_snapshot,
                scripts=tuple(script_definitions),
            )
            inventory = TaskInventorySnapshot(task_id=task.task_id, nodes=node_snapshots)
            write_atomic_json(staging / "task.json", task.model_dump(mode="json"))
            write_atomic_json(
                staging / "snapshot/inventory.json", inventory.model_dump(mode="json")
            )
            write_atomic_json(
                staging / "snapshot/manifest.json", task_snapshot.model_dump(mode="json")
            )
            for node in node_snapshots:
                node_root = staging / "nodes" / node.node_key
                write_atomic_json(node_root / "node.json", node.model_dump(mode="json"))
                initial = NodeExecutionResult(
                    task_id=task.task_id,
                    node_key=node.node_key,
                    status=NodeStatus.PENDING,
                    cleanup=CleanupResult(attempted=False, succeeded=False),
                )
                write_atomic_json(node_root / "result.json", initial.model_dump(mode="json"))
            fsync_directory(staging)
            os.rename(staging, final_layout.task_dir)
            fsync_directory(self.tasks_dir)
        finally:
            shutil.rmtree(staging, ignore_errors=True)

    def load(self, task_id: str) -> TaskRecord:
        layout = TaskLayout(self.data_dir, task_id)
        return TaskRecord.model_validate(read_versioned_json(layout.task_json))

    def load_node(self, task_id: str, key: str) -> NodeSnapshot:
        layout = TaskLayout(self.data_dir, task_id)
        return NodeSnapshot.model_validate(read_versioned_json(layout.node_json(key)))

    def load_node_result(self, task_id: str, key: str) -> NodeExecutionResult:
        layout = TaskLayout(self.data_dir, task_id)
        path = layout.node_dir(key) / "result.json"
        return NodeExecutionResult.model_validate(read_versioned_json(path))

    def update_node(self, node: NodeSnapshot) -> None:
        layout = TaskLayout(self.data_dir, node.task_id)
        write_atomic_json(layout.node_json(node.node_key), node.model_dump(mode="json"))

    def load_snapshot(self, task_id: str) -> TaskScriptSnapshot:
        layout = TaskLayout(self.data_dir, task_id)
        path = layout.snapshot_dir / "manifest.json"
        return TaskScriptSnapshot.model_validate(read_versioned_json(path))

    def update_task(self, task: TaskRecord) -> None:
        layout = TaskLayout(self.data_dir, task.task_id)
        write_atomic_json(layout.task_json, task.model_dump(mode="json"))

    def commit_node_result(self, result: NodeExecutionResult) -> None:
        layout = TaskLayout(self.data_dir, result.task_id)
        path = layout.node_dir(result.node_key) / "result.json"
        write_atomic_json(path, result.model_dump(mode="json"))

    def commit_script_result(self, result: ScriptExecutionResult) -> None:
        layout = TaskLayout(self.data_dir, result.task_id)
        path = layout.script_result_json(result.node_key, result.script_id)
        write_atomic_json(path, result.model_dump(mode="json"))

    def load_script_result(self, task_id: str, key: str, script_id: str) -> ScriptExecutionResult:
        layout = TaskLayout(self.data_dir, task_id)
        path = layout.script_result_json(key, script_id)
        return ScriptExecutionResult.model_validate(read_versioned_json(path))

    def iterate_script_results(self, task_id: str) -> Iterator[ScriptExecutionResult]:
        task = self.load(task_id)
        snapshot = self.load_snapshot(task_id)
        for key in task.node_keys:
            for script in snapshot.scripts:
                path = TaskLayout(self.data_dir, task_id).script_result_json(key, script.id)
                if path.is_file():
                    yield ScriptExecutionResult.model_validate(read_versioned_json(path))

    def raw_writer(self, task_id: str, key: str, script_id: str, filename: str) -> RawWriter:
        layout = TaskLayout(self.data_dir, task_id)
        return RawWriter(layout.raw_path(key, script_id, filename))

    def iterate_tasks(self) -> Iterator[TaskRecord]:
        if not self.tasks_dir.exists():
            return
        tasks: list[TaskRecord] = []
        for path in self.tasks_dir.iterdir():
            if not path.is_dir() or path.name.startswith("."):
                continue
            validate_task_id(path.name)
            tasks.append(self.load(path.name))
        yield from sorted(tasks, key=lambda task: (task.created_at, task.task_id), reverse=True)

    def delete(self, task_id: str) -> None:
        validate_task_id(task_id)
        task_dir = self.tasks_dir / task_id
        if task_dir.is_symlink():
            raise ValueError("task directory must not be a symlink")
        if not task_dir.exists():
            raise FileNotFoundError(f"task does not exist: {task_id}")
        tasks_root = self.tasks_dir.resolve(strict=True)
        resolved = task_dir.resolve(strict=True)
        if resolved.parent != tasks_root:
            raise ValueError("task directory is outside the tasks root")
        shutil.rmtree(task_dir)
        fsync_directory(self.tasks_dir)
