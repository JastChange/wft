from dataclasses import dataclass

from wft.clock import format_timestamp, utc_now
from wft.ids import new_uuid7
from wft.inventory.models import Node, NodeSelector
from wft.scripts.models import ScriptDefinition
from wft.scripts.snapshot import ScriptSnapshot
from wft.storage.layout import node_key
from wft.storage.task_store import TaskStore
from wft.tasks.models import (
    ScriptSelection,
    ScriptSnapshotMetadata,
    SelectorSnapshot,
    TaskRecord,
    TaskStatus,
    TaskSummary,
    TaskType,
)


@dataclass(frozen=True)
class CreateTaskRequest:
    task_type: TaskType
    selector: NodeSelector
    nodes: tuple[Node, ...]
    snapshot: ScriptSnapshot
    script_ids: tuple[str, ...] = ()
    plan_id: str | None = None
    concurrency: int = 10

    def __post_init__(self) -> None:
        if not self.nodes:
            raise ValueError("at least one node must be selected")
        if bool(self.script_ids) == bool(self.plan_id):
            raise ValueError("select script_ids or plan_id, but not both")
        if not 1 <= self.concurrency <= 50:
            raise ValueError("concurrency must be between 1 and 50")


def _resolve_scripts(request: CreateTaskRequest) -> tuple[ScriptDefinition, ...]:
    by_id = {script.id: script for script in request.snapshot.manifest.scripts}
    if request.plan_id:
        plans = {plan.id: plan for plan in request.snapshot.manifest.plans}
        try:
            selected_ids = plans[request.plan_id].scripts
        except KeyError as exc:
            raise ValueError(f"unknown plan: {request.plan_id}") from exc
    else:
        selected_ids = request.script_ids
    if len(set(selected_ids)) != len(selected_ids):
        raise ValueError("script selection contains duplicates")
    try:
        selected = tuple(by_id[script_id] for script_id in selected_ids)
    except KeyError as exc:
        raise ValueError(f"unknown script: {exc.args[0]}") from exc
    for script in selected:
        for node in request.nodes:
            if node.os not in script.supported_os:
                raise ValueError(
                    f"script {script.id} does not support node OS {node.os} on {node.name}"
                )
    return selected


def create_task(store: TaskStore, request: CreateTaskRequest) -> TaskRecord:
    _resolve_scripts(request)
    task_id = new_uuid7()
    metadata = ScriptSnapshotMetadata(
        repository_url=request.snapshot.repository_url,
        branch=request.snapshot.branch,
        commit_sha=request.snapshot.commit_sha,
        commit_time=request.snapshot.commit_time,
        manifest_sha256=request.snapshot.manifest_sha256,
        used_cached_snapshot=request.snapshot.used_cache,
    )
    keys = tuple(node_key(node.name) for node in request.nodes)
    if len(set(keys)) != len(keys):
        raise ValueError("selected node names must be unique")
    selection = ScriptSelection(script_ids=request.script_ids, plan_id=request.plan_id)
    task = TaskRecord(
        task_id=task_id,
        task_type=request.task_type,
        status=TaskStatus.PENDING,
        created_at=format_timestamp(utc_now()),
        selector=SelectorSnapshot(
            node_names=request.selector.node_names,
            groups=request.selector.groups,
            tags=request.selector.tags,
            all_enabled=request.selector.all_enabled,
        ),
        node_keys=keys,
        script_snapshot=metadata,
        selection=selection,
        concurrency=request.concurrency,
        summary=TaskSummary(
            nodes_total=len(request.nodes),
            nodes_completed=0,
            nodes_failed=0,
            nodes_cancelled=0,
            problems=0,
        ),
    )
    store.create(task, request.nodes, request.snapshot)
    return task
