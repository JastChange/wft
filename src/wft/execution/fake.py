import asyncio
from collections import Counter
from collections.abc import Mapping

from wft.clock import format_timestamp, utc_now
from wft.tasks.models import (
    CleanupResult,
    NodeExecutionResult,
    NodeSnapshot,
    NodeStatus,
    TaskScriptSnapshot,
)


class FakeNodeExecutor:
    """Deterministic executor for orchestration tests and local demonstrations."""

    def __init__(
        self,
        *,
        delays: Mapping[str, float] | None = None,
        exceptions: Mapping[str, Exception] | None = None,
        results: Mapping[str, NodeExecutionResult] | None = None,
    ) -> None:
        self.delays = dict(delays or {})
        self.exceptions = dict(exceptions or {})
        self.results = dict(results or {})
        self.call_counts: Counter[str] = Counter()
        self.commit_shas: set[str] = set()
        self.script_orders: set[tuple[str, ...]] = set()
        self.active = 0
        self.max_active = 0

    async def execute(
        self,
        node: NodeSnapshot,
        snapshot: TaskScriptSnapshot,
    ) -> NodeExecutionResult:
        self.call_counts[node.node_name] += 1
        self.commit_shas.add(snapshot.metadata.commit_sha)
        self.script_orders.add(tuple(script.id for script in snapshot.scripts))
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        started_at = format_timestamp(utc_now())
        try:
            await asyncio.sleep(self.delays.get(node.node_name, 0))
            if node.node_name in self.exceptions:
                raise self.exceptions[node.node_name]
            if node.node_name in self.results:
                return self.results[node.node_name]
            return NodeExecutionResult(
                task_id=node.task_id,
                node_key=node.node_key,
                status=NodeStatus.COMPLETED,
                started_at=started_at,
                finished_at=format_timestamp(utc_now()),
                script_ids=tuple(script.id for script in snapshot.scripts),
                cleanup=CleanupResult(attempted=True, succeeded=True),
            )
        finally:
            self.active -= 1
