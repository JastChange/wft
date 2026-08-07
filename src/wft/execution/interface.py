from typing import Protocol

from wft.tasks.models import NodeExecutionResult, NodeSnapshot, TaskScriptSnapshot


class NodeExecutor(Protocol):
    async def execute(
        self,
        node: NodeSnapshot,
        snapshot: TaskScriptSnapshot,
    ) -> NodeExecutionResult: ...
