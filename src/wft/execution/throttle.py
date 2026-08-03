"""Concurrency and connect-rate throttling (Contract-02 limits).

The Contract-02 schema bounds ``global_concurrency`` to 1..200 and fixes
``per_node_concurrency`` at 1; defaults mirror the spec'd limits. A
:class:`RateLimiter` paces connection attempts and a ``Semaphore`` caps
concurrent SSH executions.
"""
from __future__ import annotations

import asyncio

GLOBAL_CONCURRENCY_DEFAULT = 50
PER_NODE_CONCURRENCY = 1
CONNECT_RATE_PER_SEC_DEFAULT = 20


class RateLimiter:
    """Minimal per-second rate limiter for connection attempts.

    The first call returns immediately; each later call waits until its
    scheduled slot, spacing starts at least ``1 / rate_per_sec`` apart.
    """

    def __init__(self, rate_per_sec: float) -> None:
        if rate_per_sec <= 0:
            raise ValueError("rate_per_sec must be positive")
        self._interval = 1.0 / rate_per_sec
        self._next_at = -float("inf")

    async def wait(self) -> None:
        now = asyncio.get_running_loop().time()
        start = max(now, self._next_at)
        self._next_at = start + self._interval
        delay = start - now
        if delay > 0:
            await asyncio.sleep(delay)


class Throttle:
    """Bundle of global + per-node concurrency and connect-rate limits."""

    def __init__(
        self,
        *,
        global_concurrency: int = GLOBAL_CONCURRENCY_DEFAULT,
        connect_rate_per_sec: int = CONNECT_RATE_PER_SEC_DEFAULT,
    ) -> None:
        self.global_semaphore = asyncio.Semaphore(global_concurrency)
        self.connect_limiter = RateLimiter(connect_rate_per_sec)
        self.per_node_concurrency = PER_NODE_CONCURRENCY
