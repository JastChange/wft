"""Deterministic loopback HTTP fault server for webhook/LLM tests."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass


@dataclass(frozen=True)
class FaultResponse:
    status: int
    body: bytes
    headers: tuple[tuple[str, str], ...] = ()
    delay_seconds: float = 0.0


SCENARIOS = {
    "timeout": FaultResponse(200, b"{}", delay_seconds=0.05),
    "rate_limited": FaultResponse(429, b'{"error":"rate limited"}', (("Retry-After", "1"),)),
    "server_error": FaultResponse(503, b'{"error":"unavailable"}'),
    "invalid_json": FaultResponse(200, b"not-json"),
}


def fault_response(name: str) -> FaultResponse:
    """Return a deterministic response for a named local fault scenario."""
    try:
        return SCENARIOS[name]
    except KeyError:
        raise ValueError(f"unknown HTTP fault scenario: {name}") from None


class HTTPFaultStub:
    def __init__(self, scenario: str):
        self.response = fault_response(scenario)
        self._server = None
        self._tasks: set[asyncio.Task] = set()

    async def __aenter__(self):
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        return self

    async def __aexit__(self, *exc_info):
        self._server.close()
        await self._server.wait_closed()
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    @property
    def url(self) -> str:
        port = self._server.sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{port}/fault"

    async def _handle(self, reader, writer) -> None:
        task = asyncio.current_task()
        self._tasks.add(task)
        try:
            await reader.readuntil(b"\r\n\r\n")
            if self.response.delay_seconds:
                await asyncio.sleep(self.response.delay_seconds)
            headers = [("Content-Length", str(len(self.response.body))), *self.response.headers]
            head = [f"HTTP/1.1 {self.response.status} Test\r\n"]
            head.extend(f"{key}: {value}\r\n" for key, value in headers)
            writer.write(("".join(head) + "\r\n").encode() + self.response.body)
            await writer.drain()
        finally:
            self._tasks.discard(task)
            writer.close()
            await writer.wait_closed()
