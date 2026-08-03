from __future__ import annotations

import asyncio
import json

import pytest

from http_fault_stubs import HTTPFaultStub, fault_response


async def _get(url: str, timeout: float = 0.5):
    port = int(url.rsplit(":", 1)[1].split("/", 1)[0])
    reader, writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port), timeout)
    try:
        writer.write(b"GET /fault HTTP/1.1\r\nHost: localhost\r\n\r\n")
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(), timeout)
    finally:
        writer.close()
        await writer.wait_closed()
    head, body = raw.split(b"\r\n\r\n", 1)
    lines = head.decode().split("\r\n")
    return int(lines[0].split()[1]), dict(line.split(": ", 1) for line in lines[1:]), body


@pytest.mark.parametrize("scenario,status", [("rate_limited", 429), ("server_error", 503)])
def test_http_stub_returns_real_fault(scenario: str, status: int) -> None:
    async def run():
        async with HTTPFaultStub(scenario) as stub:
            return await _get(stub.url)
    actual, headers, _ = asyncio.run(run())
    assert actual == status
    if status == 429:
        assert headers["Retry-After"] == "1"


def test_http_stub_timeout_is_real_and_subsecond() -> None:
    async def run():
        async with HTTPFaultStub("timeout") as stub:
            with pytest.raises(asyncio.TimeoutError):
                await _get(stub.url, timeout=0.01)
    asyncio.run(run())


def test_http_stub_timeout_handler_is_cancelled_on_exit() -> None:
    async def run():
        stub = HTTPFaultStub("timeout")
        async with stub:
            task = asyncio.create_task(_get(stub.url, timeout=0.01))
            with pytest.raises(asyncio.TimeoutError):
                await task
            assert stub._tasks
        assert not stub._tasks
    asyncio.run(run())


def test_invalid_json_stub_is_not_silently_validated() -> None:
    async def run():
        async with HTTPFaultStub("invalid_json") as stub:
            return await _get(stub.url)
    _, _, body = asyncio.run(run())
    with pytest.raises(json.JSONDecodeError):
        json.loads(body)


def test_unknown_fault_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown HTTP fault"):
        fault_response("missing")
