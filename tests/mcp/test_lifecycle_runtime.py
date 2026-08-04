#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Lifecycle, concurrency, timeout, cancellation, and capability tests."""

import asyncio
from types import SimpleNamespace
from typing import Any, cast

import pytest

from kitaru.analytics.source import AnalyticsSource, current_source
from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.lifecycle import MCPConnection, MCPServerState
from kitaru.mcp.settings import MCPSettings


class FakeClient:
    """Small lifecycle fake."""

    def __init__(self, features: list[str] | None = None) -> None:
        self.close_count = 0
        self.info_count = 0
        self.info = SimpleNamespace(get=self._get_info)
        self.features = features or []

    async def _get_info(self) -> object:
        self.info_count += 1
        await asyncio.sleep(0)
        return SimpleNamespace(features=self.features)

    async def close(self) -> None:
        self.close_count += 1


def _get_state(client: FakeClient, **settings: Any) -> MCPServerState:
    return MCPServerState(
        settings=MCPSettings(**settings),
        connection=cast(MCPConnection, object()),
        client=cast(Any, client),
    )


async def test_close_is_exactly_once() -> None:
    client = FakeClient()
    state = _get_state(client)
    await state.close()
    await state.close()
    assert client.close_count == 1


async def test_concurrency_is_bounded_and_source_resets() -> None:
    client = FakeClient()
    state = _get_state(client, max_concurrency=1)
    active = 0
    maximum = 0

    async def operation() -> str:
        nonlocal active, maximum
        assert current_source.get() is AnalyticsSource.MCP
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.02)
        active -= 1
        return "ok"

    assert await asyncio.gather(state.execute(operation), state.execute(operation)) == [
        "ok",
        "ok",
    ]
    assert maximum == 1
    assert current_source.get() is AnalyticsSource.PYTHON


async def test_timeout_and_cancellation_propagate_and_reset_source() -> None:
    state = _get_state(FakeClient(), handler_timeout=0.01)

    async def blocked() -> None:
        await asyncio.sleep(10)

    with pytest.raises(TimeoutError):
        await state.execute(blocked)
    assert current_source.get() is AnalyticsSource.PYTHON

    state = _get_state(FakeClient(), handler_timeout=10)
    task = asyncio.create_task(state.execute(blocked))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert current_source.get() is AnalyticsSource.PYTHON


async def test_handler_timeout_includes_concurrency_queue_time() -> None:
    state = _get_state(FakeClient(), handler_timeout=0.01, max_concurrency=1)
    await state.semaphore.acquire()

    async def must_not_run() -> None:
        raise AssertionError("queued operation ran after its timeout")

    try:
        with pytest.raises(TimeoutError):
            await state.execute(must_not_run)
    finally:
        state.semaphore.release()
    assert current_source.get() is AnalyticsSource.PYTHON


async def test_capability_discovery_is_lazy_concurrent_and_cached() -> None:
    client = FakeClient(["idempotency.v1"])
    state = _get_state(client)
    await asyncio.gather(
        state.require_feature("idempotency.v1"),
        state.require_feature("idempotency.v1"),
    )
    assert client.info_count == 1

    unsupported = _get_state(FakeClient())
    with pytest.raises(MCPToolError, match="does not advertise") as raised:
        await unsupported.require_feature("idempotency.v1")
    assert raised.value.code == "unsupported_server"
