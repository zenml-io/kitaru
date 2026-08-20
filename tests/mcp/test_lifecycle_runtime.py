#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Lifecycle, concurrency, timeout, cancellation, and capability tests."""

import asyncio
from typing import Any, cast

import pytest

from kitaru.exports.operation import (
    ExportOperationRevoked,
    ExportOperationStateMachine,
)
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.settings import MCPSettings


class FakeClient:
    """Small lifecycle fake."""

    def __init__(self) -> None:
        self.close_count = 0

    async def close(self) -> None:
        self.close_count += 1


def _get_state(client: FakeClient, **settings: Any) -> MCPServerState:
    return MCPServerState(
        settings=MCPSettings(**settings),
        client=cast(Any, client),
    )


async def test_close_is_exactly_once() -> None:
    client = FakeClient()
    state = _get_state(client)
    await state.close()
    await state.close()
    assert client.close_count == 1


async def test_concurrency_is_bounded() -> None:
    client = FakeClient()
    state = _get_state(client, max_concurrency=1)
    active = 0
    maximum = 0

    async def operation() -> str:
        nonlocal active, maximum
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


async def test_timeout_and_cancellation_propagate() -> None:
    state = _get_state(FakeClient(), handler_timeout=0.01)

    async def blocked() -> None:
        await asyncio.sleep(10)

    with pytest.raises(TimeoutError):
        await state.execute(blocked)

    state = _get_state(FakeClient(), handler_timeout=10)
    task = asyncio.create_task(state.execute(blocked))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


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


async def test_export_timeout_revokes_and_joins_worker() -> None:
    state = _get_state(FakeClient(), handler_timeout=0.01)
    started = asyncio.Event()
    joined = False

    async def export(operation: ExportOperationStateMachine) -> None:
        nonlocal joined
        started.set()
        await asyncio.to_thread(operation.wait_for_revocation, 5)
        try:
            operation.checkpoint()
        except ExportOperationRevoked:
            operation.mark_cancelled()
            joined = True
            raise

    with pytest.raises(TimeoutError):
        await state.execute_export(export)

    assert started.is_set()
    assert joined is True
    assert state.active_export_count == 0


async def test_export_caller_cancellation_joins_worker_before_returning() -> None:
    state = _get_state(FakeClient(), handler_timeout=10)
    started = asyncio.Event()
    joined = False

    async def export(operation: ExportOperationStateMachine) -> None:
        nonlocal joined
        started.set()
        await asyncio.to_thread(operation.wait_for_revocation, 5)
        try:
            operation.checkpoint()
        except ExportOperationRevoked:
            operation.mark_cancelled()
            joined = True
            raise

    task = asyncio.create_task(state.execute_export(export))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert joined is True
    assert state.active_export_count == 0


async def test_export_cancellation_after_commit_authority_returns_actual_result() -> (
    None
):
    state = _get_state(FakeClient(), handler_timeout=10)
    commit_started = asyncio.Event()
    release_commit = asyncio.Event()

    async def export(operation: ExportOperationStateMachine) -> str:
        assert operation.try_start_commit() is True
        commit_started.set()
        await release_commit.wait()
        operation.mark_completed()
        return "published"

    task = asyncio.create_task(state.execute_export(export))
    await commit_started.wait()
    task.cancel()
    release_commit.set()

    assert await task == "published"
    assert state.active_export_count == 0


async def test_export_timeout_after_commit_authority_returns_actual_result() -> None:
    state = _get_state(FakeClient(), handler_timeout=0.01)
    release_commit = asyncio.Event()

    async def export(operation: ExportOperationStateMachine) -> str:
        assert operation.try_start_commit() is True
        asyncio.get_running_loop().call_later(0.02, release_commit.set)
        await release_commit.wait()
        operation.mark_completed()
        return "published"

    assert await state.execute_export(export) == "published"
    assert state.active_export_count == 0


async def test_close_revokes_and_joins_active_exports_before_client_close() -> None:
    client = FakeClient()
    state = _get_state(client, handler_timeout=10)
    started = asyncio.Event()
    joined = False

    async def export(operation: ExportOperationStateMachine) -> None:
        nonlocal joined
        started.set()
        await asyncio.to_thread(operation.wait_for_revocation, 5)
        try:
            operation.checkpoint()
        except ExportOperationRevoked:
            operation.mark_cancelled()
            joined = True
            raise

    task = asyncio.create_task(state.execute_export(export))
    await started.wait()
    await state.close()

    assert joined is True
    assert client.close_count == 1
    with pytest.raises(ExportOperationRevoked):
        await task
