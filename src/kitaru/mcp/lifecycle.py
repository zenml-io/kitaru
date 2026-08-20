#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Process-lifetime state and bounded MCP handler execution."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TypeVar

from kitaru.client.api_client import KitaruAPIClient
from kitaru.exports.operation import (
    ExportOperationRevoked,
    ExportOperationState,
    ExportOperationStateMachine,
)
from kitaru.mcp.settings import MCPSettings

ResultT = TypeVar("ResultT")


@dataclass(slots=True)
class MCPServerState:
    """One process-lifetime client with bounded concurrency."""

    settings: MCPSettings
    client: KitaruAPIClient
    semaphore: asyncio.Semaphore = field(init=False)
    _closed: bool = field(default=False, init=False)
    _active_exports: dict[asyncio.Task[object], ExportOperationStateMachine] = field(
        default_factory=dict, init=False
    )

    def __post_init__(self) -> None:
        """Create the semaphore from the configured maximum concurrency."""
        self.semaphore = asyncio.Semaphore(self.settings.max_concurrency)

    async def execute(self, operation: Callable[[], Awaitable[ResultT]]) -> ResultT:
        """Run one handler with bounded concurrency and timeout."""
        async with asyncio.timeout(self.settings.handler_timeout):
            async with self.semaphore:
                return await operation()

    @property
    def active_export_count(self) -> int:
        """Return the number of export workers that have not joined."""
        return len(self._active_exports)

    async def execute_export(
        self,
        operation: Callable[[ExportOperationStateMachine], Awaitable[ResultT]],
    ) -> ResultT:
        """Run an export until revocation is acknowledged or commit finishes."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.settings.handler_timeout
        async with asyncio.timeout_at(deadline):
            await self.semaphore.acquire()
        if self._closed:
            self.semaphore.release()
            raise RuntimeError("The MCP server is closing.")

        export_state = ExportOperationStateMachine()

        async def run() -> ResultT:
            return await operation(export_state)

        worker = asyncio.create_task(run())
        self._active_exports[worker] = export_state
        try:
            try:
                async with asyncio.timeout_at(deadline):
                    return await asyncio.shield(worker)
            except TimeoutError as timeout_error:
                revocation_won = export_state.request_revocation()
                try:
                    result = await self._join_export(worker)
                except ExportOperationRevoked:
                    if (
                        revocation_won
                        and export_state.state is ExportOperationState.CANCELLED
                    ):
                        raise timeout_error from None
                    raise
                if revocation_won:
                    if export_state.state is ExportOperationState.CANCELLED:
                        raise timeout_error
                    raise RuntimeError(
                        "The export did not acknowledge timeout revocation."
                    ) from None
                return result
            except asyncio.CancelledError as cancellation:
                current = asyncio.current_task()
                if current is not None:
                    current.uncancel()
                revocation_won = export_state.request_revocation()
                try:
                    result = await self._join_export(worker)
                except ExportOperationRevoked:
                    if (
                        revocation_won
                        and export_state.state is ExportOperationState.CANCELLED
                    ):
                        raise cancellation from None
                    raise
                if revocation_won:
                    if export_state.state is ExportOperationState.CANCELLED:
                        raise cancellation
                    raise RuntimeError(
                        "The export did not acknowledge caller cancellation."
                    ) from None
                return result
        finally:
            self._active_exports.pop(worker, None)
            self.semaphore.release()

    async def _join_export(self, worker: asyncio.Task[ResultT]) -> ResultT:
        """Join one shielded worker despite repeated caller cancellation."""
        while True:
            try:
                return await asyncio.shield(worker)
            except asyncio.CancelledError:
                if worker.done():
                    return worker.result()
                current = asyncio.current_task()
                if current is not None:
                    current.uncancel()

    async def close(self) -> None:
        """Join active exports, then close the lifecycle client exactly once."""
        if not self._closed:
            self._closed = True
            active = tuple(self._active_exports.items())
            for _, operation in active:
                operation.request_revocation()
            if active:
                await asyncio.gather(
                    *(self._join_export(worker) for worker, _ in active),
                    return_exceptions=True,
                )
            await self.client.close()
