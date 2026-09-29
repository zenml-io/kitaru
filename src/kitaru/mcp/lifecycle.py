#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Process-lifetime state and bounded MCP handler execution."""

import asyncio
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TypeVar

from kitaru.client.api_client import KitaruAPIClient
from kitaru.mcp.settings import MCPSettings

ResultT = TypeVar("ResultT")


@dataclass(slots=True)
class MCPServerState:
    """One process-lifetime client with bounded concurrency."""

    settings: MCPSettings
    client: KitaruAPIClient
    semaphore: asyncio.Semaphore = field(init=False)
    _handler_deadline: ContextVar[float | None] = field(init=False, repr=False)
    _closed: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        """Create the semaphore from the configured maximum concurrency."""
        self.semaphore = asyncio.Semaphore(self.settings.max_concurrency)
        self._handler_deadline = ContextVar("kitaru_mcp_handler_deadline", default=None)

    async def execute(self, operation: Callable[[], Awaitable[ResultT]]) -> ResultT:
        """Run one handler with bounded concurrency and timeout."""
        async with asyncio.timeout(self.settings.handler_timeout) as timeout:
            token = self._handler_deadline.set(timeout.when())
            try:
                async with self.semaphore:
                    return await operation()
            finally:
                self._handler_deadline.reset(token)

    def get_remaining_handler_time(self) -> float | None:
        """Return the remaining time for the current handler, if any."""
        deadline = self._handler_deadline.get()
        if deadline is None:
            return None
        return max(0.0, deadline - asyncio.get_running_loop().time())

    async def close(self) -> None:
        """Close the lifecycle client exactly once."""
        if not self._closed:
            self._closed = True
            await self.client.close()
