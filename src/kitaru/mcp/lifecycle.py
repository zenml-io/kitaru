#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Process-lifetime state and bounded MCP handler execution."""

import asyncio
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from typing import TypeVar

from kitaru.client.api_client import KitaruAPIClient
from kitaru.mcp.cache import TTLCache
from kitaru.mcp.models.failure_matrix import GroupRecords
from kitaru.mcp.settings import MCPSettings

ResultT = TypeVar("ResultT")


class MCPServerState:
    """Server settings, an optional Kitaru client, and bounded concurrency.

    A state built without a client runs only tools that need no Kitaru server;
    reading `client` from it raises.
    """

    def __init__(
        self, settings: MCPSettings, client: KitaruAPIClient | None = None
    ) -> None:
        """Create the semaphore from the configured maximum concurrency."""
        self.settings = settings
        self._client = client
        self.semaphore = asyncio.Semaphore(settings.max_concurrency)
        # Cell drill-downs must read the records the visible matrix was built
        # from, so each matrix keeps its records briefly under a snapshot id.
        self.matrix_snapshots: TTLCache[tuple[GroupRecords, ...]] = TTLCache(
            max_entries=4, ttl_seconds=300
        )
        self._handler_deadline: ContextVar[float | None] = ContextVar(
            "kitaru_mcp_handler_deadline", default=None
        )
        self._closed = False

    @property
    def client(self) -> KitaruAPIClient:
        """The Kitaru client.

        Raises:
            RuntimeError: The state was built without a client.
        """
        if self._client is None:
            raise RuntimeError("This MCP server state has no Kitaru client.")
        return self._client

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
        """Close the Kitaru client, if any, exactly once."""
        if not self._closed:
            self._closed = True
            if self._client is not None:
                await self._client.close()
