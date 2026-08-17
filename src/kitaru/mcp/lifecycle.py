#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Process-lifetime state and bounded MCP handler execution."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TypeVar

from kitaru.analytics.source import (
    AnalyticsAttribution,
    AnalyticsSource,
    current_attribution,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.mcp.settings import MCPSettings

ResultT = TypeVar("ResultT")


@dataclass(slots=True)
class MCPServerState:
    """One process-lifetime client with bounded concurrency."""

    settings: MCPSettings
    client: KitaruAPIClient
    semaphore: asyncio.Semaphore = field(init=False)
    _closed: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        self.semaphore = asyncio.Semaphore(self.settings.max_concurrency)

    async def execute(self, operation: Callable[[], Awaitable[ResultT]]) -> ResultT:
        """Run one handler with bounded concurrency, timeout, and source scope."""
        token = current_attribution.set(
            AnalyticsAttribution(source=AnalyticsSource.MCP)
        )
        try:
            async with asyncio.timeout(self.settings.handler_timeout):
                async with self.semaphore:
                    return await operation()
        finally:
            current_attribution.reset(token)

    async def close(self) -> None:
        """Close the lifecycle client exactly once."""
        if not self._closed:
            self._closed = True
            await self.client.close()
