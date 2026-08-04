#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Process-lifetime state and bounded MCP handler execution."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TypeVar

from kitaru.analytics.source import AnalyticsSource, current_source
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.connection import ResolvedCredential, ResolvedTarget
from kitaru.client.credential_store import CredentialStore
from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.settings import MCPSettings

ResultT = TypeVar("ResultT")
ClientFactory = Callable[[], KitaruAPIClient]


@dataclass(frozen=True, slots=True)
class MCPConnection:
    """Resolved fixed process connection without exposed credential values."""

    target: ResolvedTarget
    credential: ResolvedCredential
    credential_store: CredentialStore


@dataclass(slots=True)
class MCPServerState:
    """One process-lifetime client and concurrency/capability state."""

    settings: MCPSettings
    connection: MCPConnection
    client: KitaruAPIClient
    semaphore: asyncio.Semaphore = field(init=False)
    _features: frozenset[str] | None = field(default=None, init=False)
    _features_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    _closed: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        self.semaphore = asyncio.Semaphore(self.settings.max_concurrency)

    async def execute(self, operation: Callable[[], Awaitable[ResultT]]) -> ResultT:
        """Run one handler with bounded concurrency, timeout, and source scope."""
        token = current_source.set(AnalyticsSource.MCP)
        try:
            async with asyncio.timeout(self.settings.handler_timeout):
                async with self.semaphore:
                    return await operation()
        finally:
            current_source.reset(token)

    async def require_feature(self, feature: str) -> None:
        """Lazily discover and cache a protected server capability."""
        if self._features is None:
            async with self._features_lock:
                if self._features is None:
                    info = await self.client.info.get()
                    self._features = frozenset(info.features)
        if feature not in self._features:
            raise MCPToolError(
                "unsupported_server",
                f"The selected Kitaru server does not advertise {feature!r}.",
                recovery="Upgrade the server before retrying this protected workflow.",
            )

    async def close(self) -> None:
        """Close the lifecycle client exactly once."""
        if not self._closed:
            self._closed = True
            await self.client.close()
