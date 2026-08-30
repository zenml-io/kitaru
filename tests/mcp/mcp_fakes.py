#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Shared in-process MCP server builders and SDK client fakes."""

from collections.abc import Awaitable, Callable
from typing import Any, cast

from mcp.server import MCPServer, ServerRequestContext
from mcp.server.mcpserver import Context

from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.server import create_server
from kitaru.mcp.settings import CapabilityMode, MCPSettings


def build_server_context(
    client: object, *, mode: CapabilityMode = CapabilityMode.READ_ONLY
) -> tuple[MCPServer[MCPServerState], Context[MCPServerState, Any]]:
    """Build a server and request context bound to a fake SDK client."""
    state = MCPServerState(MCPSettings(), cast(Any, client))
    server = create_server(MCPSettings(mode=mode))
    request_context = ServerRequestContext(
        session=cast(Any, None),
        lifespan_context=state,
        protocol_version="2026-07-28",
        method="tools/call",
    )
    return server, Context(request_context=request_context, mcp_server=server)


class _NullResource:
    """Resource whose every method reports the target as missing."""

    def __getattr__(self, name: str) -> Callable[..., Awaitable[object]]:
        async def _missing(*_args: object, **_kwargs: object) -> object:
            raise MCPToolError("not_found", f"null client has no data for {name}")

        return _missing


class NullClient:
    """SDK client fake with every resource present and empty."""

    def __getattr__(self, name: str) -> _NullResource:
        return _NullResource()

    async def close(self) -> None:
        return None
