#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Shared MCP review-tool context and insight builders."""

import uuid
from datetime import UTC, datetime
from typing import Any, cast

from mcp.server import MCPServer, ServerRequestContext
from mcp.server.mcpserver import Context

from kitaru.api_models.v1.insight import (
    InsightResponse,
)
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.server import create_server
from kitaru.mcp.settings import CapabilityMode, MCPSettings


def build_context(
    client: object,
    mode: CapabilityMode,
    *,
    handler_timeout: float = 120.0,
) -> tuple[MCPServer[MCPServerState], Context[MCPServerState, Any]]:
    """Build a server and request context whose state uses the given mode."""
    settings = MCPSettings(mode=mode, handler_timeout=handler_timeout)
    state = MCPServerState(settings, cast(Any, client))
    server = create_server(settings)
    request_context = ServerRequestContext(
        session=cast(Any, None),
        lifespan_context=state,
        protocol_version="2026-07-28",
        method="tools/call",
    )
    return server, Context(request_context=request_context, mcp_server=server)


def build_insight(*, agent_id: uuid.UUID) -> InsightResponse:
    """Build an insight response owned by a random account."""
    now = datetime.now(UTC)
    return InsightResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        agent_id=agent_id,
        name="latency-regressed",
        title="Latency regressed",
        description=None,
        data={"type": "text", "content": "It got slower."},
        metadata={},
        created=now,
        updated=now,
    )
