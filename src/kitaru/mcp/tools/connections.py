#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Connection read and management handlers."""

from kitaru.api_models.v1.connection import (
    ConnectionCreateRequest,
    ConnectionListParams,
    ConnectionResponse,
    ConnectionUpdateRequest,
)
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.common import PageData
from kitaru.mcp.models.connections import (
    ConnectionCreate,
    ConnectionListRequest,
    ConnectionReadRequest,
    ConnectionsManageRequest,
    ConnectionUpdate,
)
from kitaru.mcp.references import resolve_connection
from kitaru.mcp.tools.params import build_list_params
from kitaru.mcp.tools.registry import build_page_data


async def handle_connection_read(
    state: MCPServerState, request: ConnectionReadRequest
) -> object:
    """Execute one bounded connection read."""
    if isinstance(request, ConnectionListRequest):
        params = build_list_params(ConnectionListParams, request, with_filter=False)
        if request.provider is not None:
            params.filter = FilterCondition(
                field="provider", op=FilterOp.EQ, value=request.provider
            )
        page = await state.client.connections.list(params)
        return build_page_data(page, request.size, PageData[ConnectionResponse])
    return await resolve_connection(state.client, request.reference)


async def handle_connections_manage(
    state: MCPServerState, request: ConnectionsManageRequest
) -> object:
    """Perform one connection mutation."""
    if isinstance(request, ConnectionCreate):
        return await state.client.connections.create(
            ConnectionCreateRequest(
                name=request.name,
                provider=request.provider,
                env=request.env,
                secrets=request.secrets,
                default=request.default,
            ),
            idempotency_key=request.idempotency_key,
        )
    if isinstance(request, ConnectionUpdate):
        values = request.model_dump(
            include={"env", "secrets", "default"}, exclude_unset=True
        )
        return await state.client.connections.update(
            request.connection_id, ConnectionUpdateRequest.model_validate(values)
        )
    return await state.client.connections.update(
        request.connection_id, ConnectionUpdateRequest(default=True)
    )
