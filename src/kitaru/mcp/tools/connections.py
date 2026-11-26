#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Connection read and management handlers."""

import uuid
from collections.abc import Mapping

from kitaru.api_models.v1.connection import (
    ConnectionCreateRequest,
    ConnectionListParams,
    ConnectionResponse,
    ConnectionUpdateRequest,
)
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.common import PageData
from kitaru.mcp.models.connections import (
    ConnectionCreate,
    ConnectionListRequest,
    ConnectionReadRequest,
    ConnectionsManageRequest,
    ConnectionUpdate,
)
from kitaru.mcp.redaction import is_credential_name
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
        if request.env:
            await _refuse_new_credentials(state, request.connection_id, request.env)
        values = request.model_dump(
            include={"env", "secrets", "default"}, exclude_unset=True
        )
        return await state.client.connections.update(
            request.connection_id, ConnectionUpdateRequest.model_validate(values)
        )
    return await state.client.connections.update(
        request.connection_id, ConnectionUpdateRequest(default=True)
    )


async def _refuse_new_credentials(
    state: MCPServerState, connection_id: uuid.UUID, env: Mapping[str, str]
) -> None:
    named = [key for key in env if is_credential_name(key)]
    if not named:
        return
    # A connection made before `env` refused credentials can hold one there; an
    # update of its other settings sends it back unchanged and must still work.
    stored = await state.client.connections.get(connection_id)
    refused = sorted(key for key in named if stored.env.get(key) != env[key])
    if refused:
        raise MCPToolError(
            "invalid_arguments",
            f"`env` keys {', '.join(refused)} name credentials; move them to "
            "`secrets`. On update, `secrets` replaces every stored secret, so a "
            "call that sends only the moved keys deletes the others.",
            recovery="Read the connection's `secret_keys`, ask the user for each "
            "value, and send them all in `secrets` with the moved keys; or leave "
            "these keys out of `env`.",
        )
