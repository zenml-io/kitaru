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
"""Connection routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.connection import (
    ConnectionCreateRequest,
    ConnectionListParams,
    ConnectionResponse,
    ConnectionUpdateRequest,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_connection_service,
)
from kitaru.server.adapters.rest.mapping.connections import (
    connection_create_to_command,
    connection_list_params_to_filter,
    connection_to_response,
)
from kitaru.server.adapters.rest.responses import error_responses
from kitaru.server.adapters.rest.route import KitaruAPIRoute, idempotent
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.connection_service import ConnectionService

router = APIRouter(route_class=KitaruAPIRoute)


@router.post(
    "", status_code=status.HTTP_201_CREATED, responses=error_responses(400, 409)
)
@idempotent
async def create_connection(
    body: ConnectionCreateRequest,
    service: Annotated[ConnectionService, Depends(get_connection_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ConnectionResponse:
    """Create a connection.

    Clients observe HTTP 201 on success, 409 when the name is already
    registered, and 422 when a key uses the reserved prefix or is set as
    both an env value and a secret. The response omits the secret values.

    Args:
        body: Connection create request.
        service: Connection service.
        actor: Caller context.

    Returns:
        Created connection without secret values.
    """
    command = connection_create_to_command(body)
    connection = await service.create_connection(command, actor=actor)
    return connection_to_response(connection, await service.get_secret_keys(connection))


@router.get("")
async def list_connections(
    service: Annotated[ConnectionService, Depends(get_connection_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[ConnectionListParams, Query()],
) -> Page[ConnectionResponse]:
    """List connections.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters. List responses never include secret values.

    Args:
        service: Connection service.
        actor: Caller context.
        params: Connection list params.

    Returns:
        Page of connections without secret values.
    """
    connection_filter = connection_list_params_to_filter(params)
    connections, next_cursor = await service.list_connections(
        connection_filter, actor=actor
    )
    return Page[ConnectionResponse](
        items=[
            connection_to_response(
                connection, await service.get_secret_keys(connection)
            )
            for connection in connections
        ],
        next_cursor=next_cursor,
    )


@router.get("/{connection_id}", responses=error_responses(404))
async def get_connection(
    connection_id: uuid.UUID,
    service: Annotated[ConnectionService, Depends(get_connection_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ConnectionResponse:
    """Get a connection by id.

    Clients observe HTTP 200 on success and 404 when no connection has this
    id.

    Args:
        connection_id: Id of the connection.
        service: Connection service.
        actor: Caller context.

    Returns:
        Stored connection without secret values.
    """
    connection = await service.get_connection(connection_id, actor=actor)
    return connection_to_response(connection, await service.get_secret_keys(connection))


@router.patch("/{connection_id}", responses=error_responses(404))
async def update_connection(
    connection_id: uuid.UUID,
    body: ConnectionUpdateRequest,
    service: Annotated[ConnectionService, Depends(get_connection_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ConnectionResponse:
    """Update a connection, merging env and secrets by key.

    Clients observe HTTP 200 on success, 404 when no connection has this id,
    and 422 when a key uses the reserved prefix or is set as both an env
    value and a secret.

    Args:
        connection_id: Id of the connection.
        body: Connection update request.
        service: Connection service.
        actor: Caller context.

    Returns:
        Updated connection without secret values.
    """
    connection = await service.update_connection(
        connection_id,
        env=body.env,
        secrets=body.secrets,
        default=body.default,
        actor=actor,
    )
    return connection_to_response(connection, await service.get_secret_keys(connection))


@router.delete(
    "/{connection_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=error_responses(404),
)
async def delete_connection(
    connection_id: uuid.UUID,
    service: Annotated[ConnectionService, Depends(get_connection_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a connection and the secret holding its values.

    Clients observe HTTP 204 on success and 404 when no connection has this
    id.

    Args:
        connection_id: Id of the connection.
        service: Connection service.
        actor: Caller context.
    """
    await service.delete_connection(connection_id, actor=actor)
