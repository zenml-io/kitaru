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
"""Connection DTO conversions."""

from kitaru.api_models.v1.connection import (
    ConnectionCreateRequest,
    ConnectionListParams,
    ConnectionResponse,
)
from kitaru.server.adapters.rest.mapping.filtering import filter_to_expression
from kitaru.server.application.models.connection import (
    ConnectionCreate,
    ConnectionFilter,
)
from kitaru.server.domain.connection import Connection


def connection_create_to_command(body: ConnectionCreateRequest) -> ConnectionCreate:
    """Convert a connection create request to its command.

    Args:
        body: Connection create request.

    Returns:
        Connection create command.
    """
    return ConnectionCreate(
        name=body.name,
        provider=body.provider,
        env=body.env,
        secrets=body.secrets,
        default=body.default,
    )


def connection_to_response(
    connection: Connection, secret_keys: list[str]
) -> ConnectionResponse:
    """Convert a connection entity to its response DTO.

    Args:
        connection: Stored connection.
        secret_keys: Key names held by the connection's internal secret.

    Returns:
        Connection response.
    """
    assert connection.created is not None
    assert connection.updated is not None
    return ConnectionResponse(
        id=connection.id,
        owner_id=connection.owner_id,
        name=connection.name,
        provider=connection.provider,
        env=connection.env,
        secret_id=connection.secret_id,
        secret_keys=secret_keys,
        default=connection.default,
        created=connection.created,
        updated=connection.updated,
    )


def connection_list_params_to_filter(
    params: ConnectionListParams,
) -> ConnectionFilter:
    """Convert connection list params to the application filter.

    Args:
        params: Connection list params.

    Returns:
        Connection filter.
    """
    return ConnectionFilter(
        expression=filter_to_expression(params.filter)
        if params.filter is not None
        else None,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )
