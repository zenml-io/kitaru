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
"""Secret routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.secret import (
    SecretCreateRequest,
    SecretListParams,
    SecretResponse,
    SecretUpdateRequest,
    SecretWithValuesResponse,
)
from kitaru.server.adapters.rest.commit_route import CommitRoute, idempotent
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_secret_service,
)
from kitaru.server.adapters.rest.mapping.secrets import (
    secret_list_params_to_filter,
    secret_to_response,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.secret_service import SecretService

router = APIRouter(route_class=CommitRoute)


@router.post("", status_code=status.HTTP_201_CREATED)
@idempotent
async def create_secret(
    body: SecretCreateRequest,
    service: Annotated[SecretService, Depends(get_secret_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> SecretResponse:
    """Create a secret.

    Clients observe HTTP 201 on success, 409 when the name is already
    registered, and 422 on invalid input. The response omits the secret
    values.

    Args:
        body: Secret create request.
        service: Secret service.
        actor: Caller context.

    Returns:
        Created secret without values.
    """
    secret = await service.create_secret(
        name=body.name, type=body.type, values=body.values, actor=actor
    )
    return secret_to_response(secret)


@router.get("")
async def list_secrets(
    service: Annotated[SecretService, Depends(get_secret_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[SecretListParams, Query()],
) -> Page[SecretResponse]:
    """List secrets.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters. List responses never include secret values.

    Args:
        service: Secret service.
        actor: Caller context.
        params: Secret list params.

    Returns:
        Page of secrets without values.
    """
    secret_filter = secret_list_params_to_filter(params)
    secrets, next_cursor = await service.list_secrets(secret_filter, actor=actor)
    return Page[SecretResponse](
        items=[secret_to_response(secret) for secret in secrets],
        next_cursor=next_cursor,
    )


@router.get("/{secret_id}")
async def get_secret(
    secret_id: uuid.UUID,
    service: Annotated[SecretService, Depends(get_secret_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    include_values: bool = False,
) -> SecretResponse | SecretWithValuesResponse:
    """Get a secret by id.

    Clients observe HTTP 200 on success and 404 when no secret has this
    id.

    Args:
        secret_id: Id of the secret.
        service: Secret service.
        actor: Caller context.
        include_values: Whether to include the secret values.

    Returns:
        Stored secret.
    """
    secret = await service.get_secret(secret_id, actor=actor)
    return secret_to_response(secret, include_values=include_values)


@router.patch("/{secret_id}")
async def update_secret(
    secret_id: uuid.UUID,
    body: SecretUpdateRequest,
    service: Annotated[SecretService, Depends(get_secret_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> SecretResponse:
    """Update a secret.

    Clients observe HTTP 200 on success, 404 when no secret has this id,
    and 422 on invalid input.

    Args:
        secret_id: Id of the secret.
        body: Secret update request.
        service: Secret service.
        actor: Caller context.

    Returns:
        Updated secret without values.
    """
    secret = await service.update_secret(
        secret_id, type=body.type, values=body.values, actor=actor
    )
    return secret_to_response(secret)


@router.delete("/{secret_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_secret(
    secret_id: uuid.UUID,
    service: Annotated[SecretService, Depends(get_secret_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a secret.

    Clients observe HTTP 204 on success and 404 when no secret has this
    id.

    Args:
        secret_id: Id of the secret.
        service: Secret service.
        actor: Caller context.
    """
    await service.delete_secret(secret_id, actor=actor)
