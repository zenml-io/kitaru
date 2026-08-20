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
"""API key routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.api_key import (
    ApiKeyCreateRequest,
    ApiKeyIssuedResponse,
    ApiKeyListParams,
    ApiKeyResponse,
    ApiKeyRotateRequest,
    ApiKeyUpdateRequest,
)
from kitaru.api_models.v1.base import Page
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_api_key_service,
)
from kitaru.server.adapters.rest.mapping.api_keys import (
    api_key_list_params_to_filter,
    api_key_to_issued_response,
    api_key_to_response,
)
from kitaru.server.adapters.rest.route import KitaruAPIRoute, idempotent
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.api_key_service import ApiKeyService

router = APIRouter(route_class=KitaruAPIRoute)


@router.post("", status_code=status.HTTP_201_CREATED)
@idempotent(encrypt_response=True)
async def create_api_key(
    body: ApiKeyCreateRequest,
    service: Annotated[ApiKeyService, Depends(get_api_key_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ApiKeyIssuedResponse:
    """Create an API key.

    Clients observe HTTP 201 on success, 409 when the name is already
    registered, and 422 on invalid input. The response carries the plaintext
    key exactly once.

    Args:
        body: API key create request.
        service: API key service.
        actor: Caller context.

    Returns:
        Created API key including the plaintext key.
    """
    api_key, key = await service.create_api_key(name=body.name, actor=actor)
    return api_key_to_issued_response(api_key, key)


@router.get("")
async def list_api_keys(
    service: Annotated[ApiKeyService, Depends(get_api_key_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[ApiKeyListParams, Query()],
) -> Page[ApiKeyResponse]:
    """List API keys of the caller.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: API key service.
        actor: Caller context.
        params: API key list params.

    Returns:
        Page of API keys.
    """
    api_key_filter = api_key_list_params_to_filter(params)
    api_keys, next_cursor = await service.list_api_keys(api_key_filter, actor=actor)
    return Page[ApiKeyResponse](
        items=[api_key_to_response(api_key) for api_key in api_keys],
        next_cursor=next_cursor,
    )


@router.get("/{api_key_id}")
async def get_api_key(
    api_key_id: uuid.UUID,
    service: Annotated[ApiKeyService, Depends(get_api_key_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ApiKeyResponse:
    """Get an API key by id.

    Clients observe HTTP 200 on success and 404 when the caller owns no api
    key with this id.

    Args:
        api_key_id: Id of the API key.
        service: API key service.
        actor: Caller context.

    Returns:
        Stored API key.
    """
    api_key = await service.get_api_key(api_key_id, actor=actor)
    return api_key_to_response(api_key)


@router.patch("/{api_key_id}")
async def update_api_key(
    api_key_id: uuid.UUID,
    body: ApiKeyUpdateRequest,
    service: Annotated[ApiKeyService, Depends(get_api_key_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ApiKeyResponse:
    """Update an API key.

    Clients observe HTTP 200 on success, 404 when the caller owns no API key
    with this id, and 422 on invalid input.

    Args:
        api_key_id: Id of the API key.
        body: API key update request.
        service: API key service.
        actor: Caller context.

    Returns:
        Updated API key.
    """
    api_key = await service.update_api_key(api_key_id, active=body.active, actor=actor)
    return api_key_to_response(api_key)


@router.post("/{api_key_id}/rotate")
@idempotent(encrypt_response=True)
async def rotate_api_key(
    api_key_id: uuid.UUID,
    body: ApiKeyRotateRequest,
    service: Annotated[ApiKeyService, Depends(get_api_key_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ApiKeyIssuedResponse:
    """Rotate an API key.

    Clients observe HTTP 200 on success, 404 when the caller owns no API key
    with this id, and 422 on invalid input. The response carries the new
    plaintext key exactly once.

    Args:
        api_key_id: Id of the API key.
        body: API key rotate request.
        service: API key service.
        actor: Caller context.

    Returns:
        Rotated API key including the new plaintext key.
    """
    api_key, key = await service.rotate_api_key(
        api_key_id, retain_period_minutes=body.retain_period_minutes, actor=actor
    )
    return api_key_to_issued_response(api_key, key)


@router.delete("/{api_key_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_api_key(
    api_key_id: uuid.UUID,
    service: Annotated[ApiKeyService, Depends(get_api_key_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete an API key.

    Clients observe HTTP 204 on success and 404 when the caller owns no api
    key with this id.

    Args:
        api_key_id: Id of the API key.
        service: API key service.
        actor: Caller context.
    """
    await service.delete_api_key(api_key_id, actor=actor)
