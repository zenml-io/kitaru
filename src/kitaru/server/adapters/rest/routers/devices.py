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
"""Device routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.device import (
    DeviceListParams,
    DeviceResponse,
    DeviceUpdateRequest,
    DeviceVerifyRequest,
)
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import authorize, get_device_service
from kitaru.server.adapters.rest.mapping.devices import (
    device_list_params_to_filter,
    device_to_response,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.device_service import DeviceService

router = APIRouter(route_class=CommitRoute)


@router.get("")
async def list_devices(
    service: Annotated[DeviceService, Depends(get_device_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[DeviceListParams, Query()],
) -> Page[DeviceResponse]:
    """List devices of the caller.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Device service.
        actor: Caller context.
        params: Device list params.

    Returns:
        Page of devices.
    """
    device_filter = device_list_params_to_filter(params)
    devices, next_cursor = await service.list_devices(device_filter, actor=actor)
    return Page[DeviceResponse](
        items=[device_to_response(device) for device in devices],
        next_cursor=next_cursor,
    )


@router.get("/{device_id}")
async def get_device(
    device_id: uuid.UUID,
    service: Annotated[DeviceService, Depends(get_device_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> DeviceResponse:
    """Get a device by id.

    Clients observe HTTP 200 on success and 404 when the caller owns no device
    with this id.

    Args:
        device_id: Id of the device.
        service: Device service.
        actor: Caller context.

    Returns:
        Stored device.
    """
    device = await service.get_device(device_id, actor=actor)
    return device_to_response(device)


@router.post("/{device_id}/verify")
async def verify_device(
    device_id: uuid.UUID,
    body: DeviceVerifyRequest,
    service: Annotated[DeviceService, Depends(get_device_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> DeviceResponse:
    """Approve a pending device authorization.

    The device polling the token endpoint receives a token for the caller's
    account on its next poll. Clients observe HTTP 200 on success, 404 when no
    such device exists or another account already approved it, and 422 when
    the user code does not match, the authorization expired, or the device is
    locked. Three failed attempts lock the device.

    Args:
        device_id: Id of the device.
        body: Device verify request.
        service: Device service.
        actor: Caller context.

    Returns:
        Verified device.
    """
    device = await service.verify_device(
        device_id,
        user_code=body.user_code,
        trusted=body.trusted,
        actor=actor,
    )
    return device_to_response(device)


@router.patch("/{device_id}")
async def update_device(
    device_id: uuid.UUID,
    body: DeviceUpdateRequest,
    service: Annotated[DeviceService, Depends(get_device_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> DeviceResponse:
    """Update a device.

    Locking a device rejects every token issued for it. Clients observe HTTP
    200 on success, 404 when the caller owns no device with this id, and 422 on
    invalid input.

    Args:
        device_id: Id of the device.
        body: Device update request.
        service: Device service.
        actor: Caller context.

    Returns:
        Updated device.
    """
    device = await service.update_device(
        device_id,
        locked=body.locked,
        trusted=body.trusted,
        actor=actor,
    )
    return device_to_response(device)


@router.delete("/{device_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_device(
    device_id: uuid.UUID,
    service: Annotated[DeviceService, Depends(get_device_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a device.

    Every token issued for the device stops authenticating. Clients observe
    HTTP 204 on success and 404 when the caller owns no device with this id.

    Args:
        device_id: Id of the device.
        service: Device service.
        actor: Caller context.
    """
    await service.delete_device(device_id, actor=actor)
