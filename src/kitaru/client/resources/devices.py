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
"""Devices resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.device import (
    DeviceListParams,
    DeviceResponse,
    DeviceUpdateRequest,
    DeviceVerifyRequest,
)
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class DevicesResource:
    """Device API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def get(
        self, device_id: uuid.UUID, user_code: str | None = None
    ) -> DeviceResponse:
        """Get a device by id.

        Args:
            device_id: Id of the device.
            user_code: User code of a device no account approved yet.

        Raises:
            APIError: The request failed, including 404 for a missing device.

        Returns:
            Stored device.
        """
        params = {} if user_code is None else {"user_code": user_code}
        response = await self._client.request(
            "GET", f"/v1/devices/{device_id}", params=params
        )
        return DeviceResponse.model_validate(response.json())

    async def list(
        self,
        params: DeviceListParams | None = None,
    ) -> Page[DeviceResponse]:
        """List devices of the caller.

        Args:
            params: Device list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of devices.
        """
        params = params or DeviceListParams()
        response = await self._client.request(
            "GET",
            "/v1/devices",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[DeviceResponse].model_validate(response.json())

    async def iter(
        self,
        params: DeviceListParams | None = None,
    ) -> AsyncIterator[DeviceResponse]:
        """Iterate over all devices of the caller.

        Args:
            params: Device list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every device.
        """
        async for item in iterate_pages(params or DeviceListParams(), self.list):
            yield item

    async def verify(
        self, device_id: uuid.UUID, request: DeviceVerifyRequest
    ) -> DeviceResponse:
        """Approve a pending device authorization.

        Args:
            device_id: Id of the device.
            request: Device verify request.

        Raises:
            APIError: The request failed, including 404 for a missing device
                and 422 for a user code that does not match.

        Returns:
            Verified device.
        """
        response = await self._client.request(
            "POST",
            f"/v1/devices/{device_id}/verify",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return DeviceResponse.model_validate(response.json())

    async def update(
        self, device_id: uuid.UUID, request: DeviceUpdateRequest
    ) -> DeviceResponse:
        """Update a device.

        Args:
            device_id: Id of the device.
            request: Device update request, unset fields stay unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing device.

        Returns:
            Updated device.
        """
        response = await self._client.request(
            "PATCH",
            f"/v1/devices/{device_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return DeviceResponse.model_validate(response.json())

    async def delete(self, device_id: uuid.UUID) -> None:
        """Delete a device.

        Args:
            device_id: Id of the device.

        Raises:
            APIError: The request failed, including 404 for a missing device.
        """
        await self._client.request("DELETE", f"/v1/devices/{device_id}")
