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
"""Connections resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.connection import (
    ConnectionCreateRequest,
    ConnectionListParams,
    ConnectionResponse,
    ConnectionUpdateRequest,
)
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class ConnectionsResource:
    """Connection API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(
        self, request: ConnectionCreateRequest, idempotency_key: str | None = None
    ) -> ConnectionResponse:
        """Create a connection.

        Args:
            request: Connection create request.
            idempotency_key: Idempotency key overriding the transport's
                random default.

        Raises:
            APIError: The request failed, including 409 for a duplicate name.

        Returns:
            Created connection without secret values.
        """
        response = await self._client.request(
            "POST",
            "/api/v1/connections",
            json=request.model_dump(mode="json", exclude_unset=True),
            idempotency_key=idempotency_key,
        )
        return ConnectionResponse.model_validate(response.json())

    async def get(self, connection_id: uuid.UUID) -> ConnectionResponse:
        """Get a connection by id.

        Args:
            connection_id: Id of the connection.

        Raises:
            APIError: The request failed, including 404 for a missing
                connection.

        Returns:
            Stored connection without secret values.
        """
        response = await self._client.request(
            "GET", f"/api/v1/connections/{connection_id}"
        )
        return ConnectionResponse.model_validate(response.json())

    async def list(
        self,
        params: ConnectionListParams | None = None,
    ) -> Page[ConnectionResponse]:
        """List connections.

        Args:
            params: Connection list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of connections without secret values.
        """
        params = params or ConnectionListParams()
        response = await self._client.request(
            "GET",
            "/api/v1/connections",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[ConnectionResponse].model_validate(response.json())

    async def iter(
        self,
        params: ConnectionListParams | None = None,
    ) -> AsyncIterator[ConnectionResponse]:
        """Iterate over all connections.

        Args:
            params: Connection list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every connection without secret values.
        """
        async for item in iterate_pages(params or ConnectionListParams(), self.list):
            yield item

    async def update(
        self, connection_id: uuid.UUID, request: ConnectionUpdateRequest
    ) -> ConnectionResponse:
        """Update a connection.

        Args:
            connection_id: Id of the connection.
            request: Connection update request, unset fields stay unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing
                connection.

        Returns:
            Updated connection without secret values.
        """
        response = await self._client.request(
            "PATCH",
            f"/api/v1/connections/{connection_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ConnectionResponse.model_validate(response.json())

    async def delete(self, connection_id: uuid.UUID) -> None:
        """Delete a connection and the secret holding its values.

        Args:
            connection_id: Id of the connection.

        Raises:
            APIError: The request failed, including 404 for a missing
                connection.
        """
        await self._client.request("DELETE", f"/api/v1/connections/{connection_id}")
