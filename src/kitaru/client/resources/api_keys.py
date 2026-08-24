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
"""API keys resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.api_key import (
    ApiKeyCreateRequest,
    ApiKeyIssuedResponse,
    ApiKeyListParams,
    ApiKeyResponse,
    ApiKeyRotateRequest,
    ApiKeyUpdateRequest,
)
from kitaru.api_models.v1.base import Page
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class ApiKeysResource:
    """API key API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(
        self, request: ApiKeyCreateRequest, idempotency_key: str | None = None
    ) -> ApiKeyIssuedResponse:
        """Create an API key.

        The response carries the plaintext key exactly once.

        Args:
            request: API key create request.
            idempotency_key: Idempotency key overriding the transport's
                random default.

        Raises:
            APIError: The request failed, including 409 for a duplicate name.

        Returns:
            Created API key including the plaintext key.
        """
        response = await self._client.request(
            "POST",
            "/api/v1/api-keys",
            json=request.model_dump(mode="json", exclude_unset=True),
            idempotency_key=idempotency_key,
        )
        return ApiKeyIssuedResponse.model_validate(response.json())

    async def get(self, api_key_id: uuid.UUID) -> ApiKeyResponse:
        """Get an API key by id.

        Args:
            api_key_id: Id of the API key.

        Raises:
            APIError: The request failed, including 404 for a missing API key.

        Returns:
            Stored API key.
        """
        response = await self._client.request("GET", f"/api/v1/api-keys/{api_key_id}")
        return ApiKeyResponse.model_validate(response.json())

    async def list(
        self,
        params: ApiKeyListParams | None = None,
    ) -> Page[ApiKeyResponse]:
        """List API keys of the caller.

        Args:
            params: API key list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of API keys.
        """
        params = params or ApiKeyListParams()
        response = await self._client.request(
            "GET",
            "/api/v1/api-keys",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[ApiKeyResponse].model_validate(response.json())

    async def iter(
        self,
        params: ApiKeyListParams | None = None,
    ) -> AsyncIterator[ApiKeyResponse]:
        """Iterate over all API keys of the caller.

        Args:
            params: API key list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every API key.
        """
        async for item in iterate_pages(params or ApiKeyListParams(), self.list):
            yield item

    async def update(
        self, api_key_id: uuid.UUID, request: ApiKeyUpdateRequest
    ) -> ApiKeyResponse:
        """Update an API key.

        Args:
            api_key_id: Id of the API key.
            request: API key update request, unset fields stay unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing API key.

        Returns:
            Updated API key.
        """
        response = await self._client.request(
            "PATCH",
            f"/api/v1/api-keys/{api_key_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ApiKeyResponse.model_validate(response.json())

    async def rotate(
        self,
        api_key_id: uuid.UUID,
        request: ApiKeyRotateRequest | None = None,
        idempotency_key: str | None = None,
    ) -> ApiKeyIssuedResponse:
        """Rotate an API key.

        The response carries the new plaintext key exactly once.

        Args:
            api_key_id: Id of the API key.
            request: API key rotate request.
            idempotency_key: Idempotency key overriding the transport's
                random default.

        Raises:
            APIError: The request failed, including 404 for a missing API key.

        Returns:
            Rotated API key including the new plaintext key.
        """
        request = request or ApiKeyRotateRequest()
        response = await self._client.request(
            "POST",
            f"/api/v1/api-keys/{api_key_id}/rotate",
            json=request.model_dump(mode="json", exclude_unset=True),
            idempotency_key=idempotency_key,
        )
        return ApiKeyIssuedResponse.model_validate(response.json())

    async def delete(self, api_key_id: uuid.UUID) -> None:
        """Delete an API key.

        Args:
            api_key_id: Id of the API key.

        Raises:
            APIError: The request failed, including 404 for a missing API key.
        """
        await self._client.request("DELETE", f"/api/v1/api-keys/{api_key_id}")
