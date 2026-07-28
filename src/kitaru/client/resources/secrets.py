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
"""Secrets resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Literal, overload

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.secret import (
    SecretCreateRequest,
    SecretListParams,
    SecretResponse,
    SecretUpdateRequest,
    SecretWithValuesResponse,
)

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class SecretsResource:
    """Secret API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(self, request: SecretCreateRequest) -> SecretResponse:
        """Create a secret.

        Args:
            request: Secret create request.

        Raises:
            APIError: The request failed, including 409 for a duplicate name.

        Returns:
            Created secret without values.
        """
        response = await self._client.request(
            "POST",
            "/v1/secrets",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return SecretResponse.model_validate(response.json())

    @overload
    async def get(
        self, secret_id: uuid.UUID, include_values: Literal[True]
    ) -> SecretWithValuesResponse: ...

    @overload
    async def get(
        self, secret_id: uuid.UUID, include_values: Literal[False] = False
    ) -> SecretResponse: ...

    async def get(
        self, secret_id: uuid.UUID, include_values: bool = False
    ) -> SecretResponse | SecretWithValuesResponse:
        """Get a secret by id.

        Args:
            secret_id: Id of the secret.
            include_values: Whether to include the secret values.

        Raises:
            APIError: The request failed, including 404 for a missing
                secret.

        Returns:
            Stored secret, with values when requested.
        """
        response = await self._client.request(
            "GET",
            f"/v1/secrets/{secret_id}",
            params={"include_values": include_values},
        )
        if include_values:
            return SecretWithValuesResponse.model_validate(response.json())
        return SecretResponse.model_validate(response.json())

    async def list(
        self,
        params: SecretListParams | None = None,
    ) -> Page[SecretResponse]:
        """List secrets.

        Args:
            params: Secret list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of secrets without values.
        """
        params = params or SecretListParams()
        response = await self._client.request(
            "GET",
            "/v1/secrets",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[SecretResponse].model_validate(response.json())

    async def iter(
        self,
        params: SecretListParams | None = None,
    ) -> AsyncIterator[SecretResponse]:
        """Iterate over all secrets.

        Args:
            params: Secret list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every secret without values.
        """
        params = params or SecretListParams()
        while True:
            page = await self.list(params)
            for item in page.items:
                yield item
            if page.next_cursor is None:
                break
            params = params.model_copy(update={"cursor": page.next_cursor})

    async def update(
        self, secret_id: uuid.UUID, request: SecretUpdateRequest
    ) -> SecretResponse:
        """Update a secret.

        Args:
            secret_id: Id of the secret.
            request: Secret update request, unset fields stay unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing
                secret.

        Returns:
            Updated secret without values.
        """
        response = await self._client.request(
            "PATCH",
            f"/v1/secrets/{secret_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return SecretResponse.model_validate(response.json())

    async def delete(self, secret_id: uuid.UUID) -> None:
        """Delete a secret.

        Args:
            secret_id: Id of the secret.

        Raises:
            APIError: The request failed, including 404 for a missing
                secret.
        """
        await self._client.request("DELETE", f"/v1/secrets/{secret_id}")
