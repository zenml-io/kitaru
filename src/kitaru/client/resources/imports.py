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
"""Imports resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.imports import (
    ImportCreateRequest,
    ImportListParams,
    ImportResponse,
)
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class ImportsResource:
    """Import API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(
        self, request: ImportCreateRequest, idempotency_key: str | None = None
    ) -> ImportResponse:
        """Import sessions from a payload blob.

        Args:
            request: Import create request.
            idempotency_key: Idempotency key overriding the transport's
                random default.

        Raises:
            APIError: The request failed, including 404 when the importer,
                the payload blob, the agent, or an evaluator does not exist.

        Returns:
            Created import.
        """
        response = await self._client.request(
            "POST",
            "/api/v1/imports",
            json=request.model_dump(mode="json", exclude_unset=True),
            idempotency_key=idempotency_key,
        )
        return ImportResponse.model_validate(response.json())

    async def get(self, import_id: uuid.UUID) -> ImportResponse:
        """Get an import by id.

        Args:
            import_id: Id of the import.

        Raises:
            APIError: The request failed, including 404 for a missing
                import.

        Returns:
            Stored import.
        """
        response = await self._client.request("GET", f"/api/v1/imports/{import_id}")
        return ImportResponse.model_validate(response.json())

    async def list(
        self, params: ImportListParams | None = None
    ) -> Page[ImportResponse]:
        """List imports.

        Args:
            params: Import list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of imports.
        """
        params = params or ImportListParams()
        response = await self._client.request(
            "GET",
            "/api/v1/imports",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[ImportResponse].model_validate(response.json())

    async def iter(
        self, params: ImportListParams | None = None
    ) -> AsyncIterator[ImportResponse]:
        """Iterate over all imports.

        Args:
            params: Import list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every import.
        """
        async for item in iterate_pages(params or ImportListParams(), self.list):
            yield item
