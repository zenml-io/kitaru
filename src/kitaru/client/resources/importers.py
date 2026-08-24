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
"""Importers resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import ListParams, Page
from kitaru.api_models.v1.importer import (
    ImporterCreateRequest,
    ImporterListParams,
    ImporterResponse,
    ImporterUpdateRequest,
    ImporterVersionCreateRequest,
    ImporterVersionResponse,
    ImporterVersionUpdateRequest,
)
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class ImportersResource:
    """Importer API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(
        self, request: ImporterCreateRequest, idempotency_key: str | None = None
    ) -> ImporterResponse:
        """Create an importer.

        Args:
            request: Importer create request.
            idempotency_key: Idempotency key overriding the transport's
                random default.

        Raises:
            APIError: The request failed, including 409 for a duplicate name.

        Returns:
            Created importer.
        """
        response = await self._client.request(
            "POST",
            "/api/v1/importers",
            json=request.model_dump(mode="json", exclude_unset=True),
            idempotency_key=idempotency_key,
        )
        return ImporterResponse.model_validate(response.json())

    async def get(self, importer_id: uuid.UUID) -> ImporterResponse:
        """Get an importer by id.

        Args:
            importer_id: Id of the importer.

        Raises:
            APIError: The request failed, including 404 for a missing
                importer.

        Returns:
            Stored importer.
        """
        response = await self._client.request("GET", f"/api/v1/importers/{importer_id}")
        return ImporterResponse.model_validate(response.json())

    async def list(
        self, params: ImporterListParams | None = None
    ) -> Page[ImporterResponse]:
        """List importers.

        Args:
            params: Importer list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of importers.
        """
        params = params or ImporterListParams()
        response = await self._client.request(
            "GET",
            "/api/v1/importers",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[ImporterResponse].model_validate(response.json())

    async def iter(
        self, params: ImporterListParams | None = None
    ) -> AsyncIterator[ImporterResponse]:
        """Iterate over all importers.

        Args:
            params: Importer list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every importer.
        """
        async for item in iterate_pages(params or ImporterListParams(), self.list):
            yield item

    async def update(
        self, importer_id: uuid.UUID, request: ImporterUpdateRequest
    ) -> ImporterResponse:
        """Update an importer.

        Args:
            importer_id: Id of the importer.
            request: Importer update request, unset fields stay unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing
                importer.

        Returns:
            Updated importer.
        """
        response = await self._client.request(
            "PATCH",
            f"/api/v1/importers/{importer_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ImporterResponse.model_validate(response.json())

    async def delete(self, importer_id: uuid.UUID) -> None:
        """Delete an importer, cascading its versions.

        Args:
            importer_id: Id of the importer.

        Raises:
            APIError: The request failed, including 404 for a missing
                importer.
        """
        await self._client.request("DELETE", f"/api/v1/importers/{importer_id}")

    async def create_version(
        self, importer_id: uuid.UUID, request: ImporterVersionCreateRequest
    ) -> ImporterVersionResponse:
        """Create an importer version.

        Args:
            importer_id: Id of the importer.
            request: Importer version create request.

        Raises:
            APIError: The request failed, including 404 for a missing
                importer or blob.

        Returns:
            Created importer version.
        """
        response = await self._client.request(
            "POST",
            f"/api/v1/importers/{importer_id}/versions",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ImporterVersionResponse.model_validate(response.json())

    async def list_versions(
        self, importer_id: uuid.UUID, params: ListParams | None = None
    ) -> Page[ImporterVersionResponse]:
        """List an importer's versions.

        Args:
            importer_id: Id of the importer.
            params: List params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of importer versions.
        """
        params = params or ListParams()
        response = await self._client.request(
            "GET",
            f"/api/v1/importers/{importer_id}/versions",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[ImporterVersionResponse].model_validate(response.json())

    async def iter_versions(
        self, importer_id: uuid.UUID, params: ListParams | None = None
    ) -> AsyncIterator[ImporterVersionResponse]:
        """Iterate over all of an importer's versions.

        Args:
            importer_id: Id of the importer.
            params: List params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every importer version.
        """
        async for item in iterate_pages(
            params or ListParams(),
            lambda page_params: self.list_versions(importer_id, page_params),
        ):
            yield item

    async def get_version(
        self, importer_id: uuid.UUID, version: int
    ) -> ImporterVersionResponse:
        """Get an importer version by version number.

        Args:
            importer_id: Id of the importer.
            version: Version number.

        Raises:
            APIError: The request failed, including 404 for a missing
                version.

        Returns:
            Stored importer version.
        """
        response = await self._client.request(
            "GET", f"/api/v1/importers/{importer_id}/versions/{version}"
        )
        return ImporterVersionResponse.model_validate(response.json())

    async def update_version(
        self,
        importer_id: uuid.UUID,
        version: int,
        request: ImporterVersionUpdateRequest,
    ) -> ImporterVersionResponse:
        """Update an importer version's display version.

        Args:
            importer_id: Id of the importer.
            version: Version number.
            request: Importer version update request, unset fields stay
                unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing
                version.

        Returns:
            Updated importer version.
        """
        response = await self._client.request(
            "PATCH",
            f"/api/v1/importers/{importer_id}/versions/{version}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ImporterVersionResponse.model_validate(response.json())
