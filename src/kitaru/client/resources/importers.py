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
from pathlib import Path
from typing import TYPE_CHECKING, Any

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.importers import (
    ImporterCreateRequest,
    ImporterResponse,
    ImporterVersionCreateRequest,
    ImporterVersionResponse,
)
from kitaru.client.resources.plugin_registration import resolve_or_create

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient

IMPORTER_MEDIA_TYPE = "text/x-python"


class ImportersResource:
    """Importer API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(self, request: ImporterCreateRequest) -> ImporterResponse:
        """Create an importer.

        Args:
            request: Importer create request.

        Raises:
            APIError: The request failed, including 409 for a duplicate name.

        Returns:
            Created importer.
        """
        response = await self._client.request(
            "POST",
            "/v1/importers",
            json=request.model_dump(mode="json", exclude_unset=True),
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
        response = await self._client.request("GET", f"/v1/importers/{importer_id}")
        return ImporterResponse.model_validate(response.json())

    async def list(
        self,
        name: str | None = None,
        provider: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Page[ImporterResponse]:
        """List importers.

        Args:
            name: Filter on importer name.
            provider: Filter on the provider the importer reads from.
            page: Page number.
            page_size: Page size.

        Raises:
            APIError: The request failed.

        Returns:
            Page of importers.
        """
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if name is not None:
            params["name"] = name
        if provider is not None:
            params["provider"] = provider
        response = await self._client.request("GET", "/v1/importers", params=params)
        return Page[ImporterResponse].model_validate(response.json())

    async def delete(self, importer_id: uuid.UUID) -> None:
        """Delete an importer and its versions.

        Args:
            importer_id: Id of the importer.

        Raises:
            APIError: The request failed, including 404 for a missing
                importer.
        """
        await self._client.request("DELETE", f"/v1/importers/{importer_id}")

    async def create_version(
        self, importer_id: uuid.UUID, request: ImporterVersionCreateRequest
    ) -> ImporterVersionResponse:
        """Create an importer version.

        Args:
            importer_id: Id of the importer.
            request: Importer version create request.

        Raises:
            APIError: The request failed, including 404 for a missing
                importer or code blob.

        Returns:
            Created importer version.
        """
        response = await self._client.request(
            "POST",
            f"/v1/importers/{importer_id}/versions",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ImporterVersionResponse.model_validate(response.json())

    async def get_version(
        self, importer_id: uuid.UUID, version: int
    ) -> ImporterVersionResponse:
        """Get an importer version by version number.

        Args:
            importer_id: Id of the importer.
            version: Version number.

        Raises:
            APIError: The request failed, including 404 for a missing
                importer or version.

        Returns:
            Stored importer version.
        """
        response = await self._client.request(
            "GET", f"/v1/importers/{importer_id}/versions/{version}"
        )
        return ImporterVersionResponse.model_validate(response.json())

    async def list_versions(
        self, importer_id: uuid.UUID, page: int = 1, page_size: int = 20
    ) -> Page[ImporterVersionResponse]:
        """List the versions of an importer.

        Args:
            importer_id: Id of the importer.
            page: Page number.
            page_size: Page size.

        Raises:
            APIError: The request failed, including 404 for a missing
                importer.

        Returns:
            Page of importer versions.
        """
        response = await self._client.request(
            "GET",
            f"/v1/importers/{importer_id}/versions",
            params={"page": page, "page_size": page_size},
        )
        return Page[ImporterVersionResponse].model_validate(response.json())

    async def register(
        self,
        name: str,
        file: Path | str,
        entrypoint: str,
        provider: str | None = None,
    ) -> ImporterVersionResponse:
        """Register a new version of an importer from a source file.

        Uploads the file, creates the importer when it does not exist
        yet, and creates a version pointing at the uploaded code.

        Args:
            name: Importer name.
            file: Path of the source file holding the importer.
            entrypoint: Attribute implementing the importer.
            provider: Provider the importer reads from.

        Raises:
            APIError: A request failed.

        Returns:
            Created importer version.
        """
        blob = await self._client.blobs.upload(
            Path(file).read_bytes(), IMPORTER_MEDIA_TYPE
        )
        importer = await resolve_or_create(
            lambda: self.create(ImporterCreateRequest(name=name, provider=provider)),
            lambda: self.list(name=name, page_size=1),
        )
        return await self.create_version(
            importer.id,
            ImporterVersionCreateRequest(blob_id=blob.id, entrypoint=entrypoint),
        )
