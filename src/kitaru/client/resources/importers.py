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
        self._client = client

    async def create(self, request: ImporterCreateRequest) -> ImporterResponse:
        response = await self._client.request(
            "POST",
            "/v1/importers",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ImporterResponse.model_validate(response.json())

    async def get(self, importer_id: uuid.UUID) -> ImporterResponse:
        response = await self._client.request("GET", f"/v1/importers/{importer_id}")
        return ImporterResponse.model_validate(response.json())

    async def list(
        self, params: ImporterListParams | None = None
    ) -> Page[ImporterResponse]:
        params = params or ImporterListParams()
        response = await self._client.request(
            "GET",
            "/v1/importers",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[ImporterResponse].model_validate(response.json())

    async def iter(
        self, params: ImporterListParams | None = None
    ) -> AsyncIterator[ImporterResponse]:
        async for item in iterate_pages(params or ImporterListParams(), self.list):
            yield item

    async def update(
        self, importer_id: uuid.UUID, request: ImporterUpdateRequest
    ) -> ImporterResponse:
        response = await self._client.request(
            "PATCH",
            f"/v1/importers/{importer_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ImporterResponse.model_validate(response.json())

    async def delete(self, importer_id: uuid.UUID) -> None:
        await self._client.request("DELETE", f"/v1/importers/{importer_id}")

    async def create_version(
        self, importer_id: uuid.UUID, request: ImporterVersionCreateRequest
    ) -> ImporterVersionResponse:
        response = await self._client.request(
            "POST",
            f"/v1/importers/{importer_id}/versions",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ImporterVersionResponse.model_validate(response.json())

    async def list_versions(
        self, importer_id: uuid.UUID, params: ListParams | None = None
    ) -> Page[ImporterVersionResponse]:
        params = params or ListParams()
        response = await self._client.request(
            "GET",
            f"/v1/importers/{importer_id}/versions",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[ImporterVersionResponse].model_validate(response.json())

    async def iter_versions(
        self, importer_id: uuid.UUID, params: ListParams | None = None
    ) -> AsyncIterator[ImporterVersionResponse]:
        async for item in iterate_pages(
            params or ListParams(),
            lambda page_params: self.list_versions(importer_id, page_params),
        ):
            yield item

    async def get_version(
        self, importer_id: uuid.UUID, version: int
    ) -> ImporterVersionResponse:
        response = await self._client.request(
            "GET", f"/v1/importers/{importer_id}/versions/{version}"
        )
        return ImporterVersionResponse.model_validate(response.json())

    async def update_version(
        self,
        importer_id: uuid.UUID,
        version: int,
        request: ImporterVersionUpdateRequest,
    ) -> ImporterVersionResponse:
        response = await self._client.request(
            "PATCH",
            f"/v1/importers/{importer_id}/versions/{version}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ImporterVersionResponse.model_validate(response.json())
