"""Tags resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.tag import (
    TagCreateRequest,
    TagLinkCreateRequest,
    TagLinkResponse,
    TagListParams,
    TagResourceType,
    TagResponse,
    TagUpdateRequest,
)
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class TagsResource:
    """Tag API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        self._client = client

    async def create(self, request: TagCreateRequest) -> TagResponse:
        response = await self._client.request(
            "POST", "/v1/tags", json=request.model_dump(mode="json", exclude_unset=True)
        )
        return TagResponse.model_validate(response.json())

    async def list(self, params: TagListParams | None = None) -> Page[TagResponse]:
        params = params or TagListParams()
        response = await self._client.request(
            "GET", "/v1/tags", params=params.model_dump(mode="json", exclude_unset=True)
        )
        return Page[TagResponse].model_validate(response.json())

    async def iter(
        self, params: TagListParams | None = None
    ) -> AsyncIterator[TagResponse]:
        async for item in iterate_pages(params or TagListParams(), self.list):
            yield item

    async def update(self, tag_id: uuid.UUID, request: TagUpdateRequest) -> TagResponse:
        response = await self._client.request(
            "PATCH",
            f"/v1/tags/{tag_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return TagResponse.model_validate(response.json())

    async def delete(self, tag_id: uuid.UUID) -> None:
        await self._client.request("DELETE", f"/v1/tags/{tag_id}")

    async def create_link(
        self, tag_id: uuid.UUID, request: TagLinkCreateRequest
    ) -> TagLinkResponse:
        response = await self._client.request(
            "POST",
            f"/v1/tags/{tag_id}/links",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return TagLinkResponse.model_validate(response.json())

    async def delete_link(
        self, tag_id: uuid.UUID, resource_type: TagResourceType, resource_id: uuid.UUID
    ) -> None:
        await self._client.request(
            "DELETE", f"/v1/tags/{tag_id}/links/{resource_type.value}/{resource_id}"
        )
