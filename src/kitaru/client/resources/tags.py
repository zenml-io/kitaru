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
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(
        self, request: TagCreateRequest, idempotency_key: str | None = None
    ) -> TagResponse:
        """Create a tag.

        Args:
            request: Tag create request.
            idempotency_key: Idempotency key overriding the transport's
                random default.

        Raises:
            APIError: The request failed, including 409 for a duplicate name.

        Returns:
            Created tag.
        """
        response = await self._client.request(
            "POST",
            "/api/v1/tags",
            json=request.model_dump(mode="json", exclude_unset=True),
            idempotency_key=idempotency_key,
        )
        return TagResponse.model_validate(response.json())

    async def list(self, params: TagListParams | None = None) -> Page[TagResponse]:
        """List tags.

        Args:
            params: Tag list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of tags.
        """
        params = params or TagListParams()
        response = await self._client.request(
            "GET",
            "/api/v1/tags",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[TagResponse].model_validate(response.json())

    async def iter(
        self, params: TagListParams | None = None
    ) -> AsyncIterator[TagResponse]:
        """Iterate over all tags.

        Args:
            params: Tag list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every tag.
        """
        async for item in iterate_pages(params or TagListParams(), self.list):
            yield item

    async def update(self, tag_id: uuid.UUID, request: TagUpdateRequest) -> TagResponse:
        """Update a tag.

        Args:
            tag_id: Id of the tag.
            request: Tag update request.

        Raises:
            APIError: The request failed, including 404 for a missing tag.

        Returns:
            Updated tag.
        """
        response = await self._client.request(
            "PATCH",
            f"/api/v1/tags/{tag_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return TagResponse.model_validate(response.json())

    async def delete(self, tag_id: uuid.UUID) -> None:
        """Delete a tag.

        Deleting a tag cascades its links.

        Args:
            tag_id: Id of the tag.

        Raises:
            APIError: The request failed, including 404 for a missing tag.
        """
        await self._client.request("DELETE", f"/api/v1/tags/{tag_id}")

    async def create_link(
        self, tag_id: uuid.UUID, request: TagLinkCreateRequest
    ) -> TagLinkResponse:
        """Link a tag to a resource.

        Args:
            tag_id: Id of the tag.
            request: Tag link create request.

        Raises:
            APIError: The request failed, including 404 for a missing tag
                and 409 for a duplicate link.

        Returns:
            Created tag link.
        """
        response = await self._client.request(
            "POST",
            f"/api/v1/tags/{tag_id}/links",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return TagLinkResponse.model_validate(response.json())

    async def delete_link(
        self,
        tag_id: uuid.UUID,
        resource_type: TagResourceType,
        resource_id: uuid.UUID,
    ) -> None:
        """Unlink a tag from a resource.

        Args:
            tag_id: Id of the tag.
            resource_type: Kind of the linked resource.
            resource_id: Id of the linked resource.

        Raises:
            APIError: The request failed, including 404 for a missing link.
        """
        await self._client.request(
            "DELETE", f"/api/v1/tags/{tag_id}/links/{resource_type.value}/{resource_id}"
        )
