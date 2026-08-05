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
"""Annotations resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.annotation import (
    AnnotationCreateRequest,
    AnnotationListParams,
    AnnotationResponse,
    AnnotationUpdateRequest,
)
from kitaru.api_models.v1.base import Page
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class AnnotationsResource:
    """Annotation API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(self, request: AnnotationCreateRequest) -> AnnotationResponse:
        """Create an annotation, either manual or answering an investigation question.

        Args:
            request: Annotation create request.

        Raises:
            APIError: The request failed, including 404 when the session or
                investigation session does not exist.

        Returns:
            Created annotation.
        """
        response = await self._client.request(
            "POST",
            "/v1/annotations",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return AnnotationResponse.model_validate(response.json())

    async def get(self, annotation_id: uuid.UUID) -> AnnotationResponse:
        """Get an annotation by id.

        Args:
            annotation_id: Id of the annotation.

        Raises:
            APIError: The request failed, including 404 for a missing
                annotation.

        Returns:
            Stored annotation.
        """
        response = await self._client.request("GET", f"/v1/annotations/{annotation_id}")
        return AnnotationResponse.model_validate(response.json())

    async def list(
        self,
        params: AnnotationListParams | None = None,
    ) -> Page[AnnotationResponse]:
        """List annotations.

        Args:
            params: Annotation list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of annotations.
        """
        params = params or AnnotationListParams()
        response = await self._client.request(
            "GET",
            "/v1/annotations",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[AnnotationResponse].model_validate(response.json())

    async def iter(
        self,
        params: AnnotationListParams | None = None,
    ) -> AsyncIterator[AnnotationResponse]:
        """Iterate over all annotations.

        Args:
            params: Annotation list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every annotation.
        """
        async for item in iterate_pages(params or AnnotationListParams(), self.list):
            yield item

    async def update(
        self, annotation_id: uuid.UUID, request: AnnotationUpdateRequest
    ) -> AnnotationResponse:
        """Update an annotation's value.

        Args:
            annotation_id: Id of the annotation.
            request: Annotation update request.

        Raises:
            APIError: The request failed, including 404 for a missing
                annotation.

        Returns:
            Updated annotation.
        """
        response = await self._client.request(
            "PATCH",
            f"/v1/annotations/{annotation_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return AnnotationResponse.model_validate(response.json())

    async def delete(self, annotation_id: uuid.UUID) -> None:
        """Delete an annotation.

        Args:
            annotation_id: Id of the annotation.

        Raises:
            APIError: The request failed, including 404 for a missing
                annotation.
        """
        await self._client.request("DELETE", f"/v1/annotations/{annotation_id}")
