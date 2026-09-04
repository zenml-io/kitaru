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
"""Analyzers resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.analyzer import (
    AnalyzerCreateRequest,
    AnalyzerListParams,
    AnalyzerResponse,
    AnalyzerUpdateRequest,
    AnalyzerVersionCreateRequest,
    AnalyzerVersionResponse,
    AnalyzerVersionUpdateRequest,
)
from kitaru.api_models.v1.base import ListParams, Page
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class AnalyzersResource:
    """Analyzer API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(
        self, request: AnalyzerCreateRequest, idempotency_key: str | None = None
    ) -> AnalyzerResponse:
        """Create an analyzer.

        Args:
            request: Analyzer create request.
            idempotency_key: Idempotency key overriding the transport's
                random default.

        Raises:
            APIError: The request failed, including 409 for a duplicate name.

        Returns:
            Created analyzer.
        """
        response = await self._client.request(
            "POST",
            "/api/v1/analyzers",
            json=request.model_dump(mode="json", exclude_unset=True),
            idempotency_key=idempotency_key,
        )
        return AnalyzerResponse.model_validate(response.json())

    async def get(self, analyzer_id: uuid.UUID) -> AnalyzerResponse:
        """Get an analyzer by id.

        Args:
            analyzer_id: Id of the analyzer.

        Raises:
            APIError: The request failed, including 404 for a missing
                analyzer.

        Returns:
            Stored analyzer.
        """
        response = await self._client.request("GET", f"/api/v1/analyzers/{analyzer_id}")
        return AnalyzerResponse.model_validate(response.json())

    async def list(
        self, params: AnalyzerListParams | None = None
    ) -> Page[AnalyzerResponse]:
        """List analyzers.

        Args:
            params: Analyzer list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of analyzers.
        """
        params = params or AnalyzerListParams()
        response = await self._client.request(
            "GET",
            "/api/v1/analyzers",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[AnalyzerResponse].model_validate(response.json())

    async def iter(
        self, params: AnalyzerListParams | None = None
    ) -> AsyncIterator[AnalyzerResponse]:
        """Iterate over all analyzers.

        Args:
            params: Analyzer list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every analyzer.
        """
        async for item in iterate_pages(params or AnalyzerListParams(), self.list):
            yield item

    async def update(
        self, analyzer_id: uuid.UUID, request: AnalyzerUpdateRequest
    ) -> AnalyzerResponse:
        """Update an analyzer.

        Args:
            analyzer_id: Id of the analyzer.
            request: Analyzer update request, unset fields stay unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing
                analyzer.

        Returns:
            Updated analyzer.
        """
        response = await self._client.request(
            "PATCH",
            f"/api/v1/analyzers/{analyzer_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return AnalyzerResponse.model_validate(response.json())

    async def delete(self, analyzer_id: uuid.UUID) -> None:
        """Delete an analyzer, cascading its versions.

        Args:
            analyzer_id: Id of the analyzer.

        Raises:
            APIError: The request failed, including 404 for a missing
                analyzer.
        """
        await self._client.request("DELETE", f"/api/v1/analyzers/{analyzer_id}")

    async def create_version(
        self,
        analyzer_id: uuid.UUID,
        request: AnalyzerVersionCreateRequest,
        idempotency_key: str | None = None,
    ) -> AnalyzerVersionResponse:
        """Create an analyzer version.

        Args:
            analyzer_id: Id of the analyzer.
            request: Analyzer version create request.
            idempotency_key: Idempotency key overriding the transport's
                random default.

        Raises:
            APIError: The request failed, including 404 for a missing
                analyzer or blob.

        Returns:
            Created analyzer version.
        """
        response = await self._client.request(
            "POST",
            f"/api/v1/analyzers/{analyzer_id}/versions",
            json=request.model_dump(mode="json", exclude_unset=True),
            idempotency_key=idempotency_key,
        )
        return AnalyzerVersionResponse.model_validate(response.json())

    async def list_versions(
        self, analyzer_id: uuid.UUID, params: ListParams | None = None
    ) -> Page[AnalyzerVersionResponse]:
        """List an analyzer's versions.

        Args:
            analyzer_id: Id of the analyzer.
            params: List params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of analyzer versions.
        """
        params = params or ListParams()
        response = await self._client.request(
            "GET",
            f"/api/v1/analyzers/{analyzer_id}/versions",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[AnalyzerVersionResponse].model_validate(response.json())

    async def iter_versions(
        self, analyzer_id: uuid.UUID, params: ListParams | None = None
    ) -> AsyncIterator[AnalyzerVersionResponse]:
        """Iterate over all of an analyzer's versions.

        Args:
            analyzer_id: Id of the analyzer.
            params: List params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every analyzer version.
        """
        async for item in iterate_pages(
            params or ListParams(),
            lambda page_params: self.list_versions(analyzer_id, page_params),
        ):
            yield item

    async def get_version(
        self, analyzer_id: uuid.UUID, version: int
    ) -> AnalyzerVersionResponse:
        """Get an analyzer version by version number.

        Args:
            analyzer_id: Id of the analyzer.
            version: Version number.

        Raises:
            APIError: The request failed, including 404 for a missing
                version.

        Returns:
            Stored analyzer version.
        """
        response = await self._client.request(
            "GET", f"/api/v1/analyzers/{analyzer_id}/versions/{version}"
        )
        return AnalyzerVersionResponse.model_validate(response.json())

    async def update_version(
        self,
        analyzer_id: uuid.UUID,
        version: int,
        request: AnalyzerVersionUpdateRequest,
    ) -> AnalyzerVersionResponse:
        """Update an analyzer version's display version.

        Args:
            analyzer_id: Id of the analyzer.
            version: Version number.
            request: Analyzer version update request, unset fields stay
                unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing
                version.

        Returns:
            Updated analyzer version.
        """
        response = await self._client.request(
            "PATCH",
            f"/api/v1/analyzers/{analyzer_id}/versions/{version}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return AnalyzerVersionResponse.model_validate(response.json())
