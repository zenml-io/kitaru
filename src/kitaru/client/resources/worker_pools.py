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
"""Worker pools resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.worker_pool import (
    WorkerPoolCreateRequest,
    WorkerPoolListParams,
    WorkerPoolResponse,
    WorkerPoolUpdateRequest,
)
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class WorkerPoolsResource:
    """Worker pool API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(self, request: WorkerPoolCreateRequest) -> WorkerPoolResponse:
        """Create a worker pool.

        Args:
            request: Worker pool create request.

        Raises:
            APIError: The request failed, including 409 for a duplicate name
                and 422 when the scope names a job.

        Returns:
            Created worker pool.
        """
        response = await self._client.request(
            "POST",
            "/v1/worker-pools",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return WorkerPoolResponse.model_validate(response.json())

    async def get(self, pool_id: uuid.UUID) -> WorkerPoolResponse:
        """Get a worker pool by id.

        Args:
            pool_id: Id of the worker pool.

        Raises:
            APIError: The request failed, including 404 for a missing
                worker pool.

        Returns:
            Stored worker pool.
        """
        response = await self._client.request("GET", f"/v1/worker-pools/{pool_id}")
        return WorkerPoolResponse.model_validate(response.json())

    async def list(
        self,
        params: WorkerPoolListParams | None = None,
    ) -> Page[WorkerPoolResponse]:
        """List worker pools.

        Args:
            params: Worker pool list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of worker pools.
        """
        params = params or WorkerPoolListParams()
        response = await self._client.request(
            "GET",
            "/v1/worker-pools",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[WorkerPoolResponse].model_validate(response.json())

    async def iter(
        self,
        params: WorkerPoolListParams | None = None,
    ) -> AsyncIterator[WorkerPoolResponse]:
        """Iterate over all worker pools.

        Args:
            params: Worker pool list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every worker pool.
        """
        async for item in iterate_pages(params or WorkerPoolListParams(), self.list):
            yield item

    async def update(
        self, pool_id: uuid.UUID, request: WorkerPoolUpdateRequest
    ) -> WorkerPoolResponse:
        """Update a worker pool's name and scope.

        Args:
            pool_id: Id of the worker pool.
            request: Worker pool update request, unset fields stay unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing worker
                pool, 409 for a duplicate name, and 422 when the update
                clears the name or scope, or the new scope names a job.

        Returns:
            Updated worker pool.
        """
        response = await self._client.request(
            "PATCH",
            f"/v1/worker-pools/{pool_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return WorkerPoolResponse.model_validate(response.json())

    async def delete(self, pool_id: uuid.UUID) -> None:
        """Delete a worker pool.

        Args:
            pool_id: Id of the worker pool.

        Raises:
            APIError: The request failed, including 404 for a missing
                worker pool.
        """
        await self._client.request("DELETE", f"/v1/worker-pools/{pool_id}")
