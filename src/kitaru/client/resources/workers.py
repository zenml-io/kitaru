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
"""Workers resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.worker import (
    WorkerCreateRequest,
    WorkerHeartbeatRequest,
    WorkerHeartbeatResponse,
    WorkerListParams,
    WorkerRegistrationResponse,
    WorkerResponse,
)
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class WorkersResource:
    """Worker API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(self, request: WorkerCreateRequest) -> WorkerRegistrationResponse:
        """Register a worker, upserting by name.

        Args:
            request: Worker create request.

        Raises:
            APIError: The request failed.

        Returns:
            Registered worker, its token, and the token's expiry.
        """
        response = await self._client.request(
            "POST",
            "/v1/workers",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return WorkerRegistrationResponse.model_validate(response.json())

    async def heartbeat(
        self, worker_id: uuid.UUID, request: WorkerHeartbeatRequest
    ) -> WorkerHeartbeatResponse:
        """Report the tasks a worker currently holds.

        Args:
            worker_id: Id of the worker.
            request: Worker heartbeat request.

        Raises:
            APIError: The request failed, including 404 for a missing worker.

        Returns:
            Held tasks the worker should stop running.
        """
        response = await self._client.request(
            "POST",
            f"/v1/workers/{worker_id}/heartbeat",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return WorkerHeartbeatResponse.model_validate(response.json())

    async def get(self, worker_id: uuid.UUID) -> WorkerResponse:
        """Get a worker by id.

        Args:
            worker_id: Id of the worker.

        Raises:
            APIError: The request failed, including 404 for a missing worker.

        Returns:
            Stored worker.
        """
        response = await self._client.request("GET", f"/v1/workers/{worker_id}")
        return WorkerResponse.model_validate(response.json())

    async def list(
        self, params: WorkerListParams | None = None
    ) -> Page[WorkerResponse]:
        """List workers.

        Args:
            params: Worker list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of workers.
        """
        params = params or WorkerListParams()
        response = await self._client.request(
            "GET",
            "/v1/workers",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[WorkerResponse].model_validate(response.json())

    async def iter(
        self, params: WorkerListParams | None = None
    ) -> AsyncIterator[WorkerResponse]:
        """Iterate over all workers.

        Args:
            params: Worker list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every worker.
        """
        async for item in iterate_pages(params or WorkerListParams(), self.list):
            yield item

    async def delete(self, worker_id: uuid.UUID) -> None:
        """Delete a worker.

        Args:
            worker_id: Id of the worker.

        Raises:
            APIError: The request failed, including 404 for a missing worker.
        """
        await self._client.request("DELETE", f"/v1/workers/{worker_id}")
