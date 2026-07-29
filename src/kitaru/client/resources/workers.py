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
    WorkerResponse,
)
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class WorkersResource:
    """Worker API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        self._client = client

    async def create(self, request: WorkerCreateRequest) -> WorkerResponse:
        response = await self._client.request(
            "POST",
            "/v1/workers",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return WorkerResponse.model_validate(response.json())

    async def get(self, worker_id: uuid.UUID) -> WorkerResponse:
        response = await self._client.request("GET", f"/v1/workers/{worker_id}")
        return WorkerResponse.model_validate(response.json())

    async def list(
        self, params: WorkerListParams | None = None
    ) -> Page[WorkerResponse]:
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
        async for item in iterate_pages(params or WorkerListParams(), self.list):
            yield item

    async def heartbeat(
        self, worker_id: uuid.UUID, request: WorkerHeartbeatRequest
    ) -> WorkerHeartbeatResponse:
        response = await self._client.request(
            "POST",
            f"/v1/workers/{worker_id}/heartbeat",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return WorkerHeartbeatResponse.model_validate(response.json())

    async def delete(self, worker_id: uuid.UUID) -> None:
        await self._client.request("DELETE", f"/v1/workers/{worker_id}")
