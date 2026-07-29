"""Tasks resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.task import (
    TaskClaimRequest,
    TaskClaimResponse,
    TaskListParams,
    TaskResponse,
    TaskSpecResponse,
    TaskUpdateRequest,
)
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class TasksResource:
    """Task API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        self._client = client

    async def get(self, task_id: uuid.UUID) -> TaskResponse:
        response = await self._client.request("GET", f"/v1/tasks/{task_id}")
        return TaskResponse.model_validate(response.json())

    async def get_spec(self, task_id: uuid.UUID) -> TaskSpecResponse:
        response = await self._client.request("GET", f"/v1/tasks/{task_id}/spec")
        return TaskSpecResponse.model_validate(response.json())

    async def list(self, params: TaskListParams | None = None) -> Page[TaskResponse]:
        params = params or TaskListParams()
        response = await self._client.request(
            "GET",
            "/v1/tasks",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[TaskResponse].model_validate(response.json())

    async def iter(
        self, params: TaskListParams | None = None
    ) -> AsyncIterator[TaskResponse]:
        async for item in iterate_pages(params or TaskListParams(), self.list):
            yield item

    async def claim(self, request: TaskClaimRequest) -> TaskClaimResponse:
        response = await self._client.request(
            "POST",
            "/v1/tasks/claim",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return TaskClaimResponse.model_validate(response.json())

    async def update(
        self, task_id: uuid.UUID, request: TaskUpdateRequest
    ) -> TaskResponse:
        response = await self._client.request(
            "PATCH",
            f"/v1/tasks/{task_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return TaskResponse.model_validate(response.json())
