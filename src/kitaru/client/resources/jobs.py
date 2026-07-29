"""Jobs resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.job import JobListParams, JobResponse, JobTasksListParams
from kitaru.api_models.v1.task import TaskResponse
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class JobsResource:
    """Job API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        self._client = client

    async def get(self, job_id: uuid.UUID) -> JobResponse:
        response = await self._client.request("GET", f"/v1/jobs/{job_id}")
        return JobResponse.model_validate(response.json())

    async def list(self, params: JobListParams | None = None) -> Page[JobResponse]:
        params = params or JobListParams()
        response = await self._client.request(
            "GET", "/v1/jobs", params=params.model_dump(mode="json", exclude_unset=True)
        )
        return Page[JobResponse].model_validate(response.json())

    async def iter(
        self, params: JobListParams | None = None
    ) -> AsyncIterator[JobResponse]:
        async for item in iterate_pages(params or JobListParams(), self.list):
            yield item

    async def list_tasks(
        self, job_id: uuid.UUID, params: JobTasksListParams | None = None
    ) -> Page[TaskResponse]:
        params = params or JobTasksListParams()
        response = await self._client.request(
            "GET",
            f"/v1/jobs/{job_id}/tasks",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[TaskResponse].model_validate(response.json())

    async def iter_tasks(
        self, job_id: uuid.UUID, params: JobTasksListParams | None = None
    ) -> AsyncIterator[TaskResponse]:
        async for item in iterate_pages(
            params or JobTasksListParams(),
            lambda page_params: self.list_tasks(job_id, page_params),
        ):
            yield item

    async def cancel(self, job_id: uuid.UUID) -> JobResponse:
        response = await self._client.request("POST", f"/v1/jobs/{job_id}/cancel")
        return JobResponse.model_validate(response.json())

    async def delete(self, job_id: uuid.UUID) -> None:
        await self._client.request("DELETE", f"/v1/jobs/{job_id}")
