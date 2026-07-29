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
"""Jobs resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.job import JobListParams, JobResponse, JobTasksListParams
from kitaru.api_models.v1.task import TaskResponse

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class JobsResource:
    """Job API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def get(self, job_id: uuid.UUID) -> JobResponse:
        """Get a job by id.

        Args:
            job_id: Id of the job.

        Raises:
            APIError: The request failed, including 404 for a missing job.

        Returns:
            Stored job.
        """
        response = await self._client.request("GET", f"/v1/jobs/{job_id}")
        return JobResponse.model_validate(response.json())

    async def list(self, params: JobListParams | None = None) -> Page[JobResponse]:
        """List jobs.

        Args:
            params: Job list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of jobs.
        """
        params = params or JobListParams()
        response = await self._client.request(
            "GET", "/v1/jobs", params=params.model_dump(mode="json", exclude_unset=True)
        )
        return Page[JobResponse].model_validate(response.json())

    async def iter(
        self, params: JobListParams | None = None
    ) -> AsyncIterator[JobResponse]:
        """Iterate over all jobs.

        Args:
            params: Job list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every job.
        """
        params = params or JobListParams()
        while True:
            page = await self.list(params)
            for item in page.items:
                yield item
            if page.next_cursor is None:
                break
            params = params.model_copy(update={"cursor": page.next_cursor})

    async def list_tasks(
        self, job_id: uuid.UUID, params: JobTasksListParams | None = None
    ) -> Page[TaskResponse]:
        """List the tasks of a job.

        Args:
            job_id: Id of the job.
            params: Job tasks list params.

        Raises:
            APIError: The request failed, including 404 for a missing job.

        Returns:
            Page of tasks.
        """
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
        """Iterate over all tasks of a job.

        Args:
            job_id: Id of the job.
            params: Job tasks list params.

        Raises:
            APIError: The request failed, including 404 for a missing job.

        Returns:
            Async iterator over every task of the job.
        """
        params = params or JobTasksListParams()
        while True:
            page = await self.list_tasks(job_id, params)
            for item in page.items:
                yield item
            if page.next_cursor is None:
                break
            params = params.model_copy(update={"cursor": page.next_cursor})

    async def cancel(self, job_id: uuid.UUID) -> JobResponse:
        """Request cancellation of a job.

        Args:
            job_id: Id of the job.

        Raises:
            APIError: The request failed, including 409 for a settled job.

        Returns:
            Job carrying the cancel request.
        """
        response = await self._client.request("POST", f"/v1/jobs/{job_id}/cancel")
        return JobResponse.model_validate(response.json())

    async def delete(self, job_id: uuid.UUID) -> None:
        """Delete a job, cascading its tasks.

        Args:
            job_id: Id of the job.

        Raises:
            APIError: The request failed, including 404 for a missing job.
        """
        await self._client.request("DELETE", f"/v1/jobs/{job_id}")
