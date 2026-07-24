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
from typing import TYPE_CHECKING, Any

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.jobs import (
    JobHeartbeatResponse,
    JobResponse,
    JobSpecResponse,
    JobStatus,
    JobUpdateRequest,
    ReplayDiffResponse,
    StandaloneJobClaimRequest,
    ToolLookupRequest,
    ToolLookupResponse,
)

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

    async def list(
        self,
        experiment_run_id: uuid.UUID | None = None,
        original_session_id: uuid.UUID | None = None,
        status: JobStatus | None = None,
        standalone: bool | None = None,
        worker_id: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Page[JobResponse]:
        """List jobs.

        Args:
            experiment_run_id: Filter on experiment run id.
            original_session_id: Filter on the replayed session id.
            status: Filter on job status.
            standalone: Filter on standalone jobs.
            worker_id: Filter on the claiming worker id.
            page: Page number.
            page_size: Page size.

        Raises:
            APIError: The request failed.

        Returns:
            Page of jobs.
        """
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if experiment_run_id is not None:
            params["experiment_run_id"] = str(experiment_run_id)
        if original_session_id is not None:
            params["original_session_id"] = str(original_session_id)
        if status is not None:
            params["status"] = status.value
        if standalone is not None:
            params["standalone"] = standalone
        if worker_id is not None:
            params["worker_id"] = worker_id
        response = await self._client.request("GET", "/v1/jobs", params=params)
        return Page[JobResponse].model_validate(response.json())

    async def get(self, job_id: uuid.UUID) -> JobResponse:
        """Get a job by id.

        Args:
            job_id: Id of the job.

        Raises:
            APIError: The request failed, including 404 for a missing
                job.

        Returns:
            Stored job.
        """
        response = await self._client.request("GET", f"/v1/jobs/{job_id}")
        return JobResponse.model_validate(response.json())

    async def get_spec(self, job_id: uuid.UUID) -> JobSpecResponse:
        """Resolve the spec a runner executes a job with.

        Args:
            job_id: Id of the job.

        Raises:
            APIError: The request failed, including 404 for a missing
                job and 409 when the stamped agent version has no run
                spec.

        Returns:
            Resolved job spec.
        """
        response = await self._client.request("GET", f"/v1/jobs/{job_id}/spec")
        return JobSpecResponse.model_validate(response.json())

    async def update(self, job_id: uuid.UUID, request: JobUpdateRequest) -> JobResponse:
        """Transition a job through the runner status updates.

        Args:
            job_id: Id of the job.
            request: Job update request.

        Raises:
            APIError: The request failed, including 404 for a missing
                job and 409 for an illegal transition or completing
                without a linked result session.

        Returns:
            Updated job.
        """
        response = await self._client.request(
            "PATCH",
            f"/v1/jobs/{job_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return JobResponse.model_validate(response.json())

    async def claim(
        self, job_id: uuid.UUID, request: StandaloneJobClaimRequest
    ) -> JobResponse:
        """Claim a standalone job for a worker.

        Args:
            job_id: Id of the job.
            request: Standalone job claim request.

        Raises:
            APIError: The request failed, including 404 for a missing
                job and 409 when the job belongs to an experiment
                run or is not pending.

        Returns:
            Claimed job.
        """
        response = await self._client.request(
            "POST",
            f"/v1/jobs/{job_id}/claim",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return JobResponse.model_validate(response.json())

    async def release(self, job_id: uuid.UUID) -> JobResponse:
        """Requeue a claimed or running job for another attempt.

        Args:
            job_id: Id of the job.

        Raises:
            APIError: The request failed, including 404 for a missing
                job and 409 when the job is not claimed or running.

        Returns:
            Requeued job.
        """
        response = await self._client.request("POST", f"/v1/jobs/{job_id}/release")
        return JobResponse.model_validate(response.json())

    async def retry(self, job_id: uuid.UUID) -> JobResponse:
        """Requeue a finished standalone job for another attempt.

        Args:
            job_id: Id of the job.

        Raises:
            APIError: The request failed, including 404 for a missing
                job and 409 when the job belongs to an experiment
                run or is not failed, timed out, or canceled.

        Returns:
            Requeued job.
        """
        response = await self._client.request("POST", f"/v1/jobs/{job_id}/retry")
        return JobResponse.model_validate(response.json())

    async def delete(self, job_id: uuid.UUID) -> None:
        """Delete a standalone job, including its unreferenced config.

        Args:
            job_id: Id of the job.

        Raises:
            APIError: The request failed, including 404 for a missing
                job and 409 when the job belongs to an experiment
                run or is claimed or running.
        """
        await self._client.request("DELETE", f"/v1/jobs/{job_id}")

    async def heartbeat(self, job_id: uuid.UUID) -> JobHeartbeatResponse:
        """Record a worker heartbeat on a job.

        Terminal jobs report the stop flag instead of recording.

        Args:
            job_id: Id of the job.

        Raises:
            APIError: The request failed, including 404 for a missing
                job and 409 when the job is pending.

        Returns:
            Heartbeat response with the status and stop flag.
        """
        response = await self._client.request("POST", f"/v1/jobs/{job_id}/heartbeat")
        return JobHeartbeatResponse.model_validate(response.json())

    async def tool_lookup(
        self, job_id: uuid.UUID, request: ToolLookupRequest
    ) -> ToolLookupResponse:
        """Resolve a history tool policy lookup within its scope.

        Args:
            job_id: Id of the job.
            request: Tool lookup request.

        Raises:
            APIError: The request failed, including 404 for a missing
                job and 422 for a cache key mismatch or a tool without
                a history policy.

        Returns:
            Tool lookup response.
        """
        response = await self._client.request(
            "POST",
            f"/v1/jobs/{job_id}/tool-lookup",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ToolLookupResponse.model_validate(response.json())

    async def get_diff(self, job_id: uuid.UUID) -> ReplayDiffResponse:
        """Compute the full diff between a job's sessions.

        Args:
            job_id: Id of the job.

        Raises:
            APIError: The request failed, including 404 for a missing
                job and 409 when the job has no result session yet.

        Returns:
            Computed replay diff.
        """
        response = await self._client.request("GET", f"/v1/jobs/{job_id}/diff")
        return ReplayDiffResponse.model_validate(response.json())
