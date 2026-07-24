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
"""Experiment runs resource."""

import uuid
from typing import TYPE_CHECKING, Any

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.experiment_runs import (
    ExperimentRunResponse,
    ExperimentRunStatus,
)
from kitaru.api_models.v1.jobs import (
    JobClaimRequest,
    JobClaimResponse,
    JobResponse,
    JobStatus,
)

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class ExperimentRunsResource:
    """Experiment run API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def list(
        self,
        experiment_id: uuid.UUID | None = None,
        status: ExperimentRunStatus | None = None,
        tag: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Page[ExperimentRunResponse]:
        """List experiment runs.

        Args:
            experiment_id: Filter on experiment id.
            status: Filter on run status.
            tag: Filter on attached tag name.
            page: Page number.
            page_size: Page size.

        Raises:
            APIError: The request failed, including 404 for a missing
                experiment.

        Returns:
            Page of experiment runs.
        """
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if experiment_id is not None:
            params["experiment_id"] = str(experiment_id)
        if status is not None:
            params["status"] = status.value
        if tag is not None:
            params["tag"] = tag
        response = await self._client.request(
            "GET", "/v1/experiment-runs", params=params
        )
        return Page[ExperimentRunResponse].model_validate(response.json())

    async def get(self, run_id: uuid.UUID) -> ExperimentRunResponse:
        """Get an experiment run by id.

        Args:
            run_id: Id of the experiment run.

        Raises:
            APIError: The request failed, including 404 for a missing
                experiment run.

        Returns:
            Stored experiment run.
        """
        response = await self._client.request("GET", f"/v1/experiment-runs/{run_id}")
        return ExperimentRunResponse.model_validate(response.json())

    async def list_jobs(
        self,
        run_id: uuid.UUID,
        status: JobStatus | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Page[JobResponse]:
        """List the jobs of an experiment run.

        Args:
            run_id: Id of the experiment run.
            status: Filter on job status.
            page: Page number.
            page_size: Page size.

        Raises:
            APIError: The request failed, including 404 for a missing
                experiment run.

        Returns:
            Page of jobs.
        """
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if status is not None:
            params["status"] = status.value
        response = await self._client.request(
            "GET",
            f"/v1/experiment-runs/{run_id}/jobs",
            params=params,
        )
        return Page[JobResponse].model_validate(response.json())

    async def claim(
        self, run_id: uuid.UUID, request: JobClaimRequest
    ) -> JobClaimResponse:
        """Atomically claim pending jobs of an experiment run.

        Args:
            run_id: Id of the experiment run.
            request: Job claim request.

        Raises:
            APIError: The request failed, including 404 for a missing
                experiment run.

        Returns:
            Claimed jobs.
        """
        response = await self._client.request(
            "POST",
            f"/v1/experiment-runs/{run_id}/claim",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return JobClaimResponse.model_validate(response.json())

    async def cancel(self, run_id: uuid.UUID) -> ExperimentRunResponse:
        """Cancel an experiment run.

        Args:
            run_id: Id of the experiment run.

        Raises:
            APIError: The request failed, including 404 for a missing
                experiment run and 409 when the run is already terminal.

        Returns:
            Updated experiment run.
        """
        response = await self._client.request(
            "POST", f"/v1/experiment-runs/{run_id}/cancel"
        )
        return ExperimentRunResponse.model_validate(response.json())

    async def delete(self, run_id: uuid.UUID) -> None:
        """Delete a terminal experiment run, including its jobs.

        Args:
            run_id: Id of the experiment run.

        Raises:
            APIError: The request failed, including 404 for a missing
                experiment run and 409 when the run is not terminal.
        """
        await self._client.request("DELETE", f"/v1/experiment-runs/{run_id}")
