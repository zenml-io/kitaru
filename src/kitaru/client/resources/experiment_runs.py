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
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.experiment_run import (
    ExperimentRunJobsListParams,
    ExperimentRunListParams,
    ExperimentRunResponse,
)
from kitaru.api_models.v1.job import JobResponse

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

    async def get(self, experiment_run_id: uuid.UUID) -> ExperimentRunResponse:
        """Get an experiment run by id.

        Args:
            experiment_run_id: Id of the run.

        Raises:
            APIError: The request failed, including 404 for a missing run.

        Returns:
            Stored experiment run.
        """
        response = await self._client.request(
            "GET", f"/v1/experiment-runs/{experiment_run_id}"
        )
        return ExperimentRunResponse.model_validate(response.json())

    async def list(
        self, params: ExperimentRunListParams | None = None
    ) -> Page[ExperimentRunResponse]:
        """List experiment runs.

        Args:
            params: Experiment run list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of experiment runs.
        """
        params = params or ExperimentRunListParams()
        response = await self._client.request(
            "GET",
            "/v1/experiment-runs",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[ExperimentRunResponse].model_validate(response.json())

    async def iter(
        self, params: ExperimentRunListParams | None = None
    ) -> AsyncIterator[ExperimentRunResponse]:
        """Iterate over all experiment runs.

        Args:
            params: Experiment run list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every experiment run.
        """
        params = params or ExperimentRunListParams()
        while True:
            page = await self.list(params)
            for item in page.items:
                yield item
            if page.next_cursor is None:
                break
            params = params.model_copy(update={"cursor": page.next_cursor})

    async def delete(self, experiment_run_id: uuid.UUID) -> None:
        """Delete an experiment run and its jobs.

        Args:
            experiment_run_id: Id of the run.

        Raises:
            APIError: The request failed, including 404 for a missing run.
        """
        await self._client.request("DELETE", f"/v1/experiment-runs/{experiment_run_id}")

    async def list_jobs(
        self,
        experiment_run_id: uuid.UUID,
        params: ExperimentRunJobsListParams | None = None,
    ) -> Page[JobResponse]:
        """List the jobs backing an experiment run's replays.

        Args:
            experiment_run_id: Id of the run.
            params: Experiment run jobs list params.

        Raises:
            APIError: The request failed, including 404 for a missing run.

        Returns:
            Page of jobs.
        """
        params = params or ExperimentRunJobsListParams()
        response = await self._client.request(
            "GET",
            f"/v1/experiment-runs/{experiment_run_id}/jobs",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[JobResponse].model_validate(response.json())

    async def iter_jobs(
        self,
        experiment_run_id: uuid.UUID,
        params: ExperimentRunJobsListParams | None = None,
    ) -> AsyncIterator[JobResponse]:
        """Iterate over all jobs backing an experiment run's replays.

        Args:
            experiment_run_id: Id of the run.
            params: Experiment run jobs list params.

        Raises:
            APIError: The request failed, including 404 for a missing run.

        Returns:
            Async iterator over every job of the run.
        """
        params = params or ExperimentRunJobsListParams()
        while True:
            page = await self.list_jobs(experiment_run_id, params)
            for item in page.items:
                yield item
            if page.next_cursor is None:
                break
            params = params.model_copy(update={"cursor": page.next_cursor})

    async def cancel(self, experiment_run_id: uuid.UUID) -> ExperimentRunResponse:
        """Request cancellation of a running experiment run.

        Args:
            experiment_run_id: Id of the run.

        Raises:
            APIError: The request failed, including 404 for a missing run
                and 409 when the run is not running.

        Returns:
            Run carrying the cancel request.
        """
        response = await self._client.request(
            "POST", f"/v1/experiment-runs/{experiment_run_id}/cancel"
        )
        return ExperimentRunResponse.model_validate(response.json())
