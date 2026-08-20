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
"""Experiments resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.experiment import (
    ExperimentCreateRequest,
    ExperimentListParams,
    ExperimentResponse,
    ExperimentUpdateRequest,
)
from kitaru.api_models.v1.experiment_run import (
    ExperimentRunCreateRequest,
    ExperimentRunResponse,
)
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class ExperimentsResource:
    """Experiment API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(self, request: ExperimentCreateRequest) -> ExperimentResponse:
        """Create an experiment.

        Args:
            request: Experiment create request.

        Raises:
            APIError: The request failed, including 404 when the agent does
                not exist or an evaluator name or version is unknown, and
                409 for a duplicate name.

        Returns:
            Created experiment.
        """
        response = await self._client.request(
            "POST",
            "/api/v1/experiments",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ExperimentResponse.model_validate(response.json())

    async def get(
        self, experiment_id: uuid.UUID, *, max_bytes: int | None = None
    ) -> ExperimentResponse:
        """Get an experiment by id.

        Args:
            experiment_id: Id of the experiment.
            max_bytes: Maximum response bytes to read.

        Raises:
            APIError: The request failed, including 404 for a missing
                experiment.

        Returns:
            Stored experiment.
        """
        response = await self._client.request(
            "GET",
            f"/api/v1/experiments/{experiment_id}",
            max_response_bytes=max_bytes,
        )
        return ExperimentResponse.model_validate(response.json())

    async def list(
        self, params: ExperimentListParams | None = None
    ) -> Page[ExperimentResponse]:
        """List experiments.

        Args:
            params: Experiment list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of experiments.
        """
        params = params or ExperimentListParams()
        response = await self._client.request(
            "GET",
            "/api/v1/experiments",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[ExperimentResponse].model_validate(response.json())

    async def iter(
        self, params: ExperimentListParams | None = None
    ) -> AsyncIterator[ExperimentResponse]:
        """Iterate over all experiments.

        Args:
            params: Experiment list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every experiment.
        """
        async for item in iterate_pages(params or ExperimentListParams(), self.list):
            yield item

    async def update(
        self, experiment_id: uuid.UUID, request: ExperimentUpdateRequest
    ) -> ExperimentResponse:
        """Update an experiment.

        Args:
            experiment_id: Id of the experiment.
            request: Experiment update request, unset fields stay unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing
                experiment or an unknown evaluator name or version.

        Returns:
            Updated experiment.
        """
        response = await self._client.request(
            "PATCH",
            f"/api/v1/experiments/{experiment_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ExperimentResponse.model_validate(response.json())

    async def delete(self, experiment_id: uuid.UUID) -> None:
        """Delete an experiment.

        Args:
            experiment_id: Id of the experiment.

        Raises:
            APIError: The request failed, including 404 for a missing
                experiment.
        """
        await self._client.request("DELETE", f"/api/v1/experiments/{experiment_id}")

    async def start_run(
        self, experiment_id: uuid.UUID, request: ExperimentRunCreateRequest
    ) -> ExperimentRunResponse:
        """Start an experiment run, fanning out one replay per cohort version session.

        Args:
            experiment_id: Id of the experiment.
            request: Experiment run create request.

        Raises:
            APIError: The request failed, including 404 for a missing
                experiment, cohort version, or agent version, and 422 for an
                empty or mismatched cohort version, a mismatched agent
                version, or a missing agent version resolution.

        Returns:
            Created run.
        """
        response = await self._client.request(
            "POST",
            f"/api/v1/experiments/{experiment_id}/runs",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ExperimentRunResponse.model_validate(response.json())
