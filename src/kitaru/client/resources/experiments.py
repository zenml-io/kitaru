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
from typing import TYPE_CHECKING, Any

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.experiment_runs import (
    ExperimentRunCreateRequest,
    ExperimentRunResponse,
)
from kitaru.api_models.v1.experiments import (
    ExperimentCreateRequest,
    ExperimentResponse,
    ExperimentUpdateRequest,
)

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
        """Create an experiment over a cohort with an inline replay config.

        Args:
            request: Experiment create request.

        Raises:
            APIError: The request failed, including 404 for a missing
                cohort, 409 for a duplicate name, and 422 for an invalid
                config.

        Returns:
            Created experiment.
        """
        response = await self._client.request(
            "POST",
            "/v1/experiments",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ExperimentResponse.model_validate(response.json())

    async def list(
        self,
        name: str | None = None,
        tag: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Page[ExperimentResponse]:
        """List experiments.

        Args:
            name: Filter on experiment name.
            tag: Filter on attached tag name.
            page: Page number.
            page_size: Page size.

        Raises:
            APIError: The request failed.

        Returns:
            Page of experiments.
        """
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if name is not None:
            params["name"] = name
        if tag is not None:
            params["tag"] = tag
        response = await self._client.request("GET", "/v1/experiments", params=params)
        return Page[ExperimentResponse].model_validate(response.json())

    async def get(self, experiment_id: uuid.UUID) -> ExperimentResponse:
        """Get an experiment by id.

        Args:
            experiment_id: Id of the experiment.

        Raises:
            APIError: The request failed, including 404 for a missing
                experiment.

        Returns:
            Stored experiment.
        """
        response = await self._client.request("GET", f"/v1/experiments/{experiment_id}")
        return ExperimentResponse.model_validate(response.json())

    async def update(
        self, experiment_id: uuid.UUID, request: ExperimentUpdateRequest
    ) -> ExperimentResponse:
        """Update an experiment.

        Args:
            experiment_id: Id of the experiment.
            request: Experiment update request, unset fields stay unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing
                experiment, 409 for a duplicate name or a cohort or config
                change on an experiment with runs.

        Returns:
            Updated experiment.
        """
        response = await self._client.request(
            "PATCH",
            f"/v1/experiments/{experiment_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ExperimentResponse.model_validate(response.json())

    async def delete(self, experiment_id: uuid.UUID) -> None:
        """Delete an experiment, including its tag links.

        Args:
            experiment_id: Id of the experiment.

        Raises:
            APIError: The request failed, including 404 for a missing
                experiment and 409 while the experiment has runs.
        """
        await self._client.request("DELETE", f"/v1/experiments/{experiment_id}")

    async def create_run(
        self, experiment_id: uuid.UUID, request: ExperimentRunCreateRequest
    ) -> ExperimentRunResponse:
        """Start an experiment run.

        Args:
            experiment_id: Id of the experiment.
            request: Experiment run create request.

        Raises:
            APIError: The request failed, including 404 for a missing
                experiment or agent version, 409 when no runnable agent
                version resolves, and 422 for a version of another agent.

        Returns:
            Created experiment run.
        """
        response = await self._client.request(
            "POST",
            f"/v1/experiments/{experiment_id}/runs",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ExperimentRunResponse.model_validate(response.json())

    async def list_runs(
        self,
        experiment_id: uuid.UUID,
        page: int = 1,
        page_size: int = 20,
    ) -> Page[ExperimentRunResponse]:
        """List the runs of an experiment.

        Args:
            experiment_id: Id of the experiment.
            page: Page number.
            page_size: Page size.

        Raises:
            APIError: The request failed, including 404 for a missing
                experiment.

        Returns:
            Page of experiment runs.
        """
        response = await self._client.request(
            "GET",
            f"/v1/experiments/{experiment_id}/runs",
            params={"page": page, "page_size": page_size},
        )
        return Page[ExperimentRunResponse].model_validate(response.json())
