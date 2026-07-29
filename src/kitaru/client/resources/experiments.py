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
            APIError: The request failed, including 404 for an unknown
                evaluator name or version and 409 for a duplicate name.

        Returns:
            Created experiment.
        """
        response = await self._client.request(
            "POST",
            "/v1/experiments",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ExperimentResponse.model_validate(response.json())

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
            "/v1/experiments",
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
        params = params or ExperimentListParams()
        while True:
            page = await self.list(params)
            for item in page.items:
                yield item
            if page.next_cursor is None:
                break
            params = params.model_copy(update={"cursor": page.next_cursor})

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
            f"/v1/experiments/{experiment_id}",
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
        await self._client.request("DELETE", f"/v1/experiments/{experiment_id}")
