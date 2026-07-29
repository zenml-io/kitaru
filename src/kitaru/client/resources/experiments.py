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
        self._client = client

    async def create(self, request: ExperimentCreateRequest) -> ExperimentResponse:
        response = await self._client.request(
            "POST",
            "/v1/experiments",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ExperimentResponse.model_validate(response.json())

    async def get(self, experiment_id: uuid.UUID) -> ExperimentResponse:
        response = await self._client.request("GET", f"/v1/experiments/{experiment_id}")
        return ExperimentResponse.model_validate(response.json())

    async def list(
        self, params: ExperimentListParams | None = None
    ) -> Page[ExperimentResponse]:
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
        async for item in iterate_pages(params or ExperimentListParams(), self.list):
            yield item

    async def update(
        self, experiment_id: uuid.UUID, request: ExperimentUpdateRequest
    ) -> ExperimentResponse:
        response = await self._client.request(
            "PATCH",
            f"/v1/experiments/{experiment_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ExperimentResponse.model_validate(response.json())

    async def delete(self, experiment_id: uuid.UUID) -> None:
        await self._client.request("DELETE", f"/v1/experiments/{experiment_id}")

    async def start_run(
        self, experiment_id: uuid.UUID, request: ExperimentRunCreateRequest
    ) -> ExperimentRunResponse:
        response = await self._client.request(
            "POST",
            f"/v1/experiments/{experiment_id}/runs",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ExperimentRunResponse.model_validate(response.json())
