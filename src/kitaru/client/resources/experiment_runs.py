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
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class ExperimentRunsResource:
    """Experiment run API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        self._client = client

    async def get(self, run_id: uuid.UUID) -> ExperimentRunResponse:
        response = await self._client.request("GET", f"/v1/experiment-runs/{run_id}")
        return ExperimentRunResponse.model_validate(response.json())

    async def list(
        self, params: ExperimentRunListParams | None = None
    ) -> Page[ExperimentRunResponse]:
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
        async for item in iterate_pages(params or ExperimentRunListParams(), self.list):
            yield item

    async def list_jobs(
        self, run_id: uuid.UUID, params: ExperimentRunJobsListParams | None = None
    ) -> Page[JobResponse]:
        params = params or ExperimentRunJobsListParams()
        response = await self._client.request(
            "GET",
            f"/v1/experiment-runs/{run_id}/jobs",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[JobResponse].model_validate(response.json())

    async def iter_jobs(
        self, run_id: uuid.UUID, params: ExperimentRunJobsListParams | None = None
    ) -> AsyncIterator[JobResponse]:
        async for item in iterate_pages(
            params or ExperimentRunJobsListParams(),
            lambda page_params: self.list_jobs(run_id, page_params),
        ):
            yield item

    async def cancel(self, run_id: uuid.UUID) -> ExperimentRunResponse:
        response = await self._client.request(
            "POST", f"/v1/experiment-runs/{run_id}/cancel"
        )
        return ExperimentRunResponse.model_validate(response.json())

    async def delete(self, run_id: uuid.UUID) -> None:
        await self._client.request("DELETE", f"/v1/experiment-runs/{run_id}")
