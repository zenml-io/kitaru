"""Evaluations resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.evaluation import (
    EvaluationBatchCreateRequest,
    EvaluationListParams,
    EvaluationResponse,
)
from kitaru.api_models.v1.job import JobResponse
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class EvaluationsResource:
    """Evaluation API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        self._client = client

    async def create(self, request: EvaluationBatchCreateRequest) -> JobResponse:
        response = await self._client.request(
            "POST",
            "/v1/evaluations",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return JobResponse.model_validate(response.json())

    async def get(self, evaluation_id: uuid.UUID) -> EvaluationResponse:
        response = await self._client.request("GET", f"/v1/evaluations/{evaluation_id}")
        return EvaluationResponse.model_validate(response.json())

    async def list(
        self, params: EvaluationListParams | None = None
    ) -> Page[EvaluationResponse]:
        params = params or EvaluationListParams()
        response = await self._client.request(
            "GET",
            "/v1/evaluations",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[EvaluationResponse].model_validate(response.json())

    async def iter(
        self, params: EvaluationListParams | None = None
    ) -> AsyncIterator[EvaluationResponse]:
        async for item in iterate_pages(params or EvaluationListParams(), self.list):
            yield item
