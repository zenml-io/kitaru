"""Evaluators resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import ListParams, Page
from kitaru.api_models.v1.evaluator import (
    EvaluatorCreateRequest,
    EvaluatorListParams,
    EvaluatorResponse,
    EvaluatorUpdateRequest,
    EvaluatorVersionCreateRequest,
    EvaluatorVersionResponse,
    EvaluatorVersionUpdateRequest,
)
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class EvaluatorsResource:
    """Evaluator API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        self._client = client

    async def create(self, request: EvaluatorCreateRequest) -> EvaluatorResponse:
        response = await self._client.request(
            "POST",
            "/v1/evaluators",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return EvaluatorResponse.model_validate(response.json())

    async def get(self, evaluator_id: uuid.UUID) -> EvaluatorResponse:
        response = await self._client.request("GET", f"/v1/evaluators/{evaluator_id}")
        return EvaluatorResponse.model_validate(response.json())

    async def list(
        self, params: EvaluatorListParams | None = None
    ) -> Page[EvaluatorResponse]:
        params = params or EvaluatorListParams()
        response = await self._client.request(
            "GET",
            "/v1/evaluators",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[EvaluatorResponse].model_validate(response.json())

    async def iter(
        self, params: EvaluatorListParams | None = None
    ) -> AsyncIterator[EvaluatorResponse]:
        async for item in iterate_pages(params or EvaluatorListParams(), self.list):
            yield item

    async def update(
        self, evaluator_id: uuid.UUID, request: EvaluatorUpdateRequest
    ) -> EvaluatorResponse:
        response = await self._client.request(
            "PATCH",
            f"/v1/evaluators/{evaluator_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return EvaluatorResponse.model_validate(response.json())

    async def delete(self, evaluator_id: uuid.UUID) -> None:
        await self._client.request("DELETE", f"/v1/evaluators/{evaluator_id}")

    async def create_version(
        self, evaluator_id: uuid.UUID, request: EvaluatorVersionCreateRequest
    ) -> EvaluatorVersionResponse:
        response = await self._client.request(
            "POST",
            f"/v1/evaluators/{evaluator_id}/versions",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return EvaluatorVersionResponse.model_validate(response.json())

    async def list_versions(
        self, evaluator_id: uuid.UUID, params: ListParams | None = None
    ) -> Page[EvaluatorVersionResponse]:
        params = params or ListParams()
        response = await self._client.request(
            "GET",
            f"/v1/evaluators/{evaluator_id}/versions",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[EvaluatorVersionResponse].model_validate(response.json())

    async def iter_versions(
        self, evaluator_id: uuid.UUID, params: ListParams | None = None
    ) -> AsyncIterator[EvaluatorVersionResponse]:
        async for item in iterate_pages(
            params or ListParams(),
            lambda page_params: self.list_versions(evaluator_id, page_params),
        ):
            yield item

    async def get_version(
        self, evaluator_id: uuid.UUID, version: int
    ) -> EvaluatorVersionResponse:
        response = await self._client.request(
            "GET", f"/v1/evaluators/{evaluator_id}/versions/{version}"
        )
        return EvaluatorVersionResponse.model_validate(response.json())

    async def update_version(
        self,
        evaluator_id: uuid.UUID,
        version: int,
        request: EvaluatorVersionUpdateRequest,
    ) -> EvaluatorVersionResponse:
        response = await self._client.request(
            "PATCH",
            f"/v1/evaluators/{evaluator_id}/versions/{version}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return EvaluatorVersionResponse.model_validate(response.json())
