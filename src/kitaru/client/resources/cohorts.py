"""Cohorts resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import ListParams, Page
from kitaru.api_models.v1.cohort import (
    CohortCreateRequest,
    CohortListParams,
    CohortResponse,
    CohortUpdateRequest,
)
from kitaru.api_models.v1.session import SessionResponse
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class CohortsResource:
    """Cohort API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        self._client = client

    async def create(self, request: CohortCreateRequest) -> CohortResponse:
        response = await self._client.request(
            "POST",
            "/v1/cohorts",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return CohortResponse.model_validate(response.json())

    async def get(self, cohort_id: uuid.UUID) -> CohortResponse:
        response = await self._client.request("GET", f"/v1/cohorts/{cohort_id}")
        return CohortResponse.model_validate(response.json())

    async def list(
        self, params: CohortListParams | None = None
    ) -> Page[CohortResponse]:
        params = params or CohortListParams()
        response = await self._client.request(
            "GET",
            "/v1/cohorts",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[CohortResponse].model_validate(response.json())

    async def iter(
        self, params: CohortListParams | None = None
    ) -> AsyncIterator[CohortResponse]:
        async for item in iterate_pages(params or CohortListParams(), self.list):
            yield item

    async def list_sessions(
        self, cohort_id: uuid.UUID, params: ListParams | None = None
    ) -> Page[SessionResponse]:
        params = params or ListParams()
        response = await self._client.request(
            "GET",
            f"/v1/cohorts/{cohort_id}/sessions",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[SessionResponse].model_validate(response.json())

    async def iter_sessions(
        self, cohort_id: uuid.UUID, params: ListParams | None = None
    ) -> AsyncIterator[SessionResponse]:
        async for item in iterate_pages(
            params or ListParams(),
            lambda page_params: self.list_sessions(cohort_id, page_params),
        ):
            yield item

    async def update(
        self, cohort_id: uuid.UUID, request: CohortUpdateRequest
    ) -> CohortResponse:
        response = await self._client.request(
            "PATCH",
            f"/v1/cohorts/{cohort_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return CohortResponse.model_validate(response.json())

    async def delete(self, cohort_id: uuid.UUID) -> None:
        await self._client.request("DELETE", f"/v1/cohorts/{cohort_id}")
