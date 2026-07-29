"""Sessions resource."""

import builtins
import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.evaluation import EvaluationResponse
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionEvaluationsRequest,
    SessionListParams,
    SessionResponse,
    SessionUpdateRequest,
)
from kitaru.api_models.v1.session_node import (
    SessionNodeBatchRequest,
    SessionNodeListParams,
    SessionNodeResponse,
)
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class SessionsResource:
    """Session API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        self._client = client

    async def create(self, request: SessionCreateRequest) -> SessionResponse:
        response = await self._client.request(
            "POST",
            "/v1/sessions",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return SessionResponse.model_validate(response.json())

    async def get(self, session_id: uuid.UUID) -> SessionResponse:
        response = await self._client.request("GET", f"/v1/sessions/{session_id}")
        return SessionResponse.model_validate(response.json())

    async def list(
        self, params: SessionListParams | None = None
    ) -> Page[SessionResponse]:
        params = params or SessionListParams()
        response = await self._client.request(
            "GET",
            "/v1/sessions",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[SessionResponse].model_validate(response.json())

    async def iter(
        self, params: SessionListParams | None = None
    ) -> AsyncIterator[SessionResponse]:
        async for item in iterate_pages(params or SessionListParams(), self.list):
            yield item

    async def update(
        self, session_id: uuid.UUID, request: SessionUpdateRequest
    ) -> SessionResponse:
        response = await self._client.request(
            "PATCH",
            f"/v1/sessions/{session_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return SessionResponse.model_validate(response.json())

    async def delete(self, session_id: uuid.UUID) -> None:
        await self._client.request("DELETE", f"/v1/sessions/{session_id}")

    async def ingest_nodes(
        self, session_id: uuid.UUID, request: SessionNodeBatchRequest
    ) -> builtins.list[SessionNodeResponse]:
        response = await self._client.request(
            "POST",
            f"/v1/sessions/{session_id}/nodes",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return [SessionNodeResponse.model_validate(item) for item in response.json()]

    async def list_nodes(
        self,
        session_id: uuid.UUID,
        params: SessionNodeListParams | None = None,
        *,
        include_payloads: bool | None = None,
    ) -> Page[SessionNodeResponse]:
        params = params or SessionNodeListParams()
        if include_payloads is not None:
            params = params.model_copy(update={"include_payloads": include_payloads})
        response = await self._client.request(
            "GET",
            f"/v1/sessions/{session_id}/nodes",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[SessionNodeResponse].model_validate(response.json())

    async def iter_nodes(
        self,
        session_id: uuid.UUID,
        params: SessionNodeListParams | None = None,
        *,
        include_payloads: bool | None = None,
    ) -> AsyncIterator[SessionNodeResponse]:
        params = params or SessionNodeListParams()
        if include_payloads is not None:
            params = params.model_copy(update={"include_payloads": include_payloads})
        async for item in iterate_pages(
            params,
            lambda page_params: self.list_nodes(session_id, page_params),
        ):
            yield item

    async def merge_evaluations(
        self, session_id: uuid.UUID, request: SessionEvaluationsRequest
    ) -> builtins.list[EvaluationResponse]:
        response = await self._client.request(
            "POST",
            f"/v1/sessions/{session_id}/evaluations",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return [EvaluationResponse.model_validate(item) for item in response.json()]
