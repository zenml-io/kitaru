"""Replays resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.replay import (
    ReplayCreateRequest,
    ReplayListParams,
    ReplayResponse,
    ToolLookupRequest,
    ToolLookupResponse,
)
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class ReplaysResource:
    """Replay API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        self._client = client

    async def create(self, request: ReplayCreateRequest) -> ReplayResponse:
        response = await self._client.request(
            "POST",
            "/v1/replays",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ReplayResponse.model_validate(response.json())

    async def get(self, replay_id: uuid.UUID) -> ReplayResponse:
        response = await self._client.request("GET", f"/v1/replays/{replay_id}")
        return ReplayResponse.model_validate(response.json())

    async def list(
        self, params: ReplayListParams | None = None
    ) -> Page[ReplayResponse]:
        params = params or ReplayListParams()
        response = await self._client.request(
            "GET",
            "/v1/replays",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[ReplayResponse].model_validate(response.json())

    async def iter(
        self, params: ReplayListParams | None = None
    ) -> AsyncIterator[ReplayResponse]:
        async for item in iterate_pages(params or ReplayListParams(), self.list):
            yield item

    async def tool_lookup(
        self, replay_id: uuid.UUID, request: ToolLookupRequest
    ) -> ToolLookupResponse:
        response = await self._client.request(
            "POST",
            f"/v1/replays/{replay_id}/tool-lookup",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ToolLookupResponse.model_validate(response.json())
