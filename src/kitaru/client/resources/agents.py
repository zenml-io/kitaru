"""Agents resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.agent import (
    AgentCreateRequest,
    AgentListParams,
    AgentResponse,
    AgentUpdateRequest,
)
from kitaru.api_models.v1.agent_version import (
    AgentVersionCreateRequest,
    AgentVersionResponse,
)
from kitaru.api_models.v1.base import ListParams, Page
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class AgentsResource:
    """Agent API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        self._client = client

    async def create(self, request: AgentCreateRequest) -> AgentResponse:
        response = await self._client.request(
            "POST",
            "/v1/agents",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return AgentResponse.model_validate(response.json())

    async def get(self, agent_id: uuid.UUID) -> AgentResponse:
        response = await self._client.request("GET", f"/v1/agents/{agent_id}")
        return AgentResponse.model_validate(response.json())

    async def list(self, params: AgentListParams | None = None) -> Page[AgentResponse]:
        params = params or AgentListParams()
        response = await self._client.request(
            "GET",
            "/v1/agents",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[AgentResponse].model_validate(response.json())

    async def iter(
        self, params: AgentListParams | None = None
    ) -> AsyncIterator[AgentResponse]:
        async for item in iterate_pages(params or AgentListParams(), self.list):
            yield item

    async def update(
        self, agent_id: uuid.UUID, request: AgentUpdateRequest
    ) -> AgentResponse:
        response = await self._client.request(
            "PATCH",
            f"/v1/agents/{agent_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return AgentResponse.model_validate(response.json())

    async def delete(self, agent_id: uuid.UUID) -> None:
        await self._client.request("DELETE", f"/v1/agents/{agent_id}")

    async def create_version(
        self, agent_id: uuid.UUID, request: AgentVersionCreateRequest
    ) -> AgentVersionResponse:
        response = await self._client.request(
            "POST",
            f"/v1/agents/{agent_id}/versions",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return AgentVersionResponse.model_validate(response.json())

    async def list_versions(
        self, agent_id: uuid.UUID, params: ListParams | None = None
    ) -> Page[AgentVersionResponse]:
        params = params or ListParams()
        response = await self._client.request(
            "GET",
            f"/v1/agents/{agent_id}/versions",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[AgentVersionResponse].model_validate(response.json())

    async def iter_versions(
        self, agent_id: uuid.UUID, params: ListParams | None = None
    ) -> AsyncIterator[AgentVersionResponse]:
        async for item in iterate_pages(
            params or ListParams(),
            lambda page_params: self.list_versions(agent_id, page_params),
        ):
            yield item
