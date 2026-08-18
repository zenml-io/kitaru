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
    AgentVersionListParams,
    AgentVersionResponse,
)
from kitaru.api_models.v1.base import Page
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class AgentsResource:
    """Agent API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(
        self, request: AgentCreateRequest, idempotency_key: str | None = None
    ) -> AgentResponse:
        """Create an agent.

        Args:
            request: Agent create request.
            idempotency_key: Idempotency key overriding the transport's
                random default.

        Raises:
            APIError: The request failed, including 409 for a duplicate name.

        Returns:
            Created agent.
        """
        response = await self._client.request(
            "POST",
            "/api/v1/agents",
            json=request.model_dump(mode="json", exclude_unset=True),
            idempotency_key=idempotency_key,
        )
        return AgentResponse.model_validate(response.json())

    async def get(self, agent_id: uuid.UUID) -> AgentResponse:
        """Get an agent by id.

        Args:
            agent_id: Id of the agent.

        Raises:
            APIError: The request failed, including 404 for a missing agent.

        Returns:
            Stored agent.
        """
        response = await self._client.request("GET", f"/api/v1/agents/{agent_id}")
        return AgentResponse.model_validate(response.json())

    async def list(
        self,
        params: AgentListParams | None = None,
    ) -> Page[AgentResponse]:
        """List agents.

        Args:
            params: Agent list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of agents.
        """
        params = params or AgentListParams()
        response = await self._client.request(
            "GET",
            "/api/v1/agents",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[AgentResponse].model_validate(response.json())

    async def iter(
        self,
        params: AgentListParams | None = None,
    ) -> AsyncIterator[AgentResponse]:
        """Iterate over all agents.

        Args:
            params: Agent list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every agent.
        """
        async for item in iterate_pages(params or AgentListParams(), self.list):
            yield item

    async def update(
        self, agent_id: uuid.UUID, request: AgentUpdateRequest
    ) -> AgentResponse:
        """Update an agent.

        Args:
            agent_id: Id of the agent.
            request: Agent update request, unset fields stay unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing agent.

        Returns:
            Updated agent.
        """
        response = await self._client.request(
            "PATCH",
            f"/api/v1/agents/{agent_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return AgentResponse.model_validate(response.json())

    async def delete(self, agent_id: uuid.UUID) -> None:
        """Delete an agent.

        Args:
            agent_id: Id of the agent.

        Raises:
            APIError: The request failed, including 404 for a missing agent.
        """
        await self._client.request("DELETE", f"/api/v1/agents/{agent_id}")

    async def create_version(
        self,
        agent_id: uuid.UUID,
        request: AgentVersionCreateRequest,
        idempotency_key: str | None = None,
    ) -> AgentVersionResponse:
        """Create a new version of an agent.

        Args:
            agent_id: Id of the agent.
            request: Agent version create request.
            idempotency_key: Idempotency key overriding the transport's
                random default.

        Raises:
            APIError: The request failed, including 404 for a missing agent.

        Returns:
            Created agent version.
        """
        response = await self._client.request(
            "POST",
            f"/api/v1/agents/{agent_id}/versions",
            json=request.model_dump(mode="json", exclude_unset=True),
            idempotency_key=idempotency_key,
        )
        return AgentVersionResponse.model_validate(response.json())

    async def list_versions(
        self, agent_id: uuid.UUID, params: AgentVersionListParams | None = None
    ) -> Page[AgentVersionResponse]:
        """List the versions of an agent.

        Args:
            agent_id: Id of the agent.
            params: Agent version list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of agent versions.
        """
        params = params or AgentVersionListParams()
        response = await self._client.request(
            "GET",
            f"/api/v1/agents/{agent_id}/versions",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[AgentVersionResponse].model_validate(response.json())

    async def iter_versions(
        self, agent_id: uuid.UUID, params: AgentVersionListParams | None = None
    ) -> AsyncIterator[AgentVersionResponse]:
        """Iterate over every version of an agent.

        Args:
            agent_id: Id of the agent.
            params: Agent version list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every version of the agent.
        """
        async for item in iterate_pages(
            params or AgentVersionListParams(),
            lambda page_params: self.list_versions(agent_id, page_params),
        ):
            yield item
