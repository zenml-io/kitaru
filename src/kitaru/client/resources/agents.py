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
from typing import TYPE_CHECKING, Any

from kitaru.api_models.v1.agents import (
    AgentCreateRequest,
    AgentResponse,
    AgentUpdateRequest,
)
from kitaru.api_models.v1.base import Page

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

    async def create(self, request: AgentCreateRequest) -> AgentResponse:
        """Create an agent.

        Args:
            request: Agent create request.

        Raises:
            APIError: The request failed, including 409 for a duplicate name.

        Returns:
            Created agent.
        """
        response = await self._client.request(
            "POST",
            "/v1/agents",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return AgentResponse.model_validate(response.json())

    async def get(self, agent_id: uuid.UUID) -> AgentResponse:
        """Get an agent by id.

        Args:
            agent_id: Id of the agent.

        Raises:
            APIError: The request failed, including 404 for a missing
                agent.

        Returns:
            Stored agent.
        """
        response = await self._client.request("GET", f"/v1/agents/{agent_id}")
        return AgentResponse.model_validate(response.json())

    async def list(
        self,
        name: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Page[AgentResponse]:
        """List agents.

        Args:
            name: Filter on agent name.
            page: Page number.
            page_size: Page size.

        Raises:
            APIError: The request failed.

        Returns:
            Page of agents.
        """
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if name is not None:
            params["name"] = name
        response = await self._client.request("GET", "/v1/agents", params=params)
        return Page[AgentResponse].model_validate(response.json())

    async def update(
        self, agent_id: uuid.UUID, request: AgentUpdateRequest
    ) -> AgentResponse:
        """Update an agent.

        Args:
            agent_id: Id of the agent.
            request: Agent update request, unset fields stay unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing
                agent and 409 for a duplicate name.

        Returns:
            Updated agent.
        """
        response = await self._client.request(
            "PATCH",
            f"/v1/agents/{agent_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return AgentResponse.model_validate(response.json())

    async def delete(self, agent_id: uuid.UUID) -> None:
        """Delete an agent.

        Args:
            agent_id: Id of the agent.

        Raises:
            APIError: The request failed, including 404 for a missing
                agent and 409 while the agent still has versions.
        """
        await self._client.request("DELETE", f"/v1/agents/{agent_id}")
