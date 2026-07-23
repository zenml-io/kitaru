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
"""Agent versions resource."""

import uuid
from typing import TYPE_CHECKING

from kitaru.api_models.v1.agent_versions import (
    AgentVersionCreateRequest,
    AgentVersionResponse,
    AgentVersionUpdateRequest,
)
from kitaru.api_models.v1.base import Page

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class AgentVersionsResource:
    """Agent version API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(
        self, agent_id: uuid.UUID, request: AgentVersionCreateRequest
    ) -> AgentVersionResponse:
        """Create an agent version.

        Args:
            agent_id: Id of the agent.
            request: Agent version create request.

        Raises:
            APIError: The request failed, including 404 for a missing
                agent or secret and 409 for a duplicate version.

        Returns:
            Created agent version.
        """
        response = await self._client.request(
            "POST",
            f"/v1/agents/{agent_id}/versions",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return AgentVersionResponse.model_validate(response.json())

    async def list(
        self,
        agent_id: uuid.UUID,
        page: int = 1,
        page_size: int = 20,
    ) -> Page[AgentVersionResponse]:
        """List the versions of an agent.

        Args:
            agent_id: Id of the agent.
            page: Page number.
            page_size: Page size.

        Raises:
            APIError: The request failed, including 404 for a missing
                agent.

        Returns:
            Page of agent versions.
        """
        response = await self._client.request(
            "GET",
            f"/v1/agents/{agent_id}/versions",
            params={"page": page, "page_size": page_size},
        )
        return Page[AgentVersionResponse].model_validate(response.json())

    async def get(self, version_id: uuid.UUID) -> AgentVersionResponse:
        """Get an agent version by id.

        Args:
            version_id: Id of the agent version.

        Raises:
            APIError: The request failed, including 404 for a missing
                agent version.

        Returns:
            Stored agent version.
        """
        response = await self._client.request("GET", f"/v1/agent-versions/{version_id}")
        return AgentVersionResponse.model_validate(response.json())

    async def update(
        self, version_id: uuid.UUID, request: AgentVersionUpdateRequest
    ) -> AgentVersionResponse:
        """Update an agent version.

        Args:
            version_id: Id of the agent version.
            request: Agent version update request, unset fields stay
                unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing
                agent version or secret.

        Returns:
            Updated agent version.
        """
        response = await self._client.request(
            "PATCH",
            f"/v1/agent-versions/{version_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return AgentVersionResponse.model_validate(response.json())

    async def delete(self, version_id: uuid.UUID) -> None:
        """Delete an agent version.

        Args:
            version_id: Id of the agent version.

        Raises:
            APIError: The request failed, including 404 for a missing
                agent version.
        """
        await self._client.request("DELETE", f"/v1/agent-versions/{version_id}")
