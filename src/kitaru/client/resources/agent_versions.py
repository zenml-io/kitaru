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

from kitaru.api_models.v1.agent_version import (
    AgentVersionResponse,
    AgentVersionUpdateRequest,
)

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

    async def get(self, agent_version_id: uuid.UUID) -> AgentVersionResponse:
        """Get an agent version by id.

        Args:
            agent_version_id: Id of the agent version.

        Raises:
            APIError: The request failed, including 404 for a missing agent
                version.

        Returns:
            Stored agent version.
        """
        response = await self._client.request(
            "GET", f"/api/v1/agent-versions/{agent_version_id}"
        )
        return AgentVersionResponse.model_validate(response.json())

    async def update(
        self, agent_version_id: uuid.UUID, request: AgentVersionUpdateRequest
    ) -> AgentVersionResponse:
        """Update an agent version.

        Args:
            agent_version_id: Id of the agent version.
            request: Agent version update request, unset fields stay
                unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing agent
                version.

        Returns:
            Updated agent version.
        """
        response = await self._client.request(
            "PATCH",
            f"/api/v1/agent-versions/{agent_version_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return AgentVersionResponse.model_validate(response.json())

    async def delete(self, agent_version_id: uuid.UUID) -> None:
        """Delete an agent version.

        Args:
            agent_version_id: Id of the agent version.

        Raises:
            APIError: The request failed, including 404 for a missing agent
                version.
        """
        await self._client.request(
            "DELETE", f"/api/v1/agent-versions/{agent_version_id}"
        )
