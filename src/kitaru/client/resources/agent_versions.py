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
        self._client = client

    async def get(self, version_id: uuid.UUID) -> AgentVersionResponse:
        response = await self._client.request("GET", f"/v1/agent-versions/{version_id}")
        return AgentVersionResponse.model_validate(response.json())

    async def update(
        self, version_id: uuid.UUID, request: AgentVersionUpdateRequest
    ) -> AgentVersionResponse:
        response = await self._client.request(
            "PATCH",
            f"/v1/agent-versions/{version_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return AgentVersionResponse.model_validate(response.json())

    async def delete(self, version_id: uuid.UUID) -> None:
        await self._client.request("DELETE", f"/v1/agent-versions/{version_id}")
