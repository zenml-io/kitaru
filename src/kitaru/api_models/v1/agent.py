"""Agent API models."""

import uuid

from pydantic import Field

from kitaru.api_models.v1.base import ListParams, OwnedResponseModel, RequestModel


class AgentCreateRequest(RequestModel):
    """Agent create request."""

    name: str = Field(description="Agent name.")
    description: str | None = Field(default=None, description="Agent description.")


class AgentUpdateRequest(RequestModel):
    """Agent update request."""

    name: str | None = Field(default=None, description="New agent name.")
    description: str | None = Field(default=None, description="New description.")


class AgentListParams(ListParams):
    """Agent list params."""

    name: str | None = Field(default=None, description="Filter on agent name.")


class AgentResponse(OwnedResponseModel):
    """Agent response."""

    id: uuid.UUID = Field(description="Agent id.")
    name: str = Field(description="Agent name.")
    description: str | None = Field(description="Agent description.")
    latest_version: int = Field(description="Latest version number.")
