"""Agent version API models."""

import uuid

from pydantic import Field, PositiveInt

from kitaru.api_models.v1.base import OwnedResponseModel, RequestModel


class RunSpec(RequestModel):
    """Agent process specification."""

    command: str = Field(description="Process command.")
    working_dir: str | None = Field(default=None, description="Working directory.")
    env: dict[str, str] = Field(default_factory=dict, description="Environment values.")
    secret_ids: list[uuid.UUID] = Field(
        default_factory=list, description="Secrets merged into the environment."
    )
    timeout_seconds: PositiveInt = Field(
        default=3600, description="Process timeout in seconds."
    )


class AgentCapabilities(RequestModel):
    """Agent capabilities."""

    tools: list[str] = Field(default_factory=list, description="Tool names.")
    mcp_servers: list[str] = Field(
        default_factory=list, description="MCP server names."
    )
    skills: list[str] = Field(default_factory=list, description="Skill names.")


class AgentVersionCreateRequest(RequestModel):
    """Agent version create request."""

    display_version: str | None = Field(
        default=None, description="Human-readable version."
    )
    description: str | None = Field(default=None, description="Version description.")
    run_spec: RunSpec | None = Field(default=None, description="Process specification.")
    capabilities: AgentCapabilities | None = Field(
        default=None, description="Agent capabilities."
    )


class AgentVersionUpdateRequest(RequestModel):
    """Agent version update request."""

    display_version: str | None = Field(
        default=None, description="New human-readable version."
    )
    description: str | None = Field(default=None, description="New description.")
    run_spec: RunSpec | None = Field(
        default=None, description="New process specification."
    )
    capabilities: AgentCapabilities | None = Field(
        default=None, description="New agent capabilities."
    )


class AgentVersionResponse(OwnedResponseModel):
    """Agent version response."""

    id: uuid.UUID = Field(description="Agent version id.")
    agent_id: uuid.UUID = Field(description="Agent id.")
    version: int = Field(description="Server-assigned version number.")
    display_version: str | None = Field(description="Human-readable version.")
    description: str | None = Field(description="Version description.")
    run_spec: RunSpec | None = Field(description="Process specification.")
    capabilities: AgentCapabilities = Field(description="Agent capabilities.")
