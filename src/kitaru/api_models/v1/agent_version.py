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
"""Agent version API models."""

import uuid
from typing import Annotated, Literal

from pydantic import Field, PositiveInt

from kitaru.api_models.v1.base import (
    DiscriminatedRequestModel,
    OwnedResponseModel,
    RequestModel,
)
from kitaru.api_models.v1.filter import FilterableListParams


class CommandRunSpec(DiscriminatedRequestModel):
    """Command run spec."""

    type: Literal["command"] = Field(default="command")
    command: str = Field(description="Shell command to run.")
    working_dir: str | None = Field(default=None, description="Working directory.")
    env: dict[str, str] = Field(
        default_factory=dict, description="Process environment."
    )
    secret_ids: list[uuid.UUID] = Field(
        default_factory=list, description="Secrets merged into the process environment."
    )
    timeout_seconds: PositiveInt = Field(default=3600, description="Process timeout.")


class TriggerRunSpec(DiscriminatedRequestModel):
    """Trigger run spec."""

    type: Literal["trigger"] = Field(default="trigger")
    entrypoint: str = Field(
        description="Trigger function to load, as module:attribute."
    )
    env: dict[str, str] = Field(
        default_factory=dict, description="Process environment."
    )
    secret_ids: list[uuid.UUID] = Field(
        default_factory=list, description="Secrets merged into the process environment."
    )
    timeout_seconds: PositiveInt = Field(default=3600, description="Process timeout.")
    import_deadline_seconds: PositiveInt = Field(
        default=86400,
        description="Seconds the resulting session may stay pending import.",
    )


RunSpec = Annotated[CommandRunSpec | TriggerRunSpec, Field(discriminator="type")]


class AgentCapabilities(RequestModel):
    """Agent capabilities."""

    tools: list[str] = Field(
        default_factory=list, description="Tools the agent exposes."
    )
    mcp_servers: list[str] = Field(
        default_factory=list, description="MCP servers the agent connects to."
    )
    skills: list[str] = Field(
        default_factory=list, description="Skills the agent exposes."
    )


class AgentVersionCreateRequest(RequestModel):
    """Agent version create request."""

    display_version: str | None = Field(
        default=None, description="Human-readable designator."
    )
    description: str | None = Field(default=None, description="Version description.")
    run_spec: RunSpec | None = Field(default=None, description="Run spec.")
    capabilities: AgentCapabilities | None = Field(
        default=None, description="Agent capabilities."
    )


class AgentVersionUpdateRequest(RequestModel):
    """Agent version update request."""

    display_version: str | None = Field(
        default=None, description="New human-readable designator."
    )
    description: str | None = Field(
        default=None, description="New version description."
    )
    run_spec: RunSpec | None = Field(default=None, description="New run spec.")
    capabilities: AgentCapabilities | None = Field(
        default=None, description="New agent capabilities."
    )


class AgentVersionListParams(FilterableListParams):
    """Agent version list params."""


class AgentVersionResponse(OwnedResponseModel):
    """Agent version response."""

    id: uuid.UUID = Field(description="Agent version id.")
    agent_id: uuid.UUID = Field(description="Agent this version belongs to.")
    version: int = Field(description="Server-assigned version number.")
    display_version: str | None = Field(description="Human-readable designator.")
    description: str | None = Field(description="Version description.")
    run_spec: RunSpec | None = Field(description="Run spec.")
    capabilities: AgentCapabilities = Field(description="Agent capabilities.")
