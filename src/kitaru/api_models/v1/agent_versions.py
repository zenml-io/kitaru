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
from datetime import datetime
from enum import StrEnum

from pydantic import Field, PositiveInt

from kitaru.api_models.v1.base import RequestModel, ResponseModel


class ExecutionTarget(StrEnum):
    """Execution target."""

    POOL = "pool"
    ON_DEMAND = "on_demand"


class RunSpec(RequestModel):
    """Agent run specification."""

    command: str = Field(description="Bash command starting the agent.")
    working_dir: str | None = Field(
        default=None, description="Working directory for the command."
    )
    env: dict[str, str] = Field(
        default_factory=dict, description="Literal environment variables."
    )
    secret_ids: list[uuid.UUID] = Field(
        default_factory=list,
        description="Ids of secrets whose entries become environment variables.",
    )
    timeout_seconds: PositiveInt = Field(description="Wall clock limit.")
    image: str | None = Field(
        default=None, description="Container image running the agent."
    )
    default_execution_target: ExecutionTarget = Field(
        default=ExecutionTarget.POOL,
        description="Execution target for runs that omit one.",
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

    version: str = Field(description="Version label.")
    description: str | None = Field(default=None, description="Version description.")
    run_spec: RunSpec | None = Field(default=None, description="Run specification.")
    capabilities: AgentCapabilities | None = Field(
        default=None, description="Agent capabilities."
    )


class AgentVersionUpdateRequest(RequestModel):
    """Agent version update request."""

    description: str | None = Field(
        default=None, description="New version description."
    )
    run_spec: RunSpec | None = Field(default=None, description="New run specification.")
    capabilities: AgentCapabilities | None = Field(
        default=None, description="New agent capabilities."
    )


class AgentVersionResponse(ResponseModel):
    """Agent version response."""

    id: uuid.UUID = Field(description="Agent version id.")
    owner_id: uuid.UUID = Field(description="Id of the owning account.")
    agent_id: uuid.UUID = Field(description="Id of the agent.")
    version: str = Field(description="Version label.")
    description: str | None = Field(description="Version description.")
    run_spec: RunSpec | None = Field(description="Run specification.")
    capabilities: AgentCapabilities = Field(description="Agent capabilities.")
    created: datetime = Field(description="Creation time.")
    updated: datetime = Field(description="Last modification time.")
