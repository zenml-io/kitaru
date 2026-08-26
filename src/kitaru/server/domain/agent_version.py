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
"""Agent version entity, value objects, and errors."""

import uuid
from datetime import datetime
from typing import Annotated

from pydantic import AfterValidator, Field

from kitaru.base import FrozenModel
from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import VersionName


class AgentVersionNotFound(NotFoundError):
    """Raised when an agent version lookup does not resolve."""

    def __init__(self, agent_version_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            agent_version_id: Id of the missing agent version.
        """
        super().__init__(f"Agent version {agent_version_id} was not found")


class AgentVersionInUse(ConflictError):
    """Raised when an agent version is referenced by an experiment run."""

    def __init__(self, agent_version_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            agent_version_id: Id of the agent version in use.
        """
        super().__init__(
            f"Agent version {agent_version_id} is in use by an experiment run"
        )


class AgentVersionWithoutRunSpec(ValidationError):
    """Raised when an agent version without a run spec is asked to run."""

    def __init__(self, agent_version_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            agent_version_id: Id of the agent version.
        """
        super().__init__(f"Agent version {agent_version_id} has no run spec")


class AgentVersionAgentMismatch(ValidationError):
    """Raised when an agent version does not belong to the named agent."""

    def __init__(self, agent_version_id: uuid.UUID, agent_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            agent_version_id: Id of the agent version.
            agent_id: Id of the agent named alongside it.
        """
        super().__init__(
            f"Agent version {agent_version_id} does not belong to agent {agent_id}"
        )


class InvalidTimeout(ValidationError):
    """Raised when a run spec timeout is not positive."""


def validate_timeout_seconds(value: int) -> int:
    """Validate a run spec timeout is positive.

    Args:
        value: Timeout in seconds to validate.

    Raises:
        InvalidTimeout: ``value`` is not positive.

    Returns:
        Validated timeout.
    """
    if value <= 0:
        raise InvalidTimeout("Timeout must be positive")
    return value


TimeoutSeconds = Annotated[int, AfterValidator(validate_timeout_seconds)]


class RunSpec(FrozenModel):
    """Run spec."""

    command: str
    working_dir: str | None = None
    env: dict[str, str] = Field(default_factory=dict)
    secret_ids: list[uuid.UUID] = Field(default_factory=list)
    timeout_seconds: TimeoutSeconds = 3600


class AgentCapabilities(FrozenModel):
    """Agent capabilities."""

    tools: list[str] = Field(default_factory=list)
    mcp_servers: list[str] = Field(default_factory=list)
    skills: list[str] = Field(default_factory=list)


class AgentVersion(DomainModel):
    """Agent version."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    agent_id: uuid.UUID
    version: int = 0
    display_version: VersionName | None = None
    description: str | None = None
    run_spec: RunSpec | None = None
    capabilities: AgentCapabilities = Field(default_factory=AgentCapabilities)
    created: datetime | None = None
    updated: datetime | None = None

    def update_display_version(self, display_version: VersionName | None) -> None:
        """Set a new human-readable designator.

        Args:
            display_version: New display version.
        """
        self.display_version = display_version

    def update_description(self, description: str | None) -> None:
        """Set a new version description.

        Args:
            description: New description.
        """
        self.description = description

    def update_run_spec(self, run_spec: RunSpec | None) -> None:
        """Set a new run spec.

        Args:
            run_spec: New run spec.
        """
        self.run_spec = run_spec

    def update_capabilities(self, capabilities: AgentCapabilities) -> None:
        """Set new agent capabilities.

        Args:
            capabilities: New capabilities.
        """
        self.capabilities = capabilities
