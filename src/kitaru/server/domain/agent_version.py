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
"""Agent version entity, run value objects, and errors."""

import uuid
from datetime import datetime
from typing import Annotated

from pydantic import AfterValidator, Field, PositiveInt

from kitaru.server.base import FrozenModel
from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.execution import ExecutionTarget
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import Name


class AgentVersionNotFound(NotFoundError):
    """Raised when an agent version lookup does not resolve."""

    def __init__(self, version_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            version_id: Id of the missing agent version.
        """
        super().__init__(f"Agent version {version_id} was not found")


class DuplicateAgentVersion(ConflictError):
    """Raised when an agent version is already registered for its agent."""

    def __init__(self, version: str) -> None:
        """Initialize the error.

        Args:
            version: Version that is already registered.
        """
        super().__init__(f"Agent version '{version}' is already registered")


class AgentVersionInUse(ConflictError):
    """Raised when an agent version deletion is blocked by existing references."""

    def __init__(self, version_id: uuid.UUID, referrer: str) -> None:
        """Initialize the error.

        Args:
            version_id: Id of the referenced agent version.
            referrer: Kind of resource referencing the agent version.
        """
        super().__init__(f"Agent version {version_id} is referenced by {referrer}")


class AgentVersionFrozen(ConflictError):
    """Raised when a run spec or capability change hits a replayed version."""

    def __init__(self, version_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            version_id: Id of the frozen agent version.
        """
        super().__init__(f"Agent version {version_id} is frozen by existing replays")


class AgentVersionNotRunnable(ConflictError):
    """Raised when an operation requires a runnable agent version."""

    def __init__(self, version_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            version_id: Id of the agent version.
        """
        super().__init__(f"Agent version {version_id} has no run spec")


class MissingRunImage(ConflictError):
    """Raised when an operation requires a run spec image."""

    def __init__(self, version_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            version_id: Id of the agent version.
        """
        super().__init__(f"Agent version {version_id} has no run image")


class NoRunnableAgentVersion(ConflictError):
    """Raised when an agent has no runnable version to resolve."""

    def __init__(self, agent_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            agent_id: Id of the agent.
        """
        super().__init__(f"Agent {agent_id} has no runnable version")


class InvalidAgentVersion(ValidationError):
    """Raised when an agent version violates its shape rules."""


class InvalidRunSpec(ValidationError):
    """Raised when a run spec violates its shape rules."""


def validate_secret_ids(value: list[uuid.UUID]) -> list[uuid.UUID]:
    """Validate run spec secret ids against duplicates.

    Args:
        value: Secret ids to validate.

    Raises:
        InvalidRunSpec: ``value`` contains duplicates.

    Returns:
        Validated secret ids.
    """
    if len(set(value)) != len(value):
        raise InvalidRunSpec("Run spec secret ids contain duplicates")
    return value


SecretIds = Annotated[list[uuid.UUID], AfterValidator(validate_secret_ids)]


class RunSpec(FrozenModel):
    """Agent run specification."""

    command: str
    working_dir: str | None = None
    env: dict[str, str] = Field(default_factory=dict)
    secret_ids: SecretIds = Field(default_factory=list)
    timeout_seconds: PositiveInt
    image: str | None = None
    default_execution_target: ExecutionTarget = ExecutionTarget.POOL


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
    version: Name
    description: str | None = None
    run_spec: RunSpec | None = None
    capabilities: AgentCapabilities = Field(default_factory=AgentCapabilities)
    created: datetime | None = None
    updated: datetime | None = None

    def update_description(self, description: str | None) -> None:
        """Set a new version description.

        Args:
            description: New description, ``None`` clears it.
        """
        self.description = description

    def update_run_spec(self, run_spec: RunSpec | None, frozen: bool) -> None:
        """Set a new run specification.

        Args:
            run_spec: New run specification, ``None`` clears it.
            frozen: Whether a replay references the version.

        Raises:
            AgentVersionFrozen: A replay references the version.
        """
        if frozen:
            raise AgentVersionFrozen(self.id)
        self.run_spec = run_spec

    def update_capabilities(
        self, capabilities: AgentCapabilities, frozen: bool
    ) -> None:
        """Set new agent capabilities.

        Args:
            capabilities: New capabilities.
            frozen: Whether a replay references the version.

        Raises:
            AgentVersionFrozen: A replay references the version.
        """
        if frozen:
            raise AgentVersionFrozen(self.id)
        self.capabilities = capabilities
