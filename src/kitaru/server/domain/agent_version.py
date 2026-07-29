"""Agent version entity and execution value objects."""

import uuid
from datetime import datetime

from pydantic import Field, PositiveInt

from kitaru.base import FrozenModel
from kitaru.server.domain.base import ConflictError, DomainModel, NotFoundError
from kitaru.server.domain.ids import uuid7


class RunSpec(FrozenModel):
    """Process command recorded on an agent version."""

    command: str
    working_dir: str | None = None
    env: dict[str, str] = Field(default_factory=dict)
    secret_ids: list[uuid.UUID] = Field(default_factory=list)
    timeout_seconds: PositiveInt = 3600


class AgentCapabilities(FrozenModel):
    """Declared agent capabilities."""

    tools: list[str] = Field(default_factory=list)
    mcp_servers: list[str] = Field(default_factory=list)
    skills: list[str] = Field(default_factory=list)


class AgentVersionNotFound(NotFoundError):
    """Raised when an agent version lookup does not resolve."""

    def __init__(self, version_id: uuid.UUID) -> None:
        super().__init__(f"Agent version {version_id} was not found")


class AgentVersionInUse(ConflictError):
    """Raised when an agent version has dependent resources."""

    def __init__(self, version_id: uuid.UUID) -> None:
        super().__init__(f"Agent version {version_id} is in use")


class AgentVersionFrozen(ConflictError):
    """Raised when execution fields on a used version would change."""

    def __init__(self, version_id: uuid.UUID) -> None:
        super().__init__(f"Agent version {version_id} execution fields are frozen")


class AgentVersion(DomainModel):
    """Versioned agent configuration."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    agent_id: uuid.UUID
    version: PositiveInt
    display_version: str | None = None
    description: str | None = None
    run_spec: RunSpec | None = None
    capabilities: AgentCapabilities = Field(default_factory=AgentCapabilities)
    created: datetime | None = None
    updated: datetime | None = None

    def update_display_version(self, display_version: str | None) -> None:
        """Set the human-readable version."""
        self.display_version = display_version

    def update_description(self, description: str | None) -> None:
        """Set the version description."""
        self.description = description

    def update_run_spec(self, run_spec: RunSpec | None, frozen: bool) -> None:
        """Set the process specification when execution fields remain mutable."""
        if frozen and run_spec != self.run_spec:
            raise AgentVersionFrozen(self.id)
        self.run_spec = run_spec

    def update_capabilities(
        self, capabilities: AgentCapabilities, frozen: bool
    ) -> None:
        """Set declared capabilities when execution fields remain mutable."""
        if frozen and capabilities != self.capabilities:
            raise AgentVersionFrozen(self.id)
        self.capabilities = capabilities
