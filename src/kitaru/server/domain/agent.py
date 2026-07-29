"""Agent entity and errors."""

import uuid
from datetime import datetime

from pydantic import Field

from kitaru.server.domain.base import ConflictError, DomainModel, NotFoundError
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import Name


class AgentNotFound(NotFoundError):
    """Raised when an agent lookup does not resolve."""

    def __init__(self, agent: uuid.UUID | str) -> None:
        super().__init__(f"Agent {agent} was not found")


class DuplicateAgentName(ConflictError):
    """Raised when an agent name is already registered."""

    def __init__(self, name: str) -> None:
        super().__init__(f"Agent name '{name}' is already registered")


class AgentInUse(ConflictError):
    """Raised when an agent still has dependent resources."""

    def __init__(self, agent_id: uuid.UUID) -> None:
        super().__init__(f"Agent {agent_id} is in use")


class Agent(DomainModel):
    """Registered agent."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    name: Name
    description: str | None = None
    latest_version: int = 0
    created: datetime | None = None
    updated: datetime | None = None

    def update_name(self, name: str) -> None:
        """Set the agent name."""
        self.name = name

    def update_description(self, description: str | None) -> None:
        """Set the agent description."""
        self.description = description
