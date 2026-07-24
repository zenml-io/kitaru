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
"""Agent entity and errors."""

import uuid
from datetime import datetime

from pydantic import Field

from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import Name


class AgentNotFound(NotFoundError):
    """Raised when an agent lookup does not resolve."""

    def __init__(self, agent_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            agent_id: Id of the missing agent.
        """
        super().__init__(f"Agent {agent_id} was not found")


class DuplicateAgentName(ConflictError):
    """Raised when an agent name is already registered."""

    def __init__(self, name: str) -> None:
        """Initialize the error.

        Args:
            name: Name that is already registered.
        """
        super().__init__(f"Agent name '{name}' is already registered")


class AgentInUse(ConflictError):
    """Raised when an agent deletion is blocked by existing references."""

    def __init__(self, agent_id: uuid.UUID, referrer: str) -> None:
        """Initialize the error.

        Args:
            agent_id: Id of the referenced agent.
            referrer: Kind of resource referencing the agent.
        """
        super().__init__(f"Agent {agent_id} is referenced by {referrer}")


class InvalidAgent(ValidationError):
    """Raised when an agent violates its shape rules."""


class Agent(DomainModel):
    """Agent."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    name: Name
    description: str | None = None
    created: datetime | None = None
    updated: datetime | None = None

    def update_name(self, name: str) -> None:
        """Set a new agent name.

        Args:
            name: New name.
        """
        self.name = name

    def update_description(self, description: str | None) -> None:
        """Set a new agent description.

        Args:
            description: New description, ``None`` clears it.
        """
        self.description = description
