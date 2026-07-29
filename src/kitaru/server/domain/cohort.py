"""Cohort entity and errors."""

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


class CohortNotFound(NotFoundError):
    """Raised when a cohort lookup does not resolve."""

    def __init__(self, cohort: uuid.UUID | str) -> None:
        super().__init__(f"Cohort {cohort} was not found")


class DuplicateCohortName(ConflictError):
    """Raised when a cohort name is already registered."""

    def __init__(self, name: str) -> None:
        super().__init__(f"Cohort name '{name}' is already registered")


class InvalidCohortMembers(ValidationError):
    """Raised when cohort sessions do not form a valid snapshot."""


class CohortInUse(ConflictError):
    """Raised when a cohort has dependent experiment runs."""

    def __init__(self, cohort_id: uuid.UUID) -> None:
        super().__init__(f"Cohort {cohort_id} is in use")


class Cohort(DomainModel):
    """Immutable snapshot of ordered sessions."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    name: Name
    description: str | None = None
    agent_id: uuid.UUID
    session_count: int = 0
    created: datetime | None = None
    updated: datetime | None = None

    def check_members(
        self, session_agent_ids: list[uuid.UUID], expected_count: int
    ) -> None:
        """Validate membership count and agent identity."""
        if expected_count < 1:
            raise InvalidCohortMembers("A cohort must contain at least one session")
        if len(session_agent_ids) != expected_count:
            raise InvalidCohortMembers("One or more cohort sessions were not found")
        if any(agent_id != self.agent_id for agent_id in session_agent_ids):
            raise InvalidCohortMembers("Every cohort session must belong to its agent")
        self.session_count = expected_count

    def update_name(self, name: str) -> None:
        """Set the cohort name."""
        self.name = name

    def update_description(self, description: str | None) -> None:
        """Set the cohort description."""
        self.description = description
