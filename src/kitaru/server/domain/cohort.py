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
from kitaru.server.domain.session import Session, SessionStatus


class CohortNotFound(NotFoundError):
    """Raised when a cohort lookup does not resolve."""

    def __init__(self, cohort_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            cohort_id: Id of the missing cohort.
        """
        super().__init__(f"Cohort {cohort_id} was not found")


class DuplicateCohortName(ConflictError):
    """Raised when a cohort name is already registered."""

    def __init__(self, name: str) -> None:
        """Initialize the error.

        Args:
            name: Name that is already registered.
        """
        super().__init__(f"Cohort name '{name}' is already registered")


class CohortInUse(ConflictError):
    """Raised when a cohort deletion is blocked by existing references."""

    def __init__(self, cohort_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            cohort_id: Id of the referenced cohort.
        """
        super().__init__(f"Cohort {cohort_id} is referenced by experiments")


class InvalidCohort(ValidationError):
    """Raised when a cohort violates its shape rules."""


class Cohort(DomainModel):
    """Cohort."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    name: Name
    description: str | None = None
    agent_id: uuid.UUID
    session_count: int = 0
    created: datetime | None = None
    updated: datetime | None = None

    def check_members(self, sessions: list[Session]) -> None:
        """Check that sessions are valid members of this cohort.

        Args:
            sessions: Member sessions.

        Raises:
            InvalidCohort: A session belongs to another agent or is in
                progress.
        """
        for session in sessions:
            if session.agent_id != self.agent_id:
                raise InvalidCohort(
                    f"Session {session.id} does not belong to agent {self.agent_id}"
                )
            if session.status is SessionStatus.IN_PROGRESS:
                raise InvalidCohort(f"Session {session.id} is in progress")

    def update_name(self, name: str) -> None:
        """Set a new cohort name.

        Args:
            name: New name.
        """
        self.name = name

    def update_description(self, description: str | None) -> None:
        """Set a new cohort description.

        Args:
            description: New description, ``None`` clears it.
        """
        self.description = description
