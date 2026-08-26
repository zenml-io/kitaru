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
from collections.abc import Sequence
from datetime import datetime
from typing import Any

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
    """Raised when a cohort has a version referenced by an experiment run."""

    def __init__(self, cohort_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            cohort_id: Id of the cohort in use.
        """
        super().__init__(f"Cohort {cohort_id} is in use by an experiment run")


class Cohort(DomainModel):
    """Cohort."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    name: Name
    description: str | None = None
    agent_id: uuid.UUID
    metadata: dict[str, Any] = Field(default_factory=dict)
    latest_version: int = 0
    created: datetime | None = None
    updated: datetime | None = None

    def check_members(self, session_ids: Sequence[uuid.UUID]) -> None:
        """Validate a version's computed member list contains no duplicates.

        Existence of each session and its agent match are validated by the
        caller, which has access to the session repository.

        Raises:
            ValidationError: A session id repeats.
        """
        if len(session_ids) != len(set(session_ids)):
            raise ValidationError(
                f"Cohort {self.id} member list contains duplicate sessions"
            )

    def update_name(self, name: str) -> None:
        """Set a new cohort name.

        Args:
            name: New name.
        """
        self.name = name

    def update_description(self, description: str | None) -> None:
        """Set a new cohort description.

        Args:
            description: New description.
        """
        self.description = description

    def update_metadata(self, metadata: dict[str, Any]) -> None:
        """Set new cohort metadata.

        Args:
            metadata: New metadata.
        """
        self.metadata = metadata
