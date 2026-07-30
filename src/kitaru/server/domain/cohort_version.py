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
"""Cohort version entity, membership deltas, and errors."""

import uuid
from collections.abc import Sequence
from datetime import datetime

from pydantic import Field

from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import VersionName


class CohortVersionNotFound(NotFoundError):
    """Raised when a cohort version lookup does not resolve."""

    def __init__(self, cohort_id: uuid.UUID, version: int) -> None:
        """Initialize the error.

        Args:
            cohort_id: Id of the cohort.
            version: Missing version number.
        """
        super().__init__(f"Version {version} of cohort {cohort_id} was not found")


class CohortVersionIdNotFound(NotFoundError):
    """Raised when a cohort version lookup by id does not resolve."""

    def __init__(self, cohort_version_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            cohort_version_id: Id of the missing cohort version.
        """
        super().__init__(f"Cohort version {cohort_version_id} was not found")


class CohortVersionInUse(ConflictError):
    """Raised when a cohort version is referenced by an experiment run."""

    def __init__(self, cohort_version_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            cohort_version_id: Id of the cohort version in use.
        """
        super().__init__(
            f"Cohort version {cohort_version_id} is in use by an experiment run"
        )


def apply_membership_delta(
    base: Sequence[uuid.UUID],
    add: Sequence[uuid.UUID],
    remove: Sequence[uuid.UUID],
) -> list[uuid.UUID]:
    """Compute a new version's member list from a base list and a delta.

    The new list is ``base`` minus ``remove``, with ``add`` appended.

    Args:
        base: Ordered member list of the version the delta applies to.
        add: Session ids to append.
        remove: Session ids to drop.

    Raises:
        ValidationError: ``add`` or ``remove`` contains a duplicate id,
            ``remove`` names a session absent from ``base``, or ``add``
            names a session already present in ``base``.

    Returns:
        New ordered member list.
    """
    if len(add) != len(set(add)):
        raise ValidationError("Add list contains a duplicate session id")
    if len(remove) != len(set(remove)):
        raise ValidationError("Remove list contains a duplicate session id")
    base_ids = set(base)
    if not set(remove) <= base_ids:
        raise ValidationError("Cannot remove a session that is not in the base version")
    if set(add) & base_ids:
        raise ValidationError(
            "Cannot add a session that is already in the base version"
        )
    remove_ids = set(remove)
    new_members = [session_id for session_id in base if session_id not in remove_ids]
    return new_members + list(add)


class CohortVersion(DomainModel):
    """Cohort version."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    cohort_id: uuid.UUID
    version: int = 0
    display_version: VersionName | None = None
    session_count: int
    created: datetime | None = None
    updated: datetime | None = None

    def update_display_version(self, display_version: VersionName | None) -> None:
        """Set a new display version.

        Args:
            display_version: New display version.
        """
        self.display_version = display_version
