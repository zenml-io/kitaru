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
"""Cohort repository interface."""

import uuid
from collections.abc import Sequence
from typing import Protocol

from kitaru.server.application.models.cohort import CohortFilter, CohortSessionsFilter
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.session import Session


class CohortRepository(Protocol):
    """Cohort persistence operations."""

    async def create(self, cohort: Cohort, session_ids: Sequence[uuid.UUID]) -> Cohort:
        """Persist a new cohort with its fixed member sessions, in order.

        Args:
            cohort: Cohort to store.
            session_ids: Ordered member session ids, immutable afterward.

        Raises:
            DuplicateCohortName: The cohort name is already registered.

        Returns:
            Stored cohort with timestamps set.
        """
        ...

    async def get(self, cohort_id: uuid.UUID) -> Cohort:
        """Load a cohort by id.

        Args:
            cohort_id: Id of the cohort.

        Raises:
            CohortNotFound: No cohort has this id.

        Returns:
            Stored cohort.
        """
        ...

    async def query(
        self, cohort_filter: CohortFilter
    ) -> tuple[list[Cohort], str | None]:
        """Query cohorts matching a filter.

        Args:
            cohort_filter: Filter and pagination parameters.

        Returns:
            Page of matching cohorts and the next cursor.
        """
        ...

    async def update(self, cohort: Cohort) -> Cohort:
        """Persist changes to an existing cohort.

        Args:
            cohort: Cohort with modified fields.

        Raises:
            CohortNotFound: No cohort has this id.
            DuplicateCohortName: The cohort name is already registered.

        Returns:
            Stored cohort with the updated timestamp renewed.
        """
        ...

    async def delete(self, cohort_id: uuid.UUID) -> None:
        """Delete a cohort by id.

        Deleting a cohort cascades its member links.

        Args:
            cohort_id: Id of the cohort.

        Raises:
            CohortNotFound: No cohort has this id.
        """
        ...

    async def list_sessions(
        self, sessions_filter: CohortSessionsFilter
    ) -> tuple[list[Session], str | None]:
        """List a cohort's member sessions in cohort order.

        Args:
            sessions_filter: Filter and pagination parameters.

        Returns:
            Page of member sessions and the next cursor.
        """
        ...
