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
from typing import Protocol

from kitaru.server.application.models.cohorts import (
    CohortFilter,
    CohortSessionsFilter,
)
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.session import Session


class CohortRepository(Protocol):
    """Cohort persistence operations."""

    async def create(self, cohort: Cohort, session_ids: list[uuid.UUID]) -> Cohort:
        """Persist a new cohort with its ordered membership.

        Args:
            cohort: Cohort to store.
            session_ids: Ids of the member sessions, in position order.

        Raises:
            DuplicateCohortName: The cohort name is already registered.
            AgentNotFound: No agent has the cohort's agent id.

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

    async def query(self, cohort_filter: CohortFilter) -> tuple[list[Cohort], int]:
        """Query cohorts matching a filter.

        Args:
            cohort_filter: Filter and pagination parameters.

        Returns:
            Page of matching cohorts and the total match count.
        """
        ...

    async def query_sessions(
        self, cohort_id: uuid.UUID, sessions_filter: CohortSessionsFilter
    ) -> tuple[list[Session], int]:
        """Query the member sessions of a cohort ordered by position.

        Args:
            cohort_id: Id of the cohort.
            sessions_filter: Pagination parameters.

        Raises:
            CohortNotFound: No cohort has this id.

        Returns:
            Page of member sessions and the total member count.
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
        """Delete a cohort by id, including its membership and tag links.

        Args:
            cohort_id: Id of the cohort.

        Raises:
            CohortNotFound: No cohort has this id.
            CohortInUse: The cohort is referenced by an experiment.
        """
        ...
