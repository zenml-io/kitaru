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
"""Cohort version repository interface."""

import uuid
from collections.abc import Sequence
from typing import Protocol

from kitaru.server.application.models.cohort import CohortVersionFilter
from kitaru.server.domain.cohort_version import CohortVersion


class CohortVersionRepository(Protocol):
    """Cohort version persistence operations."""

    async def create(
        self, version: CohortVersion, session_ids: Sequence[uuid.UUID]
    ) -> CohortVersion:
        """Persist a new cohort version with a server-assigned version number.

        The version number comes from an ``UPDATE ... RETURNING`` bump of the
        parent cohort's latest_version, in the same transaction as the
        insert.

        Args:
            version: Cohort version to store.
            session_ids: Ordered member session ids to link.

        Raises:
            CohortNotFound: No cohort has the version's cohort id.

        Returns:
            Stored cohort version with its assigned version number and
            timestamps set.
        """
        ...

    async def get(self, cohort_version_id: uuid.UUID) -> CohortVersion:
        """Load a cohort version by id.

        Args:
            cohort_version_id: Id of the cohort version.

        Raises:
            CohortVersionIdNotFound: No cohort version has this id.

        Returns:
            Stored cohort version.
        """
        ...

    async def get_by_number(self, cohort_id: uuid.UUID, version: int) -> CohortVersion:
        """Load a cohort version by cohort id and version number.

        Args:
            cohort_id: Id of the cohort.
            version: Version number.

        Raises:
            CohortVersionNotFound: No version with this number exists for
                this cohort.

        Returns:
            Stored cohort version.
        """
        ...

    async def query(
        self, version_filter: CohortVersionFilter
    ) -> tuple[list[CohortVersion], str | None]:
        """Query cohort versions matching a filter.

        Args:
            version_filter: Filter and pagination parameters.

        Returns:
            Page of matching cohort versions and the next cursor.
        """
        ...

    async def list_session_ids(self, cohort_version_id: uuid.UUID) -> list[uuid.UUID]:
        """List a version's member session ids, in order.

        Args:
            cohort_version_id: Id of the cohort version.

        Raises:
            CohortVersionIdNotFound: No cohort version has this id.

        Returns:
            Ordered member session ids.
        """
        ...

    async def update(self, version: CohortVersion) -> CohortVersion:
        """Persist changes to an existing cohort version.

        Args:
            version: Cohort version with modified fields.

        Raises:
            CohortVersionIdNotFound: No cohort version has this id.

        Returns:
            Stored cohort version with the updated timestamp renewed.
        """
        ...

    async def delete(self, cohort_version_id: uuid.UUID) -> None:
        """Delete a cohort version by id, cascading its member links.

        Args:
            cohort_version_id: Id of the cohort version.

        Raises:
            CohortVersionIdNotFound: No cohort version has this id.
            CohortVersionInUse: An experiment run references this version.
        """
        ...
