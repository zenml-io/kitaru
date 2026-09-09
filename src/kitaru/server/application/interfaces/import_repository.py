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
"""Import repository interface."""

import uuid
from typing import Protocol

from kitaru.server.application.models.imports import ImportFilter
from kitaru.server.domain.imports import Import


class ImportRepository(Protocol):
    """Import persistence operations."""

    async def create(self, import_: Import) -> Import:
        """Persist a new import.

        Args:
            import_: Import to store.

        Returns:
            Stored import with timestamps set.
        """
        ...

    async def get(self, import_id: uuid.UUID) -> Import:
        """Load an import by id.

        Args:
            import_id: Id of the import.

        Raises:
            ImportNotFound: No import has this id.

        Returns:
            Stored import.
        """
        ...

    async def get_by_job_id(self, job_id: uuid.UUID) -> Import | None:
        """Load the import owning a job, if any.

        Args:
            job_id: Id of the job.

        Returns:
            Stored import, or ``None`` when the job holds no import.
        """
        ...

    async def query(
        self, import_filter: ImportFilter
    ) -> tuple[list[Import], str | None]:
        """Query imports matching a filter.

        Args:
            import_filter: Filter and pagination parameters.

        Returns:
            Page of matching imports and the next cursor.
        """
        ...

    async def update(self, import_: Import) -> Import:
        """Persist changes to an existing import.

        Args:
            import_: Import with modified fields.

        Raises:
            ImportNotFound: No import has this id.

        Returns:
            Stored import with the updated timestamp renewed.
        """
        ...
