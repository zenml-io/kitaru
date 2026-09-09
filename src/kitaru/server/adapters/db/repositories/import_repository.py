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
"""SQL import repository."""

import uuid
from collections.abc import Mapping

from sqlalchemy import select

from kitaru.server.adapters.db.filtering import (
    FilterBinding,
    compile_filter_expression,
)
from kitaru.server.adapters.db.orm.imports import ImportORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.imports import ImportFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.imports import Import, ImportNotFound

IMPORT_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "id": ImportORM.id,
    "agent_id": ImportORM.agent_id,
    "job_id": ImportORM.job_id,
}


class SQLImportRepository(BaseSQLRepository[ImportORM]):
    """Import repository backed by the application database."""

    orm_class = ImportORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return ImportNotFound(entity_id)

    async def create(self, import_: Import) -> Import:
        """Persist a new import.

        Args:
            import_: Import to store.

        Returns:
            Stored import with timestamps set.
        """
        row = ImportORM.from_domain(import_)
        await self._add(row)
        return row.to_domain()

    async def get(self, import_id: uuid.UUID) -> Import:
        """Load an import by id.

        Args:
            import_id: Id of the import.

        Raises:
            ImportNotFound: No import has this id.

        Returns:
            Stored import.
        """
        row = await self._get_row(import_id)
        return row.to_domain()

    async def get_by_job_id(self, job_id: uuid.UUID) -> Import | None:
        """Load the import owning a job, if any.

        Args:
            job_id: Id of the job.

        Returns:
            Stored import, or ``None`` when the job holds no import.
        """
        statement = select(ImportORM).where(ImportORM.job_id == job_id)
        row = (await self._session.scalars(statement)).one_or_none()
        return row.to_domain() if row is not None else None

    async def query(
        self, import_filter: ImportFilter
    ) -> tuple[list[Import], str | None]:
        """Query imports matching a filter.

        Args:
            import_filter: Filter and pagination parameters.

        Returns:
            Page of matching imports and the next cursor.
        """
        statement = select(ImportORM)
        if import_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    import_filter.expression, IMPORT_FILTER_BINDINGS
                )
            )
        rows, next_cursor = await paginate(
            self._session, statement, import_filter, id_column=ImportORM.id
        )
        return [row.to_domain() for row in rows], next_cursor

    async def update(self, import_: Import) -> Import:
        """Persist changes to an existing import.

        Args:
            import_: Import with modified fields.

        Raises:
            ImportNotFound: No import has this id.

        Returns:
            Stored import with the updated timestamp renewed.
        """
        row = await self._get_row(import_.id)
        row.apply(import_)
        await self._flush()
        return row.to_domain()
