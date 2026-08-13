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
"""Shared SQL repository base."""

import uuid
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Generic, NoReturn, TypeVar

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute, defer

from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.orm.base import UUIDPrimaryKeyMixin
from kitaru.server.domain.base import DomainError, NotFoundError

RowT = TypeVar("RowT", bound=UUIDPrimaryKeyMixin)

ConstraintErrors = Mapping[str, Callable[[], DomainError]]


class BaseSQLRepository(Generic[RowT]):
    """Base class for SQL repositories."""

    orm_class: type[RowT]

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
        """
        self._session = session

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError

    async def _get_row(self, entity_id: uuid.UUID, exclusive: bool = False) -> RowT:
        """Load a row by id.

        Args:
            entity_id: Id of the row.
            exclusive: Whether to lock the row for the duration of the
                transaction.

        Raises:
            NotFoundError: No row has this id.

        Returns:
            Stored row.
        """
        row = await self._session.get(
            self.orm_class, entity_id, with_for_update=exclusive
        )
        if row is None:
            raise self._not_found(entity_id)
        return row

    async def _load_by_ids(
        self,
        ids: Sequence[uuid.UUID],
        exclusive: bool = False,
        deferred_columns: Sequence[InstrumentedAttribute[Any]] = (),
    ) -> dict[uuid.UUID, RowT]:
        """Load rows by id into a dict keyed by id, missing ids omitted.

        A deferred column is left out of the select and loads on first read,
        so only a caller that overwrites it without reading it should defer.

        Args:
            ids: Ids of the rows.
            exclusive: Whether to lock the rows in id order for the duration
                of the transaction.
            deferred_columns: Columns to leave out of the select.

        Returns:
            Rows keyed by id.
        """
        if not ids:
            return {}
        statement = select(self.orm_class).where(self.orm_class.id.in_(ids))
        if deferred_columns:
            statement = statement.options(
                *(defer(column) for column in deferred_columns)
            )
        if exclusive:
            statement = statement.order_by(self.orm_class.id.asc()).with_for_update()
        rows = (await self._session.scalars(statement)).all()
        return {row.id: row for row in rows}

    async def _add(
        self, row: UUIDPrimaryKeyMixin, constraints: ConstraintErrors | None = None
    ) -> None:
        """Add a row and flush, translating constraint violations.

        Typed against the mixin rather than this repository's bound `RowT`,
        since a repository managing more than one table (a link table
        alongside its owning table, for example) adds rows of a second class
        through the same method.

        Args:
            row: Row to add.
            constraints: Domain error factories keyed by constraint name.

        Raises:
            DomainError: A mapped constraint was violated.
        """
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                await self._session.flush()
        except IntegrityError as exc:
            self._raise_translated(exc, constraints)

    async def _flush(self, constraints: ConstraintErrors | None = None) -> None:
        """Flush pending changes, translating constraint violations.

        Args:
            constraints: Domain error factories keyed by constraint name.

        Raises:
            DomainError: A mapped constraint was violated.
        """
        try:
            async with self._session.begin_nested():
                await self._session.flush()
        except IntegrityError as exc:
            self._raise_translated(exc, constraints)

    async def _delete_row(
        self, entity_id: uuid.UUID, constraints: ConstraintErrors | None = None
    ) -> None:
        """Delete a row by id, translating constraint violations.

        Args:
            entity_id: Id of the row.
            constraints: Domain error factories keyed by constraint name, for
                a referencing row that restricts the delete.

        Raises:
            NotFoundError: No row has this id.
            DomainError: A mapped constraint was violated.
        """
        row = await self._get_row(entity_id)
        try:
            async with self._session.begin_nested():
                await self._session.delete(row)
                await self._session.flush()
        except IntegrityError as exc:
            self._raise_translated(exc, constraints)

    def _raise_translated(
        self, exc: IntegrityError, constraints: ConstraintErrors | None
    ) -> NoReturn:
        """Raise the mapped domain error for a constraint violation.

        Args:
            exc: Integrity error raised by the flush.
            constraints: Domain error factories keyed by constraint name.

        Raises:
            DomainError: A mapped constraint was violated.
            IntegrityError: No mapping matched the violated constraint.
        """
        name = violated_constraint(exc)
        if constraints is not None and name is not None and name in constraints:
            raise constraints[name]() from exc
        raise exc
