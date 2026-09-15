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
"""Shared query pagination."""

import uuid
from collections.abc import Sequence
from datetime import datetime
from typing import Any, Protocol, TypeVar

from asyncpg.exceptions import QueryCanceledError
from sqlalchemy import ColumnElement, Select, and_, func, or_, select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute

from kitaru.server.adapters.db.orm.base import UUIDPrimaryKeyMixin
from kitaru.server.application.pagination import decode_cursor, encode_cursor
from kitaru.server.base import ListFilter
from kitaru.server.domain.base import QueryTimeoutError, ValidationError

LIST_QUERY_TIMEOUT_INFO_KEY = "list_query_timeout_seconds"

RowT = TypeVar("RowT", bound=UUIDPrimaryKeyMixin)


class PageOrder(Protocol):
    """Page order."""

    @property
    def columns(self) -> Sequence[InstrumentedAttribute[Any]]:
        """Keyset columns, in sort precedence."""
        ...

    def order_by(self) -> Sequence[ColumnElement[Any]]:
        """Return the ordering clauses of the keyset columns."""
        ...

    def after(self, payload: str) -> ColumnElement[bool]:
        """Build the predicate matching the rows that follow a cursor.

        Args:
            payload: Cursor payload written by ``payload()``.

        Raises:
            ValidationError: The payload does not fit the keyset.

        Returns:
            Predicate matching the rows after the cursor row.
        """
        ...

    def payload(self, values: Sequence[Any]) -> str:
        """Write the keyset values of a row into a cursor payload.

        Args:
            values: Values of the keyset columns, in sort precedence.

        Returns:
            Cursor payload.
        """
        ...


class IdOrder:
    """Page order by UUIDv7 id."""

    def __init__(self, id_column: InstrumentedAttribute[uuid.UUID], sort: str) -> None:
        """Initialize the order.

        Args:
            id_column: Primary key column.
            sort: List sort, whose direction the order follows.
        """
        _, _, direction = sort.partition(":")
        self._id_column = id_column
        self._descending = direction == "desc"

    @property
    def columns(self) -> Sequence[InstrumentedAttribute[Any]]:
        """Keyset columns, in sort precedence."""
        return [self._id_column]

    def order_by(self) -> Sequence[ColumnElement[Any]]:
        """Return the ordering clauses of the keyset columns."""
        return [self._id_column.desc() if self._descending else self._id_column.asc()]

    def after(self, payload: str) -> ColumnElement[bool]:
        """Build the predicate matching the rows that follow a cursor.

        Args:
            payload: Cursor payload written by ``payload()``.

        Raises:
            ValidationError: The payload is not an id.

        Returns:
            Predicate matching the rows after the cursor row.
        """
        try:
            last_id = uuid.UUID(payload)
        except ValueError as exc:
            raise ValidationError("Invalid cursor") from exc
        if self._descending:
            return self._id_column < last_id
        return self._id_column > last_id

    def payload(self, values: Sequence[Any]) -> str:
        """Write the keyset values of a row into a cursor payload.

        Args:
            values: Id of the row.

        Returns:
            Cursor payload.
        """
        return str(values[0])


class IndexOrder:
    """Page order by an ascending integer position."""

    def __init__(self, index_column: InstrumentedAttribute[int]) -> None:
        """Initialize the order.

        Args:
            index_column: Integer column defining the sort order.
        """
        self._index_column = index_column

    @property
    def columns(self) -> Sequence[InstrumentedAttribute[Any]]:
        """Keyset columns, in sort precedence."""
        return [self._index_column]

    def order_by(self) -> Sequence[ColumnElement[Any]]:
        """Return the ordering clauses of the keyset columns."""
        return [self._index_column.asc()]

    def after(self, payload: str) -> ColumnElement[bool]:
        """Build the predicate matching the rows that follow a cursor.

        Args:
            payload: Cursor payload written by ``payload()``.

        Raises:
            ValidationError: The payload is not an integer.

        Returns:
            Predicate matching the rows after the cursor row.
        """
        try:
            last_index = int(payload)
        except ValueError as exc:
            raise ValidationError("Invalid cursor") from exc
        return self._index_column > last_index

    def payload(self, values: Sequence[Any]) -> str:
        """Write the keyset values of a row into a cursor payload.

        Args:
            values: Index of the row.

        Returns:
            Cursor payload.
        """
        return str(values[0])


class StartOrder:
    """Page order by an ascending nullable start time, untimed rows last."""

    def __init__(
        self,
        started_at_column: InstrumentedAttribute[datetime | None],
        id_column: InstrumentedAttribute[uuid.UUID],
    ) -> None:
        """Initialize the order.

        Args:
            started_at_column: Start time column defining the sort order.
            id_column: Primary key column breaking start time ties.
        """
        self._started_at_column = started_at_column
        self._id_column = id_column

    @property
    def columns(self) -> Sequence[InstrumentedAttribute[Any]]:
        """Keyset columns, in sort precedence."""
        return [self._started_at_column, self._id_column]

    def order_by(self) -> Sequence[ColumnElement[Any]]:
        """Return the ordering clauses of the keyset columns."""
        return [self._started_at_column.asc().nulls_last(), self._id_column.asc()]

    def after(self, payload: str) -> ColumnElement[bool]:
        """Build the predicate matching the rows that follow a cursor.

        Args:
            payload: Cursor payload written by ``payload()``.

        Raises:
            ValidationError: The payload does not carry a start time and an id.

        Returns:
            Predicate matching the rows after the cursor row.
        """
        started_at, separator, row_id = payload.rpartition("|")
        if not separator:
            raise ValidationError("Invalid cursor")
        try:
            last_started_at = datetime.fromisoformat(started_at) if started_at else None
            last_id = uuid.UUID(row_id)
        except ValueError as exc:
            raise ValidationError("Invalid cursor") from exc
        if last_started_at is None:
            return and_(self._started_at_column.is_(None), self._id_column > last_id)
        return or_(
            self._started_at_column > last_started_at,
            and_(self._started_at_column == last_started_at, self._id_column > last_id),
            self._started_at_column.is_(None),
        )

    def payload(self, values: Sequence[Any]) -> str:
        """Write the keyset values of a row into a cursor payload.

        Args:
            values: Start time and id of the row.

        Returns:
            Cursor payload.
        """
        started_at, row_id = values
        return f"{started_at.isoformat() if started_at is not None else ''}|{row_id}"


async def _apply_list_query_timeout(session: AsyncSession) -> None:
    """Apply the configured statement timeout to the current transaction.

    Args:
        session: Database session for the query.
    """
    timeout_seconds = session.info.get(LIST_QUERY_TIMEOUT_INFO_KEY)
    if not isinstance(timeout_seconds, int) or timeout_seconds <= 0:
        return
    await session.execute(
        select(func.set_config("statement_timeout", str(timeout_seconds * 1000), True))
    )


def _translate_query_timeout(error: DBAPIError) -> None:
    """Translate a statement cancellation into the timeout domain error.

    Args:
        error: Database error to inspect.

    Raises:
        QueryTimeoutError: The statement was canceled by the timeout.
    """
    cause: BaseException | None = error.orig
    while cause is not None:
        if isinstance(cause, QueryCanceledError):
            raise QueryTimeoutError("List query timed out") from error
        cause = cause.__cause__


async def paginate(
    session: AsyncSession,
    statement: Select[tuple[RowT]],
    list_filter: ListFilter,
    order: PageOrder,
) -> tuple[Sequence[RowT], str | None]:
    """Execute a filtered select as one page plus the next cursor.

    The keyset columns of the order are selected alongside the entity so
    the cursor is written from the row rather than read off the entity.

    Args:
        session: Database session for the query.
        statement: Filtered select of the entity, without ordering or
            pagination.
        list_filter: List filter carrying the cursor, size, and filter hash.
        order: Order the page walks.

    Returns:
        Page of matching entities and the next cursor, or None on the last
        page.
    """
    filter_hash = list_filter.compute_filter_hash()
    cursor = None
    if list_filter.cursor is not None:
        cursor = decode_cursor(list_filter.cursor, list_filter.sort, filter_hash)

    paged = statement.add_columns(*order.columns).order_by(*order.order_by())
    if cursor is not None:
        paged = paged.where(order.after(cursor.id))

    paged = paged.limit(list_filter.size + 1)
    await _apply_list_query_timeout(session)
    try:
        rows = (await session.execute(paged)).all()
    except DBAPIError as error:
        _translate_query_timeout(error)
        raise
    next_cursor = None
    if len(rows) > list_filter.size:
        rows = rows[: list_filter.size]
        next_cursor = encode_cursor(
            list_filter.sort, order.payload(tuple(rows[-1])[1:]), filter_hash
        )
    return [row[0] for row in rows], next_cursor
