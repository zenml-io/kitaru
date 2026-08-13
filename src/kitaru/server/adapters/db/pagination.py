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
from typing import TypeVar

from asyncpg.exceptions import QueryCanceledError
from sqlalchemy import Select, func, select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute

from kitaru.server.adapters.db.orm.base import UUIDPrimaryKeyMixin
from kitaru.server.application.pagination import decode_cursor, encode_cursor
from kitaru.server.base import ListFilter
from kitaru.server.domain.base import QueryTimeoutError

LIST_QUERY_TIMEOUT_INFO_KEY = "list_query_timeout_seconds"

RowT = TypeVar("RowT", bound=UUIDPrimaryKeyMixin)


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
    id_column: InstrumentedAttribute[uuid.UUID],
) -> tuple[Sequence[RowT], str | None]:
    """Execute a filtered select as one page plus the next cursor.

    Args:
        session: Database session for the query.
        statement: Filtered select without ordering or pagination.
        list_filter: List filter carrying the cursor, size, and sort.
        id_column: Primary key column.

    Returns:
        Page of matching rows and the next cursor, or None on the last page.
    """
    _, _, direction = list_filter.sort.partition(":")
    descending = direction == "desc"
    filter_hash = list_filter.compute_filter_hash()
    cursor = None
    if list_filter.cursor is not None:
        cursor = decode_cursor(list_filter.cursor, list_filter.sort, filter_hash)

    statement = statement.order_by(id_column.desc() if descending else id_column.asc())
    if cursor is not None:
        last_id = uuid.UUID(cursor.id)
        statement = statement.where(
            id_column < last_id if descending else id_column > last_id
        )

    statement = statement.limit(list_filter.size + 1)
    await _apply_list_query_timeout(session)
    try:
        rows = (await session.scalars(statement)).all()
    except DBAPIError as error:
        _translate_query_timeout(error)
        raise
    next_cursor = None
    if len(rows) > list_filter.size:
        rows = rows[: list_filter.size]
        last_row = rows[-1]
        next_cursor = encode_cursor(list_filter.sort, str(last_row.id), filter_hash)
    return rows, next_cursor


async def paginate_by_index(
    session: AsyncSession,
    statement: Select[tuple[RowT]],
    list_filter: ListFilter,
    index_column: InstrumentedAttribute[int],
) -> tuple[Sequence[RowT], str | None]:
    """Execute a filtered select as one page plus the next cursor.

    Unlike ``paginate()``, the keyset rides an integer column in fixed
    ascending order rather than the UUIDv7 id in either direction, for a
    nested resource whose wire identity is its position within its parent.

    Args:
        session: Database session for the query.
        statement: Filtered select without ordering or pagination.
        list_filter: List filter carrying the cursor, size, and filter hash.
        index_column: Integer column defining the sort order.

    Returns:
        Page of matching rows and the next cursor, or None on the last page.
    """
    filter_hash = list_filter.compute_filter_hash()
    cursor = None
    if list_filter.cursor is not None:
        cursor = decode_cursor(list_filter.cursor, list_filter.sort, filter_hash)

    statement = statement.order_by(index_column.asc())
    if cursor is not None:
        statement = statement.where(index_column > int(cursor.id))

    statement = statement.limit(list_filter.size + 1)
    await _apply_list_query_timeout(session)
    try:
        rows = (await session.scalars(statement)).all()
    except DBAPIError as error:
        _translate_query_timeout(error)
        raise
    next_cursor = None
    if len(rows) > list_filter.size:
        rows = rows[: list_filter.size]
        last_index = getattr(rows[-1], index_column.key)
        next_cursor = encode_cursor(list_filter.sort, str(last_index), filter_hash)
    return rows, next_cursor


async def paginate_join_by_index(
    session: AsyncSession,
    statement: Select[tuple[RowT, int]],
    list_filter: ListFilter,
    index_column: InstrumentedAttribute[int],
) -> tuple[Sequence[RowT], str | None]:
    """Execute a filtered two-column select as one page plus the next cursor.

    Like ``paginate_by_index``, but for an entity paginated by an index
    column that lives on a joined link table rather than on the entity
    itself. The statement selects the entity alongside the index column, and
    each result row is unpacked before being returned.

    Args:
        session: Database session for the query.
        statement: Filtered select of the entity and its index column,
            without ordering or pagination.
        list_filter: List filter carrying the cursor, size, and filter hash.
        index_column: Integer column defining the sort order.

    Returns:
        Page of matching entities and the next cursor, or None on the last
        page.
    """
    filter_hash = list_filter.compute_filter_hash()
    cursor = None
    if list_filter.cursor is not None:
        cursor = decode_cursor(list_filter.cursor, list_filter.sort, filter_hash)

    statement = statement.order_by(index_column.asc())
    if cursor is not None:
        statement = statement.where(index_column > int(cursor.id))

    statement = statement.limit(list_filter.size + 1)
    await _apply_list_query_timeout(session)
    try:
        rows = (await session.execute(statement)).all()
    except DBAPIError as error:
        _translate_query_timeout(error)
        raise
    next_cursor = None
    if len(rows) > list_filter.size:
        rows = rows[: list_filter.size]
        last_index = rows[-1][1]
        next_cursor = encode_cursor(list_filter.sort, str(last_index), filter_hash)
    return [row[0] for row in rows], next_cursor
