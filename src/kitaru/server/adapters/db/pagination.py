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

from collections.abc import Sequence
from typing import Any, TypeVar

from sqlalchemy import Select, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import ColumnExpressionArgument

RowT = TypeVar("RowT")


async def paginate(
    session: AsyncSession,
    statement: Select[tuple[RowT]],
    order_by: ColumnExpressionArgument[Any],
    page: int,
    page_size: int,
) -> tuple[Sequence[RowT], int]:
    """Execute a filtered select as one page plus the total match count.

    Args:
        session: Database session for both queries.
        statement: Filtered select without ordering or pagination.
        order_by: Column expression ordering the results.
        page: One-based page number.
        page_size: Number of rows per page.

    Returns:
        Page of matching rows and the total match count.
    """
    count_statement = select(func.count()).select_from(statement.subquery())
    total = (await session.execute(count_statement)).scalar_one()
    statement = (
        statement.order_by(order_by).offset((page - 1) * page_size).limit(page_size)
    )
    rows = (await session.scalars(statement)).all()
    return rows, total
