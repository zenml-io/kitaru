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
"""Shared bulk row loading."""

import uuid
from collections.abc import Sequence
from typing import TypeVar

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from kitaru.server.adapters.db.orm.base import UUIDPrimaryKeyMixin

RowT = TypeVar("RowT", bound=UUIDPrimaryKeyMixin)


async def load_by_ids(
    session: AsyncSession,
    schema: type[RowT],
    ids: Sequence[uuid.UUID],
) -> dict[uuid.UUID, RowT]:
    """Load rows by id into a dict keyed by id, missing ids omitted.

    Args:
        session: Database session for the query.
        schema: ORM class to load.
        ids: Ids of the rows.

    Returns:
        Rows keyed by id.
    """
    if not ids:
        return {}
    statement = select(schema).where(schema.id.in_(ids))
    rows = (await session.scalars(statement)).all()
    return {row.id: row for row in rows}
