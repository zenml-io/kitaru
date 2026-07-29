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
"""SQL session node repository."""

import uuid
from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.orm import defer

from kitaru.server.adapters.db.orm.session_node import SessionNodeORM
from kitaru.server.adapters.db.pagination import paginate_by_index
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.session_node import SessionNodeFilter
from kitaru.server.domain.session_node import SessionNode


class SQLSessionNodeRepository(BaseSQLRepository[SessionNodeORM]):
    """Session node repository backed by the application database."""

    orm_class = SessionNodeORM

    async def get_by_indexes(
        self, session_id: uuid.UUID, indexes: Sequence[int]
    ) -> dict[int, SessionNode]:
        """Bulk-load the stored nodes of a session at the given indexes.

        Args:
            session_id: Id of the owning session.
            indexes: Indexes to load.

        Returns:
            Stored nodes keyed by index, missing indexes omitted.
        """
        if not indexes:
            return {}
        statement = select(SessionNodeORM).where(
            SessionNodeORM.session_id == session_id,
            SessionNodeORM.index.in_(indexes),
        )
        rows = (await self._session.scalars(statement)).all()
        return {row.index: row.to_domain(include_payloads=True) for row in rows}

    async def upsert_batch(
        self, session_id: uuid.UUID, nodes: list[SessionNode]
    ) -> list[SessionNode]:
        """Insert or replace nodes upserted on (session, index).

        The rows already stored under a batch's ids are found through one
        bulk id lookup, so an insert or a whole-row replace never issues a
        per-row get.

        Args:
            session_id: Id of the owning session.
            nodes: Fully resolved nodes to store, in batch order.

        Returns:
            Stored nodes in batch order.
        """
        _ = session_id
        if not nodes:
            return []
        existing_by_id = await self._load_by_ids([node.id for node in nodes])
        stored_rows: list[SessionNodeORM] = []
        for node in nodes:
            row = existing_by_id.get(node.id)
            if row is None:
                row = SessionNodeORM.from_domain(node)
                self._session.add(row)
            else:
                row.apply_domain(node)
            stored_rows.append(row)
        await self._flush()
        return [row.to_domain(include_payloads=True) for row in stored_rows]

    async def query(
        self, session_node_filter: SessionNodeFilter
    ) -> tuple[list[SessionNode], str | None]:
        """Query the nodes of a session, ordered by index ascending.

        Args:
            session_node_filter: Filter and pagination parameters.

        Returns:
            Page of matching nodes and the next cursor.
        """
        statement = select(SessionNodeORM).where(
            SessionNodeORM.session_id == session_node_filter.session_id
        )
        if not session_node_filter.include_payloads:
            statement = statement.options(
                defer(SessionNodeORM.inputs),
                defer(SessionNodeORM.outputs),
                defer(SessionNodeORM.attributes),
            )
        rows, next_cursor = await paginate_by_index(
            self._session,
            statement,
            session_node_filter,
            index_column=SessionNodeORM.index,
        )
        return [
            row.to_domain(include_payloads=session_node_filter.include_payloads)
            for row in rows
        ], next_cursor
