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
from collections.abc import Collection, Sequence

from sqlalchemy import Select, select
from sqlalchemy.orm import defer

from kitaru.api_models.v1.session import SessionOrigin
from kitaru.server.adapters.db.orm.cohort_version_session import (
    CohortVersionSessionORM,
)
from kitaru.server.adapters.db.orm.session import SessionORM
from kitaru.server.adapters.db.orm.session_node import SessionNodeORM
from kitaru.server.adapters.db.pagination import paginate_by_index
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.session_node import SessionNodeFilter
from kitaru.server.domain.session_node import SessionNode

RECORDED_HISTORY_ORIGINS = [SessionOrigin.RECORDED.value, SessionOrigin.IMPORTED.value]

PAYLOAD_COLUMNS = (
    SessionNodeORM.reasoning,
    SessionNodeORM.inputs,
    SessionNodeORM.outputs,
    SessionNodeORM.attributes,
)


class SQLSessionNodeRepository(BaseSQLRepository[SessionNodeORM]):
    """Session node repository backed by the application database."""

    orm_class = SessionNodeORM

    async def get_by_indexes(
        self, session_id: uuid.UUID, indexes: Sequence[int], include_payloads: bool
    ) -> dict[int, SessionNode]:
        """Bulk-load the stored nodes of a session at the given indexes.

        Args:
            session_id: Id of the owning session.
            indexes: Indexes to load.
            include_payloads: Whether to read reasoning, inputs, outputs,
                and attributes.

        Returns:
            Stored nodes keyed by index, missing indexes omitted.
        """
        if not indexes:
            return {}
        statement = select(SessionNodeORM).where(
            SessionNodeORM.session_id == session_id,
            SessionNodeORM.index.in_(indexes),
        )
        if not include_payloads:
            statement = statement.options(
                *(defer(column) for column in PAYLOAD_COLUMNS)
            )
        rows = (await self._session.scalars(statement)).all()
        return {
            row.index: row.to_domain(include_payloads=include_payloads) for row in rows
        }

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
        # Defer the payload columns because apply_domain replaces them below
        # without ever reading them, so the deferred load never fires.
        existing_by_id = await self._load_by_ids(
            [node.id for node in nodes], deferred_columns=PAYLOAD_COLUMNS
        )
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
                *(defer(column) for column in PAYLOAD_COLUMNS)
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

    async def list_all(
        self, session_id: uuid.UUID, include_payloads: bool
    ) -> list[SessionNode]:
        """Read every node of a session, ordered by index ascending.

        Args:
            session_id: Id of the owning session.
            include_payloads: Whether to read reasoning, inputs, outputs,
                and attributes.

        Returns:
            Every node of the session.
        """
        statement = (
            select(SessionNodeORM)
            .where(SessionNodeORM.session_id == session_id)
            .order_by(SessionNodeORM.index)
        )
        if not include_payloads:
            statement = statement.options(
                *(defer(column) for column in PAYLOAD_COLUMNS)
            )
        rows = (await self._session.scalars(statement)).all()
        return [row.to_domain(include_payloads=include_payloads) for row in rows]

    async def get_indexes_by_ids(
        self, session_id: uuid.UUID, node_ids: Collection[uuid.UUID]
    ) -> dict[uuid.UUID, int]:
        """Bulk-load the index of the named nodes of a session, keyed by node id.

        Args:
            session_id: Id of the owning session.
            node_ids: Ids to look up.

        Returns:
            Each requested node id mapped to its index, missing ids omitted.
        """
        if not node_ids:
            return {}
        statement = select(SessionNodeORM.id, SessionNodeORM.index).where(
            SessionNodeORM.session_id == session_id,
            SessionNodeORM.id.in_(node_ids),
        )
        rows = (await self._session.execute(statement)).all()
        return {node_id: index for node_id, index in rows}

    async def _latest_match(
        self, statement: Select[tuple[SessionNodeORM]]
    ) -> SessionNode | None:
        """Run a cache-key search statement and return its newest match.

        Args:
            statement: Filtered select, ordering and limit not yet applied.

        Returns:
            Highest-id matching node, or ``None`` on a miss.
        """
        statement = statement.order_by(SessionNodeORM.id.desc()).limit(1)
        row = (await self._session.scalars(statement)).one_or_none()
        return row.to_domain(include_payloads=True) if row is not None else None

    async def find_latest_by_cache_key_in_session(
        self, session_id: uuid.UUID, cache_key: str
    ) -> SessionNode | None:
        """Find the newest node with a cache key within one session.

        Args:
            session_id: Id of the session to search.
            cache_key: Tool call cache key to match.

        Returns:
            Highest-id matching node, or ``None`` on a miss.
        """
        return await self._latest_match(
            select(SessionNodeORM).where(
                SessionNodeORM.session_id == session_id,
                SessionNodeORM.cache_key == cache_key,
            )
        )

    async def find_nth_by_cache_key_in_session(
        self, session_id: uuid.UUID, cache_key: str, occurrence: int
    ) -> SessionNode | None:
        """Find the nth node with a cache key within one session, in index order.

        Args:
            session_id: Id of the session to search.
            cache_key: Tool call cache key to match.
            occurrence: Zero-based match position in index order.

        Returns:
            Matching node at the position, or ``None`` on a miss.
        """
        statement = (
            select(SessionNodeORM)
            .where(
                SessionNodeORM.session_id == session_id,
                SessionNodeORM.cache_key == cache_key,
            )
            .order_by(SessionNodeORM.index)
            .offset(occurrence)
            .limit(1)
        )
        row = (await self._session.scalars(statement)).one_or_none()
        return row.to_domain(include_payloads=True) if row is not None else None

    async def find_latest_by_cache_key_in_agent(
        self, agent_id: uuid.UUID, cache_key: str
    ) -> SessionNode | None:
        """Find the newest node with a cache key across an agent's recorded history.

        Only sessions with a recorded or imported origin are searched, so a
        replay's own result session is never a match.

        Args:
            agent_id: Id of the agent to search.
            cache_key: Tool call cache key to match.

        Returns:
            Highest-id matching node, or ``None`` on a miss.
        """
        return await self._latest_match(
            select(SessionNodeORM)
            .join(SessionORM, SessionORM.id == SessionNodeORM.session_id)
            .where(
                SessionORM.agent_id == agent_id,
                SessionORM.origin.in_(RECORDED_HISTORY_ORIGINS),
                SessionNodeORM.cache_key == cache_key,
            )
        )

    async def find_latest_by_cache_key_in_cohort_version(
        self, cohort_version_id: uuid.UUID, cache_key: str
    ) -> SessionNode | None:
        """Find the newest node with a cache key across a cohort version's sessions.

        Args:
            cohort_version_id: Id of the cohort version to search.
            cache_key: Tool call cache key to match.

        Returns:
            Highest-id matching node, or ``None`` on a miss.
        """
        return await self._latest_match(
            select(SessionNodeORM)
            .join(
                CohortVersionSessionORM,
                CohortVersionSessionORM.session_id == SessionNodeORM.session_id,
            )
            .where(
                CohortVersionSessionORM.cohort_version_id == cohort_version_id,
                SessionNodeORM.cache_key == cache_key,
            )
        )
