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
"""Session node repository interface."""

import uuid
from collections.abc import Collection, Sequence
from typing import Protocol

from kitaru.server.application.models.session_node import SessionNodeFilter
from kitaru.server.domain.session_node import SessionNode


class SessionNodeRepository(Protocol):
    """Session node persistence operations."""

    async def get_by_indexes(
        self, session_id: uuid.UUID, indexes: Sequence[int], include_payloads: bool
    ) -> dict[int, SessionNode]:
        """Bulk-load the stored nodes of a session at the given indexes.

        Args:
            session_id: Id of the owning session.
            indexes: Indexes to load.
            include_payloads: Whether to read the inputs, outputs, and
                attributes.

        Returns:
            Stored nodes keyed by index, missing indexes omitted.
        """
        ...

    async def upsert_batch(
        self, session_id: uuid.UUID, nodes: list[SessionNode]
    ) -> list[SessionNode]:
        """Insert or replace nodes upserted on (session, index).

        Each node's id is inserted as given for a new index and preserved as
        given for a replaced index, both resolved by the caller.

        Args:
            session_id: Id of the owning session.
            nodes: Fully resolved nodes to store, in batch order.

        Returns:
            Stored nodes in batch order.
        """
        ...

    async def query(
        self, session_node_filter: SessionNodeFilter
    ) -> tuple[list[SessionNode], str | None]:
        """Query the nodes of a session, ordered by index ascending.

        Args:
            session_node_filter: Filter and pagination parameters.

        Returns:
            Page of matching nodes and the next cursor.
        """
        ...

    async def list_all(
        self, session_id: uuid.UUID, include_payloads: bool
    ) -> list[SessionNode]:
        """Read every node of a session, ordered by index ascending.

        Args:
            session_id: Id of the owning session.
            include_payloads: Whether to read the inputs, outputs, and
                attributes.

        Returns:
            Every node of the session.
        """
        ...

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
        ...

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
        ...

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
        ...

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
        ...

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
        ...
