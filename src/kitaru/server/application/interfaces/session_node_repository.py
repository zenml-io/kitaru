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
from typing import Protocol

from kitaru.server.domain.session_node import SessionNode


class SessionNodeRepository(Protocol):
    """Session node persistence operations."""

    async def upsert(self, nodes: list[SessionNode]) -> list[SessionNode]:
        """Insert or update nodes by id as one atomic batch.

        Callers guarantee that every primary parent exists, either stored
        or earlier in the batch.

        Args:
            nodes: Nodes to store, all belonging to one session.

        Raises:
            SessionNotFound: No session has the nodes' session id.
            DuplicateSessionNodeId: A node id is already registered in
                another session.
            DuplicateNodeSequence: A node sequence is already registered in
                the session.
            DuplicateNodeExternalId: A node external id is already
                registered in the session.
            DuplicateNodeKey: A node key is already registered in the
                session.

        Returns:
            Stored nodes in batch order with timestamps set.
        """
        ...

    async def list_for_session(
        self, session_id: uuid.UUID, include_payloads: bool
    ) -> list[SessionNode]:
        """Load the nodes of a session ordered by sequence.

        Args:
            session_id: Id of the session.
            include_payloads: Whether to load inputs, outputs, and
                attributes.

        Returns:
            Nodes ordered by sequence.
        """
        ...

    async def find_tool_result(
        self,
        cache_key: str,
        session_ids: list[uuid.UUID] | None,
        agent_id: uuid.UUID | None,
    ) -> SessionNode | None:
        """Find the most recent completed tool call with a cache key.

        Nodes whose attributes mark them mocked are excluded. Exactly one
        of the scope arguments is set.

        Args:
            cache_key: Cache key to match.
            session_ids: Sessions to search within.
            agent_id: Agent whose sessions to search within.

        Returns:
            Most recent matching node with payloads, ``None`` on a miss.
        """
        ...
