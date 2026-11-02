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
"""Atomic creation of sessions and their initial nodes."""

from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.session import SessionCreate
from kitaru.server.application.models.session_node import SessionNodeUpsert
from kitaru.server.application.services.session_node_service import SessionNodeService
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.domain.session import Session

_NODE_BATCH_SIZE = 200


class SessionIngestionService:
    """Session ingestion use cases."""

    def __init__(self, sessions: SessionService, nodes: SessionNodeService) -> None:
        """Initialize the service.

        Args:
            sessions: Session service sharing the current transaction.
            nodes: Node service sharing the current transaction.
        """
        self._sessions = sessions
        self._nodes = nodes

    async def ingest_session(
        self,
        command: SessionCreate,
        nodes: list[SessionNodeUpsert],
        actor: AuthContext,
    ) -> Session:
        """Create a session and its initial nodes in the current transaction.

        Args:
            command: Session to create.
            nodes: Complete initial nodes.
            actor: Caller context.

        Returns:
            Created session with updated node rollups.
        """
        session = await self._sessions.create_session(command, actor=actor)
        # Bound SQL parameter counts while retaining one request transaction.
        for start in range(0, len(nodes), _NODE_BATCH_SIZE):
            await self._nodes.ingest_nodes(
                session.id, nodes[start : start + _NODE_BATCH_SIZE], actor=actor
            )
        return await self._sessions.get_session(session.id, actor=actor)
