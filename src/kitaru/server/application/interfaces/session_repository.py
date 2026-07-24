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
"""Session repository interface."""

import uuid
from typing import Protocol

from kitaru.server.application.models.sessions import SessionFilter
from kitaru.server.domain.session import Session


class SessionRepository(Protocol):
    """Session persistence operations."""

    async def create(self, session: Session) -> Session:
        """Persist a new session.

        Args:
            session: Session to store.

        Raises:
            AgentNotFound: No agent has the session's agent id.
            AgentVersionNotFound: No agent version has the session's agent
                version id.
            DuplicateSessionExternalId: The provider and external id pair is
                already registered.

        Returns:
            Stored session with timestamps set.
        """
        ...

    async def get(self, session_id: uuid.UUID) -> Session:
        """Load a session by id.

        Args:
            session_id: Id of the session.

        Raises:
            SessionNotFound: No session has this id.

        Returns:
            Stored session.
        """
        ...

    async def query(self, session_filter: SessionFilter) -> tuple[list[Session], int]:
        """Query sessions matching a filter.

        Args:
            session_filter: Filter and pagination parameters.

        Returns:
            Page of matching sessions and the total match count.
        """
        ...

    async def update(self, session: Session) -> Session:
        """Persist changes to an existing session.

        Args:
            session: Session with modified fields.

        Raises:
            SessionNotFound: No session has this id.
            DuplicateSessionExternalId: The provider and external id pair is
                already registered.

        Returns:
            Stored session with the updated timestamp renewed.
        """
        ...

    async def delete(self, session_id: uuid.UUID) -> None:
        """Delete a session by id, including its nodes and tag links.

        Args:
            session_id: Id of the session.

        Raises:
            SessionNotFound: No session has this id.
            SessionInUse: The session is a member of a cohort or referenced
                by a job.
        """
        ...
