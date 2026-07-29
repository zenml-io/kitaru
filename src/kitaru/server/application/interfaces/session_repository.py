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

from kitaru.server.application.models.session import SessionFilter
from kitaru.server.domain.session import Session, SessionRollups


class SessionRepository(Protocol):
    """Session persistence operations."""

    async def create(self, session: Session) -> Session:
        """Persist a new session.

        Args:
            session: Session to store.

        Raises:
            DuplicateSessionExternalId: The provider and external id pair is
                already registered.

        Returns:
            Stored session with timestamps set.
        """
        ...

    async def get(self, session_id: uuid.UUID, exclusive: bool = False) -> Session:
        """Load a session by id.

        Args:
            session_id: Id of the session.
            exclusive: Whether to lock the row for the duration of the
                transaction.

        Raises:
            SessionNotFound: No session has this id.

        Returns:
            Stored session.
        """
        ...

    async def query(
        self, session_filter: SessionFilter
    ) -> tuple[list[Session], str | None]:
        """Query sessions matching a filter.

        Args:
            session_filter: Filter and pagination parameters.

        Returns:
            Page of matching sessions and the next cursor.
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
        """Delete a session by id.

        Deleting a session cascades its nodes.

        Args:
            session_id: Id of the session.

        Raises:
            SessionNotFound: No session has this id.
        """
        ...

    async def apply_rollups(
        self, session_id: uuid.UUID, deltas: SessionRollups
    ) -> None:
        """Apply rollup deltas to a session's cost, tokens, and call counts.

        The update is one atomic statement, adding each delta to the stored
        value, so a retried identical batch nets a zero delta.

        Args:
            session_id: Id of the session.
            deltas: Rollup deltas to add.

        Raises:
            SessionNotFound: No session has this id.
        """
        ...
