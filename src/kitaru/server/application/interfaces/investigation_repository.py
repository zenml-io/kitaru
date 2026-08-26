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
"""Investigation repository interface."""

import uuid
from collections.abc import Sequence
from typing import Protocol

from kitaru.server.application.models.investigation import (
    InvestigationFilter,
    InvestigationSessionFilter,
)
from kitaru.server.domain.investigation import Investigation, InvestigationSession


class InvestigationRepository(Protocol):
    """Investigation persistence operations."""

    async def create(
        self, investigation: Investigation, sessions: Sequence[InvestigationSession]
    ) -> Investigation:
        """Persist a new investigation with its linked sessions.

        Args:
            investigation: Investigation to store.
            sessions: Ordered investigation_session rows to link.

        Raises:
            AgentNotFound: No agent has the investigation's agent id.
            SessionNotFound: No session has one of the linked session ids.

        Returns:
            Stored investigation with timestamps set.
        """
        ...

    async def get(
        self, investigation_id: uuid.UUID, exclusive: bool = False
    ) -> Investigation:
        """Load an investigation by id.

        Args:
            investigation_id: Id of the investigation.
            exclusive: Whether to lock the row for the duration of the
                transaction.

        Raises:
            InvestigationNotFound: No investigation has this id.

        Returns:
            Stored investigation.
        """
        ...

    async def query(
        self, investigation_filter: InvestigationFilter
    ) -> tuple[list[Investigation], str | None]:
        """Query investigations matching a filter.

        Args:
            investigation_filter: Filter and pagination parameters.

        Returns:
            Page of matching investigations and the next cursor.
        """
        ...

    async def update(self, investigation: Investigation) -> Investigation:
        """Persist changes to an existing investigation.

        Args:
            investigation: Investigation with modified fields.

        Raises:
            InvestigationNotFound: No investigation has this id.

        Returns:
            Stored investigation with the updated timestamp renewed.
        """
        ...

    async def delete(self, investigation_id: uuid.UUID) -> None:
        """Delete an investigation by id, cascading its links and answers.

        Args:
            investigation_id: Id of the investigation.

        Raises:
            InvestigationNotFound: No investigation has this id.
        """
        ...

    async def get_session(
        self, investigation_session_id: uuid.UUID, exclusive: bool = False
    ) -> InvestigationSession:
        """Load an investigation session by id.

        Args:
            investigation_session_id: Id of the investigation session.
            exclusive: Whether to lock the row for the duration of the
                transaction.

        Raises:
            InvestigationSessionNotFound: No investigation session has this
                id.

        Returns:
            Stored investigation session.
        """
        ...

    async def get_session_by_session_id(
        self,
        investigation_id: uuid.UUID,
        session_id: uuid.UUID,
        exclusive: bool = False,
    ) -> InvestigationSession:
        """Load an investigation session by investigation id and session id.

        Args:
            investigation_id: Id of the investigation.
            session_id: Id of the linked session.
            exclusive: Whether to lock the row for the duration of the
                transaction.

        Raises:
            InvestigationSessionNotFound: No investigation session links this
                investigation and session.

        Returns:
            Stored investigation session.
        """
        ...

    async def query_sessions(
        self, session_filter: InvestigationSessionFilter
    ) -> tuple[list[InvestigationSession], str | None]:
        """Query an investigation's sessions, ordered by position ascending.

        Args:
            session_filter: Filter and pagination parameters.

        Returns:
            Page of matching investigation sessions and the next cursor.
        """
        ...

    async def update_session(
        self, session: InvestigationSession
    ) -> InvestigationSession:
        """Persist changes to an existing investigation session.

        Args:
            session: Investigation session with modified fields.

        Raises:
            InvestigationSessionNotFound: No investigation session has this
                id.

        Returns:
            Stored investigation session with the updated timestamp renewed.
        """
        ...
