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
"""Analytics tracking deferred until the request session commits."""

import uuid
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from kitaru.analytics.client import AnalyticsClient
from kitaru.analytics.events import AnalyticsEvent

_BUFFER_KEY = "kitaru_analytics_buffer"


@dataclass
class _BufferedTrack:
    """Buffered track message."""

    user_id: uuid.UUID
    event: str
    properties: dict[str, Any]


@dataclass
class _BufferedIdentify:
    """Buffered identify message."""

    user_id: uuid.UUID
    traits: dict[str, Any]


@dataclass
class _BufferedAlias:
    """Buffered alias message."""

    user_id: uuid.UUID
    previous_id: uuid.UUID


_BufferedMessage = _BufferedTrack | _BufferedIdentify | _BufferedAlias


@dataclass
class _AnalyticsBuffer:
    """Messages queued on a session until it commits."""

    client: AnalyticsClient
    messages: list[_BufferedMessage] = field(default_factory=list)


class ServerAnalytics:
    """Analytics tracker that buffers calls until the session commits."""

    def __init__(
        self,
        client: AnalyticsClient,
        session: AsyncSession,
        server_id: uuid.UUID | None,
        version: str,
    ) -> None:
        """Initialize the tracker.

        Args:
            client: Analytics client the buffered messages are delivered
                through.
            session: Request-scoped database session the messages are
                buffered on.
            server_id: Enrolled server id, None when unset.
            version: Kitaru version, merged into every event's properties.
        """
        self._client = client
        self._session = session
        self._server_id = server_id
        self._version = version

    def track(
        self,
        user_id: uuid.UUID,
        event: AnalyticsEvent | str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        """Buffer a track message for delivery once the session commits.

        Args:
            user_id: User id.
            event: Event name.
            properties: Event properties, merged with the server id and
                version.
        """
        if not self._client.enabled:
            return
        self._get_buffer().messages.append(
            _BufferedTrack(
                user_id=user_id, event=event, properties=self._merge(properties)
            )
        )

    def identify(
        self, user_id: uuid.UUID, traits: dict[str, Any] | None = None
    ) -> None:
        """Buffer an identify message for delivery once the session commits.

        Args:
            user_id: User id.
            traits: User traits, merged with the server id and version.
        """
        if not self._client.enabled:
            return
        self._get_buffer().messages.append(
            _BufferedIdentify(user_id=user_id, traits=self._merge(traits))
        )

    def alias(self, user_id: uuid.UUID, previous_id: uuid.UUID) -> None:
        """Buffer an alias message for delivery once the session commits.

        Args:
            user_id: User id the alias points to.
            previous_id: User id the events were recorded under.
        """
        if not self._client.enabled:
            return
        self._get_buffer().messages.append(
            _BufferedAlias(user_id=user_id, previous_id=previous_id)
        )

    def _merge(self, values: dict[str, Any] | None) -> dict[str, Any]:
        """Merge the server id and version into a message's values.

        Args:
            values: Event properties or user traits.

        Returns:
            Merged values.
        """
        return {
            **(values or {}),
            "server_id": self._server_id,
            "version": self._version,
        }

    def _get_buffer(self) -> _AnalyticsBuffer:
        """Get the session's buffer, creating it on first use.

        Returns:
            Buffer stored on the request session.
        """
        buffer = self._session.info.get(_BUFFER_KEY)
        if buffer is None:
            buffer = _AnalyticsBuffer(client=self._client)
            self._session.info[_BUFFER_KEY] = buffer
        return buffer


def flush_analytics_buffer(session: Session) -> None:
    """Deliver every message buffered on a committed session.

    Args:
        session: Sync session underlying a committed AsyncSession.
    """
    buffer: _AnalyticsBuffer | None = session.info.pop(_BUFFER_KEY, None)
    if buffer is None:
        return
    for message in buffer.messages:
        if isinstance(message, _BufferedTrack):
            buffer.client.track(message.user_id, message.event, message.properties)
        elif isinstance(message, _BufferedIdentify):
            buffer.client.identify(message.user_id, message.traits)
        else:
            buffer.client.alias(message.user_id, message.previous_id)


def discard_analytics_buffer(session: Session) -> None:
    """Discard every message buffered on a rolled back session.

    Args:
        session: Sync session underlying a rolled back AsyncSession.
    """
    session.info.pop(_BUFFER_KEY, None)
