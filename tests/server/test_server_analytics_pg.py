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
"""Integration tests for the deferred-to-commit analytics tracking mechanism."""

import uuid
from typing import Any

import pytest

from conftest import pg_session, postgres_available
from kitaru.analytics.client import AnalyticsClient
from kitaru.analytics.events import AnalyticsEvent
from kitaru.server.adapters.db.analytics import register_analytics_listeners
from kitaru.server.application.services.server_analytics import ServerAnalytics


@pytest.fixture(autouse=True)
def analytics_listeners() -> None:
    """Register the analytics session listeners."""
    register_analytics_listeners()


class _RecordingAnalyticsClient(AnalyticsClient):
    """Analytics client recording track calls instead of sending them."""

    def __init__(self, enabled: bool = True) -> None:
        """Initialize the client.

        Args:
            enabled: Whether messages are sent.
        """
        super().__init__(enabled=enabled)
        self.tracked: list[tuple[uuid.UUID, str, dict[str, Any]]] = []

    def track(
        self,
        user_id: uuid.UUID,
        event: AnalyticsEvent | str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        """Record a track call instead of queuing it for delivery.

        Args:
            user_id: User id.
            event: Event name.
            properties: Event properties.
        """
        self.tracked.append((user_id, event, properties or {}))


async def test_track_then_commit_delivers_to_client() -> None:
    """A buffered track message reaches the client once the session commits."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    client = _RecordingAnalyticsClient()
    user_id = uuid.uuid4()
    async with pg_session() as session:
        analytics = ServerAnalytics(
            client=client, session=session, server_id=None, version="0.0.0"
        )
        analytics.track(user_id, AnalyticsEvent.SESSION_COMPLETED, {"foo": "bar"})
        await session.commit()

    assert len(client.tracked) == 1
    tracked_user_id, tracked_event, tracked_properties = client.tracked[0]
    assert tracked_user_id == user_id
    assert tracked_event == AnalyticsEvent.SESSION_COMPLETED
    assert tracked_properties["foo"] == "bar"
    assert tracked_properties["server_id"] is None
    assert tracked_properties["version"] == "0.0.0"


async def test_track_then_rollback_delivers_nothing() -> None:
    """A buffered track message is discarded when the session rolls back."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    client = _RecordingAnalyticsClient()
    async with pg_session() as session:
        analytics = ServerAnalytics(
            client=client, session=session, server_id=None, version="0.0.0"
        )
        analytics.track(uuid.uuid4(), AnalyticsEvent.SESSION_COMPLETED)
        await session.rollback()

    assert client.tracked == []


async def test_second_commit_does_not_resend() -> None:
    """A session committing twice only delivers its buffered messages once."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    client = _RecordingAnalyticsClient()
    async with pg_session() as session:
        analytics = ServerAnalytics(
            client=client, session=session, server_id=None, version="0.0.0"
        )
        analytics.track(uuid.uuid4(), AnalyticsEvent.SESSION_COMPLETED)
        await session.commit()
        await session.commit()

    assert len(client.tracked) == 1


async def test_disabled_client_buffers_nothing() -> None:
    """Tracking through a disabled client never buffers or delivers a message."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    client = _RecordingAnalyticsClient(enabled=False)
    async with pg_session() as session:
        analytics = ServerAnalytics(
            client=client, session=session, server_id=None, version="0.0.0"
        )
        analytics.track(uuid.uuid4(), AnalyticsEvent.SESSION_COMPLETED)
        await session.commit()

    assert client.tracked == []
