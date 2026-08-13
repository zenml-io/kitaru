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
from kitaru.server.application.services.server_analytics import (
    ServerAnalytics,
    current_actor,
)
from kitaru.server.domain.account import Account


@pytest.fixture(autouse=True)
def analytics_listeners() -> None:
    """Register the analytics session listeners."""
    register_analytics_listeners()


class _RecordingAnalyticsClient(AnalyticsClient):
    """Analytics client recording calls instead of sending them."""

    def __init__(self, enabled: bool = True) -> None:
        """Initialize the client.

        Args:
            enabled: Whether messages are sent.
        """
        super().__init__(enabled=enabled)
        self.tracked: list[tuple[uuid.UUID, str, dict[str, Any]]] = []
        self.identified: list[tuple[uuid.UUID, dict[str, Any]]] = []
        self.aliased: list[tuple[uuid.UUID, uuid.UUID]] = []

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

    def identify(
        self, user_id: uuid.UUID, traits: dict[str, Any] | None = None
    ) -> None:
        """Record an identify call instead of queuing it for delivery.

        Args:
            user_id: User id.
            traits: User traits.
        """
        self.identified.append((user_id, traits or {}))

    def alias(self, user_id: uuid.UUID, previous_id: uuid.UUID) -> None:
        """Record an alias call instead of queuing it for delivery.

        Args:
            user_id: User id the alias points to.
            previous_id: User id the events were recorded under.
        """
        self.aliased.append((user_id, previous_id))


async def test_track_then_commit_delivers_to_client() -> None:
    """A buffered track message reaches the client once the session commits."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    client = _RecordingAnalyticsClient()
    user_id = uuid.uuid4()
    async with pg_session() as session:
        analytics = ServerAnalytics(client=client, session=session)
        analytics.track(user_id, AnalyticsEvent.SESSION_COMPLETED, {"foo": "bar"})
        await session.commit()

    assert len(client.tracked) == 1
    tracked_user_id, tracked_event, tracked_properties = client.tracked[0]
    assert tracked_user_id == user_id
    assert tracked_event == AnalyticsEvent.SESSION_COMPLETED
    assert tracked_properties["foo"] == "bar"
    assert "service_account" not in tracked_properties
    assert "control_plane_user_id" not in tracked_properties


async def test_track_then_rollback_delivers_nothing() -> None:
    """A buffered track message is discarded when the session rolls back."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    client = _RecordingAnalyticsClient()
    async with pg_session() as session:
        analytics = ServerAnalytics(client=client, session=session)
        analytics.track(uuid.uuid4(), AnalyticsEvent.SESSION_COMPLETED)
        await session.rollback()

    assert client.tracked == []


async def test_second_commit_does_not_resend() -> None:
    """A session committing twice only delivers its buffered messages once."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    client = _RecordingAnalyticsClient()
    async with pg_session() as session:
        analytics = ServerAnalytics(client=client, session=session)
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
        analytics = ServerAnalytics(client=client, session=session)
        analytics.track(uuid.uuid4(), AnalyticsEvent.SESSION_COMPLETED)
        await session.commit()

    assert client.tracked == []


async def test_identify_then_commit_delivers_to_client() -> None:
    """A buffered identify message reaches the client once the session commits."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    client = _RecordingAnalyticsClient()
    user_id = uuid.uuid4()
    async with pg_session() as session:
        analytics = ServerAnalytics(client=client, session=session)
        analytics.identify(user_id, {"is_admin": True})
        await session.commit()

    assert len(client.identified) == 1
    identified_user_id, traits = client.identified[0]
    assert identified_user_id == user_id
    assert traits["is_admin"] is True


async def test_identify_then_rollback_delivers_nothing() -> None:
    """A buffered identify message is discarded when the session rolls back."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    client = _RecordingAnalyticsClient()
    async with pg_session() as session:
        analytics = ServerAnalytics(client=client, session=session)
        analytics.identify(uuid.uuid4())
        await session.rollback()

    assert client.identified == []


async def test_identify_and_track_deliver_in_order() -> None:
    """Both message kinds buffered on one session reach the client on commit."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    client = _RecordingAnalyticsClient()
    async with pg_session() as session:
        analytics = ServerAnalytics(client=client, session=session)
        analytics.identify(uuid.uuid4())
        analytics.track(uuid.uuid4(), AnalyticsEvent.SESSION_COMPLETED)
        await session.commit()

    assert len(client.identified) == 1
    assert len(client.tracked) == 1


async def test_alias_then_commit_delivers_to_client() -> None:
    """A buffered alias message reaches the client once the session commits."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    client = _RecordingAnalyticsClient()
    user_id = uuid.uuid4()
    previous_id = uuid.uuid4()
    async with pg_session() as session:
        analytics = ServerAnalytics(client=client, session=session)
        analytics.alias(user_id, previous_id)
        await session.commit()

    assert client.aliased == [(user_id, previous_id)]


async def test_alias_then_rollback_delivers_nothing() -> None:
    """A buffered alias message is discarded when the session rolls back."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    client = _RecordingAnalyticsClient()
    async with pg_session() as session:
        analytics = ServerAnalytics(client=client, session=session)
        analytics.alias(uuid.uuid4(), uuid.uuid4())
        await session.rollback()

    assert client.aliased == []


async def test_track_merges_the_current_actor() -> None:
    """The acting account lands in tracked properties but not identify traits."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    client = _RecordingAnalyticsClient()
    account = Account(name="runner", is_service_account=True, external_id=uuid.uuid4())
    token = current_actor.set(account)
    try:
        async with pg_session() as session:
            analytics = ServerAnalytics(client=client, session=session)
            analytics.track(account.id, AnalyticsEvent.SESSION_COMPLETED)
            analytics.identify(account.id)
            await session.commit()
    finally:
        current_actor.reset(token)

    _, _, properties = client.tracked[0]
    assert properties["service_account"] is True
    assert properties["control_plane_user_id"] == account.external_id
    _, traits = client.identified[0]
    assert "service_account" not in traits
    assert "control_plane_user_id" not in traits


async def test_track_omits_control_plane_user_id_without_external_id() -> None:
    """An actor without an external id contributes only the service flag."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    client = _RecordingAnalyticsClient()
    account = Account(name="ann")
    token = current_actor.set(account)
    try:
        async with pg_session() as session:
            analytics = ServerAnalytics(client=client, session=session)
            analytics.track(account.id, AnalyticsEvent.SESSION_COMPLETED)
            await session.commit()
    finally:
        current_actor.reset(token)

    _, _, properties = client.tracked[0]
    assert properties["service_account"] is False
    assert "control_plane_user_id" not in properties
