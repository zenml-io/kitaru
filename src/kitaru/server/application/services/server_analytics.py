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

import platform
import uuid
from contextvars import ContextVar
from dataclasses import dataclass, field
from importlib.metadata import version
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from kitaru.analytics.client import AnalyticsClient
from kitaru.analytics.environment import get_environment
from kitaru.analytics.events import AnalyticsEvent
from kitaru.analytics.source import current_attribution
from kitaru.api_models.v1.info import AuthScheme
from kitaru.server.domain.account import Account

_BUFFER_KEY = "kitaru_analytics_buffer"

current_actor: ContextVar[Account | None] = ContextVar(
    "kitaru_analytics_actor", default=None
)


def build_analytics_context(
    server_id: uuid.UUID,
    auth_scheme: AuthScheme,
    organization_id: uuid.UUID | None = None,
    organization_name: str = "",
    workspace_name: str = "",
) -> dict[str, Any]:
    """Build the analytics context for this process.

    Args:
        server_id: Persisted server id.
        auth_scheme: Active authentication scheme.
        organization_id: Control plane organization id.
        organization_name: Control plane organization name.
        workspace_name: Control plane workspace name.

    Returns:
        Context values merged into every message.
    """
    context: dict[str, Any] = {
        "server_id": server_id,
        "server_version": version("kitaru"),
        "auth_scheme": auth_scheme.value,
        "environment": get_environment(),
        "os": platform.system().lower(),
        "python_version": platform.python_version(),
    }
    # Under the control plane scheme the enrolled server id is the workspace
    # id assigned by the control plane.
    if auth_scheme is AuthScheme.CONTROL_PLANE:
        context["workspace_id"] = server_id
        if organization_id:
            context["organization_id"] = organization_id
        if organization_name:
            context["organization_name"] = organization_name
        if workspace_name:
            context["workspace_name"] = workspace_name
    return context


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

    def __init__(self, client: AnalyticsClient, session: AsyncSession) -> None:
        """Initialize the tracker.

        Args:
            client: Analytics client the buffered messages are delivered
                through.
            session: Request-scoped database session the messages are
                buffered on.
        """
        self._client = client
        self._session = session

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
            properties: Event properties, merged with the acting account.
        """
        if not self._client.enabled:
            return
        properties = dict(properties or {})
        actor = current_actor.get()
        if actor is not None:
            properties["service_account"] = actor.is_service_account
            if actor.external_id is not None:
                properties["control_plane_user_id"] = actor.external_id
        attribution = current_attribution.get()
        if attribution.version is not None:
            properties["client_version"] = attribution.version
        if attribution.skill is not None:
            properties["skill"] = attribution.skill
        self._get_buffer().messages.append(
            _BufferedTrack(user_id=user_id, event=event, properties=properties)
        )

    def identify(
        self, user_id: uuid.UUID, traits: dict[str, Any] | None = None
    ) -> None:
        """Buffer an identify message for delivery once the session commits.

        Args:
            user_id: User id.
            traits: User traits.
        """
        if not self._client.enabled:
            return
        self._get_buffer().messages.append(
            _BufferedIdentify(user_id=user_id, traits=traits or {})
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
