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
"""Request-scoped database, settings, server-state, and analytics providers."""

import uuid
from collections.abc import AsyncGenerator
from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from kitaru.analytics.client import AnalyticsClient
from kitaru.server.adapters.rest.request_state import (
    attach_request_session,
    request_uses_read_engine,
)
from kitaru.server.api.config import APISettings
from kitaru.server.application.interfaces.ephemeral_workers import EphemeralWorkers
from kitaru.server.application.services.server_analytics import (
    ServerAnalytics,
)
from kitaru.server.database.service import DatabaseService


async def get_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    """Provide a request-scoped database session.

    The session is attached to the request for ``KitaruAPIRoute`` to commit
    before the response is returned. Any exception skips the commit and
    pending writes roll back when the session closes. Routes marked with
    ``read_only`` bind the session to the read-replica engine.

    Args:
        request: Incoming request.

    Yields:
        Session bound to the application database engine.
    """
    database: DatabaseService = request.app.state.database
    read_only = request_uses_read_engine(request)
    async for session in database.get_async_session(read_only=read_only):
        attach_request_session(request, session)
        yield session


async def get_auth_session(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> AsyncGenerator[AsyncSession, None]:
    """Provide the database session the auth path runs on.

    On most routes this is the shared request session. A ``read_only``
    route binds the request session to the read-replica engine, so the
    auth path instead gets its own session on the writer engine here.
    ``_resolve_auth_context`` commits the session once authentication
    resolves, and closes it when it is the writer-bound one.

    Args:
        request: Incoming request.
        session: Request-scoped database session.

    Yields:
        The request session, or a writer-bound session scoped to the auth
        path on a read-only route.
    """
    if not request_uses_read_engine(request):
        yield session
        return
    database: DatabaseService = request.app.state.database
    async for auth_session in database.get_async_session():
        yield auth_session


def get_engine(request: Request) -> AsyncEngine:
    """Provide the application database engine.

    Args:
        request: Incoming request.

    Returns:
        Engine attached to the application state.
    """
    database: DatabaseService = request.app.state.database
    return database.engine


def get_app_settings(request: Request) -> APISettings:
    """Return API settings attached to the application state.

    Args:
        request: Incoming request.

    Returns:
        API settings for this process.
    """
    settings: APISettings = request.app.state.settings
    return settings


def get_server_id_state(request: Request) -> uuid.UUID | None:
    """Return the persisted server id attached to the application state.

    Args:
        request: Incoming request.

    Returns:
        Server id for this process, or None before startup resolved it.
    """
    server_id: uuid.UUID | None = request.app.state.server_id
    return server_id


def get_ui_version_state(request: Request) -> str | None:
    """Return the served UI version attached to the application state.

    Args:
        request: Incoming request.

    Returns:
        UI version for this process, or None when no UI is served.
    """
    ui_version: str | None = request.app.state.ui_version
    return ui_version


def get_analytics_client(request: Request) -> AnalyticsClient:
    """Return the analytics client attached to the application state.

    Args:
        request: Incoming request.

    Returns:
        Analytics client for this process.
    """
    analytics: AnalyticsClient = request.app.state.analytics
    return analytics


def get_server_analytics(
    session: Annotated[AsyncSession, Depends(get_session)],
    client: Annotated[AnalyticsClient, Depends(get_analytics_client)],
) -> ServerAnalytics:
    """Return a server analytics tracker for the current request.

    Args:
        session: Request-scoped database session.
        client: Analytics client for this process.

    Returns:
        Tracker buffering track calls until the request session commits.
    """
    return ServerAnalytics(client=client, session=session)


def get_ephemeral_workers(request: Request) -> EphemeralWorkers | None:
    """Return the ephemeral worker backend attached to the application state.

    Args:
        request: Incoming request.

    Returns:
        Ephemeral worker backend for this process, or None when none is configured.
    """
    ephemeral_workers: EphemeralWorkers | None = request.app.state.ephemeral_workers
    return ephemeral_workers
