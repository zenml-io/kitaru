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
"""FastAPI dependency providers."""

from collections.abc import AsyncGenerator
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from kitaru.server.adapters.auth.auth_service import (
    AuthenticationError,
    AuthService,
    ScopeError,
)
from kitaru.server.api.config import APISettings, AuthScheme
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.database.service import DatabaseService

CSRF_HEADER = "X-CSRF-Token"
BearerCredential = tuple[str, str | None]


async def get_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    """Provide a request-scoped database session.

    The session commits after the route handler succeeds. Any exception skips
    the commit and pending writes roll back when the session closes.

    Args:
        request: Incoming request.

    Yields:
        Session bound to the application database engine.
    """
    database: DatabaseService = request.app.state.database
    async for session in database.get_async_session():
        yield session
        await session.commit()


def get_app_settings(request: Request) -> APISettings:
    """Return API settings attached to the application state.

    Args:
        request: Incoming request.

    Returns:
        API settings for this process.
    """
    settings: APISettings = request.app.state.settings
    return settings


def get_auth_service(request: Request) -> AuthService:
    """Return an authentication service for the current request.

    Args:
        request: Incoming request.

    Raises:
        RuntimeError: The control plane client is not configured.

    Returns:
        Authentication service bound to shared application state.
    """
    control_plane = request.app.state.control_plane
    if control_plane is None:
        raise RuntimeError("Control plane client is not configured.")
    return AuthService(get_app_settings(request), control_plane)


def get_optional_bearer_credential(
    request: Request,
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> BearerCredential | None:
    """Read an optional bearer credential from the request.

    Args:
        request: Incoming request.
        settings: API settings for this process.

    Returns:
        Credential string without the ``Bearer`` prefix and optional CSRF
        token, or ``None``.
    """
    header = request.headers.get("Authorization")
    csrf_token = request.headers.get(CSRF_HEADER)
    if header:
        scheme, _, credential = header.partition(" ")
        if scheme.lower() == "bearer" and credential:
            return credential, csrf_token
    if settings.AUTH_COOKIE_NAME:
        cookie = request.cookies.get(settings.AUTH_COOKIE_NAME)
        if cookie:
            return cookie, csrf_token
    return None


async def authorize(
    request: Request,
    settings: Annotated[APISettings, Depends(get_app_settings)],
    credential: Annotated[
        BearerCredential | None, Depends(get_optional_bearer_credential)
    ],
) -> AuthContext:
    """Authorize a request and return its auth context.

    With the ``none`` auth scheme every request is accepted and receives an
    anonymous context. Other schemes require a bearer credential.

    Args:
        request: Incoming request.
        settings: Service settings governing auth behavior.
        credential: Bearer token plus optional CSRF token.

    Raises:
        HTTPException: The credential is missing, invalid, or out of scope.

    Returns:
        Resolved scope and principal for use-case calls.
    """
    if settings.AUTH_SCHEME is AuthScheme.NONE:
        return AuthContext()

    if credential is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer credential.",
        )

    try:
        return await get_auth_service(request).resolve(
            credential=credential[0],
            csrf_token=credential[1],
        )
    except AuthenticationError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
        ) from exc
    except ScopeError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(exc),
        ) from exc
