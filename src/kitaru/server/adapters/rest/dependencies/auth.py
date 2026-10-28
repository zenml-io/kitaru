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
"""Credential, authentication, and authorization providers."""

from collections.abc import AsyncGenerator
from typing import Annotated, NamedTuple

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from kitaru.api_models.v1.info import AuthScheme
from kitaru.server.adapters.auth.auth_service import (
    AuthenticationError,
    AuthService,
)
from kitaru.server.adapters.auth.control_plane import (
    ControlPlaneAuthenticator,
    ControlPlaneClient,
)
from kitaru.server.adapters.auth.passwords import BcryptPasswordHasher
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.api_key_repository import (
    SQLApiKeyRepository,
)
from kitaru.server.adapters.db.repositories.device_repository import (
    SQLDeviceRepository,
)
from kitaru.server.adapters.rest.dependencies.request import (
    get_app_settings,
    get_auth_session,
    get_engine,
    get_server_analytics,
    get_session,
)
from kitaru.server.adapters.rest.request_state import request_uses_read_engine
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import (
    AuthContext,
    TaskAuthContext,
    TaskPrincipal,
    WorkerAuthContext,
    WorkerPrincipal,
)
from kitaru.server.application.models.device import DevicePolicy
from kitaru.server.application.services.device_service import DeviceService
from kitaru.server.application.services.server_analytics import (
    ServerAnalytics,
    current_actor,
)
from kitaru.server.domain.account import AccountNotFound

CSRF_HEADER = "X-CSRF-Token"


class RequestCredential(NamedTuple):
    """Credential read off an incoming request."""

    token: str
    csrf_token: str | None
    from_cookie: bool


def _build_device_service(
    session: AsyncSession, engine: AsyncEngine, settings: APISettings
) -> DeviceService:
    """Build a device service bound to the given session.

    Args:
        session: Database session backing the device repository.
        engine: Application database engine.
        settings: API settings for this process.

    Returns:
        Device service bound to the SQL repository.
    """
    return DeviceService(
        repository=SQLDeviceRepository(session, engine),
        policy=DevicePolicy(
            auth_timeout_seconds=settings.DEVICE_AUTH_TIMEOUT_SECONDS,
            polling_interval_seconds=settings.DEVICE_AUTH_POLLING_INTERVAL_SECONDS,
            max_failed_attempts=settings.MAX_FAILED_DEVICE_AUTH_ATTEMPTS,
            expiration_minutes=settings.DEVICE_EXPIRATION_MINUTES,
            trusted_expiration_minutes=settings.TRUSTED_DEVICE_EXPIRATION_MINUTES,
        ),
    )


def get_device_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    engine: Annotated[AsyncEngine, Depends(get_engine)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> DeviceService:
    """Return a device service for the current request.

    Args:
        session: Request-scoped database session.
        engine: Application database engine.
        settings: API settings for this process.

    Returns:
        Device service bound to the SQL repository.
    """
    return _build_device_service(session, engine, settings)


def get_auth_service(
    request: Request,
    auth_session: Annotated[AsyncSession, Depends(get_auth_session)],
    engine: Annotated[AsyncEngine, Depends(get_engine)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> AuthService:
    """Return an authentication service for the current request.

    Repositories are bound to the auth session so a ``read_only`` route
    still writes authentication side effects, such as an API key's
    ``last_used`` timestamp, to the writer engine.

    Args:
        request: Incoming request.
        auth_session: Database session the auth path writes through.
        engine: Application database engine.
        analytics: Analytics tracker for the current request.

    Returns:
        Authentication service bound to the SQL repositories.
    """
    settings = get_app_settings(request)
    account_repository = SQLAccountRepository(auth_session)
    client: ControlPlaneClient | None = request.app.state.control_plane_client
    control_plane = None
    if client is not None:
        # Settings validation requires SERVER_ID under the control plane
        # scheme, the only scheme that constructs the client.
        assert settings.SERVER_ID is not None
        control_plane = ControlPlaneAuthenticator(
            client=client,
            account_repository=account_repository,
            server_id=settings.SERVER_ID,
            analytics=analytics,
        )
    return AuthService(
        settings=settings,
        account_repository=account_repository,
        api_key_repository=SQLApiKeyRepository(auth_session),
        password_hasher=BcryptPasswordHasher(),
        device_service=_build_device_service(auth_session, engine, settings),
        control_plane=control_plane,
    )


def get_bearer_credential(request: Request) -> str | None:
    """Read the bearer credential from the request authorization header.

    Args:
        request: Incoming request.

    Returns:
        Credential string without the ``Bearer`` prefix, or ``None``.
    """
    header = request.headers.get("Authorization")
    if not header:
        return None
    scheme, _, credential = header.partition(" ")
    if scheme.lower() != "bearer" or not credential:
        return None
    return credential


def get_optional_bearer_credential(
    request: Request,
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> RequestCredential | None:
    """Read an optional bearer credential from the request.

    Args:
        request: Incoming request.
        settings: API settings for this process.

    Returns:
        Credential without the ``Bearer`` prefix, the CSRF token, and where
        the credential came from, or ``None``.
    """
    credential = get_bearer_credential(request)
    csrf_token = request.headers.get(CSRF_HEADER)
    if credential is not None:
        return RequestCredential(credential, csrf_token, from_cookie=False)
    if settings.AUTH_COOKIE_NAME:
        cookie = request.cookies.get(settings.AUTH_COOKIE_NAME)
        if cookie:
            return RequestCredential(cookie, csrf_token, from_cookie=True)
    return None


def require_local_account_management(
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> None:
    """Reject account writes unless this server owns its accounts.

    Args:
        settings: Service settings governing auth behavior.

    Raises:
        HTTPException: The local auth scheme is not active.
    """
    if settings.AUTH_SCHEME is not AuthScheme.LOCAL:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This server does not manage its own accounts.",
        )


async def _resolve_auth_context(
    request: Request,
    settings: Annotated[APISettings, Depends(get_app_settings)],
    credential: Annotated[
        RequestCredential | None, Depends(get_optional_bearer_credential)
    ],
    auth_service: Annotated[AuthService, Depends(get_auth_service)],
    auth_session: Annotated[AsyncSession, Depends(get_auth_session)],
) -> AsyncGenerator[AuthContext, None]:
    """Resolve a request into its auth context, gating on nothing but validity.

    The resolved account is published as the analytics actor for the rest of
    the request. Auth writes commit once authentication succeeds.

    Args:
        request: Incoming request.
        settings: Service settings governing auth behavior.
        credential: Bearer token plus optional CSRF token.
        auth_service: Authentication service for the current request.
        auth_session: Database session the auth path runs on.

    Raises:
        HTTPException: The credential is missing or invalid.
        RuntimeError: The default account was not initialized at startup.

    Yields:
        Resolved account and principal for use-case calls.
    """
    context = await _authenticate(settings, credential, auth_service)
    # The credential was used no matter how the rest of the request ends,
    # so auth writes such as last_used commit here instead of with the
    # handler's work.
    await auth_session.commit()
    if request_uses_read_engine(request):
        # Nothing after authentication uses the writer-bound auth session.
        # Close it here so its connection frees before the handler runs.
        await auth_session.close()
    token = current_actor.set(context.account)
    try:
        yield context
    finally:
        current_actor.reset(token)


async def _authenticate(
    settings: APISettings,
    credential: RequestCredential | None,
    auth_service: AuthService,
) -> AuthContext:
    """Authenticate a request credential into its auth context.

    With the ``none`` auth scheme a request without a worker or task
    credential runs as the default account. Other schemes require a bearer
    credential.

    Args:
        settings: Service settings governing auth behavior.
        credential: Bearer token plus optional CSRF token.
        auth_service: Authentication service for the current request.

    Raises:
        HTTPException: The credential is missing or invalid.
        RuntimeError: The default account was not initialized at startup.

    Returns:
        Resolved account and principal for use-case calls.
    """
    if settings.AUTH_SCHEME is AuthScheme.NONE:
        if credential is not None:
            principal_context = await auth_service.try_resolve_worker_or_task(
                credential.token
            )
            if principal_context is not None:
                return principal_context
        try:
            return await auth_service.resolve_default_account()
        except AccountNotFound as exc:
            raise RuntimeError("Default account is not initialized.") from exc

    if credential is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer credential.",
        )

    try:
        return await auth_service.resolve(
            credential=credential.token,
            csrf_token=credential.csrf_token,
            from_cookie=credential.from_cookie,
        )
    except AuthenticationError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
        ) from exc


def _reject_disallowed_principal(
    context: AuthContext, allow_worker: bool, allow_task: bool
) -> AuthContext:
    """Reject a resolved context whose principal kind this route does not accept.

    Args:
        context: Resolved auth context.
        allow_worker: Whether a worker principal may pass.
        allow_task: Whether a task principal may pass.

    Raises:
        HTTPException: The principal is a worker or task principal the route
            does not accept.

    Returns:
        The context, unchanged.
    """
    if isinstance(context.principal, WorkerPrincipal) and not allow_worker:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Worker credentials are not accepted on this route.",
        )
    if isinstance(context.principal, TaskPrincipal) and not allow_task:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Task credentials are not accepted on this route.",
        )
    return context


async def authorize(
    context: Annotated[AuthContext, Depends(_resolve_auth_context)],
) -> AuthContext:
    """Authorize a request, accepting only an account principal.

    Args:
        context: Resolved auth context.

    Raises:
        HTTPException: The principal is a worker or task principal.

    Returns:
        Resolved account context for use-case calls.
    """
    return _reject_disallowed_principal(context, allow_worker=False, allow_task=False)


async def authorize_with_worker(
    context: Annotated[AuthContext, Depends(_resolve_auth_context)],
) -> AuthContext:
    """Authorize a request, accepting an account or worker principal.

    Args:
        context: Resolved auth context.

    Raises:
        HTTPException: The principal is a task principal.

    Returns:
        Resolved account or worker context for use-case calls.
    """
    return _reject_disallowed_principal(context, allow_worker=True, allow_task=False)


async def authorize_with_task(
    context: Annotated[AuthContext, Depends(_resolve_auth_context)],
) -> AuthContext:
    """Authorize a request, accepting an account or task principal.

    Args:
        context: Resolved auth context.

    Raises:
        HTTPException: The principal is a worker principal.

    Returns:
        Resolved account or task context for use-case calls.
    """
    return _reject_disallowed_principal(context, allow_worker=False, allow_task=True)


async def authorize_with_worker_or_task(
    context: Annotated[AuthContext, Depends(_resolve_auth_context)],
) -> AuthContext:
    """Authorize a request, accepting an account, worker, or task principal.

    Args:
        context: Resolved auth context.

    Returns:
        Resolved context for use-case calls.
    """
    return _reject_disallowed_principal(context, allow_worker=True, allow_task=True)


async def authorize_worker_only(
    context: Annotated[AuthContext, Depends(_resolve_auth_context)],
) -> WorkerAuthContext:
    """Authorize a request, accepting only a worker principal.

    Args:
        context: Resolved auth context.

    Raises:
        HTTPException: The caller holds no worker credential.

    Returns:
        Resolved worker context for use-case calls.
    """
    if not isinstance(context, WorkerAuthContext):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="A worker credential is required on this route.",
        )
    return context


async def authorize_task_only(
    context: Annotated[AuthContext, Depends(_resolve_auth_context)],
) -> TaskAuthContext:
    """Authorize a request, accepting only a task principal.

    Args:
        context: Resolved auth context.

    Raises:
        HTTPException: The caller holds no task credential.

    Returns:
        Resolved task context for use-case calls.
    """
    if not isinstance(context, TaskAuthContext):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="A task credential is required on this route.",
        )
    return context
