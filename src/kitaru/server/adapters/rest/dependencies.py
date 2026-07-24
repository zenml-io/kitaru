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
)
from kitaru.server.adapters.auth.passwords import BcryptPasswordHasher
from kitaru.server.adapters.db.encryption import AesGcmCipher
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import (
    SQLAgentRepository,
)
from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.api_key_repository import (
    SQLApiKeyRepository,
)
from kitaru.server.adapters.db.repositories.cohort_repository import (
    SQLCohortRepository,
)
from kitaru.server.adapters.db.repositories.experiment_repository import (
    SQLExperimentRepository,
)
from kitaru.server.adapters.db.repositories.experiment_run_repository import (
    SQLExperimentRunRepository,
)
from kitaru.server.adapters.db.repositories.replay_config_repository import (
    SQLReplayConfigRepository,
)
from kitaru.server.adapters.db.repositories.replay_repository import (
    SQLReplayRepository,
)
from kitaru.server.adapters.db.repositories.secret_repository import (
    SQLSecretRepository,
)
from kitaru.server.adapters.db.repositories.session_node_repository import (
    SQLSessionNodeRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.adapters.db.repositories.tag_repository import (
    SQLTagRepository,
)
from kitaru.server.adapters.db.repositories.worker_repository import (
    SQLWorkerRepository,
)
from kitaru.server.api.config import APISettings, AuthScheme
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.account_service import AccountService
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.agent_version_service import (
    AgentVersionService,
)
from kitaru.server.application.services.api_key_service import ApiKeyService
from kitaru.server.application.services.cohort_service import CohortService
from kitaru.server.application.services.experiment_run_service import (
    ExperimentRunService,
)
from kitaru.server.application.services.experiment_service import (
    ExperimentService,
)
from kitaru.server.application.services.replay_service import ReplayService
from kitaru.server.application.services.secret_service import SecretService
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.application.services.tag_service import TagService
from kitaru.server.application.services.worker_service import WorkerService
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.account import AccountNotFound

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


def get_account_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> AccountService:
    """Return an account service for the current request.

    Args:
        session: Request-scoped database session.

    Returns:
        Account service bound to the SQL repository.
    """
    return AccountService(
        repository=SQLAccountRepository(session),
        password_hasher=BcryptPasswordHasher(),
    )


def get_api_key_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> ApiKeyService:
    """Return an API key service for the current request.

    Args:
        session: Request-scoped database session.

    Returns:
        API key service bound to the SQL repository.
    """
    return ApiKeyService(repository=SQLApiKeyRepository(session))


def get_agent_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> AgentService:
    """Return an agent service for the current request.

    Args:
        session: Request-scoped database session.

    Returns:
        Agent service bound to the SQL repository.
    """
    return AgentService(repository=SQLAgentRepository(session))


def get_agent_version_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> AgentVersionService:
    """Return an agent version service for the current request.

    Args:
        session: Request-scoped database session.
        settings: API settings for this process.

    Returns:
        Agent version service bound to the SQL repositories.
    """
    return AgentVersionService(
        repository=SQLAgentVersionRepository(session),
        agent_repository=SQLAgentRepository(session),
        secret_repository=SQLSecretRepository(
            session, AesGcmCipher(settings.SECRET_ENCRYPTION_KEY)
        ),
        replay_repository=SQLReplayRepository(session),
    )


def get_cohort_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> CohortService:
    """Return a cohort service for the current request.

    Args:
        session: Request-scoped database session.

    Returns:
        Cohort service bound to the SQL repositories.
    """
    return CohortService(
        repository=SQLCohortRepository(session),
        session_repository=SQLSessionRepository(session),
        agent_repository=SQLAgentRepository(session),
    )


def get_experiment_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> ExperimentService:
    """Return an experiment service for the current request.

    Args:
        session: Request-scoped database session.

    Returns:
        Experiment service bound to the SQL repositories.
    """
    return ExperimentService(
        repository=SQLExperimentRepository(session),
        run_repository=SQLExperimentRunRepository(session),
        cohort_repository=SQLCohortRepository(session),
        agent_version_repository=SQLAgentVersionRepository(session),
        replay_config_repository=SQLReplayConfigRepository(session),
    )


def get_experiment_run_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> ExperimentRunService:
    """Return an experiment run service for the current request.

    Args:
        session: Request-scoped database session.
        settings: API settings for this process.

    Returns:
        Experiment run service bound to the SQL repositories.
    """
    return ExperimentRunService(
        repository=SQLExperimentRunRepository(session),
        replay_repository=SQLReplayRepository(session),
        replay_config_repository=SQLReplayConfigRepository(session),
        experiment_repository=SQLExperimentRepository(session),
        session_repository=SQLSessionRepository(session),
        heartbeat_timeout_seconds=settings.REPLAY_HEARTBEAT_TIMEOUT_SECONDS,
        max_attempts=settings.REPLAY_MAX_ATTEMPTS,
    )


def get_replay_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> ReplayService:
    """Return a replay service for the current request.

    Args:
        session: Request-scoped database session.
        settings: API settings for this process.

    Returns:
        Replay service bound to the SQL repositories.
    """
    return ReplayService(
        repository=SQLReplayRepository(session),
        replay_config_repository=SQLReplayConfigRepository(session),
        session_repository=SQLSessionRepository(session),
        agent_version_repository=SQLAgentVersionRepository(session),
        session_node_repository=SQLSessionNodeRepository(session),
        experiment_run_repository=SQLExperimentRunRepository(session),
        experiment_repository=SQLExperimentRepository(session),
        cohort_repository=SQLCohortRepository(session),
        secret_repository=SQLSecretRepository(
            session, AesGcmCipher(settings.SECRET_ENCRYPTION_KEY)
        ),
        heartbeat_timeout_seconds=settings.REPLAY_HEARTBEAT_TIMEOUT_SECONDS,
        max_attempts=settings.REPLAY_MAX_ATTEMPTS,
    )


def get_secret_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> SecretService:
    """Return a secret service for the current request.

    Args:
        session: Request-scoped database session.
        settings: API settings for this process.

    Returns:
        Secret service bound to the SQL repository.
    """
    return SecretService(
        repository=SQLSecretRepository(
            session, AesGcmCipher(settings.SECRET_ENCRYPTION_KEY)
        )
    )


def get_session_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> SessionService:
    """Return a session service for the current request.

    Args:
        session: Request-scoped database session.

    Returns:
        Session service bound to the SQL repositories.
    """
    return SessionService(
        repository=SQLSessionRepository(session),
        agent_repository=SQLAgentRepository(session),
        agent_version_repository=SQLAgentVersionRepository(session),
        node_repository=SQLSessionNodeRepository(session),
        replay_repository=SQLReplayRepository(session),
    )


def get_session_node_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> SessionNodeService:
    """Return a session node service for the current request.

    Args:
        session: Request-scoped database session.

    Returns:
        Session node service bound to the SQL repositories.
    """
    return SessionNodeService(
        repository=SQLSessionNodeRepository(session),
        session_repository=SQLSessionRepository(session),
    )


def get_tag_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> TagService:
    """Return a tag service for the current request.

    Args:
        session: Request-scoped database session.

    Returns:
        Tag service bound to the SQL repository.
    """
    return TagService(repository=SQLTagRepository(session))


def get_worker_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> WorkerService:
    """Return a worker service for the current request.

    Args:
        session: Request-scoped database session.
        settings: API settings for this process.

    Returns:
        Worker service bound to the SQL repository.
    """
    return WorkerService(
        repository=SQLWorkerRepository(session),
        liveness_timeout_seconds=settings.WORKER_LIVENESS_TIMEOUT_SECONDS,
    )


def get_auth_service(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> AuthService:
    """Return an authentication service for the current request.

    Args:
        request: Incoming request.
        session: Request-scoped database session.

    Returns:
        Authentication service bound to the SQL repositories.
    """
    return AuthService(
        settings=get_app_settings(request),
        account_repository=SQLAccountRepository(session),
        api_key_repository=SQLApiKeyRepository(session),
        password_hasher=BcryptPasswordHasher(),
    )


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
    settings: Annotated[APISettings, Depends(get_app_settings)],
    credential: Annotated[
        BearerCredential | None, Depends(get_optional_bearer_credential)
    ],
    auth_service: Annotated[AuthService, Depends(get_auth_service)],
) -> AuthContext:
    """Authorize a request and return its auth context.

    With the ``none`` auth scheme every request is accepted and runs as the
    default account. Other schemes require a bearer credential.

    Args:
        settings: Service settings governing auth behavior.
        credential: Bearer token plus optional CSRF token.
        auth_service: Authentication service for the current request.

    Raises:
        HTTPException: The credential is missing or invalid.
        RuntimeError: The default account was not initialized at startup.

    Returns:
        Resolved scope and principal for use-case calls.
    """
    if settings.AUTH_SCHEME is AuthScheme.NONE:
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
            credential=credential[0],
            csrf_token=credential[1],
        )
    except AuthenticationError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
        ) from exc
