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
from typing import Annotated, NamedTuple

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from kitaru.analytics.client import AnalyticsClient
from kitaru.api_models.v1.info import AuthScheme
from kitaru.server.adapters.auth.auth_service import (
    AuthenticationError,
    AuthenticationServiceUnavailableError,
    AuthService,
)
from kitaru.server.adapters.auth.cloud_auth_service import CloudAuthService
from kitaru.server.adapters.auth.control_plane import (
    ControlPlaneAuthenticator,
    ControlPlaneClient,
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
from kitaru.server.adapters.db.repositories.blob_repository import SQLBlobRepository
from kitaru.server.adapters.db.repositories.cohort_repository import (
    SQLCohortRepository,
)
from kitaru.server.adapters.db.repositories.cohort_version_repository import (
    SQLCohortVersionRepository,
)
from kitaru.server.adapters.db.repositories.device_repository import (
    SQLDeviceRepository,
)
from kitaru.server.adapters.db.repositories.evaluation_repository import (
    SQLEvaluationRepository,
)
from kitaru.server.adapters.db.repositories.experiment_repository import (
    SQLExperimentRepository,
)
from kitaru.server.adapters.db.repositories.experiment_run_repository import (
    SQLExperimentRunRepository,
)
from kitaru.server.adapters.db.repositories.job_repository import SQLJobRepository
from kitaru.server.adapters.db.repositories.plugin_repository import (
    SQLPluginRepository,
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
from kitaru.server.adapters.db.repositories.tag_repository import SQLTagRepository
from kitaru.server.adapters.db.repositories.task_repository import SQLTaskRepository
from kitaru.server.adapters.db.repositories.worker_repository import (
    SQLWorkerRepository,
)
from kitaru.server.adapters.rest.commit_route import attach_request_session
from kitaru.server.api.composition import build_event_dispatcher
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.device import DevicePolicy
from kitaru.server.application.models.task import TaskPolicy
from kitaru.server.application.services.account_service import AccountService
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.agent_version_service import (
    AgentVersionService,
)
from kitaru.server.application.services.api_key_service import ApiKeyService
from kitaru.server.application.services.blob_service import BlobService
from kitaru.server.application.services.cohort_service import CohortService
from kitaru.server.application.services.cohort_version_service import (
    CohortVersionService,
)
from kitaru.server.application.services.device_service import DeviceService
from kitaru.server.application.services.evaluation_service import EvaluationService
from kitaru.server.application.services.experiment_run_service import (
    ExperimentRunService,
)
from kitaru.server.application.services.experiment_service import ExperimentService
from kitaru.server.application.services.job_service import JobService
from kitaru.server.application.services.plugin_service import PluginService
from kitaru.server.application.services.replay_service import ReplayService
from kitaru.server.application.services.secret_service import SecretService
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.application.services.tag_service import TagService
from kitaru.server.application.services.task_service import TaskService
from kitaru.server.application.services.task_transitions import TaskTransitions
from kitaru.server.application.services.worker_service import WorkerService
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.account import AccountNotFound
from kitaru.server.domain.plugin import PluginKind

CSRF_HEADER = "X-CSRF-Token"


class RequestCredential(NamedTuple):
    """Credential read off an incoming request."""

    token: str
    csrf_token: str | None
    from_cookie: bool


async def get_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    """Provide a request-scoped database session.

    The session is attached to the request for ``CommitRoute`` to commit
    before the response is returned. Any exception skips the commit and
    pending writes roll back when the session closes.

    Args:
        request: Incoming request.

    Yields:
        Session bound to the application database engine.
    """
    database: DatabaseService = request.app.state.database
    async for session in database.get_async_session():
        attach_request_session(request, session)
        yield session


def get_app_settings(request: Request) -> APISettings:
    """Return API settings attached to the application state.

    Args:
        request: Incoming request.

    Returns:
        API settings for this process.
    """
    settings: APISettings = request.app.state.settings
    return settings


def get_analytics_client(request: Request) -> AnalyticsClient:
    """Return the analytics client attached to the application state.

    Args:
        request: Incoming request.

    Returns:
        Analytics client for this process.
    """
    analytics: AnalyticsClient = request.app.state.analytics
    return analytics


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
) -> AgentVersionService:
    """Return an agent version service for the current request.

    Args:
        session: Request-scoped database session.

    Returns:
        Agent version service bound to the SQL repositories.
    """
    return AgentVersionService(
        repository=SQLAgentVersionRepository(session),
        task_repository=SQLTaskRepository(session),
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


def get_blob_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> BlobService:
    """Return a blob service for the current request.

    Args:
        session: Request-scoped database session.
        settings: API settings for this process.

    Returns:
        Blob service bound to the SQL repository.
    """
    return BlobService(
        repository=SQLBlobRepository(session),
        max_size_bytes=settings.MAX_BLOB_SIZE_BYTES,
    )


def get_evaluator_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PluginService:
    """Return a plugin service bound to the evaluator kind.

    Args:
        session: Request-scoped database session.

    Returns:
        Plugin service bound to the SQL repositories.
    """
    return PluginService(
        kind=PluginKind.EVALUATOR,
        repository=SQLPluginRepository(session),
        blob_repository=SQLBlobRepository(session),
    )


def get_importer_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PluginService:
    """Return a plugin service bound to the importer kind.

    Args:
        session: Request-scoped database session.

    Returns:
        Plugin service bound to the SQL repositories.
    """
    return PluginService(
        kind=PluginKind.IMPORTER,
        repository=SQLPluginRepository(session),
        blob_repository=SQLBlobRepository(session),
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
        task_repository=SQLTaskRepository(session),
    )


def get_task_policy(settings: APISettings) -> TaskPolicy:
    """Build the task execution policy from the process settings.

    Args:
        settings: API settings for this process.

    Returns:
        Task execution policy.
    """
    return TaskPolicy(
        heartbeat_timeout_seconds=settings.TASK_HEARTBEAT_TIMEOUT_SECONDS,
        retry_limit=settings.TASK_RETRY_LIMIT,
        sweep_batch_limit=settings.TASK_SWEEP_BATCH_LIMIT,
        evaluator_timeout_seconds=settings.EVALUATOR_TASK_TIMEOUT_SECONDS,
        importer_timeout_seconds=settings.IMPORTER_TASK_TIMEOUT_SECONDS,
        max_result_bytes=settings.MAX_TASK_RESULT_BYTES,
        evaluation_pair_limit=settings.EVALUATION_PAIR_LIMIT,
    )


def _build_task_transitions(session: AsyncSession) -> TaskTransitions:
    """Build the request-scoped task transition dispatch.

    Args:
        session: Request-scoped database session.

    Returns:
        Transition dispatch publishing on the request's event dispatcher.
    """
    return TaskTransitions(
        task_repository=SQLTaskRepository(session),
        job_repository=SQLJobRepository(session),
        dispatcher=build_event_dispatcher(session),
    )


def get_job_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> JobService:
    """Return a job service for the current request.

    Args:
        session: Request-scoped database session.
        settings: API settings for this process.

    Returns:
        Job service bound to the SQL repositories.
    """
    return JobService(
        repository=SQLJobRepository(session),
        task_repository=SQLTaskRepository(session),
        session_repository=SQLSessionRepository(session),
        agent_repository=SQLAgentRepository(session),
        agent_version_repository=SQLAgentVersionRepository(session),
        plugin_repository=SQLPluginRepository(session),
        blob_repository=SQLBlobRepository(session),
        transitions=_build_task_transitions(session),
        policy=get_task_policy(settings),
    )


def get_task_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> TaskService:
    """Return a task service for the current request.

    Args:
        session: Request-scoped database session.
        settings: API settings for this process.

    Returns:
        Task service bound to the SQL repositories.
    """
    return TaskService(
        repository=SQLTaskRepository(session),
        worker_repository=SQLWorkerRepository(session),
        session_repository=SQLSessionRepository(session),
        agent_version_repository=SQLAgentVersionRepository(session),
        plugin_repository=SQLPluginRepository(session),
        blob_repository=SQLBlobRepository(session),
        secret_repository=SQLSecretRepository(
            session, AesGcmCipher(settings.SECRET_ENCRYPTION_KEY)
        ),
        transitions=_build_task_transitions(session),
        policy=get_task_policy(settings),
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
        plugin_repository=SQLPluginRepository(session),
        experiment_run_repository=SQLExperimentRunRepository(session),
        cohort_version_repository=SQLCohortVersionRepository(session),
        session_repository=SQLSessionRepository(session),
        agent_version_repository=SQLAgentVersionRepository(session),
        replay_repository=SQLReplayRepository(session),
        job_repository=SQLJobRepository(session),
        task_repository=SQLTaskRepository(session),
    )


def get_replay_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> ReplayService:
    """Return a replay service for the current request.

    Args:
        session: Request-scoped database session.

    Returns:
        Replay service bound to the SQL repositories.
    """
    return ReplayService(
        repository=SQLReplayRepository(session),
        experiment_repository=SQLExperimentRepository(session),
        experiment_run_repository=SQLExperimentRunRepository(session),
        job_repository=SQLJobRepository(session),
        task_repository=SQLTaskRepository(session),
        session_repository=SQLSessionRepository(session),
        session_node_repository=SQLSessionNodeRepository(session),
        agent_version_repository=SQLAgentVersionRepository(session),
        plugin_repository=SQLPluginRepository(session),
    )


def get_experiment_run_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> ExperimentRunService:
    """Return an experiment run service for the current request.

    Args:
        session: Request-scoped database session.

    Returns:
        Experiment run service bound to the SQL repositories.
    """
    return ExperimentRunService(
        repository=SQLExperimentRunRepository(session),
        replay_repository=SQLReplayRepository(session),
        job_repository=SQLJobRepository(session),
        transitions=_build_task_transitions(session),
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
        agent_repository=SQLAgentRepository(session),
    )


def get_cohort_version_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> CohortVersionService:
    """Return a cohort version service for the current request.

    Args:
        session: Request-scoped database session.

    Returns:
        Cohort version service bound to the SQL repositories.
    """
    return CohortVersionService(
        repository=SQLCohortVersionRepository(session),
        cohort_repository=SQLCohortRepository(session),
        session_repository=SQLSessionRepository(session),
    )


def get_evaluation_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> EvaluationService:
    """Return an evaluation service for the current request.

    Args:
        session: Request-scoped database session.

    Returns:
        Evaluation service bound to the SQL repositories.
    """
    return EvaluationService(
        repository=SQLEvaluationRepository(session),
        session_repository=SQLSessionRepository(session),
    )


def get_device_service(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> DeviceService:
    """Return a device service for the current request.

    Args:
        request: Incoming request.
        session: Request-scoped database session.
        settings: API settings for this process.

    Returns:
        Device service bound to the SQL repository.
    """
    database: DatabaseService = request.app.state.database
    return DeviceService(
        repository=SQLDeviceRepository(session, database.engine),
        policy=DevicePolicy(
            auth_timeout_seconds=settings.DEVICE_AUTH_TIMEOUT_SECONDS,
            polling_interval_seconds=settings.DEVICE_AUTH_POLLING_INTERVAL_SECONDS,
            max_failed_attempts=settings.MAX_FAILED_DEVICE_AUTH_ATTEMPTS,
            expiration_minutes=settings.DEVICE_EXPIRATION_MINUTES,
            trusted_expiration_minutes=settings.TRUSTED_DEVICE_EXPIRATION_MINUTES,
        ),
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
) -> WorkerService:
    """Return a worker service for the current request.

    Args:
        session: Request-scoped database session.

    Returns:
        Worker service bound to the SQL repository.
    """
    return WorkerService(repository=SQLWorkerRepository(session))


def get_auth_service(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
    device_service: Annotated[DeviceService, Depends(get_device_service)],
) -> AuthService:
    """Return an authentication service for the current request.

    Args:
        request: Incoming request.
        session: Request-scoped database session.
        device_service: Device service for the current request.

    Returns:
        Authentication service bound to the SQL repositories.
    """
    settings = get_app_settings(request)
    account_repository = SQLAccountRepository(session)
    client: ControlPlaneClient | None = request.app.state.control_plane_client
    control_plane = None
    cloud_credential_resolver = None
    if client is not None and settings.AUTH_SCHEME is AuthScheme.CONTROL_PLANE:
        control_plane = ControlPlaneAuthenticator(
            client=client,
            account_repository=account_repository,
            server_id=settings.SERVER_ID,
        )
    elif client is not None and settings.AUTH_SCHEME is AuthScheme.CLOUD:
        cloud_credential_resolver = CloudAuthService(
            server_id=settings.SERVER_ID,
            control_plane=client,
            account_repository=account_repository,
        )
    return AuthService(
        settings=settings,
        account_repository=account_repository,
        api_key_repository=SQLApiKeyRepository(session),
        password_hasher=BcryptPasswordHasher(),
        device_service=device_service,
        control_plane=control_plane,
        cloud_credential_resolver=cloud_credential_resolver,
    )


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
    header = request.headers.get("Authorization")
    csrf_token = request.headers.get(CSRF_HEADER)
    if header:
        scheme, _, credential = header.partition(" ")
        if scheme.lower() == "bearer" and credential:
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


async def authorize(
    request: Request,
    settings: Annotated[APISettings, Depends(get_app_settings)],
    credential: Annotated[
        RequestCredential | None, Depends(get_optional_bearer_credential)
    ],
    auth_service: Annotated[AuthService, Depends(get_auth_service)],
) -> AuthContext:
    """Authorize a request and return its auth context.

    With the ``none`` auth scheme every request is accepted and runs as the
    default account. Other schemes require a bearer credential.

    Args:
        request: Incoming HTTP request.
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
            credential=credential.token,
            csrf_token=credential.csrf_token,
            from_cookie=credential.from_cookie,
            action=_request_action(request.method),
        )
    except AuthenticationServiceUnavailableError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc
    except AuthenticationError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
        ) from exc


def _request_action(method: str) -> str:
    """Map an HTTP method to a Cloud RBAC action."""
    if method in {"GET", "HEAD", "OPTIONS"}:
        return "read"
    if method == "POST":
        return "create"
    if method in {"PUT", "PATCH"}:
        return "update"
    if method == "DELETE":
        return "delete"
    return "read"
