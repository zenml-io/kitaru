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
from functools import partial
from typing import Annotated, NamedTuple

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from kitaru.analytics.client import AnalyticsClient
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
from kitaru.server.adapters.db.encryption import AesGcmCipher
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import (
    SQLAgentRepository,
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.api_key_repository import (
    SQLApiKeyRepository,
)
from kitaru.server.adapters.db.repositories.blob_repository import SQLBlobRepository
from kitaru.server.adapters.db.repositories.cohort_repository import (
    SQLCohortRepository,
)
from kitaru.server.adapters.db.repositories.device_repository import (
    SQLDeviceRepository,
)
from kitaru.server.adapters.db.repositories.evaluation_repository import (
    SQLEvaluationRepository,
)
from kitaru.server.adapters.db.repositories.experiment_repository import (
    SQLExperimentRepository,
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
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionNodeRepository,
    SQLSessionRepository,
)
from kitaru.server.adapters.db.repositories.tag_repository import SQLTagRepository
from kitaru.server.adapters.db.repositories.task_repository import SQLTaskRepository
from kitaru.server.adapters.db.repositories.worker_repository import (
    SQLWorkerRepository,
)
from kitaru.server.adapters.rest.commit_route import attach_request_session
from kitaru.server.api.config import APISettings
from kitaru.server.application.evaluation_recording import record_task_evaluations
from kitaru.server.application.events import (
    EventRegistry,
    JobSettled,
    ReplaySettled,
    TaskTerminal,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.device import DevicePolicy
from kitaru.server.application.replay_pipeline import (
    append_result_evaluations,
    settle_replay,
)
from kitaru.server.application.run_finalization import finalize_run_if_drained
from kitaru.server.application.services.account_service import AccountService
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.agent_version_service import (
    AgentVersionService,
)
from kitaru.server.application.services.api_key_service import ApiKeyService
from kitaru.server.application.services.blob_service import BlobService
from kitaru.server.application.services.cohort_service import CohortService
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
from kitaru.server.application.services.session_node_service import SessionNodeService
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.application.services.tag_service import TagService
from kitaru.server.application.services.task_service import TaskService
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


class ExecutionServices(NamedTuple):
    """Request-scoped services that participate in task events."""

    jobs: JobService
    tasks: TaskService
    workers: WorkerService


def get_execution_services(
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> ExecutionServices:
    """Compose task, job, worker, and event processing once per request."""
    agent_repository = SQLAgentRepository(session)
    agent_version_repository = SQLAgentVersionRepository(session)
    blob_repository = SQLBlobRepository(session)
    evaluation_repository = SQLEvaluationRepository(session)
    job_repository = SQLJobRepository(session)
    plugin_repository = SQLPluginRepository(session)
    replay_repository = SQLReplayRepository(session)
    secret_repository = SQLSecretRepository(
        session, AesGcmCipher(settings.SECRET_ENCRYPTION_KEY)
    )
    session_repository = SQLSessionRepository(session)
    task_repository = SQLTaskRepository(session)
    worker_repository = SQLWorkerRepository(session)
    events = EventRegistry()
    job_service = JobService(
        job_repository=job_repository,
        task_repository=task_repository,
        agent_repository=agent_repository,
        agent_version_repository=agent_version_repository,
        plugin_repository=plugin_repository,
        blob_repository=blob_repository,
        session_repository=session_repository,
        events=events,
        evaluation_pair_limit=settings.MAX_EVALUATION_PAIRS,
    )
    task_service = TaskService(
        task_repository=task_repository,
        worker_repository=worker_repository,
        agent_version_repository=agent_version_repository,
        plugin_repository=plugin_repository,
        blob_repository=blob_repository,
        secret_repository=secret_repository,
        session_repository=session_repository,
        events=events,
        heartbeat_timeout_seconds=settings.TASK_HEARTBEAT_TIMEOUT_SECONDS,
        retry_cap=settings.TASK_MAX_ATTEMPTS,
        sweep_limit=settings.TASK_STALENESS_SWEEP_LIMIT,
        evaluator_timeout_seconds=settings.EVALUATOR_TASK_TIMEOUT_SECONDS,
        importer_timeout_seconds=settings.IMPORTER_TASK_TIMEOUT_SECONDS,
        max_result_bytes=settings.MAX_TASK_RESULT_BYTES,
    )
    job_service.set_task_service(task_service)
    task_service.set_job_service(job_service)
    events.register(
        TaskTerminal,
        partial(
            record_task_evaluations,
            evaluation_repository=evaluation_repository,
            session_repository=session_repository,
        ),
    )
    events.register(
        TaskTerminal,
        partial(
            append_result_evaluations,
            replay_repository=replay_repository,
            job_service=job_service,
        ),
    )
    events.register(
        JobSettled,
        partial(
            settle_replay,
            replay_repository=replay_repository,
            events=events,
        ),
    )
    events.register(
        ReplaySettled,
        partial(
            finalize_run_if_drained,
            replay_repository=replay_repository,
            run_repository=SQLExperimentRunRepository(session),
        ),
    )
    return ExecutionServices(
        jobs=job_service,
        tasks=task_service,
        workers=WorkerService(worker_repository),
    )


def get_job_service(
    services: Annotated[ExecutionServices, Depends(get_execution_services)],
) -> JobService:
    """Return the request-scoped job service."""
    return services.jobs


def get_task_service(
    services: Annotated[ExecutionServices, Depends(get_execution_services)],
) -> TaskService:
    """Return the request-scoped task service."""
    return services.tasks


def get_worker_service(
    services: Annotated[ExecutionServices, Depends(get_execution_services)],
) -> WorkerService:
    """Return the request-scoped worker service."""
    return services.workers


def get_agent_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> AgentService:
    """Return an agent service."""
    return AgentService(SQLAgentRepository(session))


def get_agent_version_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> AgentVersionService:
    """Return an agent-version service."""
    return AgentVersionService(
        repository=SQLAgentVersionRepository(session),
        agent_repository=SQLAgentRepository(session),
        secret_repository=SQLSecretRepository(
            session, AesGcmCipher(settings.SECRET_ENCRYPTION_KEY)
        ),
    )


def get_blob_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> BlobService:
    """Return a content-addressed blob service."""
    return BlobService(
        SQLBlobRepository(session),
        max_size_bytes=settings.MAX_BLOB_SIZE_BYTES,
    )


def get_cohort_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> CohortService:
    """Return a cohort service."""
    return CohortService(
        repository=SQLCohortRepository(session),
        session_repository=SQLSessionRepository(session),
        agent_repository=SQLAgentRepository(session),
    )


def get_evaluation_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> EvaluationService:
    """Return an evaluation service."""
    return EvaluationService(
        repository=SQLEvaluationRepository(session),
        session_repository=SQLSessionRepository(session),
        plugin_repository=SQLPluginRepository(session),
    )


def _get_plugin_service(
    session: AsyncSession,
    kind: PluginKind,
) -> PluginService:
    return PluginService(
        repository=SQLPluginRepository(session),
        blob_repository=SQLBlobRepository(session),
        kind=kind,
    )


def get_evaluator_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PluginService:
    """Return a plugin service bound to evaluators."""
    return _get_plugin_service(session, PluginKind.EVALUATOR)


def get_importer_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PluginService:
    """Return a plugin service bound to importers."""
    return _get_plugin_service(session, PluginKind.IMPORTER)


def get_session_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> SessionService:
    """Return a session lifecycle service."""
    return SessionService(
        repository=SQLSessionRepository(session),
        agent_repository=SQLAgentRepository(session),
        agent_version_repository=SQLAgentVersionRepository(session),
        task_repository=SQLTaskRepository(session),
    )


def get_session_node_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> SessionNodeService:
    """Return a session-node ingestion service."""
    return SessionNodeService(
        repository=SQLSessionNodeRepository(session),
        session_repository=SQLSessionRepository(session),
    )


def get_tag_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> TagService:
    """Return a tag service."""
    return TagService(SQLTagRepository(session))


def get_experiment_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    services: Annotated[ExecutionServices, Depends(get_execution_services)],
) -> ExperimentService:
    """Return an experiment definition and fan-out service."""
    return ExperimentService(
        repository=SQLExperimentRepository(session),
        run_repository=SQLExperimentRunRepository(session),
        cohort_repository=SQLCohortRepository(session),
        session_repository=SQLSessionRepository(session),
        agent_version_repository=SQLAgentVersionRepository(session),
        plugin_repository=SQLPluginRepository(session),
        replay_repository=SQLReplayRepository(session),
        task_repository=SQLTaskRepository(session),
        job_service=services.jobs,
    )


def get_experiment_run_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    services: Annotated[ExecutionServices, Depends(get_execution_services)],
) -> ExperimentRunService:
    """Return an experiment-run service."""
    return ExperimentRunService(
        repository=SQLExperimentRunRepository(session),
        replay_repository=SQLReplayRepository(session),
        job_repository=SQLJobRepository(session),
        job_service=services.jobs,
    )


def get_replay_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    services: Annotated[ExecutionServices, Depends(get_execution_services)],
) -> ReplayService:
    """Return a standalone replay and history-lookup service."""
    return ReplayService(
        repository=SQLReplayRepository(session),
        session_repository=SQLSessionRepository(session),
        session_node_repository=SQLSessionNodeRepository(session),
        agent_repository=SQLAgentRepository(session),
        agent_version_repository=SQLAgentVersionRepository(session),
        plugin_repository=SQLPluginRepository(session),
        run_repository=SQLExperimentRunRepository(session),
        task_repository=SQLTaskRepository(session),
        job_service=services.jobs,
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
    if client is not None:
        control_plane = ControlPlaneAuthenticator(
            client=client,
            account_repository=account_repository,
            server_id=settings.SERVER_ID,
        )
    return AuthService(
        settings=settings,
        account_repository=account_repository,
        api_key_repository=SQLApiKeyRepository(session),
        password_hasher=BcryptPasswordHasher(),
        device_service=device_service,
        control_plane=control_plane,
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
        )
    except AuthenticationError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
        ) from exc
