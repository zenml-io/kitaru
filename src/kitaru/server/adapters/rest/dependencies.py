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
from importlib.metadata import version
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
from kitaru.server.api.config import UNSET_SERVER_ID, APISettings
from kitaru.server.application.models.auth import (
    AuthContext,
    TaskAuthContext,
    TaskPrincipal,
    WorkerAuthContext,
    WorkerPrincipal,
)
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
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.application.services.tag_service import TagService
from kitaru.server.application.services.task_service import TaskService
from kitaru.server.application.services.task_spec import TaskSpecBuilder
from kitaru.server.application.services.task_transitions import TaskTransitions
from kitaru.server.application.services.worker_service import WorkerService
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.account import AccountNotFound
from kitaru.server.domain.plugin import PluginKind

CSRF_HEADER = "X-CSRF-Token"
KITARU_VERSION = version("kitaru")


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


def get_server_analytics(
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
    client: Annotated[AnalyticsClient, Depends(get_analytics_client)],
) -> ServerAnalytics:
    """Return a server analytics tracker for the current request.

    Args:
        session: Request-scoped database session.
        settings: API settings for this process.
        client: Analytics client for this process.

    Returns:
        Tracker buffering track calls until the request session commits.
    """
    server_id = None if settings.SERVER_ID == UNSET_SERVER_ID else settings.SERVER_ID
    return ServerAnalytics(
        client=client, session=session, server_id=server_id, version=KITARU_VERSION
    )


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
    return AgentVersionService(repository=SQLAgentVersionRepository(session))


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
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> PluginService:
    """Return a plugin service bound to the evaluator kind.

    Args:
        session: Request-scoped database session.
        analytics: Analytics tracker for the current request.

    Returns:
        Plugin service bound to the SQL repositories.
    """
    return PluginService(
        kind=PluginKind.EVALUATOR,
        repository=SQLPluginRepository(session),
        blob_repository=SQLBlobRepository(session),
        analytics=analytics,
    )


def get_importer_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> PluginService:
    """Return a plugin service bound to the importer kind.

    Args:
        session: Request-scoped database session.
        analytics: Analytics tracker for the current request.

    Returns:
        Plugin service bound to the SQL repositories.
    """
    return PluginService(
        kind=PluginKind.IMPORTER,
        repository=SQLPluginRepository(session),
        blob_repository=SQLBlobRepository(session),
        analytics=analytics,
    )


def get_session_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> SessionService:
    """Return a session service for the current request.

    Args:
        session: Request-scoped database session.
        analytics: Analytics tracker for the current request.

    Returns:
        Session service bound to the SQL repositories.
    """
    return SessionService(
        repository=SQLSessionRepository(session),
        task_repository=SQLTaskRepository(session),
        agent_version_repository=SQLAgentVersionRepository(session),
        analytics=analytics,
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


def _build_task_transitions(
    session: AsyncSession, analytics: ServerAnalytics
) -> TaskTransitions:
    """Build the request-scoped task transition dispatch.

    Args:
        session: Request-scoped database session.
        analytics: Analytics tracker for the current request.

    Returns:
        Transition dispatch publishing on the request's event dispatcher.
    """
    return TaskTransitions(
        task_repository=SQLTaskRepository(session),
        job_repository=SQLJobRepository(session),
        dispatcher=build_event_dispatcher(session, analytics),
        analytics=analytics,
    )


def get_job_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> JobService:
    """Return a job service for the current request.

    Args:
        session: Request-scoped database session.
        settings: API settings for this process.
        analytics: Analytics tracker for the current request.

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
        transitions=_build_task_transitions(session, analytics),
        policy=get_task_policy(settings),
    )


def get_task_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> TaskService:
    """Return a task service for the current request.

    Args:
        session: Request-scoped database session.
        settings: API settings for this process.
        analytics: Analytics tracker for the current request.

    Returns:
        Task service bound to the SQL repositories.
    """
    policy = get_task_policy(settings)
    spec_builder = TaskSpecBuilder(
        agent_version_repository=SQLAgentVersionRepository(session),
        plugin_repository=SQLPluginRepository(session),
        blob_repository=SQLBlobRepository(session),
        secret_repository=SQLSecretRepository(
            session, AesGcmCipher(settings.SECRET_ENCRYPTION_KEY)
        ),
        policy=policy,
    )
    return TaskService(
        repository=SQLTaskRepository(session),
        worker_repository=SQLWorkerRepository(session),
        session_repository=SQLSessionRepository(session),
        job_repository=SQLJobRepository(session),
        spec_builder=spec_builder,
        transitions=_build_task_transitions(session, analytics),
        policy=policy,
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
        task_repository=SQLTaskRepository(session),
    )


def get_experiment_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> ExperimentService:
    """Return an experiment service for the current request.

    Args:
        session: Request-scoped database session.
        analytics: Analytics tracker for the current request.

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
        analytics=analytics,
    )


def get_replay_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> ReplayService:
    """Return a replay service for the current request.

    Args:
        session: Request-scoped database session.
        analytics: Analytics tracker for the current request.

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
        analytics=analytics,
    )


def get_experiment_run_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> ExperimentRunService:
    """Return an experiment run service for the current request.

    Args:
        session: Request-scoped database session.
        analytics: Analytics tracker for the current request.

    Returns:
        Experiment run service bound to the SQL repositories.
    """
    return ExperimentRunService(
        repository=SQLExperimentRunRepository(session),
        replay_repository=SQLReplayRepository(session),
        job_repository=SQLJobRepository(session),
        transitions=_build_task_transitions(session, analytics),
        analytics=analytics,
    )


def get_cohort_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> CohortService:
    """Return a cohort service for the current request.

    Args:
        session: Request-scoped database session.
        analytics: Analytics tracker for the current request.

    Returns:
        Cohort service bound to the SQL repositories.
    """
    return CohortService(
        repository=SQLCohortRepository(session),
        agent_repository=SQLAgentRepository(session),
        analytics=analytics,
    )


def get_cohort_version_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> CohortVersionService:
    """Return a cohort version service for the current request.

    Args:
        session: Request-scoped database session.
        analytics: Analytics tracker for the current request.

    Returns:
        Cohort version service bound to the SQL repositories.
    """
    return CohortVersionService(
        repository=SQLCohortVersionRepository(session),
        cohort_repository=SQLCohortRepository(session),
        session_repository=SQLSessionRepository(session),
        analytics=analytics,
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
    settings: Annotated[APISettings, Depends(get_app_settings)],
    credential: Annotated[
        RequestCredential | None, Depends(get_optional_bearer_credential)
    ],
    auth_service: Annotated[AuthService, Depends(get_auth_service)],
) -> AuthContext:
    """Resolve a request into its auth context, gating on nothing but validity.

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
