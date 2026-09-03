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

import uuid
from collections.abc import AsyncGenerator
from typing import Annotated, NamedTuple

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

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
from kitaru.server.adapters.db.blob_data_store import DatabaseBlobDataStore
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
from kitaru.server.adapters.db.repositories.annotation_repository import (
    SQLAnnotationRepository,
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
from kitaru.server.adapters.db.repositories.idempotency_key_repository import (
    SQLIdempotencyKeyRepository,
)
from kitaru.server.adapters.db.repositories.insight_repository import (
    SQLInsightRepository,
)
from kitaru.server.adapters.db.repositories.investigation_repository import (
    SQLInvestigationRepository,
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
from kitaru.server.adapters.permissions.admin_flag import AdminFlagPermissionProvider
from kitaru.server.adapters.permissions.allow_all import AllowAllPermissionProvider
from kitaru.server.adapters.rest.request_state import (
    attach_request_session,
    request_uses_read_engine,
)
from kitaru.server.api.composition import build_event_dispatcher
from kitaru.server.api.config import APISettings
from kitaru.server.application.interfaces.blob_data_store import (
    BlobDataStore,
    BlobDataStores,
)
from kitaru.server.application.interfaces.ephemeral_workers import (
    EphemeralWorkersBackend,
)
from kitaru.server.application.interfaces.idempotency_key_repository import (
    IdempotencyKeyRepository,
)
from kitaru.server.application.models.auth import (
    AuthContext,
    TaskAuthContext,
    TaskPrincipal,
    WorkerAuthContext,
    WorkerPrincipal,
)
from kitaru.server.application.models.device import DevicePolicy
from kitaru.server.application.models.task import TaskPolicy
from kitaru.server.application.payload_store import PayloadStore
from kitaru.server.application.services.account_service import AccountService
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.agent_version_service import (
    AgentVersionService,
)
from kitaru.server.application.services.annotation_service import AnnotationService
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
from kitaru.server.application.services.insight_service import InsightService
from kitaru.server.application.services.investigation_service import (
    InvestigationService,
)
from kitaru.server.application.services.job_service import JobService
from kitaru.server.application.services.permission_service import PermissionService
from kitaru.server.application.services.plugin_service import PluginService
from kitaru.server.application.services.replay_service import ReplayService
from kitaru.server.application.services.sample_data_seeding import (
    SampleDataSeeder,
)
from kitaru.server.application.services.secret_service import SecretService
from kitaru.server.application.services.server_analytics import (
    ServerAnalytics,
    current_actor,
)
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
from kitaru.server.domain.blob import BlobStorageBackend
from kitaru.server.domain.plugin import PluginKind

CSRF_HEADER = "X-CSRF-Token"


class RequestCredential(NamedTuple):
    """Credential read off an incoming request."""

    token: str
    csrf_token: str | None
    from_cookie: bool


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


def get_permission_service(
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> PermissionService:
    """Return a permission service for the current request.

    Args:
        settings: API settings for this process.

    Returns:
        Permission service bound to the auth scheme's provider.
    """
    if settings.AUTH_SCHEME is AuthScheme.CONTROL_PLANE:
        return PermissionService(AllowAllPermissionProvider())
    return PermissionService(AdminFlagPermissionProvider())


def get_account_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    permission_service: Annotated[PermissionService, Depends(get_permission_service)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> AccountService:
    """Return an account service for the current request.

    Args:
        session: Request-scoped database session.
        permission_service: Permission service for the current request.
        analytics: Analytics tracker for the current request.

    Returns:
        Account service bound to the SQL repository.
    """
    return AccountService(
        repository=SQLAccountRepository(session),
        password_hasher=BcryptPasswordHasher(),
        permission_service=permission_service,
        analytics=analytics,
    )


def get_agent_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> AgentService:
    """Return an agent service for the current request.

    Args:
        session: Request-scoped database session.
        analytics: Analytics tracker for the current request.

    Returns:
        Agent service bound to the SQL repository.
    """
    return AgentService(repository=SQLAgentRepository(session), analytics=analytics)


def get_agent_version_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> AgentVersionService:
    """Return an agent version service for the current request.

    Args:
        session: Request-scoped database session.
        analytics: Analytics tracker for the current request.

    Returns:
        Agent version service bound to the SQL repositories.
    """
    return AgentVersionService(
        repository=SQLAgentVersionRepository(session), analytics=analytics
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


def get_blob_data_stores(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> BlobDataStores:
    """Return the blob content stores configured for the current process.

    Args:
        request: Incoming request.
        session: Request-scoped database session.
        settings: API settings for this process.

    Returns:
        Content stores keyed by the backend they serve.
    """
    stores: dict[BlobStorageBackend, BlobDataStore] = {
        BlobStorageBackend.DATABASE: DatabaseBlobDataStore(session)
    }
    s3_store: BlobDataStore | None = request.app.state.s3_blob_data_store
    if s3_store is not None:
        stores[BlobStorageBackend.S3] = s3_store
    return BlobDataStores(stores, settings.BLOB_STORAGE.backend)


def get_ephemeral_workers(request: Request) -> EphemeralWorkersBackend | None:
    """Return the ephemeral worker backend attached to the application state.

    Args:
        request: Incoming request.

    Returns:
        Ephemeral worker backend for this process, or None when none is configured.
    """
    ephemeral_workers: EphemeralWorkersBackend | None = (
        request.app.state.ephemeral_workers
    )
    return ephemeral_workers


def get_blob_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
    data_stores: Annotated[BlobDataStores, Depends(get_blob_data_stores)],
) -> BlobService:
    """Return a blob service for the current request.

    Args:
        session: Request-scoped database session.
        settings: API settings for this process.
        data_stores: Content stores keyed by the backend they serve.

    Returns:
        Blob service bound to the SQL repository.
    """
    return BlobService(
        repository=SQLBlobRepository(session),
        data_stores=data_stores,
        max_size_bytes=settings.MAX_BLOB_SIZE_BYTES,
    )


def get_payload_store(
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
    data_stores: Annotated[BlobDataStores, Depends(get_blob_data_stores)],
) -> PayloadStore:
    """Return a payload store for the current request.

    Args:
        session: Request-scoped database session.
        settings: API settings for this process.
        data_stores: Content stores keyed by the backend they serve.

    Returns:
        Payload store bound to the SQL repository.
    """
    return PayloadStore(
        repository=SQLBlobRepository(session),
        data_stores=data_stores,
        threshold_bytes=settings.PAYLOAD_OFFLOAD_THRESHOLD_BYTES,
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
    engine: Annotated[AsyncEngine, Depends(get_engine)],
    payload_store: Annotated[PayloadStore, Depends(get_payload_store)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> SessionService:
    """Return a session service for the current request.

    Args:
        session: Request-scoped database session.
        engine: Application database engine.
        payload_store: Payload store for the current request.
        analytics: Analytics tracker for the current request.

    Returns:
        Session service bound to the SQL repositories.
    """
    return SessionService(
        repository=SQLSessionRepository(session, engine),
        task_repository=SQLTaskRepository(session),
        agent_version_repository=SQLAgentVersionRepository(session),
        replay_repository=SQLReplayRepository(session),
        payload_store=payload_store,
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
    session: AsyncSession, engine: AsyncEngine, analytics: ServerAnalytics
) -> TaskTransitions:
    """Build the request-scoped task transition dispatch.

    Args:
        session: Request-scoped database session.
        engine: Application database engine.
        analytics: Analytics tracker for the current request.

    Returns:
        Transition dispatch publishing on the request's event dispatcher.
    """
    return TaskTransitions(
        task_repository=SQLTaskRepository(session),
        job_repository=SQLJobRepository(session),
        dispatcher=build_event_dispatcher(session, engine, analytics),
        analytics=analytics,
        plugin_repository=SQLPluginRepository(session),
    )


def get_job_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    engine: Annotated[AsyncEngine, Depends(get_engine)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> JobService:
    """Return a job service for the current request.

    Args:
        session: Request-scoped database session.
        engine: Application database engine.
        settings: API settings for this process.
        analytics: Analytics tracker for the current request.

    Returns:
        Job service bound to the SQL repositories.
    """
    return JobService(
        repository=SQLJobRepository(session),
        task_repository=SQLTaskRepository(session),
        session_repository=SQLSessionRepository(session, engine),
        agent_repository=SQLAgentRepository(session),
        agent_version_repository=SQLAgentVersionRepository(session),
        plugin_repository=SQLPluginRepository(session),
        blob_repository=SQLBlobRepository(session),
        transitions=_build_task_transitions(session, engine, analytics),
        policy=get_task_policy(settings),
    )


def get_task_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    engine: Annotated[AsyncEngine, Depends(get_engine)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> TaskService:
    """Return a task service for the current request.

    Args:
        session: Request-scoped database session.
        engine: Application database engine.
        settings: API settings for this process.
        analytics: Analytics tracker for the current request.

    Returns:
        Task service bound to the SQL repositories.
    """
    policy = get_task_policy(settings)
    replay_repository = SQLReplayRepository(session)
    spec_builder = TaskSpecBuilder(
        agent_version_repository=SQLAgentVersionRepository(session),
        plugin_repository=SQLPluginRepository(session),
        blob_repository=SQLBlobRepository(session),
        secret_repository=SQLSecretRepository(
            session, AesGcmCipher(settings.SECRET_ENCRYPTION_KEY)
        ),
        replay_repository=replay_repository,
        policy=policy,
    )
    return TaskService(
        repository=SQLTaskRepository(session),
        worker_repository=SQLWorkerRepository(session),
        session_repository=SQLSessionRepository(session, engine),
        job_repository=SQLJobRepository(session),
        replay_repository=replay_repository,
        spec_builder=spec_builder,
        transitions=_build_task_transitions(session, engine, analytics),
        policy=policy,
    )


def get_session_node_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    engine: Annotated[AsyncEngine, Depends(get_engine)],
    payload_store: Annotated[PayloadStore, Depends(get_payload_store)],
) -> SessionNodeService:
    """Return a session node service for the current request.

    Args:
        session: Request-scoped database session.
        engine: Application database engine.
        payload_store: Payload store for the current request.

    Returns:
        Session node service bound to the SQL repositories.
    """
    return SessionNodeService(
        repository=SQLSessionNodeRepository(session),
        session_repository=SQLSessionRepository(session, engine),
        task_repository=SQLTaskRepository(session),
        payload_store=payload_store,
    )


def get_experiment_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    engine: Annotated[AsyncEngine, Depends(get_engine)],
    payload_store: Annotated[PayloadStore, Depends(get_payload_store)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> ExperimentService:
    """Return an experiment service for the current request.

    Args:
        session: Request-scoped database session.
        engine: Application database engine.
        payload_store: Payload store for the current request.
        analytics: Analytics tracker for the current request.

    Returns:
        Experiment service bound to the SQL repositories.
    """
    return ExperimentService(
        repository=SQLExperimentRepository(session),
        plugin_repository=SQLPluginRepository(session),
        experiment_run_repository=SQLExperimentRunRepository(session),
        agent_repository=SQLAgentRepository(session),
        cohort_version_repository=SQLCohortVersionRepository(session),
        session_repository=SQLSessionRepository(session, engine),
        agent_version_repository=SQLAgentVersionRepository(session),
        replay_repository=SQLReplayRepository(session),
        job_repository=SQLJobRepository(session),
        task_repository=SQLTaskRepository(session),
        evaluation_repository=SQLEvaluationRepository(session),
        transitions=_build_task_transitions(session, engine, analytics),
        payload_store=payload_store,
        analytics=analytics,
    )


def get_replay_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    engine: Annotated[AsyncEngine, Depends(get_engine)],
    payload_store: Annotated[PayloadStore, Depends(get_payload_store)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> ReplayService:
    """Return a replay service for the current request.

    Args:
        session: Request-scoped database session.
        engine: Application database engine.
        payload_store: Payload store for the current request.
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
        evaluation_repository=SQLEvaluationRepository(session),
        session_repository=SQLSessionRepository(session, engine),
        session_node_repository=SQLSessionNodeRepository(session),
        agent_version_repository=SQLAgentVersionRepository(session),
        plugin_repository=SQLPluginRepository(session),
        payload_store=payload_store,
        analytics=analytics,
    )


def get_experiment_run_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    engine: Annotated[AsyncEngine, Depends(get_engine)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> ExperimentRunService:
    """Return an experiment run service for the current request.

    Args:
        session: Request-scoped database session.
        engine: Application database engine.
        analytics: Analytics tracker for the current request.

    Returns:
        Experiment run service bound to the SQL repositories.
    """
    return ExperimentRunService(
        repository=SQLExperimentRunRepository(session),
        replay_repository=SQLReplayRepository(session),
        job_repository=SQLJobRepository(session),
        transitions=_build_task_transitions(session, engine, analytics),
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
    engine: Annotated[AsyncEngine, Depends(get_engine)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> CohortVersionService:
    """Return a cohort version service for the current request.

    Args:
        session: Request-scoped database session.
        engine: Application database engine.
        analytics: Analytics tracker for the current request.

    Returns:
        Cohort version service bound to the SQL repositories.
    """
    return CohortVersionService(
        repository=SQLCohortVersionRepository(session),
        cohort_repository=SQLCohortRepository(session),
        session_repository=SQLSessionRepository(session, engine),
        analytics=analytics,
    )


def get_evaluation_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    engine: Annotated[AsyncEngine, Depends(get_engine)],
) -> EvaluationService:
    """Return an evaluation service for the current request.

    Args:
        session: Request-scoped database session.
        engine: Application database engine.

    Returns:
        Evaluation service bound to the SQL repositories.
    """
    return EvaluationService(
        repository=SQLEvaluationRepository(session),
        session_repository=SQLSessionRepository(session, engine),
    )


def get_insight_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> InsightService:
    """Return an insight service for the current request.

    Args:
        session: Request-scoped database session.
        analytics: Analytics tracker for the current request.

    Returns:
        Insight service bound to the SQL repositories.
    """
    return InsightService(
        repository=SQLInsightRepository(session),
        agent_repository=SQLAgentRepository(session),
        analytics=analytics,
    )


def get_investigation_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    engine: Annotated[AsyncEngine, Depends(get_engine)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> InvestigationService:
    """Return an investigation service for the current request.

    Args:
        session: Request-scoped database session.
        engine: Application database engine.
        analytics: Analytics tracker for the current request.

    Returns:
        Investigation service bound to the SQL repositories.
    """
    return InvestigationService(
        repository=SQLInvestigationRepository(session),
        agent_repository=SQLAgentRepository(session),
        session_repository=SQLSessionRepository(session, engine),
        analytics=analytics,
    )


def get_annotation_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    engine: Annotated[AsyncEngine, Depends(get_engine)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> AnnotationService:
    """Return an annotation service for the current request.

    Args:
        session: Request-scoped database session.
        engine: Application database engine.
        analytics: Analytics tracker for the current request.

    Returns:
        Annotation service bound to the SQL repositories.
    """
    return AnnotationService(
        repository=SQLAnnotationRepository(session),
        investigation_repository=SQLInvestigationRepository(session),
        session_repository=SQLSessionRepository(session, engine),
        session_node_repository=SQLSessionNodeRepository(session),
        analytics=analytics,
    )


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


def get_sample_data_seeder(
    agent_service: Annotated[AgentService, Depends(get_agent_service)],
    session_service: Annotated[SessionService, Depends(get_session_service)],
    session_node_service: Annotated[
        SessionNodeService, Depends(get_session_node_service)
    ],
    evaluation_service: Annotated[EvaluationService, Depends(get_evaluation_service)],
    tag_service: Annotated[TagService, Depends(get_tag_service)],
    cohort_service: Annotated[CohortService, Depends(get_cohort_service)],
    cohort_version_service: Annotated[
        CohortVersionService, Depends(get_cohort_version_service)
    ],
    blob_service: Annotated[BlobService, Depends(get_blob_service)],
    evaluator_service: Annotated[PluginService, Depends(get_evaluator_service)],
    experiment_service: Annotated[ExperimentService, Depends(get_experiment_service)],
    investigation_service: Annotated[
        InvestigationService, Depends(get_investigation_service)
    ],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> SampleDataSeeder:
    """Return a sample data seeder for the current request.

    Args:
        agent_service: Agent service.
        session_service: Session service.
        session_node_service: Session node service.
        evaluation_service: Evaluation service.
        tag_service: Tag service.
        cohort_service: Cohort service.
        cohort_version_service: Cohort version service.
        blob_service: Blob service.
        evaluator_service: Evaluator service.
        experiment_service: Experiment service.
        investigation_service: Investigation service.
        analytics: Analytics tracker for the current request.

    Returns:
        Sample data seeder bound to the request's services.
    """
    return SampleDataSeeder(
        agent_service=agent_service,
        session_service=session_service,
        session_node_service=session_node_service,
        evaluation_service=evaluation_service,
        tag_service=tag_service,
        cohort_service=cohort_service,
        cohort_version_service=cohort_version_service,
        blob_service=blob_service,
        evaluator_service=evaluator_service,
        experiment_service=experiment_service,
        investigation_service=investigation_service,
        analytics=analytics,
    )


def get_worker_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> WorkerService:
    """Return a worker service for the current request.

    Args:
        session: Request-scoped database session.
        settings: API settings for this process.
        analytics: Analytics tracker for the current request.

    Returns:
        Worker service bound to the SQL repository.
    """
    return WorkerService(
        repository=SQLWorkerRepository(session),
        liveness_timeout_seconds=settings.WORKER_LIVENESS_TIMEOUT_SECONDS,
        analytics=analytics,
    )


def get_idempotency_key_repository(
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> IdempotencyKeyRepository:
    """Return an idempotency key repository for the current request.

    Args:
        session: Request-scoped database session.
        settings: API settings for this process.

    Returns:
        Idempotency key repository bound to the SQL implementation.
    """
    return SQLIdempotencyKeyRepository(
        session, AesGcmCipher(settings.SECRET_ENCRYPTION_KEY)
    )


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
