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
"""Service providers bound to request-scoped SQL repositories."""

from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from kitaru.api_models.v1.info import AuthScheme
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
from kitaru.server.adapters.db.repositories.connection_repository import (
    SQLConnectionRepository,
)
from kitaru.server.adapters.db.repositories.evaluation_repository import (
    SQLEvaluationRepository,
)
from kitaru.server.adapters.db.repositories.idempotency_key_repository import (
    SQLIdempotencyKeyRepository,
)
from kitaru.server.adapters.db.repositories.import_repository import (
    SQLImportRepository,
)
from kitaru.server.adapters.db.repositories.insight_repository import (
    SQLInsightRepository,
)
from kitaru.server.adapters.db.repositories.investigation_repository import (
    SQLInvestigationRepository,
)
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
from kitaru.server.adapters.rest.dependencies.request import (
    get_app_settings,
    get_engine,
    get_server_analytics,
    get_session,
)
from kitaru.server.api.config import APISettings
from kitaru.server.application.interfaces.blob_data_store import (
    BlobDataStore,
    BlobDataStores,
)
from kitaru.server.application.interfaces.idempotency_key_repository import (
    IdempotencyKeyRepository,
)
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
from kitaru.server.application.services.connection_service import (
    ConnectionService,
)
from kitaru.server.application.services.evaluation_service import EvaluationService
from kitaru.server.application.services.insight_service import InsightService
from kitaru.server.application.services.investigation_service import (
    InvestigationService,
)
from kitaru.server.application.services.permission_service import PermissionService
from kitaru.server.application.services.plugin_service import PluginService
from kitaru.server.application.services.secret_service import SecretService
from kitaru.server.application.services.server_analytics import (
    ServerAnalytics,
)
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.application.services.tag_service import TagService
from kitaru.server.application.services.worker_service import WorkerService
from kitaru.server.domain.blob import BlobStorageBackend
from kitaru.server.domain.plugin import PluginKind


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


def get_connection_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> ConnectionService:
    """Return a connection service for the current request.

    Args:
        session: Request-scoped database session.
        settings: API settings for this process.

    Returns:
        Connection service bound to the SQL repositories.
    """
    return ConnectionService(
        repository=SQLConnectionRepository(session),
        secret_repository=SQLSecretRepository(
            session, AesGcmCipher(settings.SECRET_ENCRYPTION_KEY)
        ),
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


def get_analyzer_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> PluginService:
    """Return a plugin service bound to the analyzer kind.

    Args:
        session: Request-scoped database session.
        analytics: Analytics tracker for the current request.

    Returns:
        Plugin service bound to the SQL repositories.
    """
    return PluginService(
        kind=PluginKind.ANALYZER,
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
        import_repository=SQLImportRepository(session),
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
        analyzer_timeout_seconds=settings.ANALYZER_TASK_TIMEOUT_SECONDS,
        max_result_bytes=settings.MAX_TASK_RESULT_BYTES,
        evaluation_pair_limit=settings.EVALUATION_PAIR_LIMIT,
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
        import_repository=SQLImportRepository(session),
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
