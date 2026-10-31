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
"""Providers for services that build task transitions, and helpers using them."""

import uuid
from typing import Annotated

from fastapi import BackgroundTasks, Depends
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.db.encryption import AesGcmCipher
from kitaru.server.adapters.db.repositories.agent_repository import (
    SQLAgentRepository,
)
from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.blob_repository import SQLBlobRepository
from kitaru.server.adapters.db.repositories.cohort_version_repository import (
    SQLCohortVersionRepository,
)
from kitaru.server.adapters.db.repositories.connection_repository import (
    SQLConnectionRepository,
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
from kitaru.server.adapters.db.repositories.import_repository import (
    SQLImportRepository,
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
from kitaru.server.adapters.db.repositories.task_repository import SQLTaskRepository
from kitaru.server.adapters.db.repositories.worker_repository import (
    SQLWorkerRepository,
)
from kitaru.server.adapters.rest.dependencies.auth import get_auth_service
from kitaru.server.adapters.rest.dependencies.request import (
    get_app_settings,
    get_engine,
    get_ephemeral_workers,
    get_server_analytics,
    get_server_id_state,
    get_session,
)
from kitaru.server.adapters.rest.dependencies.services import (
    get_agent_service,
    get_blob_service,
    get_cohort_service,
    get_cohort_version_service,
    get_evaluation_service,
    get_evaluator_service,
    get_investigation_service,
    get_payload_store,
    get_session_node_service,
    get_session_service,
    get_tag_service,
    get_task_policy,
    get_worker_service,
)
from kitaru.server.adapters.rest.ephemeral_workers import EphemeralWorkerStarter
from kitaru.server.api.composition import build_event_dispatcher
from kitaru.server.api.config import APISettings
from kitaru.server.application.interfaces.ephemeral_workers import EphemeralWorkers
from kitaru.server.application.payload_store import PayloadStore
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.blob_service import BlobService
from kitaru.server.application.services.cohort_service import CohortService
from kitaru.server.application.services.cohort_version_service import (
    CohortVersionService,
)
from kitaru.server.application.services.evaluation_service import EvaluationService
from kitaru.server.application.services.experiment_run_service import (
    ExperimentRunService,
)
from kitaru.server.application.services.experiment_service import ExperimentService
from kitaru.server.application.services.import_service import ImportService
from kitaru.server.application.services.investigation_service import (
    InvestigationService,
)
from kitaru.server.application.services.job_service import JobService
from kitaru.server.application.services.plugin_service import PluginService
from kitaru.server.application.services.replay_service import ReplayService
from kitaru.server.application.services.sample_data_seeding import (
    SampleDataSeeder,
)
from kitaru.server.application.services.server_analytics import (
    ServerAnalytics,
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
        import_repository=SQLImportRepository(session),
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
        agent_version_repository=SQLAgentVersionRepository(session),
        plugin_repository=SQLPluginRepository(session),
        connection_repository=SQLConnectionRepository(session),
        transitions=_build_task_transitions(session, engine, analytics),
        policy=get_task_policy(settings),
    )


def get_import_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    engine: Annotated[AsyncEngine, Depends(get_engine)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> ImportService:
    """Return an import service for the current request.

    Args:
        session: Request-scoped database session.
        engine: Application database engine.
        analytics: Analytics tracker for the current request.

    Returns:
        Import service bound to the SQL repositories.
    """
    return ImportService(
        repository=SQLImportRepository(session),
        job_repository=SQLJobRepository(session),
        task_repository=SQLTaskRepository(session),
        session_repository=SQLSessionRepository(session, engine),
        agent_repository=SQLAgentRepository(session),
        agent_version_repository=SQLAgentVersionRepository(session),
        plugin_repository=SQLPluginRepository(session),
        blob_repository=SQLBlobRepository(session),
        connection_repository=SQLConnectionRepository(session),
        transitions=_build_task_transitions(session, engine, analytics),
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
        import_repository=SQLImportRepository(session),
        connection_repository=SQLConnectionRepository(session),
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
        liveness_timeout_seconds=settings.WORKER_LIVENESS_TIMEOUT_SECONDS,
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
        connection_repository=SQLConnectionRepository(session),
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
        connection_repository=SQLConnectionRepository(session),
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


def get_ephemeral_worker_starter(
    job_service: Annotated[JobService, Depends(get_job_service)],
    worker_service: Annotated[WorkerService, Depends(get_worker_service)],
    auth_service: Annotated[AuthService, Depends(get_auth_service)],
    ephemeral_workers: Annotated[
        EphemeralWorkers | None, Depends(get_ephemeral_workers)
    ],
    settings: Annotated[APISettings, Depends(get_app_settings)],
    server_id: Annotated[uuid.UUID | None, Depends(get_server_id_state)],
    background_tasks: BackgroundTasks,
) -> EphemeralWorkerStarter:
    """Return an ephemeral worker starter for the current request.

    Args:
        job_service: Job service.
        worker_service: Worker service.
        auth_service: Authentication service for the current request.
        ephemeral_workers: Ephemeral worker backend, None when none is
            configured.
        settings: API settings for this process.
        server_id: Persisted server id, None before startup resolved it.
        background_tasks: Tasks run after the response is sent.

    Returns:
        Starter bound to the request's services and background tasks.
    """
    return EphemeralWorkerStarter(
        job_service=job_service,
        worker_service=worker_service,
        auth_service=auth_service,
        ephemeral_workers=ephemeral_workers,
        settings=settings,
        server_id=server_id,
        background_tasks=background_tasks,
    )
