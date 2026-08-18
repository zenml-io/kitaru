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
"""Agent deletion, split across transactions to bound each phase's locks."""

import uuid
from collections.abc import Awaitable, Callable
from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from kitaru.analytics.client import AnalyticsClient
from kitaru.api_models.v1.filter import FilterOp
from kitaru.server.adapters.rest.dependencies import (
    get_agent_service,
    get_analytics_client,
    get_cohort_service,
    get_experiment_service,
    get_investigation_service,
    get_server_analytics,
    get_session_service,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.cohort import CohortFilter
from kitaru.server.application.models.experiment import ExperimentFilter
from kitaru.server.application.models.investigation import InvestigationFilter
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.cohort_service import CohortService
from kitaru.server.application.services.experiment_service import ExperimentService
from kitaru.server.application.services.investigation_service import (
    InvestigationService,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.database.service import DatabaseService
from kitaru.server.filtering import FilterCondition

AgentDeleter = Callable[[uuid.UUID, AuthContext], Awaitable[None]]

# Page size for every phase of the deletion, so no single transaction holds
# locks on an unbounded slice of the agent's subtree.
DELETE_BATCH_SIZE = 100


async def delete_agent(
    agent_id: uuid.UUID,
    actor: AuthContext,
    database: DatabaseService,
    analytics: AnalyticsClient,
) -> None:
    """Delete an agent's subtree in phases, each phase its own transaction.

    Experiments go before cohorts because runs pin cohort versions.
    Experiments, cohorts, and investigations go before sessions because
    replay baselines, cohort membership, and investigation membership
    restrict session deletion. The agent row goes last, so its own cascade
    reaches only agent versions, agent version secrets, tag links, and the
    agent's tasks.

    Args:
        agent_id: Id of the agent.
        actor: Caller context.
        database: Database service the phases open sessions against.
        analytics: Analytics client for this process.

    Raises:
        AgentNotFound: No agent has this id.
    """
    engine = database.engine

    async for session in database.get_async_session():
        try:
            service = _build_agent_service(session, analytics)
            await service.get_agent(agent_id, actor=actor)
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    async def fetch_experiments(session: AsyncSession) -> list[uuid.UUID]:
        service = _build_experiment_service(session, engine, analytics)
        experiment_filter = ExperimentFilter(
            expression=FilterCondition(
                field="agent_id", op=FilterOp.EQ, value=agent_id
            ),
            size=DELETE_BATCH_SIZE,
        )
        pairs, _ = await service.list_experiments(experiment_filter, actor=actor)
        return [experiment.id for experiment, _ in pairs]

    async def delete_experiment(
        session: AsyncSession, experiment_id: uuid.UUID
    ) -> None:
        service = _build_experiment_service(session, engine, analytics)
        await service.delete_experiment(experiment_id, actor=actor)

    await _delete_all(database, fetch_experiments, delete_experiment)

    async def fetch_cohorts(session: AsyncSession) -> list[uuid.UUID]:
        service = _build_cohort_service(session, analytics)
        cohort_filter = CohortFilter(
            expression=FilterCondition(
                field="agent_id", op=FilterOp.EQ, value=agent_id
            ),
            size=DELETE_BATCH_SIZE,
        )
        cohorts, _ = await service.list_cohorts(cohort_filter, actor=actor)
        return [cohort.id for cohort in cohorts]

    async def delete_cohort(session: AsyncSession, cohort_id: uuid.UUID) -> None:
        service = _build_cohort_service(session, analytics)
        await service.delete_cohort(cohort_id, actor=actor)

    await _delete_all(database, fetch_cohorts, delete_cohort)

    async def fetch_investigations(session: AsyncSession) -> list[uuid.UUID]:
        service = _build_investigation_service(session, engine, analytics)
        investigation_filter = InvestigationFilter(
            expression=FilterCondition(
                field="agent_id", op=FilterOp.EQ, value=agent_id
            ),
            size=DELETE_BATCH_SIZE,
        )
        investigations, _ = await service.list_investigations(
            investigation_filter, actor=actor
        )
        return [investigation.id for investigation in investigations]

    async def delete_investigation(
        session: AsyncSession, investigation_id: uuid.UUID
    ) -> None:
        service = _build_investigation_service(session, engine, analytics)
        await service.delete_investigation(investigation_id, actor=actor)

    await _delete_all(database, fetch_investigations, delete_investigation)

    async def fetch_sessions(session: AsyncSession) -> list[uuid.UUID]:
        service = _build_session_service(session, engine, analytics)
        session_filter = SessionFilter(
            expression=FilterCondition(
                field="agent_id", op=FilterOp.EQ, value=agent_id
            ),
            size=DELETE_BATCH_SIZE,
        )
        sessions, _ = await service.list_sessions(session_filter, actor=actor)
        return [item.id for item in sessions]

    async def delete_session(session: AsyncSession, session_id: uuid.UUID) -> None:
        # The per-session delete cascades its nodes, so no separate node phase.
        service = _build_session_service(session, engine, analytics)
        await service.delete_session(session_id, actor=actor)

    await _delete_all(database, fetch_sessions, delete_session)

    async for session in database.get_async_session():
        try:
            service = _build_agent_service(session, analytics)
            await service.delete_agent(agent_id, actor=actor)
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def _delete_all(
    database: DatabaseService,
    fetch_page: Callable[[AsyncSession], Awaitable[list[uuid.UUID]]],
    delete_one: Callable[[AsyncSession, uuid.UUID], Awaitable[None]],
) -> None:
    """Delete every id a paged fetch returns, one batch per transaction.

    Each pass fetches the first page and deletes it, so the next pass's
    fetch never needs a cursor, the ids it would have pointed past are gone.

    Args:
        database: Database service the phases open sessions against.
        fetch_page: Load up to one batch of ids still needing deletion.
        delete_one: Delete a single id.
    """
    while True:
        page: list[uuid.UUID] = []
        async for session in database.get_async_session():
            try:
                page = await fetch_page(session)
                for row_id in page:
                    await delete_one(session, row_id)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
        if not page:
            return


def _build_agent_service(
    session: AsyncSession, analytics: AnalyticsClient
) -> AgentService:
    """Build an agent service the same way a request does.

    Args:
        session: Session the service binds its repository to.
        analytics: Analytics client for this process.

    Returns:
        Agent service.
    """
    tracker = get_server_analytics(session, analytics)
    return get_agent_service(session, tracker)


def _build_experiment_service(
    session: AsyncSession, engine: AsyncEngine, analytics: AnalyticsClient
) -> ExperimentService:
    """Build an experiment service the same way a request does.

    Args:
        session: Session the service binds its repositories to.
        engine: Application database engine.
        analytics: Analytics client for this process.

    Returns:
        Experiment service.
    """
    tracker = get_server_analytics(session, analytics)
    return get_experiment_service(session, engine, tracker)


def _build_cohort_service(
    session: AsyncSession, analytics: AnalyticsClient
) -> CohortService:
    """Build a cohort service the same way a request does.

    Args:
        session: Session the service binds its repositories to.
        analytics: Analytics client for this process.

    Returns:
        Cohort service.
    """
    tracker = get_server_analytics(session, analytics)
    return get_cohort_service(session, tracker)


def _build_investigation_service(
    session: AsyncSession, engine: AsyncEngine, analytics: AnalyticsClient
) -> InvestigationService:
    """Build an investigation service the same way a request does.

    Args:
        session: Session the service binds its repositories to.
        engine: Application database engine.
        analytics: Analytics client for this process.

    Returns:
        Investigation service.
    """
    tracker = get_server_analytics(session, analytics)
    return get_investigation_service(session, engine, tracker)


def _build_session_service(
    session: AsyncSession, engine: AsyncEngine, analytics: AnalyticsClient
) -> SessionService:
    """Build a session service the same way a request does.

    Args:
        session: Session the service binds its repositories to.
        engine: Application database engine.
        analytics: Analytics client for this process.

    Returns:
        Session service.
    """
    tracker = get_server_analytics(session, analytics)
    return get_session_service(session, engine, tracker)


def get_agent_deleter(
    request: Request,
    analytics: Annotated[AnalyticsClient, Depends(get_analytics_client)],
) -> AgentDeleter:
    """Return the agent deletion flow, bound to this process.

    Args:
        request: Incoming request.
        analytics: Analytics client for this process.

    Returns:
        Callable deleting an agent's subtree across its own transactions.
    """
    database: DatabaseService = request.app.state.database

    async def delete(agent_id: uuid.UUID, actor: AuthContext) -> None:
        await delete_agent(agent_id, actor, database, analytics)

    return delete
