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

import contextlib
import uuid
from collections.abc import Awaitable, Callable
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from kitaru.api_models.v1.filter import FilterOp
from kitaru.server.adapters.rest.dependencies import (
    get_agent_service,
    get_app_settings,
    get_cohort_service,
    get_engine,
    get_experiment_service,
    get_investigation_service,
    get_job_service,
    get_replay_service,
    get_server_analytics,
    get_session,
    get_session_service,
)
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.cohort import CohortFilter
from kitaru.server.application.models.experiment import ExperimentFilter
from kitaru.server.application.models.investigation import InvestigationFilter
from kitaru.server.application.models.replay import ReplayFilter
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.agent import AgentNotFound
from kitaru.server.domain.base import NotFoundError
from kitaru.server.filtering import AndExpression, FilterCondition

AgentDeleter = Callable[[uuid.UUID, AuthContext], Awaitable[None]]

# Page size for every phase of the deletion, so no single transaction holds
# locks on an unbounded slice of the agent's subtree.
DELETE_BATCH_SIZE = 100


async def delete_agent(
    agent_id: uuid.UUID,
    actor: AuthContext,
    session: AsyncSession,
    engine: AsyncEngine,
    settings: APISettings,
    analytics: ServerAnalytics,
) -> None:
    """Delete an agent's subtree in phases, each phase its own transaction.

    Every phase commits on the one request session, so the deletion holds a
    single pooled connection. Experiments go before cohorts because runs pin
    cohort versions. Experiments, cohorts, and investigations go before
    sessions because replay baselines, cohort membership, and investigation
    membership restrict session deletion. Standalone replays of a session go
    right before that session. The agent row goes last, so its own cascade
    reaches only agent versions, agent version secrets, tag links, and the
    agent's tasks.

    Rows a concurrent request already deleted are skipped, so two deletes of
    the same agent both complete.

    Args:
        agent_id: Id of the agent.
        actor: Caller context.
        session: Request session every phase commits on.
        engine: Application database engine.
        settings: API settings for this process.
        analytics: Analytics tracker for the current request.

    Raises:
        AgentNotFound: No agent has this id.
    """
    agent_service = get_agent_service(session, analytics)
    await agent_service.get_agent(agent_id, actor=actor)

    by_agent = FilterCondition(field="agent_id", op=FilterOp.EQ, value=agent_id)

    experiment_service = get_experiment_service(session, engine, analytics)

    async def fetch_experiments() -> list[uuid.UUID]:
        experiment_filter = ExperimentFilter(
            expression=by_agent, size=DELETE_BATCH_SIZE
        )
        pairs, _ = await experiment_service.list_experiments(
            experiment_filter, actor=actor
        )
        return [experiment.id for experiment, _ in pairs]

    async def delete_experiment(experiment_id: uuid.UUID) -> None:
        await experiment_service.delete_experiment(experiment_id, actor=actor)

    await _delete_all(session, fetch_experiments, delete_experiment)

    cohort_service = get_cohort_service(session, analytics)

    async def fetch_cohorts() -> list[uuid.UUID]:
        cohort_filter = CohortFilter(expression=by_agent, size=DELETE_BATCH_SIZE)
        cohorts, _ = await cohort_service.list_cohorts(cohort_filter, actor=actor)
        return [cohort.id for cohort in cohorts]

    async def delete_cohort(cohort_id: uuid.UUID) -> None:
        await cohort_service.delete_cohort(cohort_id, actor=actor)

    await _delete_all(session, fetch_cohorts, delete_cohort)

    investigation_service = get_investigation_service(session, engine, analytics)

    async def fetch_investigations() -> list[uuid.UUID]:
        investigation_filter = InvestigationFilter(
            expression=by_agent, size=DELETE_BATCH_SIZE
        )
        investigations, _ = await investigation_service.list_investigations(
            investigation_filter, actor=actor
        )
        return [investigation.id for investigation in investigations]

    async def delete_investigation(investigation_id: uuid.UUID) -> None:
        await investigation_service.delete_investigation(investigation_id, actor=actor)

    await _delete_all(session, fetch_investigations, delete_investigation)

    session_service = get_session_service(session, engine, analytics)
    replay_service = get_replay_service(session, engine, analytics)
    job_service = get_job_service(session, engine, settings, analytics)

    async def fetch_sessions() -> list[uuid.UUID]:
        session_filter = SessionFilter(expression=by_agent, size=DELETE_BATCH_SIZE)
        sessions, _ = await session_service.list_sessions(session_filter, actor=actor)
        return [item.id for item in sessions]

    async def delete_session(session_id: uuid.UUID) -> None:
        # Standalone replays pin their baseline session and belong to no
        # experiment, so their jobs go right before the session.
        async def fetch_standalone_replay_jobs() -> list[uuid.UUID]:
            replay_filter = ReplayFilter(
                expression=AndExpression(
                    operands=(
                        FilterCondition(
                            field="baseline_session_id",
                            op=FilterOp.EQ,
                            value=session_id,
                        ),
                        FilterCondition(field="experiment_run_id", op=FilterOp.IS_NULL),
                    )
                ),
                size=DELETE_BATCH_SIZE,
            )
            replays, _ = await replay_service.list_replays(replay_filter, actor=actor)
            return [bundle.replay.job_id for bundle in replays]

        async def delete_job(job_id: uuid.UUID) -> None:
            await job_service.delete_job(job_id, actor=actor)

        await _delete_all(session, fetch_standalone_replay_jobs, delete_job)
        # The per-session delete cascades its nodes, so no separate node phase.
        await session_service.delete_session(session_id, actor=actor)

    await _delete_all(session, fetch_sessions, delete_session)

    # A concurrent delete of the same agent may have finished first.
    with contextlib.suppress(AgentNotFound):
        await agent_service.delete_agent(agent_id, actor=actor)
    await session.commit()


async def _delete_all(
    session: AsyncSession,
    fetch_page: Callable[[], Awaitable[list[uuid.UUID]]],
    delete_one: Callable[[uuid.UUID], Awaitable[None]],
) -> None:
    """Delete every id a paged fetch returns, one batch per transaction.

    Each pass fetches the first page and deletes it, so the next pass's
    fetch never needs a cursor, the ids it would have pointed past are gone.
    An id a concurrent request already deleted is skipped.

    Args:
        session: Session the batches commit on.
        fetch_page: Load up to one batch of ids still needing deletion.
        delete_one: Delete a single id.
    """
    while True:
        page = await fetch_page()
        for row_id in page:
            try:
                await delete_one(row_id)
            except NotFoundError:
                continue
        await session.commit()
        if not page:
            return


def get_agent_deleter(
    session: Annotated[AsyncSession, Depends(get_session)],
    engine: Annotated[AsyncEngine, Depends(get_engine)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
    analytics: Annotated[ServerAnalytics, Depends(get_server_analytics)],
) -> AgentDeleter:
    """Return the agent deletion flow for the current request.

    Args:
        session: Request-scoped database session.
        engine: Application database engine.
        settings: API settings for this process.
        analytics: Analytics tracker for the current request.

    Returns:
        Callable deleting an agent's subtree across its own transactions.
    """

    async def delete(agent_id: uuid.UUID, actor: AuthContext) -> None:
        await delete_agent(agent_id, actor, session, engine, settings, analytics)

    return delete
