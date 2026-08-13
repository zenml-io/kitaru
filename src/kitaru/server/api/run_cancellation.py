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
"""Experiment run cancellation, split across transactions to order its locks."""

import uuid
from collections.abc import Awaitable, Callable
from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from kitaru.analytics.client import AnalyticsClient
from kitaru.server.adapters.rest.dependencies import (
    get_analytics_client,
    get_experiment_run_service,
    get_server_analytics,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.replay import ReplayStatusCounts
from kitaru.server.application.services.experiment_run_service import (
    ExperimentRunService,
)
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.experiment_run import ExperimentRun

RunCanceler = Callable[
    [uuid.UUID, AuthContext], Awaitable[tuple[ExperimentRun, ReplayStatusCounts]]
]


async def cancel_run(
    experiment_run_id: uuid.UUID,
    actor: AuthContext,
    database: DatabaseService,
    analytics: AnalyticsClient,
) -> tuple[ExperimentRun, ReplayStatusCounts]:
    """Mark a run canceling, then cancel its jobs in a second transaction.

    The first transaction locks the run row alone and commits, so the second
    takes its task, job, replay, and run locks in that order without already
    holding the run row.

    A run left canceling by a failure part way through is finished by calling
    this again, because marking an already canceling run is a no-op.

    Args:
        experiment_run_id: Id of the run.
        actor: Caller context.
        database: Database service the phases open sessions against.
        analytics: Analytics client for this process.

    Raises:
        ExperimentRunNotFound: No run has this id.
        IllegalExperimentRunStatusTransition: The run is not running.

    Returns:
        Run carrying the cancel request, and its replay counts by status.
    """
    async for session in database.get_async_session():
        try:
            service = _build_service(session, analytics)
            await service.mark_run_canceling(experiment_run_id, actor=actor)
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    async for session in database.get_async_session():
        try:
            service = _build_service(session, analytics)
            result = await service.cancel_run_jobs(experiment_run_id, actor=actor)
            await session.commit()
            return result
        except Exception:
            await session.rollback()
            raise
    raise RuntimeError("Database service yielded no session.")


def _build_service(
    session: AsyncSession, analytics: AnalyticsClient
) -> ExperimentRunService:
    """Build a run service the same way a request does.

    Args:
        session: Session the service binds its repositories to.
        analytics: Analytics client for this process.

    Returns:
        Experiment run service.
    """
    tracker = get_server_analytics(session, analytics)
    return get_experiment_run_service(session, tracker)


def get_run_canceler(
    request: Request,
    analytics: Annotated[AnalyticsClient, Depends(get_analytics_client)],
) -> RunCanceler:
    """Return the run cancellation flow, bound to this process.

    Args:
        request: Incoming request.
        analytics: Analytics client for this process.

    Returns:
        Callable cancelling a run across its own transactions.
    """
    database: DatabaseService = request.app.state.database

    async def cancel(
        experiment_run_id: uuid.UUID, actor: AuthContext
    ) -> tuple[ExperimentRun, ReplayStatusCounts]:
        return await cancel_run(experiment_run_id, actor, database, analytics)

    return cancel
