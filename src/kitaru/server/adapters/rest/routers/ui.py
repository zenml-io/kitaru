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
"""UI routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from kitaru.analytics.source import analytics_event_context
from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.experiment_run import EvaluationAggregateResponse
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.session import SessionListParams
from kitaru.api_models.v1.ui import (
    SampleDataCreateRequest,
    SampleDataResponse,
    SessionDetailWithEvaluationsResponse,
    SessionWithEvaluationsResponse,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_evaluation_service,
    get_experiment_run_evaluation_service,
    get_sample_data_seeder,
    get_session,
    get_session_service,
)
from kitaru.server.adapters.rest.mapping.evaluations import evaluation_to_response
from kitaru.server.adapters.rest.mapping.experiment_runs import (
    evaluation_aggregate_to_response,
)
from kitaru.server.adapters.rest.mapping.sessions import (
    session_list_params_to_filter,
    session_to_detail_response,
    session_to_response,
)
from kitaru.server.adapters.rest.responses import error_responses
from kitaru.server.adapters.rest.route import KitaruAPIRoute, read_only
from kitaru.server.application.interfaces.evaluation_repository import (
    EvaluationWithEvaluator,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.evaluation import EvaluationFilter
from kitaru.server.application.services.evaluation_service import EvaluationService
from kitaru.server.application.services.experiment_run_evaluation_service import (
    ExperimentRunEvaluationService,
)
from kitaru.server.application.services.sample_data_seeding import SampleDataSeeder
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.filtering import MAX_FILTER_IN_VALUES, FilterCondition
from kitaru.server.utils import paginate_all

router = APIRouter(route_class=KitaruAPIRoute)


async def _load_session_evaluations(
    service: EvaluationService,
    session_ids: list[uuid.UUID],
    actor: AuthContext,
) -> dict[uuid.UUID, list[EvaluationWithEvaluator]]:
    """Load the evaluations of the given sessions, grouped by session id.

    Args:
        service: Evaluation service.
        session_ids: Ids of the sessions to load evaluations for.
        actor: Caller context.

    Returns:
        Evaluations of each session, newest first.
    """
    grouped: dict[uuid.UUID, list[EvaluationWithEvaluator]] = {
        session_id: [] for session_id in session_ids
    }
    for start in range(0, len(session_ids), MAX_FILTER_IN_VALUES):
        membership = FilterCondition(
            field="session_id",
            op=FilterOp.IN,
            value=session_ids[start : start + MAX_FILTER_IN_VALUES],
        )
        items = await paginate_all(
            lambda cursor, expression=membership: service.list_evaluations(
                EvaluationFilter(expression=expression, cursor=cursor, size=1000),
                actor=actor,
            )
        )
        for item in items:
            grouped[item.evaluation.session_id].append(item)
    return grouped


@router.get("/sessions")
@read_only
async def list_sessions_with_evaluations(
    session_service: Annotated[SessionService, Depends(get_session_service)],
    evaluation_service: Annotated[EvaluationService, Depends(get_evaluation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[SessionListParams, Query()],
) -> Page[SessionWithEvaluationsResponse]:
    """List sessions, each with every evaluation of the session.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        session_service: Session service.
        evaluation_service: Evaluation service.
        actor: Caller context.
        params: Session list params.

    Returns:
        Page of sessions with their evaluations.
    """
    session_filter = session_list_params_to_filter(params)
    sessions, next_cursor = await session_service.list_sessions(
        session_filter, include_payloads=False, actor=actor
    )
    evaluations = await _load_session_evaluations(
        evaluation_service, [session.id for session in sessions], actor
    )
    return Page[SessionWithEvaluationsResponse](
        items=[
            SessionWithEvaluationsResponse(
                session=session_to_response(session),
                evaluations=[
                    evaluation_to_response(item) for item in evaluations[session.id]
                ],
            )
            for session in sessions
        ],
        next_cursor=next_cursor,
    )


@router.get("/sessions/{session_id}", responses=error_responses(404))
@read_only
async def get_session_with_evaluations(
    session_id: uuid.UUID,
    session_service: Annotated[SessionService, Depends(get_session_service)],
    evaluation_service: Annotated[EvaluationService, Depends(get_evaluation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> SessionDetailWithEvaluationsResponse:
    """Get a session by id, with every evaluation of the session.

    Clients observe HTTP 200 on success and 404 when no session has this
    id.

    Args:
        session_id: Id of the session.
        session_service: Session service.
        evaluation_service: Evaluation service.
        actor: Caller context.

    Returns:
        Stored session with its evaluations.
    """
    session = await session_service.get_session(session_id, actor=actor)
    evaluations = await _load_session_evaluations(
        evaluation_service, [session.id], actor
    )
    return SessionDetailWithEvaluationsResponse(
        session=session_to_detail_response(session),
        evaluations=[evaluation_to_response(item) for item in evaluations[session.id]],
    )


@router.get(
    "/experiment-runs/{experiment_run_id}/evaluation-aggregates",
    responses=error_responses(404),
)
@read_only
async def list_experiment_run_evaluation_aggregates(
    experiment_run_id: uuid.UUID,
    service: Annotated[
        ExperimentRunEvaluationService, Depends(get_experiment_run_evaluation_service)
    ],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> list[EvaluationAggregateResponse]:
    """Aggregate the evaluations linked to an experiment run's replays.

    The input set is the evaluations linked to the run's replays, grouped by
    name, evaluator version, and data type. Baseline and result sessions are
    aggregated separately, and each aggregate carries the per-replay
    evaluation values of the 50 most recent replays. Clients observe HTTP
    200 on success and 404 when no experiment run has this id.

    Args:
        experiment_run_id: Id of the experiment run.
        service: Experiment run evaluation service.
        actor: Caller context.

    Returns:
        One aggregate per evaluation name, evaluator version, and data
        type, sorted by name.
    """
    aggregates = await service.list_evaluation_aggregates(
        experiment_run_id, actor=actor
    )
    return [evaluation_aggregate_to_response(aggregate) for aggregate in aggregates]


@router.post(
    "/sample-data",
    status_code=status.HTTP_201_CREATED,
    responses=error_responses(409),
)
async def create_sample_data(
    seeder: Annotated[SampleDataSeeder, Depends(get_sample_data_seeder)],
    session: Annotated[AsyncSession, Depends(get_session)],
    actor: Annotated[AuthContext, Depends(authorize)],
    body: SampleDataCreateRequest | None = None,
) -> SampleDataResponse:
    """Seed the sample agent and everything recorded under it.

    Clients observe HTTP 201 on success and 409 when the agent name is
    already registered.

    Args:
        seeder: Sample data seeder.
        session: Request-scoped database session.
        actor: Caller context.
        body: Sample data create request, None uses the sample data's agent name.

    Returns:
        Agent the sample data was seeded under.
    """
    with analytics_event_context(sample_data=True):
        agent = await seeder.create_sample_agent(
            body.agent_name if body is not None else None, actor
        )
        # Commit the agent ahead of the seed because session numbers are
        # allocated on the agent row from a connection of their own, which
        # cannot see an uncommitted agent.
        await session.commit()
        await seeder.seed(agent, actor)
    return SampleDataResponse(agent_id=agent.id)
