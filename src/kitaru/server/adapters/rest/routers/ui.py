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
from collections import Counter, defaultdict
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from kitaru.analytics.source import analytics_event_context
from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.evaluation import EvaluationDataType
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.session import SessionListParams
from kitaru.api_models.v1.ui import (
    EvaluationAggregateResponse,
    EvaluationStats,
    EvaluationValue,
    ReplayEvaluationValues,
    SampleDataCreateRequest,
    SampleDataResponse,
    SessionDetailWithEvaluationsResponse,
    SessionWithEvaluationsResponse,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_evaluation_service,
    get_experiment_run_service,
    get_replay_service,
    get_sample_data_seeder,
    get_session,
    get_session_service,
)
from kitaru.server.adapters.rest.mapping.evaluations import evaluation_to_response
from kitaru.server.adapters.rest.mapping.sessions import (
    session_list_params_to_filter,
    session_to_detail_response,
    session_to_response,
)
from kitaru.server.adapters.rest.route import KitaruAPIRoute, read_only
from kitaru.server.application.interfaces.evaluation_repository import (
    EvaluationWithEvaluator,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.evaluation import EvaluationFilter
from kitaru.server.application.models.replay import ReplayFilter
from kitaru.server.application.services.evaluation_service import EvaluationService
from kitaru.server.application.services.experiment_run_service import (
    ExperimentRunService,
)
from kitaru.server.application.services.replay_service import ReplayService
from kitaru.server.application.services.sample_data_seeding import SampleDataSeeder
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.domain.evaluation import Evaluation
from kitaru.server.filtering import MAX_FILTER_IN_VALUES, AndExpression, FilterCondition
from kitaru.server.utils import paginate_all

router = APIRouter(route_class=KitaruAPIRoute)

MAX_VALUE_REPLAYS = 50

GroupKey = tuple[str, uuid.UUID | None, EvaluationDataType]


def _group_key(evaluation: Evaluation) -> GroupKey:
    """Build the aggregate group key of an evaluation.

    Args:
        evaluation: Evaluation to key.

    Returns:
        (name, evaluator_version_id, data_type) key, version id None for a
        manual evaluation.
    """
    return (evaluation.name, evaluation.evaluator_version_id, evaluation.data_type)


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


async def _load_manual_session_evaluations(
    service: EvaluationService,
    session_ids: list[uuid.UUID],
    actor: AuthContext,
) -> dict[uuid.UUID, list[EvaluationWithEvaluator]]:
    """Load the manual evaluations of the given sessions, grouped by session id.

    Args:
        service: Evaluation service.
        session_ids: Ids of the sessions to load manual evaluations for.
        actor: Caller context.

    Returns:
        Manual evaluations of each session, newest first.
    """
    grouped: dict[uuid.UUID, list[EvaluationWithEvaluator]] = {
        session_id: [] for session_id in session_ids
    }
    for start in range(0, len(session_ids), MAX_FILTER_IN_VALUES):
        expression = AndExpression(
            operands=(
                FilterCondition(
                    field="session_id",
                    op=FilterOp.IN,
                    value=session_ids[start : start + MAX_FILTER_IN_VALUES],
                ),
                FilterCondition(field="evaluator_version_id", op=FilterOp.IS_NULL),
            )
        )
        items = await paginate_all(
            lambda cursor, expression=expression: service.list_evaluations(
                EvaluationFilter(expression=expression, cursor=cursor, size=1000),
                actor=actor,
            )
        )
        for item in items:
            grouped[item.evaluation.session_id].append(item)
    return grouped


def _shared_value(values: list[float | None]) -> float | None:
    """Return the value shared by every row, None when any differs or is missing.

    Args:
        values: Per-row optional values to compare.

    Returns:
        Shared value, or None when the rows differ or any value is missing.
    """
    if not values or any(value is None for value in values):
        return None
    first = values[0]
    return first if all(value == first for value in values) else None


def _evaluation_stats(
    data_type: EvaluationDataType, evaluations: list[Evaluation]
) -> EvaluationStats:
    """Compute the stats of evaluations sharing a group key.

    Args:
        data_type: Data type of the evaluations.
        evaluations: Evaluations to aggregate.

    Returns:
        Stats of the evaluations.
    """
    scores = [
        float(evaluation.score)
        for evaluation in evaluations
        if evaluation.score is not None
    ]
    flags = [
        evaluation.passed for evaluation in evaluations if evaluation.passed is not None
    ]
    values = [
        evaluation.value for evaluation in evaluations if evaluation.value is not None
    ]
    scorable = data_type in (EvaluationDataType.FLOAT, EvaluationDataType.BOOL)
    return EvaluationStats(
        count=len(evaluations),
        mean=sum(scores) / len(scores) if scorable and scores else None,
        min=min(scores) if scorable and scores else None,
        max=max(scores) if scorable and scores else None,
        pass_rate=sum(flags) / len(flags) if flags else None,
        value_counts=dict(Counter(values))
        if data_type is EvaluationDataType.CATEGORICAL
        else None,
        min_score=_shared_value([evaluation.min_score for evaluation in evaluations]),
        max_score=_shared_value([evaluation.max_score for evaluation in evaluations]),
        target_score=_shared_value(
            [evaluation.target_score for evaluation in evaluations]
        ),
    )


def _evaluation_value(evaluation: Evaluation | None) -> EvaluationValue | None:
    """Map an evaluation to its value, None for a missing evaluation.

    Args:
        evaluation: Evaluation to map.

    Returns:
        Value of the evaluation.
    """
    if evaluation is None:
        return None
    return EvaluationValue(
        score=evaluation.score,
        value=evaluation.value,
        passed=evaluation.passed,
        min_score=evaluation.min_score,
        max_score=evaluation.max_score,
        target_score=evaluation.target_score,
    )


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


@router.get("/sessions/{session_id}")
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


@router.get("/experiment-runs/{experiment_run_id}/evaluation-aggregates")
@read_only
async def list_experiment_run_evaluation_aggregates(
    experiment_run_id: uuid.UUID,
    run_service: Annotated[ExperimentRunService, Depends(get_experiment_run_service)],
    replay_service: Annotated[ReplayService, Depends(get_replay_service)],
    evaluation_service: Annotated[EvaluationService, Depends(get_evaluation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> list[EvaluationAggregateResponse]:
    """Aggregate the evaluations linked to an experiment run's replays.

    The input set is the evaluations linked to the run's replays plus the
    manual evaluations of their baseline and result sessions, grouped by
    name, evaluator version, and data type. Baseline and result sessions are
    aggregated separately, and each aggregate carries the per-replay
    evaluation values of the 50 most recent replays. Clients observe HTTP
    200 on success and 404 when no experiment run has this id.

    Args:
        experiment_run_id: Id of the experiment run.
        run_service: Experiment run service.
        replay_service: Replay service.
        evaluation_service: Evaluation service.
        actor: Caller context.

    Returns:
        One aggregate per evaluation name, evaluator version, and data
        type, sorted by name.
    """
    await run_service.get_run(experiment_run_id, actor=actor)
    membership = FilterCondition(
        field="experiment_run_id", op=FilterOp.EQ, value=experiment_run_id
    )
    replays = await paginate_all(
        lambda cursor: replay_service.list_replays(
            ReplayFilter(expression=membership, cursor=cursor, size=1000),
            actor=actor,
        )
    )
    replays.sort(key=lambda details: details.replay.id)
    baseline_session_ids = list(
        dict.fromkeys(details.replay.baseline_session_id for details in replays)
    )
    result_session_ids = [
        details.replay.result_session_id
        for details in replays
        if details.replay.result_session_id is not None
    ]
    linked = await evaluation_service.list_replay_evaluations(
        [details.replay.id for details in replays], actor
    )
    manual = await _load_manual_session_evaluations(
        evaluation_service, baseline_session_ids + result_session_ids, actor
    )

    # Each group holds at most one row per session by construction: one
    # evaluator config per run, if_missing adoption pins one row, and a
    # duplicate name within a task is rejected at write time.
    session_evaluations: dict[uuid.UUID, dict[GroupKey, Evaluation]] = defaultdict(dict)
    evaluator_info: dict[uuid.UUID | None, tuple[str | None, int | None]] = {}
    for _, item in linked:
        evaluation = item.evaluation
        session_evaluations[evaluation.session_id][_group_key(evaluation)] = evaluation
        evaluator_info[evaluation.evaluator_version_id] = (
            item.evaluator_name,
            item.evaluator_version,
        )
    for items in manual.values():
        for item in items:
            evaluation = item.evaluation
            session_evaluations[evaluation.session_id][_group_key(evaluation)] = (
                evaluation
            )
            evaluator_info[evaluation.evaluator_version_id] = (
                item.evaluator_name,
                item.evaluator_version,
            )

    baseline_groups: dict[GroupKey, list[Evaluation]] = defaultdict(list)
    for session_id in baseline_session_ids:
        for key, evaluation in session_evaluations.get(session_id, {}).items():
            baseline_groups[key].append(evaluation)
    result_groups: dict[GroupKey, list[Evaluation]] = defaultdict(list)
    for session_id in result_session_ids:
        for key, evaluation in session_evaluations.get(session_id, {}).items():
            result_groups[key].append(evaluation)
    recent = replays[-MAX_VALUE_REPLAYS:]
    aggregates: list[EvaluationAggregateResponse] = []
    for key in sorted(
        set(baseline_groups) | set(result_groups),
        key=lambda group_key: (
            group_key[0],
            group_key[2],
            str(group_key[1]) if group_key[1] is not None else "",
        ),
    ):
        name, evaluator_version_id, data_type = key
        evaluator_name, evaluator_version = evaluator_info.get(
            evaluator_version_id, (None, None)
        )
        aggregates.append(
            EvaluationAggregateResponse(
                name=name,
                evaluator_version_id=evaluator_version_id,
                evaluator_name=evaluator_name,
                evaluator_version=evaluator_version,
                data_type=data_type,
                baseline=_evaluation_stats(data_type, baseline_groups.get(key, [])),
                result=_evaluation_stats(data_type, result_groups.get(key, [])),
                replays=[
                    ReplayEvaluationValues(
                        replay_id=details.replay.id,
                        baseline=_evaluation_value(
                            session_evaluations.get(
                                details.replay.baseline_session_id, {}
                            ).get(key)
                        ),
                        result=_evaluation_value(
                            session_evaluations.get(
                                details.replay.result_session_id, {}
                            ).get(key)
                            if details.replay.result_session_id is not None
                            else None
                        ),
                    )
                    for details in recent
                ],
            )
        )
    return aggregates


@router.post("/sample-data", status_code=status.HTTP_201_CREATED)
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
