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

from fastapi import APIRouter, Depends, Query

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.evaluation import EvaluationDataType
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.session import SessionListParams
from kitaru.api_models.v1.ui import (
    EvaluationAggregateResponse,
    EvaluationStats,
    EvaluationValue,
    ReplayEvaluationValues,
    SessionWithEvaluationsResponse,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_evaluation_service,
    get_experiment_run_service,
    get_replay_service,
    get_session_service,
)
from kitaru.server.adapters.rest.mapping.evaluations import evaluation_to_response
from kitaru.server.adapters.rest.mapping.sessions import (
    session_list_params_to_filter,
    session_to_response,
)
from kitaru.server.adapters.rest.route import KitaruAPIRoute
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
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.domain.evaluation import Evaluation
from kitaru.server.filtering import MAX_FILTER_IN_VALUES, FilterCondition
from kitaru.server.utils import paginate_all

router = APIRouter(route_class=KitaruAPIRoute)

MAX_VALUE_REPLAYS = 50


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


def _evaluation_stats(
    data_type: EvaluationDataType, evaluations: list[Evaluation]
) -> EvaluationStats:
    """Compute the stats of evaluations sharing a name and data type.

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
        score=evaluation.score, value=evaluation.value, passed=evaluation.passed
    )


@router.get("/sessions")
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
        session_filter, actor=actor
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
async def get_session_with_evaluations(
    session_id: uuid.UUID,
    session_service: Annotated[SessionService, Depends(get_session_service)],
    evaluation_service: Annotated[EvaluationService, Depends(get_evaluation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> SessionWithEvaluationsResponse:
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
    return SessionWithEvaluationsResponse(
        session=session_to_response(session),
        evaluations=[evaluation_to_response(item) for item in evaluations[session.id]],
    )


@router.get("/experiment-runs/{experiment_run_id}/evaluation-aggregates")
async def list_experiment_run_evaluation_aggregates(
    experiment_run_id: uuid.UUID,
    run_service: Annotated[ExperimentRunService, Depends(get_experiment_run_service)],
    replay_service: Annotated[ReplayService, Depends(get_replay_service)],
    evaluation_service: Annotated[EvaluationService, Depends(get_evaluation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> list[EvaluationAggregateResponse]:
    """Aggregate the evaluations of an experiment run's replays.

    Baseline and result sessions are aggregated separately, and each
    aggregate carries the per-replay evaluation values of the 50 most
    recent replays. Clients observe HTTP 200 on success and 404 when no
    experiment run has this id.

    Args:
        experiment_run_id: Id of the experiment run.
        run_service: Experiment run service.
        replay_service: Replay service.
        evaluation_service: Evaluation service.
        actor: Caller context.

    Returns:
        One aggregate per evaluation name and data type pair, sorted by
        name.
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
        details.result_session_id
        for details in replays
        if details.result_session_id is not None
    ]
    evaluations = await _load_session_evaluations(
        evaluation_service, baseline_session_ids + result_session_ids, actor
    )
    session_evaluations: dict[
        uuid.UUID, dict[tuple[str, EvaluationDataType], Evaluation]
    ] = {
        session_id: {
            (item.evaluation.name, item.evaluation.data_type): item.evaluation
            for item in reversed(items)
        }
        for session_id, items in evaluations.items()
    }
    baseline_groups: dict[tuple[str, EvaluationDataType], list[Evaluation]] = (
        defaultdict(list)
    )
    for session_id in baseline_session_ids:
        for key, evaluation in session_evaluations.get(session_id, {}).items():
            baseline_groups[key].append(evaluation)
    result_groups: dict[tuple[str, EvaluationDataType], list[Evaluation]] = defaultdict(
        list
    )
    for session_id in result_session_ids:
        for key, evaluation in session_evaluations.get(session_id, {}).items():
            result_groups[key].append(evaluation)
    recent = replays[-MAX_VALUE_REPLAYS:]
    aggregates: list[EvaluationAggregateResponse] = []
    for key in sorted(set(baseline_groups) | set(result_groups)):
        name, data_type = key
        aggregates.append(
            EvaluationAggregateResponse(
                name=name,
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
                            session_evaluations.get(details.result_session_id, {}).get(
                                key
                            )
                            if details.result_session_id is not None
                            else None
                        ),
                    )
                    for details in recent
                ],
            )
        )
    return aggregates
