"""Replay DTO conversions."""

import uuid

from kitaru.api_models.v1.replay import (
    ReplayCreateRequest,
    ReplayListParams,
    ReplayResponse,
)
from kitaru.server.adapters.rest.mapping.replay_configs import replay_config_values
from kitaru.server.application.models.replay import ReplayCreate, ReplayFilter
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.replay_config import ReplayConfig


def replay_to_response(
    replay: Replay,
    config: ReplayConfig,
    result_session_id: uuid.UUID | None,
) -> ReplayResponse:
    """Convert a replay and its configuration to a response."""
    assert replay.created is not None
    assert replay.updated is not None
    override, tool_policy, evaluators = replay_config_values(config)
    return ReplayResponse(
        id=replay.id,
        job_id=replay.job_id,
        experiment_run_id=replay.experiment_run_id,
        baseline_session_id=replay.baseline_session_id,
        result_session_id=result_session_id,
        override=override,
        tool_policy=tool_policy,
        evaluators=evaluators,
        evaluate_baselines=replay.evaluate_baselines,
        status=replay.status,
        error=replay.error,
        created=replay.created,
        updated=replay.updated,
    )


def replay_list_params_to_filter(params: ReplayListParams) -> ReplayFilter:
    """Convert replay list query parameters."""
    return ReplayFilter(**params.model_dump(mode="python"))


def replay_create_to_command(body: ReplayCreateRequest) -> ReplayCreate:
    """Convert a replay create body."""
    return ReplayCreate.model_validate(body.model_dump(mode="python"))
