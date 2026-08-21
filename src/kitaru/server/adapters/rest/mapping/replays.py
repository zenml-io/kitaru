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
"""Replay DTO conversions."""

from kitaru.api_models.v1.replay import (
    ReplayCreateRequest,
    ReplayListParams,
    ReplayResponse,
    ToolLookupResponse,
)
from kitaru.server.adapters.rest.mapping.filtering import filter_to_expression
from kitaru.server.adapters.rest.mapping.replay_config import (
    evaluator_config_input,
    evaluator_config_to_wire,
    replay_override_to_domain,
    replay_override_to_wire,
    tool_policy_to_domain,
    tool_policy_to_wire,
)
from kitaru.server.application.models.replay import (
    ReplayCreate,
    ReplayFilter,
    ToolLookupResult,
)
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.replay_config import ReplayConfig


def replay_create_to_command(body: ReplayCreateRequest) -> ReplayCreate:
    """Convert a replay create request to its application command.

    Args:
        body: Replay create request.

    Returns:
        Create command.
    """
    return ReplayCreate(
        baseline_session_id=body.baseline_session_id,
        agent_version_id=body.agent_version_id,
        override=(
            replay_override_to_domain(body.override)
            if body.override is not None
            else None
        ),
        tool_policy=(
            tool_policy_to_domain(body.tool_policy)
            if body.tool_policy is not None
            else None
        ),
        evaluators=[evaluator_config_input(config) for config in body.evaluators],
        evaluate_baselines=body.evaluate_baselines,
    )


def replay_to_response(replay: Replay, config: ReplayConfig) -> ReplayResponse:
    """Convert a replay and its config to the response DTO.

    Args:
        replay: Stored replay.
        config: The replay's config.

    Returns:
        Replay response, inlining the config.
    """
    assert replay.created is not None
    assert replay.updated is not None
    return ReplayResponse(
        id=replay.id,
        job_id=replay.job_id,
        experiment_run_id=replay.experiment_run_id,
        baseline_session_id=replay.baseline_session_id,
        result_session_id=replay.result_session_id,
        override=(
            replay_override_to_wire(config.override)
            if config.override is not None
            else None
        ),
        tool_policy=tool_policy_to_wire(config.tool_policy),
        evaluators=[
            evaluator_config_to_wire(evaluator) for evaluator in config.evaluators
        ],
        evaluate_baselines=replay.evaluate_baselines,
        status=replay.status,
        error=replay.error,
        created=replay.created,
        updated=replay.updated,
    )


def replay_list_params_to_filter(params: ReplayListParams) -> ReplayFilter:
    """Convert replay list params to the application filter.

    Args:
        params: Replay list params.

    Returns:
        Replay filter.
    """
    return ReplayFilter(
        expression=filter_to_expression(params.filter)
        if params.filter is not None
        else None,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )


def tool_lookup_result_to_response(result: ToolLookupResult) -> ToolLookupResponse:
    """Convert a tool lookup result to the response DTO.

    Args:
        result: Tool lookup result.

    Returns:
        Tool lookup response.
    """
    return ToolLookupResponse(found=result.found, result=result.result)
