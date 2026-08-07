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
"""Experiment DTO conversions."""

from typing import Any

from kitaru.api_models.v1.experiment import (
    ExperimentCreateRequest,
    ExperimentListParams,
    ExperimentResponse,
    ExperimentUpdateRequest,
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
from kitaru.server.application.models.experiment import (
    ExperimentCreate,
    ExperimentFilter,
    ExperimentUpdate,
)
from kitaru.server.domain.experiment import Experiment
from kitaru.server.domain.replay_config import ReplayConfig


def experiment_to_response(
    experiment: Experiment, config: ReplayConfig
) -> ExperimentResponse:
    """Convert an experiment and its replay config to the response DTO.

    Args:
        experiment: Stored experiment.
        config: The experiment's replay config.

    Returns:
        Experiment response, inlining the replay config.
    """
    assert experiment.created is not None
    assert experiment.updated is not None
    return ExperimentResponse(
        id=experiment.id,
        owner_id=experiment.owner_id,
        name=experiment.name,
        description=experiment.description,
        agent_id=experiment.agent_id,
        override=(
            replay_override_to_wire(config.override)
            if config.override is not None
            else None
        ),
        tool_policy=tool_policy_to_wire(config.tool_policy),
        evaluators=[
            evaluator_config_to_wire(evaluator) for evaluator in config.evaluators
        ],
        created=experiment.created,
        updated=experiment.updated,
    )


def experiment_list_params_to_filter(params: ExperimentListParams) -> ExperimentFilter:
    """Convert experiment list params to the application filter.

    Args:
        params: Experiment list params.

    Returns:
        Experiment filter.
    """
    return ExperimentFilter(
        expression=filter_to_expression(params.filter)
        if params.filter is not None
        else None,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )


def experiment_create_to_command(body: ExperimentCreateRequest) -> ExperimentCreate:
    """Convert an experiment create request to its application command.

    Args:
        body: Experiment create request.

    Returns:
        Create command.
    """
    return ExperimentCreate(
        name=body.name,
        description=body.description,
        agent_id=body.agent_id,
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
    )


def experiment_update_to_command(body: ExperimentUpdateRequest) -> ExperimentUpdate:
    """Convert an experiment update request to its application command.

    Args:
        body: Experiment update request.

    Returns:
        Update command carrying only the fields the request set.
    """
    fields = body.model_fields_set
    kwargs: dict[str, Any] = {}
    if "name" in fields:
        kwargs["name"] = body.name
    if "description" in fields:
        kwargs["description"] = body.description
    if "override" in fields:
        kwargs["override"] = (
            replay_override_to_domain(body.override)
            if body.override is not None
            else None
        )
    if "tool_policy" in fields:
        kwargs["tool_policy"] = (
            tool_policy_to_domain(body.tool_policy)
            if body.tool_policy is not None
            else None
        )
    if "evaluators" in fields:
        kwargs["evaluators"] = (
            [evaluator_config_input(config) for config in body.evaluators]
            if body.evaluators is not None
            else None
        )
    return ExperimentUpdate(**kwargs)
