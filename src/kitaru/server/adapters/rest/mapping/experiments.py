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

from kitaru.api_models.v1.experiments import (
    ExperimentCreateRequest,
    ExperimentResponse,
    ExperimentUpdateRequest,
)
from kitaru.server.adapters.rest.mapping.partial import set_fields
from kitaru.server.adapters.rest.mapping.replays import (
    override_to_domain,
    override_to_response,
    scoring_policy_to_domain,
    scoring_policy_to_response,
    tool_policy_config_to_domain,
    tool_policy_config_to_response,
)
from kitaru.server.application.models.experiments import (
    ExperimentCreate,
    ExperimentUpdate,
)
from kitaru.server.domain.experiment import Experiment
from kitaru.server.domain.replay_config import ReplayConfig


def experiment_create_to_command(body: ExperimentCreateRequest) -> ExperimentCreate:
    """Convert an experiment create request to its command.

    Args:
        body: Experiment create request.

    Returns:
        Experiment create command.
    """
    return ExperimentCreate(
        name=body.name,
        description=body.description,
        cohort_id=body.cohort_id,
        override=override_to_domain(body.override),
        tool_policy=tool_policy_config_to_domain(body.tool_policy),
        scoring_policy=scoring_policy_to_domain(body.scoring_policy),
    )


def experiment_update_to_command(body: ExperimentUpdateRequest) -> ExperimentUpdate:
    """Convert an experiment update request to its command.

    Only fields set on the request are set on the command, so an absent
    field stays distinguishable from an explicit null.

    Args:
        body: Experiment update request.

    Returns:
        Experiment update command.
    """
    fields = set_fields(body)
    if "override" in fields:
        fields["override"] = override_to_domain(body.override)
    if "tool_policy" in fields:
        fields["tool_policy"] = tool_policy_config_to_domain(body.tool_policy)
    if "scoring_policy" in fields:
        fields["scoring_policy"] = (
            scoring_policy_to_domain(body.scoring_policy)
            if body.scoring_policy
            else None
        )
    return ExperimentUpdate(**fields)


def experiment_to_response(
    experiment: Experiment, config: ReplayConfig
) -> ExperimentResponse:
    """Convert an experiment entity to its response DTO.

    Args:
        experiment: Stored experiment.
        config: Replay config of the experiment.

    Returns:
        Experiment response.
    """
    assert experiment.created is not None
    assert experiment.updated is not None
    return ExperimentResponse(
        id=experiment.id,
        owner_id=experiment.owner_id,
        name=experiment.name,
        description=experiment.description,
        cohort_id=experiment.cohort_id,
        override=override_to_response(config.override),
        tool_policy=tool_policy_config_to_response(config.tool_policy),
        scoring_policy=scoring_policy_to_response(config.scoring_policy),
        created=experiment.created,
        updated=experiment.updated,
    )
