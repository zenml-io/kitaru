"""Experiment DTO conversions."""

from kitaru.api_models.v1.experiment import (
    ExperimentCreateRequest,
    ExperimentListParams,
    ExperimentResponse,
    ExperimentUpdateRequest,
)
from kitaru.server.adapters.rest.mapping.partial import to_partial
from kitaru.server.adapters.rest.mapping.replay_configs import replay_config_values
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
    """Convert an experiment and its configuration to a response."""
    assert experiment.created is not None
    assert experiment.updated is not None
    override, tool_policy, evaluators = replay_config_values(config)
    return ExperimentResponse(
        id=experiment.id,
        owner_id=experiment.owner_id,
        name=experiment.name,
        description=experiment.description,
        override=override,
        tool_policy=tool_policy,
        evaluators=evaluators,
        created=experiment.created,
        updated=experiment.updated,
    )


def experiment_create_to_command(
    body: ExperimentCreateRequest,
) -> ExperimentCreate:
    """Convert an experiment create body."""
    return ExperimentCreate.model_validate(body.model_dump(mode="python"))


def experiment_update_to_command(
    body: ExperimentUpdateRequest,
) -> ExperimentUpdate:
    """Convert an experiment PATCH body while preserving unset fields."""
    return to_partial(ExperimentUpdate, body)


def experiment_list_params_to_filter(
    params: ExperimentListParams,
) -> ExperimentFilter:
    """Convert experiment list query parameters."""
    return ExperimentFilter(**params.model_dump(mode="python"))
