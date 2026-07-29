"""Experiment API models."""

import uuid

from pydantic import Field

from kitaru.api_models.v1.base import ListParams, OwnedResponseModel, RequestModel
from kitaru.api_models.v1.replay_config import (
    EvaluatorConfig,
    ReplayOverride,
    ToolPolicy,
)


class ExperimentCreateRequest(RequestModel):
    """Experiment create request."""

    name: str = Field(description="Experiment name.")
    description: str | None = Field(default=None, description="Experiment description.")
    override: ReplayOverride | None = Field(
        default=None, description="Replay overrides."
    )
    tool_policy: ToolPolicy | None = Field(
        default=None, description="Replay tool policy."
    )
    evaluators: list[EvaluatorConfig] = Field(
        min_length=1, description="Evaluator configurations."
    )


class ExperimentUpdateRequest(RequestModel):
    """Experiment update request."""

    name: str | None = Field(default=None, description="New experiment name.")
    description: str | None = Field(default=None, description="New description.")
    override: ReplayOverride | None = Field(
        default=None, description="Replacement replay overrides."
    )
    tool_policy: ToolPolicy | None = Field(
        default=None, description="Replacement tool policy."
    )
    evaluators: list[EvaluatorConfig] | None = Field(
        default=None, min_length=1, description="Replacement evaluators."
    )


class ExperimentListParams(ListParams):
    """Experiment list params."""

    name: str | None = Field(default=None, description="Filter on experiment name.")
    tag: str | None = Field(default=None, description="Filter on tag name.")


class ExperimentResponse(OwnedResponseModel):
    """Experiment response."""

    id: uuid.UUID = Field(description="Experiment id.")
    name: str = Field(description="Experiment name.")
    description: str | None = Field(description="Experiment description.")
    override: ReplayOverride | None = Field(description="Replay overrides.")
    tool_policy: ToolPolicy = Field(description="Replay tool policy.")
    evaluators: list[EvaluatorConfig] = Field(description="Evaluator configurations.")
