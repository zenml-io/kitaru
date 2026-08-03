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
"""Experiment API models."""

import uuid

from pydantic import Field

from kitaru.api_models.v1.base import OwnedResponseModel, RequestModel
from kitaru.api_models.v1.filter import FilterableListParams
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
        default=None, description="Override applied to every run's replays."
    )
    tool_policy: ToolPolicy | None = Field(
        default=None, description="Tool policy applied to every run's replays."
    )
    evaluators: list[EvaluatorConfig] = Field(
        min_length=1, description="Evaluators run against every run's replays."
    )


class ExperimentUpdateRequest(RequestModel):
    """Experiment update request."""

    name: str | None = Field(default=None, description="New experiment name.")
    description: str | None = Field(
        default=None, description="New experiment description."
    )
    override: ReplayOverride | None = Field(default=None, description="New override.")
    tool_policy: ToolPolicy | None = Field(default=None, description="New tool policy.")
    evaluators: list[EvaluatorConfig] | None = Field(
        default=None, description="New evaluators."
    )


class ExperimentListParams(FilterableListParams):
    """Experiment list params."""


class ExperimentResponse(OwnedResponseModel):
    """Experiment response."""

    id: uuid.UUID = Field(description="Experiment id.")
    name: str = Field(description="Experiment name.")
    description: str | None = Field(description="Experiment description.")
    override: ReplayOverride | None = Field(
        description="Override applied to every run's replays."
    )
    tool_policy: ToolPolicy = Field(
        description="Tool policy applied to every run's replays."
    )
    evaluators: list[EvaluatorConfig] = Field(
        description="Evaluators run against every run's replays."
    )
