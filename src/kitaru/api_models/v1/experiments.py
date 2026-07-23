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
from datetime import datetime

from pydantic import Field

from kitaru.api_models.v1.base import RequestModel, ResponseModel
from kitaru.api_models.v1.replays import (
    ReplayOverride,
    ScoringPolicy,
    ToolPolicyConfig,
)


class ExperimentCreateRequest(RequestModel):
    """Experiment create request."""

    name: str = Field(description="Experiment name.")
    description: str | None = Field(default=None, description="Experiment description.")
    cohort_id: uuid.UUID = Field(description="Id of the cohort.")
    override: ReplayOverride | None = Field(
        default=None, description="Execution override."
    )
    tool_policy: ToolPolicyConfig | None = Field(
        default=None,
        description="Tool policy, a passthrough policy when omitted.",
    )
    scoring_policy: ScoringPolicy = Field(description="Scoring policy.")


class ExperimentUpdateRequest(RequestModel):
    """Experiment update request."""

    name: str | None = Field(default=None, description="New experiment name.")
    description: str | None = Field(
        default=None, description="New experiment description."
    )
    cohort_id: uuid.UUID | None = Field(
        default=None, description="Id of the new cohort."
    )
    override: ReplayOverride | None = Field(
        default=None, description="New execution override."
    )
    tool_policy: ToolPolicyConfig | None = Field(
        default=None, description="New tool policy."
    )
    scoring_policy: ScoringPolicy | None = Field(
        default=None, description="New scoring policy."
    )


class ExperimentResponse(ResponseModel):
    """Experiment response."""

    id: uuid.UUID = Field(description="Experiment id.")
    owner_id: uuid.UUID = Field(description="Id of the owning account.")
    name: str = Field(description="Experiment name.")
    description: str | None = Field(description="Experiment description.")
    cohort_id: uuid.UUID = Field(description="Id of the cohort.")
    override: ReplayOverride | None = Field(description="Execution override.")
    tool_policy: ToolPolicyConfig = Field(description="Tool policy.")
    scoring_policy: ScoringPolicy = Field(description="Scoring policy.")
    created: datetime = Field(description="Creation time.")
    updated: datetime = Field(description="Last modification time.")
