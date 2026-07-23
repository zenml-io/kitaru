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
"""Experiment filter and command models."""

import uuid

from pydantic import Field, PositiveInt

from kitaru.server.base import FrozenModel
from kitaru.server.domain.replay_config import (
    ReplayOverride,
    ScoringPolicy,
    ToolPolicyConfig,
)


class ExperimentFilter(FrozenModel):
    """Experiment list filter."""

    name: str | None = None
    tag: str | None = None
    page: PositiveInt = 1
    page_size: int = Field(default=20, ge=1, le=1000)


class ExperimentCreate(FrozenModel):
    """Experiment create command."""

    name: str
    description: str | None = None
    cohort_id: uuid.UUID
    override: ReplayOverride | None = None
    tool_policy: ToolPolicyConfig | None = None
    scoring_policy: ScoringPolicy


class ExperimentUpdate(FrozenModel):
    """Experiment update command."""

    name: str | None = None
    description: str | None = None
    cohort_id: uuid.UUID | None = None
    override: ReplayOverride | None = None
    tool_policy: ToolPolicyConfig | None = None
    scoring_policy: ScoringPolicy | None = None
