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
from collections.abc import Mapping
from typing import ClassVar

from kitaru.api_models.v1.filter import FilterOp
from kitaru.base import FrozenModel
from kitaru.server.application.models.replay_config import EvaluatorConfigInput
from kitaru.server.base import ListFilter
from kitaru.server.domain.replay_config import ReplayOverride, ToolPolicy
from kitaru.server.filtering import EQUALITY_OPS, STRING_OPS, FilterField


class ExperimentFilter(ListFilter):
    """Experiment list filter."""

    filterable_fields: ClassVar[Mapping[str, FilterField]] = {
        "agent_id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS),
        "name": FilterField(value_type=str, ops=STRING_OPS),
        "tag": FilterField(value_type=str, ops=frozenset({FilterOp.EQ, FilterOp.IN})),
    }


class ExperimentCreate(FrozenModel):
    """Experiment create command."""

    name: str
    description: str | None = None
    agent_id: uuid.UUID
    override: ReplayOverride | None = None
    tool_policy: ToolPolicy | None = None
    evaluators: list[EvaluatorConfigInput]


class ExperimentUpdate(FrozenModel):
    """Experiment update command."""

    name: str | None = None
    description: str | None = None
    override: ReplayOverride | None = None
    tool_policy: ToolPolicy | None = None
    evaluators: list[EvaluatorConfigInput] | None = None
