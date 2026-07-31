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
"""Replay filter, command, and progress count models."""

import uuid
from collections.abc import Mapping
from typing import Any, ClassVar, NamedTuple

from kitaru.api_models.v1.replay import ReplayStatus
from kitaru.base import FrozenModel
from kitaru.server.application.models.replay_config import EvaluatorConfigInput
from kitaru.server.base import ListFilter
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.replay_config import ReplayConfig, ReplayOverride, ToolPolicy
from kitaru.server.filtering import EQUALITY_OPS, NULLABLE_OPS, FilterField


class ReplayFilter(ListFilter):
    """Replay list filter."""

    filterable_fields: ClassVar[Mapping[str, FilterField]] = {
        "experiment_run_id": FilterField(
            value_type=uuid.UUID, ops=EQUALITY_OPS | NULLABLE_OPS
        ),
        "baseline_session_id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS),
        "status": FilterField(value_type=ReplayStatus, ops=EQUALITY_OPS),
    }


class ReplayCreate(FrozenModel):
    """Replay create command."""

    baseline_session_id: uuid.UUID
    agent_version_id: uuid.UUID | None = None
    override: ReplayOverride | None = None
    tool_policy: ToolPolicy | None = None
    evaluators: list[EvaluatorConfigInput]
    evaluate_baselines: bool = False


class ToolLookupResult(NamedTuple):
    """Tool lookup result."""

    found: bool
    result: Any


class ReplayWithDetails(NamedTuple):
    """Replay paired with its config and result session id."""

    replay: Replay
    config: ReplayConfig
    result_session_id: uuid.UUID | None


class ReplayStatusCounts(FrozenModel):
    """Replay counts by status."""

    pending: int = 0
    evaluating: int = 0
    completed: int = 0
    failed: int = 0
    canceled: int = 0

    @property
    def total(self) -> int:
        """Total replay count across every status.

        Returns:
            Total replay count across every status.
        """
        return (
            self.pending
            + self.evaluating
            + self.completed
            + self.failed
            + self.canceled
        )

    @property
    def non_settled(self) -> int:
        """Replay count not yet in a terminal status.

        Returns:
            Replay count not yet in a terminal status.
        """
        return self.pending + self.evaluating
