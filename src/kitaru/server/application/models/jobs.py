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
"""Job filter and command models."""

import uuid
from datetime import datetime

from pydantic import Field, PositiveInt

from kitaru.server.base import FrozenModel
from kitaru.server.domain.job import JobStatus
from kitaru.server.domain.replay_config import (
    ReplayOverride,
    ScoringPolicy,
    ToolPolicyConfig,
)


class JobFilter(FrozenModel):
    """Job list filter."""

    experiment_run_id: uuid.UUID | None = None
    original_session_id: uuid.UUID | None = None
    status: JobStatus | None = None
    standalone: bool | None = None
    worker_id: str | None = None
    stale_before: datetime | None = None
    max_attempts: int | None = None
    page: PositiveInt = 1
    page_size: int = Field(default=20, ge=1, le=1000)


class ReplayCreate(FrozenModel):
    """Replay create command."""

    original_session_id: uuid.UUID
    agent_version_id: uuid.UUID | None = None
    override: ReplayOverride | None = None
    tool_policy: ToolPolicyConfig | None = None
    scoring_policy: ScoringPolicy


class JobUpdate(FrozenModel):
    """Job update command."""

    status: JobStatus
    error: str | None = None
    passed: bool | None = None
    score: float | None = None
    scores: dict[str, float] | None = None
